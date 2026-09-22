package engine

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

// Engine 是调度引擎入口。
//
// 调度语义：
//   - 实例由 (TaskID, LogicalKey) 唯一确定，是幂等的最小单位；
//   - 状态机：PENDING -> RUNNING -> SUCCEEDED / FAILED / BLOCKED；
//   - 依赖未就绪的实例保持 PENDING；上游失败时下游标记 BLOCKED（ErrUpstreamFailed）；
//   - 进程重启时，RUNNING 实例恢复为 FAILED，原因 ErrInterrupted（确定且可解释）。
type Engine struct {
	mu        sync.Mutex
	store     *Store
	specs     map[string]TaskSpec
	instances map[string]*Instance
	changed   chan struct{} // 状态迁移信号（缓冲 1，聚合通知）
	logf      func(format string, args ...any)
	wg        sync.WaitGroup
	closed    bool
}

// Open 打开引擎、加载持久化状态并执行崩溃恢复。
func Open(storePath string) (*Engine, error) {
	store, err := OpenStore(storePath)
	if err != nil {
		return nil, err
	}
	instances, err := store.Load()
	if err != nil {
		return nil, err
	}
	e := &Engine{
		store:     store,
		specs:     map[string]TaskSpec{},
		instances: instances,
		changed:   make(chan struct{}, 1),
		logf:      log.Printf,
	}
	// 恢复语义：上次进程退出时仍处于 RUNNING 的实例，
	// 其副作用不可知，统一判定为 FAILED(ErrInterrupted)，
	// 可由后续重试/触发重新执行；SUCCEEDED 实例绝不重跑。
	for _, inst := range e.instances {
		if inst.Status == StatusRunning || inst.Status == StatusPending {
			e.transitionLocked(inst, StatusFailed, ErrInterrupted.Error())
		}
	}
	if err := e.persistLocked(); err != nil {
		return nil, err
	}
	return e, nil
}

// SetLogger 替换日志函数（测试用它打印状态流转与判定依据）。
func (e *Engine) SetLogger(logf func(format string, args ...any)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logf = logf
}

// Submit 注册任务定义，并对静态依赖做成环检测。
func (e *Engine) Submit(specs ...TaskSpec) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range specs {
		if s.ID == "" {
			return fmt.Errorf("%w: empty task id", ErrUnknownTask)
		}
		e.specs[s.ID] = s
	}
	if cyc := findCycle(e.specs); cyc != nil {
		return fmt.Errorf("%w: %s", ErrCycle, strings.Join(cyc, " -> "))
	}
	return nil
}

// findCycle 对任务定义做 DFS 成环检测，返回一条环路径。
func findCycle(specs map[string]TaskSpec) []string {
	const (
		white = 0 // 未访问
		gray  = 1 // 在栈上
		black = 2 // 已完成
	)
	color := map[string]int{}
	var stack []string
	var visit func(id string) []string
	visit = func(id string) []string {
		color[id] = gray
		stack = append(stack, id)
		for _, dep := range specs[id].Deps {
			if _, ok := specs[dep]; !ok {
				continue // 未注册的依赖在触发时报错，这里不参与成环判定
			}
			switch color[dep] {
			case gray:
				// 找到环：截取栈中从 dep 开始的部分
				for i, s := range stack {
					if s == dep {
						return append(append([]string{}, stack[i:]...), dep)
					}
				}
			case white:
				if cyc := visit(dep); cyc != nil {
					return cyc
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return nil
	}
	ids := make([]string, 0, len(specs))
	for id := range specs {
		ids = append(ids, id)
	}
	sort.Strings(ids) // 排序保证判定结果确定、可解释
	for _, id := range ids {
		if color[id] == white {
			if cyc := visit(id); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// Trigger 以常规方式触发一个逻辑实例；幂等：已完成的键直接返回 nil。
func (e *Engine) Trigger(ctx context.Context, logicalKey string) error {
	return e.run(ctx, logicalKey, TriggerRegular)
}

// Backfill 对一批历史逻辑键发起补跑；与常规触发共用同一幂等键空间。
func (e *Engine) Backfill(ctx context.Context, logicalKeys []string) error {
	for _, key := range logicalKeys {
		if err := e.run(ctx, key, TriggerBackfill); err != nil {
			return err
		}
	}
	return nil
}

// run 登记实例并驱动调度直到该逻辑键全部到达终态。
func (e *Engine) run(ctx context.Context, logicalKey string, trig TriggerType) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("engine is closed")
	}
	// 幂等与冲突判定：同一逻辑键只允许一批实例。
	existing := e.instancesForKeyLocked(logicalKey)
	if len(existing) > 0 {
		allSucceeded, inFlight := true, false
		for _, inst := range existing {
			switch inst.Status {
			case StatusSucceeded:
			case StatusPending, StatusRunning:
				inFlight = true
				allSucceeded = false
			default: // FAILED / BLOCKED 终态
				allSucceeded = false
			}
		}
		switch {
		case allSucceeded:
			e.logf("trigger %s key=%s: all instances SUCCEEDED, idempotent skip", trig, logicalKey)
			e.mu.Unlock()
			return nil
		case inFlight:
			inst := existing[0]
			err := fmt.Errorf("%w: key=%s task=%s in-flight %s (trigger=%s)",
				ErrConflict, logicalKey, inst.TaskID, inst.Status, inst.Trigger)
			e.logf("trigger %s key=%s rejected: %v", trig, logicalKey, err)
			e.mu.Unlock()
			return err
		default:
			// 恢复语义：全部处于终态且存在失败 —— 仅将 FAILED/BLOCKED 重置为
			// PENDING 重跑，SUCCEEDED 实例绝不重复执行（幂等）。
			for _, inst := range existing {
				if inst.Status == StatusFailed || inst.Status == StatusBlocked {
					e.logf("resume %s: %s -> PENDING (re-trigger %s)", inst.Key(), inst.Status, trig)
					inst.Status = StatusPending
					inst.Trigger = trig
					inst.LastError = ""
					inst.UpdatedAt = time.Now()
				}
			}
			if err := e.persistLocked(); err != nil {
				e.mu.Unlock()
				return err
			}
			e.scheduleLocked()
			e.mu.Unlock()
			return e.wait(ctx, logicalKey)
		}
	}
	for _, spec := range e.specs {
		inst := &Instance{
			TaskID: spec.ID, LogicalKey: logicalKey, Trigger: trig,
			Status: StatusPending, UpdatedAt: time.Now(),
		}
		e.instances[inst.Key()] = inst
		e.logf("register %s key=%s trigger=%s", inst.Key(), logicalKey, trig)
	}
	if err := e.persistLocked(); err != nil {
		e.mu.Unlock()
		return err
	}
	e.scheduleLocked()
	e.mu.Unlock()

	return e.wait(ctx, logicalKey)
}

// scheduleLocked 推进状态机：将就绪的 PENDING 实例置为 RUNNING 并启动执行，
// 将上游失败的实例置为 BLOCKED。调用方须持有锁。可能多次被调用。
func (e *Engine) scheduleLocked() {
	for progress := true; progress; {
		progress = false
		for _, inst := range e.sortedInstancesLocked() {
			if inst.Status != StatusPending {
				continue
			}
			spec, ok := e.specs[inst.TaskID]
			if !ok {
				e.transitionLocked(inst, StatusFailed, ErrUnknownTask.Error())
				progress = true
				continue
			}
			blocked, ready := false, true
			for _, dep := range spec.Deps {
				di, ok := e.instances[dep+"@"+inst.LogicalKey]
				if !ok {
					e.transitionLocked(inst, StatusFailed,
						ErrUnknownTask.Error() + ": dependency \"" + dep + "\" not registered")
					progress = true
					blocked, ready = false, false
					break
				}
				switch di.Status {
				case StatusSucceeded:
				case StatusFailed, StatusBlocked:
					blocked = true
				default:
					ready = false
				}
			}
			if blocked {
				e.transitionLocked(inst, StatusBlocked, ErrUpstreamFailed.Error())
				progress = true
			} else if ready {
				e.transitionLocked(inst, StatusRunning, "")
				e.wg.Add(1)
				go e.execute(spec, inst.Key())
				progress = true
			}
		}
	}
	// 仍有 PENDING 但无法推进且无任何在途执行：
	// 只可能因运行时 Emit 构成环，明确失败而非死锁。
	running := false
	for _, inst := range e.instances {
		if inst.Status == StatusRunning {
			running = true
			break
		}
	}
	if running {
		_ = e.persistLocked()
		return
	}
	for _, inst := range e.sortedInstancesLocked() {
		if inst.Status == StatusPending {
			e.transitionLocked(inst, StatusFailed,
				ErrCycle.Error() + ": unschedulable (runtime dependency cycle)")
		}
	}
}

// execute 运行单个实例，含有限重试与退避。
func (e *Engine) execute(spec TaskSpec, key string) {
	defer e.wg.Done()
	e.mu.Lock()
	inst := e.instances[key]
	logicalKey, trig := inst.LogicalKey, inst.Trigger
	e.mu.Unlock()

	var runErr error
	for attempt := 1; ; attempt++ {
		e.mu.Lock()
		e.instances[key].Attempts = attempt
		e.logf("execute %s attempt=%d/%d", key, attempt, spec.MaxRetries+1)
		e.mu.Unlock()

		tc := &TaskContext{LogicalKey: logicalKey, Trigger: trig, engine: e}
		runErr = spec.Run(context.Background(), tc)
		if runErr == nil {
			break
		}
		if attempt > spec.MaxRetries {
			break
		}
		e.logf("execute %s failed (attempt=%d): %v; backoff %s", key, attempt, runErr, spec.Backoff)
		time.Sleep(spec.Backoff)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	inst = e.instances[key]
	if runErr == nil {
		e.transitionLocked(inst, StatusSucceeded, "")
	} else {
		e.transitionLocked(inst, StatusFailed, runErr.Error())
	}
	_ = e.persistLocked()
	e.scheduleLocked() // 唤醒因本实例完成而就绪的下游
	e.signalLocked()
}

// Emit 的实现：运行时动态展开下游任务（同一逻辑键内）。
func (e *Engine) emit(tc *TaskContext, spec TaskSpec) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, dup := e.specs[spec.ID]; !dup {
		e.specs[spec.ID] = spec
		if cyc := findCycle(e.specs); cyc != nil {
			delete(e.specs, spec.ID)
			return fmt.Errorf("%w: %s", ErrCycle, strings.Join(cyc, " -> "))
		}
	}
	key := spec.ID + "@" + tc.LogicalKey
	if _, exists := e.instances[key]; exists {
		return nil // 同一逻辑实例已存在：幂等跳过
	}
	e.instances[key] = &Instance{
		TaskID: spec.ID, LogicalKey: tc.LogicalKey, Trigger: tc.Trigger,
		Status: StatusPending, UpdatedAt: time.Now(),
	}
	e.logf("emit %s by key=%s", key, tc.LogicalKey)
	if err := e.persistLocked(); err != nil {
		return err
	}
	e.scheduleLocked()
	e.signalLocked()
	return nil
}

// wait 阻塞直到该逻辑键的所有实例到达终态，并返回汇总的可区分结果。
func (e *Engine) wait(ctx context.Context, logicalKey string) error {
	for {
		e.mu.Lock()
		done, failed, blocked := true, false, false
		for _, inst := range e.instancesForKeyLocked(logicalKey) {
			switch inst.Status {
			case StatusSucceeded:
			case StatusFailed:
				failed = true
			case StatusBlocked:
				blocked = true
			default:
				done = false
			}
		}
		e.mu.Unlock()
		if done {
			switch {
			case failed:
				return fmt.Errorf("key=%s: %w", logicalKey, ErrUpstreamFailed)
			case blocked:
				return fmt.Errorf("key=%s: %w", logicalKey, ErrUpstreamFailed)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.changed:
		}
	}
}

// Instance 查询实例状态快照。
func (e *Engine) Instance(taskID, logicalKey string) (*Instance, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	inst, ok := e.instances[taskID+"@"+logicalKey]
	if !ok {
		return nil, false
	}
	cp := *inst
	return &cp, true
}

// Instances 返回某逻辑键下全部实例快照（按 TaskID 排序，便于测试断言）。
func (e *Engine) Instances(logicalKey string) []*Instance {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.instancesForKeyLocked(logicalKey)
}

// Close 等待在途执行结束并落盘。
func (e *Engine) Close() error {
	e.wg.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return e.persistLocked()
}

// ---- 内部辅助 ----

func (e *Engine) transitionLocked(inst *Instance, to Status, errMsg string) {
	e.logf("transition %s: %s -> %s%s", inst.Key(), inst.Status, to,
		map[bool]string{true: " reason=" + errMsg, false: ""}[errMsg != ""])
	inst.Status = to
	inst.LastError = errMsg
	inst.UpdatedAt = time.Now()
}

func (e *Engine) signalLocked() {
	select {
	case e.changed <- struct{}{}:
	default:
	}
}

func (e *Engine) persistLocked() error { return e.store.Save(e.instances) }

func (e *Engine) instancesForKeyLocked(logicalKey string) []*Instance {
	var out []*Instance
	for _, inst := range e.instances {
		if inst.LogicalKey == logicalKey {
			out = append(out, inst)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	return out
}

func (e *Engine) sortedInstancesLocked() []*Instance {
	out := make([]*Instance, 0, len(e.instances))
	for _, inst := range e.instances {
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}
