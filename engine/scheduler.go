package engine

import (
	"context"
	"fmt"
	"log"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"
)

// Engine 是调度引擎入口。
//
// 调度语义：
//   - 实例由 (TaskID, LogicalKey) 唯一确定，是幂等的最小单位；
//   - 状态机：PENDING -> RUNNING -> SUCCEEDED / FAILED / BLOCKED，
//     失败且仍有重试预算时 RUNNING -> WAITING -> RUNNING（退避等待可跨重启）；
//   - 依赖未就绪的实例保持 PENDING；上游失败时下游标记 BLOCKED（ErrUpstreamFailed）；
//   - 运行时展开的实例记录来源（上游任务 + 产出批次），并天然依赖上游实例；
//   - 进程重启时，RUNNING 实例恢复为 FAILED，原因 ErrInterrupted（确定且可解释），
//     WAITING 实例保留剩余重试预算与 NextRetryAt 等待计划，到点自动续跑。
type Engine struct {
	mu        sync.Mutex
	store     *Store
	specs     map[string]TaskSpec // 规格模板（静态登记 + 运行时展开，同 ID 先登记者为准）
	staticIDs map[string]struct{} // Submit 静态登记的任务 ID；触发时只登记这批
	instances map[string]*Instance
	emissions map[string]*Emission // 展开批次记录，键为 Emission.Key()
	changed   chan struct{}        // 状态迁移信号（缓冲 1，聚合通知）
	stop      chan struct{}        // Close 时关闭，用于释放退避等待器
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
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	e := &Engine{
		store:     store,
		specs:     map[string]TaskSpec{},
		staticIDs: map[string]struct{}{},
		instances: state.Instances,
		emissions: state.Emissions,
		changed:   make(chan struct{}, 1),
		stop:      make(chan struct{}),
		logf:      log.Printf,
	}
	// 恢复语义：
	//   - RUNNING：上次退出时仍在执行，副作用不可知，判为 FAILED(ErrInterrupted)，
	//     可由后续重试/触发重新执行；
	//   - WAITING：失败后退避中，保留 Attempts（已扣减预算）与 NextRetryAt，
	//     重新挂起等待器到点续跑，不清零、不立即重试；
	//   - PENDING：从未开始执行，保持 PENDING，待 Submit/触发后继续调度；
	//   - SUCCEEDED：绝不重跑。
	for _, inst := range e.instances {
		if inst.Status == StatusRunning {
			e.transitionLocked(inst, StatusFailed, ErrInterrupted.Error())
		}
	}
	for _, inst := range e.instances {
		if inst.Status == StatusWaiting {
			e.logf("recover %s in WAITING attempts=%d nextRetryAt=%s",
				inst.Key(), inst.Attempts, inst.NextRetryAt.Format(time.RFC3339Nano))
			e.wg.Add(1)
			go e.waitAndRetry(inst.Key(), inst.NextRetryAt)
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
		if _, exists := e.specs[s.ID]; !exists {
			e.specs[s.ID] = s
		}
		e.staticIDs[s.ID] = struct{}{}
	}
	if cyc := findCycle(e.specs); cyc != nil {
		return fmt.Errorf("%w: %s", ErrCycle, strings.Join(cyc, " -> "))
	}
	// 重启后 Submit 也可能是恢复动作：尝试推进此前持久化的 PENDING 实例。
	e.scheduleLocked()
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
			case StatusPending, StatusRunning, StatusWaiting:
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
			// PENDING 重跑（重试预算重新计满），SUCCEEDED 实例绝不重复执行（幂等）。
			for _, inst := range existing {
				if inst.Status == StatusFailed || inst.Status == StatusBlocked {
					e.logf("resume %s: %s -> PENDING (re-trigger %s)", inst.Key(), inst.Status, trig)
					inst.Status = StatusPending
					inst.Trigger = trig
					inst.Attempts = 0
					inst.NextRetryAt = time.Time{}
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
	// 只登记静态提交的任务；运行时展开的规格属于别的逻辑键的产出，
	// 绝不能在新的业务日期里凭空冒出实例。
	for _, id := range e.sortedStaticIDsLocked() {
		spec := e.specs[id]
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
	if e.closed {
		return
	}
	for progress := true; progress; {
		progress = false
		for _, inst := range e.sortedInstancesLocked() {
			if inst.Status != StatusPending {
				continue
			}
			spec, ok := e.specs[inst.TaskID]
			if !ok {
				if inst.Source != nil {
					// 运行时展开的规格尚未随上游重新展开或 Submit 登记
					// （典型：重启后上游尚未重跑），保持 PENDING 等待，不误判失败。
					continue
				}
				e.transitionLocked(inst, StatusFailed, ErrUnknownTask.Error())
				progress = true
				continue
			}
			blocked, ready := false, true
			blockingDep := ""
			for _, dep := range spec.Deps {
				di, ok := e.instances[dep+"@"+inst.LogicalKey]
				if !ok {
					e.transitionLocked(inst, StatusFailed,
						ErrUnknownTask.Error()+": dependency \""+dep+"\" not registered")
					progress = true
					blocked, ready = false, false
					break
				}
				switch di.Status {
				case StatusSucceeded:
				case StatusFailed, StatusBlocked:
					blocked = true
					blockingDep = dep
				default:
					ready = false
				}
			}
			if blocked {
				reason := fmt.Sprintf("%s: dependency %q is %s (emitted chain dragged down by upstream)",
					ErrUpstreamFailed, blockingDep, e.instances[blockingDep+"@"+inst.LogicalKey].Status)
				e.transitionLocked(inst, StatusBlocked, reason)
				progress = true
			} else if ready {
				e.transitionLocked(inst, StatusRunning, "")
				e.wg.Add(1)
				go e.execute(inst.Key())
				progress = true
			}
		}
	}
	// 仍有 PENDING 但无法推进且无任何在途执行（RUNNING/WAITING）：
	// 只可能因运行时展开构成环或缺规格，明确失败而非死锁。
	inFlight := false
	for _, inst := range e.instances {
		if inst.Status == StatusRunning || inst.Status == StatusWaiting {
			inFlight = true
			break
		}
	}
	if inFlight {
		_ = e.persistLocked()
		return
	}
	for _, inst := range e.sortedInstancesLocked() {
		if inst.Status != StatusPending {
			continue
		}
		if _, ok := e.specs[inst.TaskID]; !ok {
			continue // 动态规格待登记，保持 PENDING（见上）
		}
		e.transitionLocked(inst, StatusFailed,
			ErrCycle.Error()+": unschedulable (runtime dependency cycle)")
	}
	_ = e.persistLocked()
}

// execute 运行单个实例的完整尝试循环，含有限重试与可跨重启的退避等待。
// 进入时实例为 RUNNING（首次由 scheduleLocked 启动；WAITING 续跑由等待器启动）。
func (e *Engine) execute(key string) {
	defer e.wg.Done()
	for {
		e.mu.Lock()
		inst, ok := e.instances[key]
		if !ok {
			e.mu.Unlock()
			return
		}
		spec := e.specs[inst.TaskID]
		attempt := inst.Attempts + 1
		inst.Attempts = attempt
		inst.Status = StatusRunning
		inst.UpdatedAt = time.Now()
		logicalKey, trig := inst.LogicalKey, inst.Trigger
		e.logf("execute %s attempt=%d/%d", key, attempt, spec.MaxRetries+1)
		_ = e.persistLocked()
		e.mu.Unlock()

		tc := &TaskContext{TaskID: inst.TaskID, LogicalKey: logicalKey, Trigger: trig, engine: e}
		runErr := spec.Run(context.Background(), tc)

		e.mu.Lock()
		inst = e.instances[key]
		switch {
		case runErr == nil:
			e.transitionLocked(inst, StatusSucceeded, "")
			e.finishLocked(inst)
			e.mu.Unlock()
			return
		case attempt > spec.MaxRetries:
			e.logf("execute %s failed (attempt=%d): %v; retries exhausted", key, attempt, runErr)
			e.transitionLocked(inst, StatusFailed, runErr.Error())
			e.finishLocked(inst)
			e.mu.Unlock()
			return
		default:
			// 仍有预算：进入 WAITING，记录计划重试时间点并持久化，
			// 由独立等待器到点续跑（崩溃重启后由 Open 重新挂起等待器）。
			next := time.Now().Add(spec.Backoff)
			inst.NextRetryAt = next
			e.transitionLocked(inst, StatusWaiting, runErr.Error())
			e.logf("execute %s failed (attempt=%d): %v; waiting until %s (%d retries left)",
				key, attempt, runErr, next.Format(time.RFC3339Nano), spec.MaxRetries+1-attempt)
			_ = e.persistLocked()
			e.signalLocked()
			e.wg.Add(1)
			go e.waitAndRetry(key, next)
			e.mu.Unlock()
			return
		}
	}
}

// waitAndRetry 占据一个 WAITING 实例的退避倒计时：到点且规格可用时将其
// 重新置为 RUNNING 并启动 execute。规格尚未重新登记（重启后）时短轮询等待，
// 等待计划（NextRetryAt）与已扣减预算（Attempts）始终保留在实例上。
func (e *Engine) waitAndRetry(key string, next time.Time) {
	defer e.wg.Done()
	for {
		d := time.Until(next)
		if d < 0 {
			d = 0
		}
		timer := time.NewTimer(d)
		select {
		case <-e.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return
		}
		inst, ok := e.instances[key]
		if !ok || inst.Status != StatusWaiting {
			e.mu.Unlock()
			return
		}
		if _, hasSpec := e.specs[inst.TaskID]; !hasSpec {
			e.logf("waiter %s: spec not registered yet, keep WAITING plan %s",
				key, next.Format(time.RFC3339Nano))
			e.mu.Unlock()
			poll := time.NewTimer(20 * time.Millisecond)
			select {
			case <-e.stop:
				poll.Stop()
				return
			case <-poll.C:
			}
			continue
		}
		inst.Status = StatusRunning
		inst.UpdatedAt = time.Now()
		e.logf("waiter %s fires at %s (planned %s); resume with attempts=%d",
			key, time.Now().Format(time.RFC3339Nano), next.Format(time.RFC3339Nano), inst.Attempts)
		e.wg.Add(1)
		go e.execute(key)
		e.mu.Unlock()
		return
	}
}

// emitBatch 的实现：按产出批次运行时展开下游任务（同一逻辑键内）。
func (e *Engine) emitBatch(tc *TaskContext, batch string, specs []TaskSpec) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	// 第一阶段：在副本上登记全部新规格并整体做环检测，任一成环则全部不生效。
	candidate := maps.Clone(e.specs)
	added := make([]string, 0, len(specs))
	for _, s := range specs {
		if s.ID == "" {
			return fmt.Errorf("%w: empty emitted task id", ErrUnknownTask)
		}
		if _, exists := candidate[s.ID]; exists {
			continue
		}
		// 天然排在展开它的上游任务之后：自动注入上游依赖并去重，调用方无需手声明。
		s.Deps = dedupeAppend(s.ID == tc.TaskID, s.Deps, tc.TaskID)
		candidate[s.ID] = s
		added = append(added, s.ID)
	}
	if cyc := findCycle(candidate); cyc != nil {
		return fmt.Errorf("%w: %s", ErrCycle, strings.Join(cyc, " -> "))
	}
	e.specs = candidate

	emKey := (&Emission{LogicalKey: tc.LogicalKey, Emitter: tc.TaskID, Batch: batch}).Key()
	rec, batchSeen := e.emissions[emKey]
	if !batchSeen {
		rec = &Emission{
			LogicalKey: tc.LogicalKey, Emitter: tc.TaskID, Batch: batch,
			CreatedAt: time.Now(),
		}
		e.emissions[emKey] = rec
	}
	for _, s := range specs {
		key := s.ID + "@" + tc.LogicalKey
		inst, exists := e.instances[key]
		if !exists {
			inst = &Instance{
				TaskID: s.ID, LogicalKey: tc.LogicalKey, Trigger: tc.Trigger,
				Status: StatusPending, UpdatedAt: time.Now(),
				Source: &EmissionSource{EmitterTaskID: tc.TaskID, Batch: batch},
			}
			e.instances[key] = inst
			e.logf("emit %s by %s@%s batch=%q", key, tc.TaskID, tc.LogicalKey, batch)
		}
		// 批次清单只追加一次：同一来源重复报同一批任务，按一次算，不越跑越多。
		if !batchSeen || !containsString(rec.TaskIDs, s.ID) {
			rec.TaskIDs = append(rec.TaskIDs, s.ID)
		}
	}
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

// Await 等待某逻辑键的全部实例到达终态，但不登记、不重置任何实例。
// 用于进程重启后接续观察自动恢复中的运行（如退避到点后续跑的实例）。
func (e *Engine) Await(ctx context.Context, logicalKey string) error {
	return e.wait(ctx, logicalKey)
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

// Emissions 返回某逻辑键（业务日期）内的全部展开批次记录，
// 可据此追溯某次运行展开了哪些任务、分别由哪条上游任务的哪次产出带出。
// 结果按 (Emitter, Batch) 排序；每条记录的 TaskIDs 为当批次展开清单。
func (e *Engine) Emissions(logicalKey string) []*Emission {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []*Emission
	for _, m := range e.emissions {
		if m.LogicalKey != logicalKey {
			continue
		}
		cp := *m
		cp.TaskIDs = append([]string(nil), m.TaskIDs...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Emitter != out[j].Emitter {
			return out[i].Emitter < out[j].Emitter
		}
		return out[i].Batch < out[j].Batch
	})
	return out
}

// Close 停止退避等待器，等待在途执行结束并落盘。
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.stop)
	e.mu.Unlock()

	e.wg.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.persistLocked()
}

// ---- 内部辅助 ----

func (e *Engine) finishLocked(inst *Instance) {
	_ = e.persistLocked()
	e.scheduleLocked() // 唤醒因本实例完成而就绪/被拖垮的下游
	e.signalLocked()
}

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

func (e *Engine) persistLocked() error {
	return e.store.Save(&State{Instances: e.instances, Emissions: e.emissions})
}

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

func (e *Engine) sortedStaticIDsLocked() []string {
	ids := make([]string, 0, len(e.staticIDs))
	for id := range e.staticIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// dedupeAppend 把上游任务 ID 注入依赖列表头部并去除重复（排除自依赖）。
func dedupeAppend(self bool, deps []string, upstream string) []string {
	out := make([]string, 0, len(deps)+1)
	seen := map[string]struct{}{}
	if !self && upstream != "" {
		out = append(out, upstream)
		seen[upstream] = struct{}{}
	}
	for _, d := range deps {
		if _, ok := seen[d]; ok || d == "" {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}

func containsString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
