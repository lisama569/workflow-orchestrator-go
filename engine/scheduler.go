package engine

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Engine 是调度引擎入口。
//
// 调度语义：
//   - 实例由 (TaskID, LogicalKey) 唯一确定，是幂等的最小单位；
//   - 状态机：PENDING -> RUNNING -> SUCCEEDED / FAILED / BLOCKED，
//     失败退避中为 RETRY_WAIT（预算与等待计划持久化）；
//   - 依赖未就绪的实例保持 PENDING；上游失败/被打断时下游标记 BLOCKED，
//     原因点名上游实例（ErrUpstreamFailed）；
//   - 进程重启时，RUNNING 恢复为 INTERRUPTED（执行到一半被打断，确定可解释），
//     RETRY_WAIT 原样保留并等待接管；SUCCEEDED 实例绝不重跑。
type Engine struct {
	mu         sync.Mutex
	store      *Store
	specs      map[string]TaskSpec
	staticIDs  map[string]bool // Submit 注册的静态任务 ID；运行时 Emit 的任务不在其中
	dynamicIDs map[string]bool // 运行时展开出的任务 ID（定义已持久化，重启后先以占位定义恢复）
	instances  map[string]*Instance
	expansions map[string]*ExpansionRecord // 展开台账（键为 dedupKey）
	owned      map[string]bool             // 本进程持有执行/退避 goroutine 的实例键
	changed    chan struct{}               // 状态迁移信号（缓冲 1，聚合通知）
	logf       func(format string, args ...any)
	wg         sync.WaitGroup
	closed     bool
	closeCh    chan struct{} // 关闭信号：退避等待中的重试立即交还（状态已持久化）
}

// Open 打开引擎、加载持久化状态并执行崩溃恢复。
func Open(storePath string) (*Engine, error) {
	store, err := OpenStore(storePath)
	if err != nil {
		return nil, err
	}
	st, err := store.Load()
	if err != nil {
		return nil, err
	}
	e := &Engine{
		store:      store,
		specs:      map[string]TaskSpec{},
		staticIDs:  map[string]bool{},
		dynamicIDs: map[string]bool{},
		instances:  st.Instances,
		expansions: st.Expansions,
		owned:      map[string]bool{},
		changed:    make(chan struct{}, 1),
		closeCh:    make(chan struct{}),
		logf:       log.Printf,
	}
	// 恢复运行时展开的任务定义：Run 闭包无法持久化，先安装 nil-Run 占位定义
	// （Deps/重试预算随定义落盘）。等待应用重新 Submit 同一 ID，
	// 或上游重跑再次 Emit 时完成重新绑定；绑定前调度到它会给出明确失败而非误判“未知任务”。
	for id, ds := range st.DynamicSpecs {
		if ds == nil {
			ds = &DynamicSpec{}
		}
		e.specs[id] = TaskSpec{
			ID:         id,
			Deps:       append([]string{}, ds.Deps...),
			MaxRetries: ds.MaxRetries,
			Backoff:    ds.Backoff,
		}
		e.dynamicIDs[id] = true
	}
	// 恢复语义：
	//   - RUNNING：执行到一半进程退出、副作用不可知，标记 INTERRUPTED（确定可解释）；
	//   - RETRY_WAIT：失败后退避中被打断，保留剩余重试预算与预定重试时刻，
	//     重触发后按原计划继续等待，不会清零次数或立即重试；
	//   - PENDING：从未开始执行，保持 PENDING；
	//   - SUCCEEDED 实例绝不重跑。
	for _, inst := range e.instances {
		if inst.Status == StatusRunning {
			e.transitionLocked(inst, StatusInterrupted, ErrInterrupted.Error())
		}
	}
	// 恢复后立刻交代失败归属：上游已处于失败终态（含本次重启打出的
	// INTERRUPTED）时，仍在等待的展开任务直接标记 BLOCKED 并点名上游，
	// 不会卡在 PENDING 或被误判成 FAILED。
	e.cascadeBlockedAfterRecoveryLocked()
	if err := e.persistLocked(); err != nil {
		return nil, err
	}
	return e, nil
}

// cascadeBlockedAfterRecoveryLocked 在重启恢复阶段把“来源上游已处于失败终态”
// 的 PENDING 展开任务标记为 BLOCKED（不动点迭代：展开链上多级任务在同一轮
// 恢复中也能逐级交代归属）。仅处理静态调度循环未运行时无人接管的来源依赖；
// 显式 Deps 在重触发后的 scheduleLocked 中按同一套规则判定。
func (e *Engine) cascadeBlockedAfterRecoveryLocked() {
	for progress := true; progress; {
		progress = false
		for _, inst := range e.sortedInstancesLocked() {
			if inst.Status != StatusPending || inst.Origin.EmitterTaskID == "" {
				continue
			}
			up, ok := e.instances[inst.Origin.EmitterTaskID+"@"+inst.LogicalKey]
			if !ok {
				continue
			}
			switch up.Status {
			case StatusFailed, StatusBlocked, StatusInterrupted:
				e.transitionLocked(inst, StatusBlocked,
					fmt.Sprintf("%s: upstream %s failed", ErrUpstreamFailed.Error(), up.Key()))
				progress = true
			}
		}
	}
}

// SetLogger 替换日志函数（测试用它打印状态流转与判定依据）。
func (e *Engine) SetLogger(logf func(format string, args ...any)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logf = logf
}

// Submit 注册任务定义，并对静态依赖做成环检测。
//
// 对“运行时已展开、重启后只剩占位定义”的任务 ID，Submit 表示应用在新进程
// 重新提供该定义（重新绑定执行体与重试参数）：定义被更新，但该 ID 仍是动态
// 任务，不会因为别的逻辑键触发而被静态实例化。
func (e *Engine) Submit(specs ...TaskSpec) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range specs {
		if s.ID == "" {
			return fmt.Errorf("%w: empty task id", ErrUnknownTask)
		}
		if e.dynamicIDs[s.ID] {
			e.logf("rebind dynamic spec %s after restart", s.ID)
		} else {
			e.staticIDs[s.ID] = true
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
		allSucceeded := true
		hasActive := false // 本进程在途（RUNNING 或带 waiter 的 RETRY_WAIT）：冲突拒绝
		var conflict *Instance
		var orphanWaiters []*Instance // 重启遗留、本进程尚无 waiter 的 RETRY_WAIT
		for _, inst := range existing {
			switch inst.Status {
			case StatusSucceeded:
			case StatusRunning:
				hasActive = true
				allSucceeded = false
				conflict = inst
			case StatusRetryWait:
				allSucceeded = false
				if e.owned[inst.Key()] {
					hasActive = true
					conflict = inst
				} else {
					orphanWaiters = append(orphanWaiters, inst)
				}
			default: // PENDING 或 FAILED / BLOCKED / INTERRUPTED 终态
				allSucceeded = false
			}
		}
		switch {
		case hasActive:
			inst := conflict
			err := fmt.Errorf("%w: key=%s task=%s in-flight %s (trigger=%s)",
				ErrConflict, logicalKey, inst.TaskID, inst.Status, inst.Trigger)
			e.logf("trigger %s key=%s rejected: %v", trig, logicalKey, err)
			e.mu.Unlock()
			return err
		case allSucceeded:
			e.logf("trigger %s key=%s: all instances SUCCEEDED, idempotent skip", trig, logicalKey)
			e.mu.Unlock()
			return nil
		default:
			// 恢复语义：终态失败的实例重置为 PENDING 重跑（INTERRUPTED 表示
			// 上次执行到一半被打断，与 RETRY_WAIT 的“退避等待”区分开）；
			// SUCCEEDED 实例绝不重复执行（幂等）。
			for _, inst := range existing {
				switch inst.Status {
				case StatusFailed, StatusBlocked, StatusInterrupted:
					e.logf("resume %s: %s -> PENDING (re-trigger %s)", inst.Key(), inst.Status, trig)
					inst.Status = StatusPending
					inst.Trigger = trig
					inst.LastError = ""
					inst.Attempts = 0
					inst.MaxAttempts = 0
					var zero time.Time
					inst.NextAttemptAt = zero
					inst.UpdatedAt = time.Now()
				}
			}
			// 接管重启前遗留、本进程尚无 waiter 的 RETRY_WAIT：
			// 剩余重试次数（Attempts/MaxAttempts）与等待计划（NextAttemptAt）原样保留。
			for _, inst := range orphanWaiters {
				e.logf("rearm %s retry waiter after restart: attempts=%d/%d next=%s",
					inst.Key(), inst.Attempts, inst.MaxAttempts,
					inst.NextAttemptAt.Format(time.RFC3339Nano))
				e.owned[inst.Key()] = true
				e.wg.Add(1)
				go e.resumeRetryWait(inst.Key())
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
	for id := range e.staticIDs {
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
			blockedBy := ""
			// 显式依赖取“定义当前版本 + 来源上游”的并集（运行时展开任务天然
			// 排在产出它的上游之后）。重启后占位定义里的 Deps 已随定义落盘，
			// 重新绑定时取并集不会丢失链路。
			depIDs := make([]string, 0, len(spec.Deps)+1)
			depIDs = append(depIDs, spec.Deps...)
			if inst.Origin.EmitterTaskID != "" && !containsDep(depIDs, inst.Origin.EmitterTaskID) {
				depIDs = append(depIDs, inst.Origin.EmitterTaskID)
			}
			for _, dep := range depIDs {
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
				case StatusFailed, StatusBlocked, StatusInterrupted:
					blocked = true
					blockedBy = di.Key()
				default: // PENDING / RUNNING / RETRY_WAIT
					ready = false
				}
			}
			if blocked {
				e.transitionLocked(inst, StatusBlocked,
					fmt.Sprintf("%s: upstream %s failed", ErrUpstreamFailed.Error(), blockedBy))
				progress = true
			} else if ready {
				if spec.Run == nil {
					// 展开定义在重启后尚未重新绑定（既没 Submit，上游也未重跑
					// 再次产出）：明确失败，而不是执行 nil 闭包或永久挂起。
					e.transitionLocked(inst, StatusFailed,
						fmt.Sprintf("%s: dynamic task %q definition not re-registered after restart",
							ErrUnknownTask.Error(), inst.TaskID))
					progress = true
					continue
				}
				inst.Attempts = 1
				e.transitionLocked(inst, StatusRunning, "")
				e.owned[inst.Key()] = true
				e.wg.Add(1)
				go e.execute(spec, inst.Key())
				progress = true
			}
		}
	}
	// 仍有 PENDING 但无法推进且无任何在途执行（含退避等待）：
	// 只可能因运行时 Emit 构成环，明确失败而非死锁。
	active := false
	for _, inst := range e.instances {
		if inst.Status == StatusRunning || inst.Status == StatusRetryWait {
			active = true
			break
		}
	}
	if active {
		_ = e.persistLocked()
		return
	}
	for _, inst := range e.sortedInstancesLocked() {
		if inst.Status == StatusPending {
			e.transitionLocked(inst, StatusFailed,
				ErrCycle.Error()+": unschedulable (runtime dependency cycle)")
		}
	}
}

// execute 运行一个已处于 RUNNING 的实例（由 scheduleLocked 启动，持有一个 wg 计数）。
func (e *Engine) execute(spec TaskSpec, key string) {
	defer e.wg.Done()
	e.runAttempt(spec, key)
}

// releaseOwnedLocked 在本进程不再持有该键的执行/退避 goroutine 时摘除标记。
func (e *Engine) releaseOwnedLocked(key string) { delete(e.owned, key) }

// runAttempt 执行单次尝试，并根据结果迁移状态：
//   - 成功：SUCCEEDED；
//   - 失败且预算耗尽：FAILED；
//   - 失败但仍有预算：RETRY_WAIT（持久化剩余预算与预定重试时刻），
//     随后由 resumeRetryWait 按计划接管 —— 等待计划不因重启丢失。
//
// 调用前置：实例已在 RUNNING 且 Attempts 已置好。
func (e *Engine) runAttempt(spec TaskSpec, key string) {
	e.mu.Lock()
	inst := e.instances[key]
	attempt := inst.Attempts
	logicalKey, trig := inst.LogicalKey, inst.Trigger
	// 新一轮执行（重置后 MaxAttempts 已清零）按当前定义初始化总预算；
	// RETRY_WAIT 跨重启续跑时保留落盘的 MaxAttempts，剩余预算不被重新绑定的定义改写。
	if inst.MaxAttempts == 0 {
		inst.MaxAttempts = spec.MaxRetries + 1
	}
	e.logf("execute %s attempt=%d/%d", key, attempt, inst.MaxAttempts)
	e.mu.Unlock()

	tc := &TaskContext{
		TaskID:     spec.ID,
		LogicalKey: logicalKey,
		Trigger:    trig,
		Attempt:    attempt,
		engine:     e,
	}
	runErr := spec.Run(context.Background(), tc)

	e.mu.Lock()
	defer e.mu.Unlock()
	// 执行期间理论上状态不会被外部改写；以当前记录为准做迁移。
	inst = e.instances[key]
	switch {
	case runErr == nil:
		e.transitionLocked(inst, StatusSucceeded, "")
	case attempt >= inst.MaxAttempts:
		e.transitionLocked(inst, StatusFailed, runErr.Error())
	default:
		inst.Backoff = spec.Backoff
		inst.NextAttemptAt = time.Now().Add(spec.Backoff)
		e.transitionLocked(inst, StatusRetryWait, runErr.Error())
		e.logf("schedule %s retry wait until %s (%s, attempts left=%d)",
			key, inst.NextAttemptAt.Format(time.RFC3339Nano), spec.Backoff, inst.MaxAttempts-attempt)
	}
	_ = e.persistLocked()
	if inst.Status == StatusRetryWait {
		// 把退避等待接到同一在途链上：execute 释放的 wg 计数由 waiter 接住，
		// owned 保持 true（仍是本进程在途）。
		e.wg.Add(1)
		go e.resumeRetryWait(key)
		e.signalLocked()
		return
	}
	e.releaseOwnedLocked(key)
	e.scheduleLocked() // 唤醒因本实例完成而就绪（或被拖垮）的下游
	e.signalLocked()
}

// resumeRetryWait 按持久化的 NextAttemptAt 继续等待并重试。
// 跨重启后由 run 重新拉起：剩余时间从原计划时刻继续计算，
// 既不清零 Attempts，也不会提前重试。
func (e *Engine) resumeRetryWait(key string) {
	defer e.wg.Done()
	e.mu.Lock()
	inst := e.instances[key]
	wait := time.Until(inst.NextAttemptAt)
	e.mu.Unlock()
	if wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-e.closeCh:
			timer.Stop()
			// 关闭时保持 RETRY_WAIT（已落盘），等待计划下次继续。
			e.mu.Lock()
			e.releaseOwnedLocked(key)
			e.mu.Unlock()
			return
		}
	}

	e.mu.Lock()
	inst = e.instances[key]
	if inst.Status != StatusRetryWait {
		e.releaseOwnedLocked(key)
		e.mu.Unlock()
		return // 已被其他路径处理
	}
	spec, ok := e.specs[inst.TaskID]
	if !ok {
		e.transitionLocked(inst, StatusFailed, ErrUnknownTask.Error())
		_ = e.persistLocked()
		e.releaseOwnedLocked(key)
		e.scheduleLocked()
		e.signalLocked()
		e.mu.Unlock()
		return
	}
	if spec.Run == nil {
		// 展开任务的退避等待跨了重启，但应用尚未重新提供执行体：
		// 不能立刻重试，明确失败交代（应用重新 Submit 后再触发可重跑）。
		e.transitionLocked(inst, StatusFailed,
			fmt.Sprintf("%s: dynamic task %q definition not re-registered after restart",
				ErrUnknownTask.Error(), inst.TaskID))
		_ = e.persistLocked()
		e.releaseOwnedLocked(key)
		e.scheduleLocked()
		e.signalLocked()
		e.mu.Unlock()
		return
	}
	inst.Attempts++
	e.logf("resume %s retry at planned time, next attempt=%d/%d",
		key, inst.Attempts, inst.MaxAttempts)
	e.transitionLocked(inst, StatusRunning, "")
	e.mu.Unlock()
	// owned 保持 true：wg 计数由当前 goroutine 持有，runAttempt 终结整条链。
	e.runAttempt(spec, key)
}

// emitBatch 是 Emit/EmitBatch 的共用实现（调用方持有 tc）。
//
// batchID 标识上游本次执行产出中的“哪一批”。来源去重键为
// (LogicalKey, EmitterKey, Attempt, BatchID)：同一来源重复上报同一批任务，
// 只登记一次、不新增实例（不会越跑越多）。
//
// 展开出的任务天然依赖上游实例（即使 spec.Deps 未显式声明），
// 因此只会在上游成功后执行；上游失败时这些实例被标记 BLOCKED，
// 一眼可看出是被上游拖垮，而不会一直挂起或凭空消失。
func (e *Engine) emitBatch(tc *TaskContext, specs []TaskSpec, batchID string) error {
	if len(specs) == 0 {
		return nil
	}
	if batchID == "" {
		// 未显式标批次时，以本次上报的任务 ID 集合作为批次指纹，
		// 保证“重复报同一批”仍是同一次，而不同集合各自成批。
		batchID = batchFingerprint(specs)
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	emitterKey := tc.TaskID + "@" + tc.LogicalKey
	origin := ExpansionOrigin{
		EmitterTaskID: tc.TaskID,
		EmitterKey:    emitterKey,
		Attempt:       tc.Attempt,
		BatchID:       batchID,
		LogicalKey:    tc.LogicalKey,
		EmittedAt:     time.Now(),
	}
	dedup := expansionDedupKey(tc.LogicalKey, emitterKey, tc.Attempt, batchID)

	// 先（重新）登记动态 spec：
	//   - 新 ID：登记为动态任务（dynamicIDs），并把发起者隐式追加为依赖；
	//   - 重启后只剩 nil-Run 占位定义：以本次产出重新绑定执行体与参数；
	//   - 已存在的完整定义（静态提交或本进程已绑定的动态任务）：保持不动。
	// 即使本次上报命中来源去重（重启后上游重跑、重新产出同一批），这一步也必须
	// 执行：否则已展开的实例仍会因为没有可执行定义而被误判失败、永久不再运行。
	var added []string
	rebound := map[string]TaskSpec{}
	for _, spec := range specs {
		if spec.ID == "" {
			return fmt.Errorf("%w: empty emitted task id", ErrUnknownTask)
		}
		old, exists := e.specs[spec.ID]
		switch {
		case !exists:
			if !containsDep(spec.Deps, tc.TaskID) {
				spec.Deps = append(append([]string{}, spec.Deps...), tc.TaskID)
			}
			e.specs[spec.ID] = spec
			e.dynamicIDs[spec.ID] = true
			added = append(added, spec.ID)
		case e.dynamicIDs[spec.ID] && old.Run == nil:
			rebound[spec.ID] = old
			if !containsDep(spec.Deps, tc.TaskID) {
				spec.Deps = append(append([]string{}, spec.Deps...), tc.TaskID)
			}
			e.specs[spec.ID] = spec
		}
	}
	if cyc := findCycle(e.specs); cyc != nil {
		for _, id := range added {
			delete(e.specs, id)
			delete(e.dynamicIDs, id)
		}
		for id, old := range rebound {
			e.specs[id] = old
		}
		return fmt.Errorf("%w: %s", ErrCycle, strings.Join(cyc, " -> "))
	}

	if rec, dup := e.expansions[dedup]; dup {
		e.logf("emit batch dedup hit source=%s attempt=%d batch=%q tasks=%v -> rebind/skip",
			emitterKey, tc.Attempt, batchID, rec.TaskIDs)
		// 同来源重复报同一批只算一次：不新增台账记录、不新增实例；
		// 但重新绑定可能刚生效，落盘并继续驱动那些已展开、待执行的实例。
		if err := e.persistLocked(); err != nil {
			return err
		}
		e.scheduleLocked()
		e.signalLocked()
		return nil
	}

	emittedIDs := make([]string, 0, len(specs))
	created := 0
	for _, spec := range specs {
		emittedIDs = append(emittedIDs, spec.ID)
		key := spec.ID + "@" + tc.LogicalKey
		if _, exists := e.instances[key]; exists {
			continue // 同实例已由其他来源/批次登记：幂等跳过，不覆盖来源
		}
		// 作用范围：实例只属于本次逻辑键（业务日期），其他运行不可见。
		e.instances[key] = &Instance{
			TaskID: spec.ID, LogicalKey: tc.LogicalKey, Trigger: tc.Trigger,
			Status: StatusPending, UpdatedAt: time.Now(), Origin: origin,
		}
		created++
	}
	sort.Strings(emittedIDs)
	e.expansions[dedup] = &ExpansionRecord{
		EmitterTaskID: tc.TaskID,
		EmitterKey:    emitterKey,
		Attempt:       tc.Attempt,
		BatchID:       batchID,
		LogicalKey:    tc.LogicalKey,
		EmittedAt:     origin.EmittedAt,
		TaskIDs:       emittedIDs,
	}
	e.logf("emit batch source=%s attempt=%d batch=%q tasks=%v created=%d",
		emitterKey, tc.Attempt, batchID, emittedIDs, created)
	if err := e.persistLocked(); err != nil {
		return err
	}
	e.scheduleLocked()
	e.signalLocked()
	return nil
}

func containsDep(deps []string, id string) bool {
	for _, d := range deps {
		if d == id {
			return true
		}
	}
	return false
}

// batchFingerprint 以排序后的任务 ID 集合生成稳定批次指纹。
func batchFingerprint(specs []TaskSpec) string {
	ids := make([]string, len(specs))
	for i, s := range specs {
		ids[i] = s.ID
	}
	sort.Strings(ids)
	return "fp:" + strings.Join(ids, ",")
}

// expansionDedupKey 组装展开台账的来源去重键。
func expansionDedupKey(logicalKey, emitterKey string, attempt int, batchID string) string {
	return logicalKey + "|" + emitterKey + "#" + strconv.Itoa(attempt) + "|" + batchID
}

// wait 阻塞直到该逻辑键的所有实例到达终态，并返回汇总的可区分结果。
func (e *Engine) wait(ctx context.Context, logicalKey string) error {
	for {
		e.mu.Lock()
		done, failed, blocked := true, false, false
		for _, inst := range e.instancesForKeyLocked(logicalKey) {
			switch inst.Status {
			case StatusSucceeded:
			case StatusFailed, StatusInterrupted:
				failed = true
			case StatusBlocked:
				blocked = true
			default: // PENDING / RUNNING / RETRY_WAIT
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

// Expansions 返回某逻辑键（业务日期）本次运行的全部展开记录，
// 按来源（上游键、执行序号、批次）排序。其他业务日期的展开不会出现。
func (e *Engine) Expansions(logicalKey string) []*ExpansionRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []*ExpansionRecord
	for _, rec := range e.expansions {
		if rec.LogicalKey != logicalKey {
			continue
		}
		cp := *rec
		cp.TaskIDs = append([]string{}, rec.TaskIDs...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EmitterKey != out[j].EmitterKey {
			return out[i].EmitterKey < out[j].EmitterKey
		}
		if out[i].Attempt != out[j].Attempt {
			return out[i].Attempt < out[j].Attempt
		}
		return out[i].BatchID < out[j].BatchID
	})
	return out
}

// Close 通知退避等待中的任务交还（RETRY_WAIT 状态已落盘），
// 等待在途执行结束后最终落盘。
func (e *Engine) Close() error {
	close(e.closeCh)
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

func (e *Engine) persistLocked() error {
	dyn := make(map[string]*DynamicSpec, len(e.dynamicIDs))
	for id := range e.dynamicIDs {
		s := e.specs[id]
		ds := &DynamicSpec{
			Deps:       append([]string{}, s.Deps...),
			MaxRetries: s.MaxRetries,
			Backoff:    s.Backoff,
		}
		dyn[id] = ds
	}
	return e.store.Save(&State{Instances: e.instances, Expansions: e.expansions, DynamicSpecs: dyn})
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
