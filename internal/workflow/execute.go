package workflow

import (
	"errors"
	"fmt"
	"time"
)

// errDynamicCycleMarker 记录在失败实例上的稳定标记，重启后仍可判为 dynamic_cycle。
const errDynamicCycleMarker = "DYNAMIC_CYCLE_DETECTED"

// execute 执行单个已激活且全部依赖成功的实例，返回可解释结论。
// 状态通过 Store CAS 推进：
//
//	pending -(领取)-> running -(成功)-> succeeded
//	                           -(失败且仍有次数)-> pending(NextRunAt) -> 退避后重试
//	                           -(失败且次数耗尽)-> failed
//
// 已处于终态的记录一律复用：succeeded 绝不重复执行（幂等），failed 结论保持，
// skipped 结论保持（upstream_failed）。
func (e *dateExec) execute(name string, spec TaskSpec) InstanceResult {
	key := InstanceKey{Task: name, Date: e.date}
	_, handler, _ := e.s.reg.Get(name)

	for {
		rec, err := e.s.store.Get(e.ctx, key)
		if err != nil {
			return InstanceResult{Key: key, Status: StatusFailed, Err: err.Error()}
		}

		// 终态短路：结论稳定、可解释、不重复执行。
		if rec != nil {
			switch rec.Status {
			case StatusSucceeded:
				e.s.log.Logf(e.ctx, "判定: %s/%s 复用已成功记录，不重复执行（幂等）", e.date, name)
				return e.remember(rec, ReasonAlreadySucceeded, "")
			case StatusFailed:
				reason := ReasonAttemptsExhausted
				if rec.Err == errDynamicCycleMarker {
					reason = ReasonDynamicCycle
				}
				e.s.log.Logf(e.ctx, "判定: %s/%s 此前已失败(%s)，结论保持", e.date, name, reason)
				return e.remember(rec, reason, rec.Err)
			case StatusSkipped:
				return e.remember(rec, ReasonUpstreamFailed, rec.Err)
			}
		}

		// 不存在 -> 建立 pending 记录（CAS 防并发重复创建）。
		if rec == nil {
			fresh := &Record{
				Key:       key,
				Status:    StatusPending,
				Source:    e.source,
				Parent:    e.implicitParentOf(name),
				UpdatedAt: e.s.now(),
			}
			ok, err := e.s.store.Put(e.ctx, fresh, nil)
			if err != nil {
				return InstanceResult{Key: key, Status: StatusFailed, Err: err.Error()}
			}
			if !ok {
				continue // 并发：其他触发抢先创建，重读后走统一判定
			}
			rec = fresh
		}

		// 退避窗口未到则等待（时钟可注入，测试可控）。
		if !rec.NextRunAt.IsZero() && e.s.now().Before(rec.NextRunAt) {
			e.s.log.Logf(e.ctx, "等待退避: %s/%s 最早重试时刻 %s",
				e.date, name, rec.NextRunAt.Format(time.RFC3339Nano))
			if !e.waitForRetry(rec.NextRunAt) {
				return e.remember(rec, ReasonRetryScheduled, e.ctx.Err().Error())
			}
			continue
		}

		// 领取：pending -> running，开启一次新尝试。
		attemptNo := len(rec.Attempts) + 1
		claim := cloneRecord(rec)
		claim.Status = StatusRunning
		claim.Attempts = append(claim.Attempts, Attempt{Number: attemptNo, StartedAt: e.s.now()})
		claim.UpdatedAt = e.s.now()
		ok, err := e.s.store.Put(e.ctx, claim, ptrStatus(StatusPending))
		if err != nil {
			return InstanceResult{Key: key, Status: StatusFailed, Err: err.Error()}
		}
		if !ok {
			e.s.log.Logf(e.ctx, "领取失败: %s/%s 已被其他执行者领取，重读状态", e.date, name)
			continue
		}
		e.s.log.Logf(e.ctx, "状态流转: %s/%s pending->running（第 %d/%d 次尝试）",
			e.date, name, attemptNo, spec.MaxAttempts)

		// 首次执行前做动态展开；重启恢复时复用已持久化的 Expanded，不重复展开。
		if len(claim.Expanded) > 0 {
			e.s.log.Logf(e.ctx, "恢复: %s/%s 重新激活已展开子任务 %v", e.date, name, claim.Expanded)
			if ir, proceed := e.reactivate(name, claim.Expanded); !proceed {
				return ir
			}
		} else {
			if ir, proceed := e.doExpand(name, spec, handler, claim); !proceed {
				return ir
			}
		}

		// 执行业务逻辑。
		out, execErr := handler.Execute(e.ctx, ExecRequest{
			Key:         key,
			Attempt:     attemptNo,
			UpstreamOut: e.upstreamOutputs(name, spec),
		})

		finished := cloneRecord(claim)
		a := &finished.Attempts[len(finished.Attempts)-1]
		a.EndedAt = e.s.now()

		if execErr == nil {
			a.Output = out.Output
			finished.Status = StatusSucceeded
			finished.Output = out.Output
			finished.NextRunAt = time.Time{}
			finished.Err = ""
			finished.UpdatedAt = e.s.now()
			ok, err := e.s.store.Put(e.ctx, finished, ptrStatus(StatusRunning))
			if err != nil {
				return InstanceResult{Key: key, Status: StatusFailed, Err: err.Error()}
			}
			if !ok {
				continue
			}
			e.s.log.Logf(e.ctx, "状态流转: %s/%s running->succeeded（第 %d 次尝试成功，产出=%q）",
				e.date, name, attemptNo, out.Output)
			return e.remember(finished, ReasonExecuted, "")
		}

		// 失败路径。
		a.Err = execErr.Error()
		finished.UpdatedAt = e.s.now()
		if attemptNo < spec.MaxAttempts {
			finished.Status = StatusPending
			finished.NextRunAt = e.s.now().Add(spec.Backoff)
			ok, err := e.s.store.Put(e.ctx, finished, ptrStatus(StatusRunning))
			if err != nil {
				return InstanceResult{Key: key, Status: StatusFailed, Err: err.Error()}
			}
			if !ok {
				continue
			}
			e.s.log.Logf(e.ctx,
				"状态流转: %s/%s running->pending（第 %d 次尝试失败: %v；退避 %s，不早于 %s 重试）",
				e.date, name, attemptNo, execErr, spec.Backoff,
				finished.NextRunAt.Format(time.RFC3339Nano))
			if !e.waitForRetry(finished.NextRunAt) {
				return e.remember(finished, ReasonRetryScheduled, e.ctx.Err().Error())
			}
			continue
		}

		finished.Status = StatusFailed
		finished.Err = execErr.Error()
		finished.UpdatedAt = e.s.now()
		ok, err = e.s.store.Put(e.ctx, finished, ptrStatus(StatusRunning))
		if err != nil {
			return InstanceResult{Key: key, Status: StatusFailed, Err: err.Error()}
		}
		if !ok {
			continue
		}
		e.s.log.Logf(e.ctx,
			"状态流转: %s/%s running->failed（第 %d/%d 次尝试失败: %v；重试次数耗尽）",
			e.date, name, attemptNo, spec.MaxAttempts, execErr)
		return e.remember(finished, ReasonAttemptsExhausted, execErr.Error())
	}
}

// doExpand 调用 Expand，校验子任务与动态边，落盘展开结果并启动子任务。
// 返回 (结论, 是否继续执行)。
func (e *dateExec) doExpand(parent string, spec TaskSpec, handler TaskHandler, claim *Record) (InstanceResult, bool) {
	key := claim.Key
	exp, err := handler.Expand(e.ctx, ExecRequest{
		Key:         key,
		Attempt:     1,
		UpstreamOut: e.upstreamOutputs(parent, spec),
	})
	if err != nil {
		return e.failExpand(parent, claim, fmt.Errorf("expand: %w", err), false), false
	}
	children, err := e.checkChildren(parent, exp.Children)
	if err != nil {
		return e.failExpand(parent, claim, err, isDynamicCycleErr(err)), false
	}

	// 先记录隐式父子边并启动子任务（激活信号先于父任务的成功完成信号），
	// 再把展开结果持久化；这样不会丢失唤醒。
	for _, c := range children {
		e.edgeMu.Lock()
		e.implicitParent[c] = parent
		e.edgeMu.Unlock()
		e.s.log.Logf(e.ctx, "动态展开: %s/%s -> 启动子任务 %s/%s（隐式依赖父任务）", e.date, parent, e.date, c)
		if e.startTask(c) {
			e.activateChild(c)
		}
	}

	claim.Expanded = append(claim.Expanded, children...)
	claim.UpdatedAt = e.s.now()
	ok, err := e.s.store.Put(e.ctx, claim, ptrStatus(StatusRunning))
	if err != nil {
		return InstanceResult{Key: key, Status: StatusFailed, Err: err.Error()}, false
	}
	if !ok {
		return InstanceResult{Key: key, Status: StatusPending}, false
	}
	return InstanceResult{}, true
}

// reactivate 重启恢复路径：按已持久化的 Expanded 重新建立父子边并启动子任务。
func (e *dateExec) reactivate(parent string, children []string) (InstanceResult, bool) {
	for _, c := range children {
		if _, _, ok := e.s.reg.Get(c); !ok {
			continue // 注册表不含则由 wait/依赖判定暴露
		}
		e.edgeMu.Lock()
		if _, exists := e.implicitParent[c]; !exists {
			e.implicitParent[c] = parent
		}
		e.edgeMu.Unlock()
		if e.startTask(c) {
			e.activateChild(c)
		}
	}
	return InstanceResult{}, true
}

// checkChildren 校验展开结果：
//   - 子任务必须已注册且必须是 Dynamic 模板（不允许“展开”普通任务）；
//   - 一个动态模板只能有唯一展开父（重复展开明确拒绝，保证身份与依赖确定）；
//   - 新的父子边不得与 静态边+已有隐式边 构成环（子任务不得能到达父任务）。
func (e *dateExec) checkChildren(parent string, raw []string) ([]string, error) {
	seen := map[string]bool{}
	var children []string
	for _, c := range raw {
		if seen[c] {
			continue
		}
		seen[c] = true
		cspec, _, ok := e.s.reg.Get(c)
		if !ok {
			return nil, fmt.Errorf("%w: %q expanded unknown child %q", ErrUnknownTask, parent, c)
		}
		if !cspec.Dynamic {
			return nil, fmt.Errorf("%w: %q expanded non-dynamic task %q", ErrInvalidDAG, parent, c)
		}
		e.edgeMu.Lock()
		if p, exists := e.implicitParent[c]; exists && p != parent {
			e.edgeMu.Unlock()
			return nil, fmt.Errorf("%w: dynamic task %q expanded by two parents %q and %q",
				ErrRejected, c, p, parent)
		}
		e.edgeMu.Unlock()
		children = append(children, c)
	}
	if path := e.dynamicCyclePath(parent, children); path != nil {
		return nil, fmt.Errorf("%w: %v", ErrDynamicCycle, path)
	}
	return children, nil
}

// dynamicCyclePath 检查新增边 parent->child 是否引入环：
// 若 child 已能沿 (静态边∪隐式边) 到达 parent，则形成环并返回路径。
func (e *dateExec) dynamicCyclePath(parent string, children []string) []string {
	for _, c := range children {
		// findPath(c, parent) 返回 [parent ... c]；反转并闭合到 c 得到 [c ... parent c]。
		if path := e.findPath(c, parent, map[string]bool{}); path != nil {
			for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
				path[i], path[j] = path[j], path[i]
			}
			return append(path, c)
		}
	}
	return nil
}

func (e *dateExec) findPath(from, to string, visited map[string]bool) []string {
	if from == to {
		return []string{from}
	}
	if visited[from] {
		return nil
	}
	visited[from] = true
	spec, _, ok := e.s.reg.Get(from)
	if !ok {
		return nil
	}
	e.edgeMu.Lock()
	extra := e.implicitParent[from]
	e.edgeMu.Unlock()
	deps := spec.DependsOn
	if extra != "" {
		deps = append(deps, extra)
	}
	for _, d := range deps {
		if p := e.findPath(d, to, visited); p != nil {
			return append(p, from)
		}
	}
	return nil
}

// failExpand 将展开阶段错误落为 failed 终态。
func (e *dateExec) failExpand(name string, claim *Record, cause error, markCycle bool) InstanceResult {
	fin := cloneRecord(claim)
	fin.Status = StatusFailed
	fin.Err = cause.Error()
	if markCycle {
		fin.Err = errDynamicCycleMarker
	}
	fin.UpdatedAt = e.s.now()
	if len(fin.Attempts) > 0 {
		fin.Attempts[len(fin.Attempts)-1].EndedAt = e.s.now()
		fin.Attempts[len(fin.Attempts)-1].Err = fin.Err
	}
	_, _ = e.s.store.Put(e.ctx, fin, ptrStatus(StatusRunning))
	e.s.log.Logf(e.ctx, "状态流转: %s/%s running->failed（展开失败: %v）", e.date, name, cause)
	reason := ReasonExecuted
	if markCycle {
		reason = ReasonDynamicCycle
	}
	return e.remember(fin, reason, cause.Error())
}

func (e *dateExec) upstreamOutputs(name string, spec TaskSpec) map[string]string {
	deps := append([]string{}, spec.DependsOn...)
	if ip := e.implicitParentOf(name); ip != "" {
		deps = append(deps, ip)
	}
	out := make(map[string]string, len(deps))
	e.resMu.Lock()
	for _, dep := range deps {
		if r := e.records[dep]; r != nil {
			out[dep] = r.Output
		}
	}
	e.resMu.Unlock()
	return out
}

// waitForRetry 睡过退避窗口；ctx 取消返回 false。
func (e *dateExec) waitForRetry(until time.Time) bool {
	d := until.Sub(e.s.now())
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-e.ctx.Done():
		return false
	}
}

func isDynamicCycleErr(err error) bool { return errors.Is(err, ErrDynamicCycle) }
