package workflow

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// dateExec 承载单个逻辑日期的一次 DAG 执行。
//
// 调度规则（无死锁的关键）：
//   - 静态 DependsOn 只允许指向普通任务（validateGraph 拒绝指向 Dynamic 模板），
//     因此所有静态边的目标在 Run 开始时必然已启动，等待不会无限挂起；
//   - Dynamic 模板任务由唯一父任务 Expand 展开后才启动（见 implicitParent），
//     隐式依赖其父任务：父成功才执行，父失败/跳过时子跳过(upstream_failed)；
//   - 父任务展开发生在其执行成功之前，子任务先于父任务完成信号被启动，
//     不存在唤醒丢失；静态图无环 + 展开边成环校验，保证全局无环。
type dateExec struct {
	s      *Scheduler
	ctx    context.Context
	date   string
	source Source

	wg sync.WaitGroup

	startMu sync.Mutex
	started map[string]bool          // 已启动 goroutine 的任务
	doneCh  map[string]chan struct{} // 任务完全结束信号
	active  map[string]chan struct{} // Dynamic 任务的激活信号

	edgeMu         sync.Mutex
	implicitParent map[string]string // 动态子任务 -> 唯一展开父任务

	resMu   sync.Mutex
	results map[string]InstanceResult // task -> 结论
	records map[string]*Record        // task -> 最新持久化记录（含产出）
}

func newDateExec(s *Scheduler, ctx context.Context, date string, src Source) *dateExec {
	return &dateExec{
		s:              s,
		ctx:            ctx,
		date:           date,
		source:         src,
		started:        map[string]bool{},
		doneCh:         map[string]chan struct{}{},
		active:         map[string]chan struct{}{},
		implicitParent: map[string]string{},
		results:        map[string]InstanceResult{},
		records:        map[string]*Record{},
	}
}

// run 并发执行整个 DAG 并汇总结论。
func (e *dateExec) run() *RunResult {
	// 普通任务全部在开始时启动（它们之间由静态边保证 DAG，无环即无死锁）。
	for _, name := range e.s.reg.Names() {
		spec, _, _ := e.s.reg.Get(name)
		if spec.Dynamic {
			continue
		}
		e.startTask(name)
	}
	// 已在历史运行中持久化的动态模板任务：即使本次父任务走终态短路不再展开，
	// 也要恢复调度（终态复用 / 退避后重试），不能误判为 not_expanded。
	if recs, err := e.s.store.ListByDate(e.ctx, e.date); err == nil {
		for _, r := range recs {
			spec, _, ok := e.s.reg.Get(r.Key.Task)
			if !ok || !spec.Dynamic {
				continue
			}
			// 先恢复隐式父子边再激活，子任务会正确等待（可能已终态的）父任务。
			if r.Parent != "" {
				e.edgeMu.Lock()
				if _, exists := e.implicitParent[r.Key.Task]; !exists {
					e.implicitParent[r.Key.Task] = r.Parent
				}
				e.edgeMu.Unlock()
			}
			if e.startTask(r.Key.Task) {
				e.activateChild(r.Key.Task)
			}
		}
	}
	e.wg.Wait()

	res := &RunResult{Source: e.source, Date: e.date}
	for _, name := range e.s.reg.Names() {
		e.resMu.Lock()
		r, ok := e.results[name]
		e.resMu.Unlock()
		if !ok {
			// Dynamic 且无人展开：确定性的“未展开”结论，而非静默跳过。
			r = InstanceResult{
				Key:    InstanceKey{Task: name, Date: e.date},
				Status: StatusSkipped,
				Reason: ReasonNotExpanded,
				Err:    "dynamic template task was not expanded by any parent",
			}
			e.s.log.Logf(e.ctx, "判定: %s/%s 跳过(%s)", e.date, name, ReasonNotExpanded)
		}
		res.Results = append(res.Results, r)
	}
	sort.Slice(res.Results, func(i, j int) bool { return res.Results[i].Key.Task < res.Results[j].Key.Task })
	return res
}

// startTask 幂等启动任务 goroutine；同一任务只会启动一次（返回 false 表示已存在）。
// 调用方必须在添加隐式父子边之后再启动动态子任务。
func (e *dateExec) startTask(name string) bool {
	e.startMu.Lock()
	defer e.startMu.Unlock()
	if e.started[name] {
		return false
	}
	spec, _, ok := e.s.reg.Get(name)
	if !ok {
		return false
	}
	done := make(chan struct{})
	act := make(chan struct{})
	e.doneCh[name] = done
	e.active[name] = act
	e.started[name] = true
	if !spec.Dynamic {
		close(act)
	}
	e.wg.Add(1)
	go e.runTask(name, spec, act, done)
	return true
}

// activateChild 关闭动态子任务的激活信号（幂等）。
func (e *dateExec) activateChild(name string) {
	e.startMu.Lock()
	defer e.startMu.Unlock()
	if ch, ok := e.active[name]; ok {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

func (e *dateExec) runTask(name string, spec TaskSpec, activated <-chan struct{}, done chan struct{}) {
	defer e.wg.Done()
	defer close(done)
	key := InstanceKey{Task: name, Date: e.date}
	finish := func(ir InstanceResult) {
		e.resMu.Lock()
		e.results[name] = ir
		e.resMu.Unlock()
	}

	// 1) 等待激活（普通任务信号已关闭；Dynamic 任务等待父任务展开）。
	select {
	case <-activated:
	case <-e.ctx.Done():
		finish(InstanceResult{Key: key, Status: StatusFailed, Err: e.ctx.Err().Error()})
		return
	}

	// 2) 依赖集合 = 静态依赖 + （动态子任务的）隐式父任务。
	deps := append([]string{}, spec.DependsOn...)
	if ip := e.implicitParentOf(name); ip != "" {
		deps = append(deps, ip)
	}

	// 3) 等待所有依赖结束。普通任务在 Run 开始即启动，通道必然存在。
	for _, dep := range deps {
		e.startMu.Lock()
		ch := e.doneCh[dep]
		e.startMu.Unlock()
		if ch == nil {
			finish(InstanceResult{Key: key, Status: StatusFailed, Reason: ReasonInvalidDAG,
				Err: fmt.Sprintf("dependency %q not scheduled", dep)})
			return
		}
		select {
		case <-ch:
		case <-e.ctx.Done():
			finish(InstanceResult{Key: key, Status: StatusFailed, Err: e.ctx.Err().Error()})
			return
		}
	}

	// 4) 任一依赖非成功 -> 确定性跳过，原因可区分。
	for _, dep := range deps {
		e.resMu.Lock()
		up := e.results[dep]
		e.resMu.Unlock()
		if up.Status != StatusSucceeded {
			ir := InstanceResult{
				Key:    key,
				Status: StatusSkipped,
				Reason: ReasonUpstreamFailed,
				Err:    fmt.Sprintf("dependency %q ended with %s(%s)", dep, up.Status, up.Reason),
			}
			e.s.log.Logf(e.ctx, "判定: %s/%s 跳过(%s) 依据=%s", e.date, name, ir.Reason, ir.Err)
			finish(ir)
			return
		}
	}

	// 5) 执行（含领取、展开、重试退避）。
	finish(e.execute(name, spec))
}

func (e *dateExec) implicitParentOf(name string) string {
	e.edgeMu.Lock()
	defer e.edgeMu.Unlock()
	return e.implicitParent[name]
}

// remember 缓存最新记录供下游读取产出。
func (e *dateExec) remember(rec *Record, reason Reason, errMsg string) InstanceResult {
	e.resMu.Lock()
	e.records[rec.Key.Task] = rec
	e.resMu.Unlock()
	return InstanceResult{Key: rec.Key, Status: rec.Status, Reason: reason, Err: errMsg}
}
