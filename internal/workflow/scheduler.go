package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Scheduler 工作流调度引擎。
//
// 并发模型：
//   - 进程内以 date 为粒度持有在途锁（inFlight），同一日期的并发触发只会有
//     一个 Run 真正执行 DAG，其余调用并入等待（或按补跑策略拒绝/跳过）；
//   - DAG 内无依赖关系的任务并发执行；每个 (任务,日期) 实例经 Store CAS
//     领取，保证即使跨进程也只被实际执行一次；
//   - 触发之间的幂等由持久化终态（succeeded/failed/skipped）保证。
type Scheduler struct {
	reg   *Registry
	store Store
	log   Logger
	now   func() time.Time

	mu       sync.Mutex
	inFlight map[string]*flight // date -> 在途 run
}

type flight struct {
	done        chan struct{}
	res         *RunResult
	err         error
	ownerSource Source
}

// Option 调度器配置项。
type Option func(*Scheduler)

// WithLogger 注入日志器。
func WithLogger(l Logger) Option { return func(s *Scheduler) { s.log = l } }

// WithClock 注入时钟（测试退避与恢复用）。
func WithClock(now func() time.Time) Option { return func(s *Scheduler) { s.now = now } }

// NewScheduler 创建调度器：先静态校验任务图，再从持久化状态执行恢复。
func NewScheduler(reg *Registry, store Store, opts ...Option) (*Scheduler, error) {
	s := &Scheduler{
		reg:      reg,
		store:    store,
		log:      NewStdLogger(),
		now:      time.Now,
		inFlight: make(map[string]*flight),
	}
	for _, o := range opts {
		o(s)
	}
	if cycle, err := validateGraph(reg); err != nil {
		s.log.Logf(context.Background(), "启动校验失败: %v cycle=%v", err, cycle)
		return nil, err
	}
	if err := s.recover(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

// recover 实现确定的重启恢复语义：
//   - running：上一进程在执行中途崩溃，该次尝试结局未知。记为一次失败尝试，
//     状态回收到 pending，使下一次触发可立即重试（尝试次数被计数，不会无限重试）；
//   - pending/succeeded/failed/skipped：原样保留，语义自明。
//
// 恢复只改写 running 记录，且每条记录独立 CAS（期望状态恰为 running）。
func (s *Scheduler) recover(ctx context.Context) error {
	recs, err := s.store.List(ctx)
	if err != nil {
		return fmt.Errorf("workflow: recover list: %w", err)
	}
	for _, r := range recs {
		if r.Status != StatusRunning {
			continue
		}
		attemptNo := len(r.Attempts)
		r.Attempts = append(r.Attempts, Attempt{
			Number:    attemptNo + 1,
			StartedAt: r.UpdatedAt,
			EndedAt:   s.now(),
			Err:       "interrupted by process restart; attempt outcome unknown",
		})
		r.Status = StatusPending
		r.UpdatedAt = s.now()
		ok, err := s.store.Put(ctx, r, ptrStatus(StatusRunning))
		if err != nil {
			return fmt.Errorf("workflow: recover put %s/%s: %w", r.Key.Date, r.Key.Task, err)
		}
		if ok {
			s.log.Logf(ctx, "恢复: 实例 %s/%s running->pending（中断尝试计为第 %d 次失败）",
				r.Key.Date, r.Key.Task, attemptNo+1)
		} else {
			s.log.Logf(ctx, "恢复: 实例 %s/%s 已被其他进程改写，跳过恢复",
				r.Key.Date, r.Key.Task)
		}
	}
	return nil
}

// Trigger 发起一次常规触发。
// 若同一日期已有在途 Run，则并入等待其结论（同一逻辑实例绝不执行第二次）。
func (s *Scheduler) Trigger(ctx context.Context, req TriggerRequest) (*RunResult, error) {
	if !validDate(req.Date) {
		return nil, fmt.Errorf("%w: bad date %q", ErrInvalidRequest, req.Date)
	}
	return s.runDate(ctx, req.Date, SourceRegular, PolicyReject, false)
}

// Backfill 对历史闭区间 [Start, End] 逐日期补跑。
//   - PolicyReject：任一日期存在在途 Run 即整体拒绝（ErrRejected），冲突不被掩盖；
//   - PolicySkipInFlight：在途日期并入等待，其中属于其他在途 Run 的实例标记跳过原因。
//
// 补跑与常规触发共享同一套 (任务,日期) 实例记录，因此成功实例直接复用，
// 不会被重复执行。
func (s *Scheduler) Backfill(ctx context.Context, req BackfillRequest) (*RunResult, error) {
	if !validDate(req.Start) || !validDate(req.End) || req.Start > req.End {
		return nil, fmt.Errorf("%w: bad interval [%s,%s]", ErrInvalidRequest, req.Start, req.End)
	}
	policy := req.Policy
	if policy == "" {
		policy = PolicyReject
	}
	dates := datesBetween(req.Start, req.End)

	if policy == PolicyReject {
		s.mu.Lock()
		for _, d := range dates {
			if f, ok := s.inFlight[d]; ok && !f.closed() {
				s.mu.Unlock()
				return nil, fmt.Errorf("%w: date %s has an in-flight run", ErrRejected, d)
			}
		}
		s.mu.Unlock()
	}

	merged := &RunResult{Source: SourceBackfill, Date: req.Start, End: req.End}
	for _, d := range dates {
		res, err := s.runDate(ctx, d, SourceBackfill, policy, policy == PolicySkipInFlight)
		if err != nil {
			return merged, err
		}
		merged.Results = append(merged.Results, res.Results...)
		merged.SkippedInFlight = append(merged.SkippedInFlight, res.SkippedInFlight...)
	}
	return merged, nil
}

// Close 关闭调度器。
func (s *Scheduler) Close() error { return s.store.Close() }

// runDate 执行单个逻辑日期。
//
// 在途冲突语义（按触发方式区分，结论可解释）：
//   - 常规触发：同一日期已有在途 Run 时并入等待其结论（Joined=true），
//     不会发起第二遍执行，同一实例只跑一次；
//   - 补跑 + PolicyReject：在途冲突在 Backfill 入口已整体拒绝，不会进入这里；
//   - 补跑 + PolicySkipInFlight：并入等待，属于在途 Run 的实例标 inflight_skipped，
//     已终态的实例仍按幂等规则给出结论。
func (s *Scheduler) runDate(ctx context.Context, date string, src Source, policy ConflictPolicy, _ bool) (*RunResult, error) {
	s.mu.Lock()
	if f, ok := s.inFlight[date]; ok {
		// 补跑 + skip_inflight：不等待在途 Run，立即给出“在途跳过”结论。
		if src == SourceBackfill && policy == PolicySkipInFlight {
			owner := f.ownerSource
			s.mu.Unlock()
			s.log.Logf(ctx, "触发: source=%s date=%s 检测到在途 Run(source=%s)，按 skip_inflight 立即跳过",
				src, date, owner)
			res := &RunResult{Source: src, Date: date, Joined: false}
			for _, n := range s.reg.Names() {
				k := InstanceKey{Task: n, Date: date}
				res.Results = append(res.Results, InstanceResult{
					Key: k, Status: StatusPending, Reason: ReasonInFlightSkipped,
					Err: "skipped because another run is in flight",
				})
				res.SkippedInFlight = append(res.SkippedInFlight, k)
			}
			return res, nil
		}
		s.mu.Unlock()
		return s.joinFlight(ctx, date, src, f, policy)
	}
	f := &flight{done: make(chan struct{}), ownerSource: src}
	s.inFlight[date] = f
	s.mu.Unlock()

	s.log.Logf(ctx, "触发: source=%s date=%s 取得在途锁，开始调度 DAG", src, date)
	res, err := s.safeRunDate(ctx, date, src)

	s.mu.Lock()
	f.res = res
	f.err = err
	close(f.done)
	delete(s.inFlight, date)
	s.mu.Unlock()

	if err != nil {
		s.log.Logf(ctx, "触发结束: source=%s date=%s err=%v", src, date, err)
	} else {
		s.log.Logf(ctx, "触发结束: source=%s date=%s 实例数=%d", src, date, len(res.Results))
	}
	return res, err
}

// joinFlight 并入一个在途 Run。
func (s *Scheduler) joinFlight(ctx context.Context, date string, src Source, f *flight, policy ConflictPolicy) (*RunResult, error) {
	if src == SourceBackfill && policy == PolicyReject {
		// 理论上 Backfill 入口已拦截；防御性明确拒绝。
		return nil, fmt.Errorf("%w: date %s has an in-flight run", ErrRejected, date)
	}
	s.log.Logf(ctx, "触发: source=%s date=%s 检测到在途 Run(source=%s)，并入等待，不重复执行",
		src, date, f.ownerSource)
	select {
	case <-f.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	res := cloneRunResult(f.res)
	res.Joined = true
	return res, nil
}

// safeRunDate 运行单个日期并从 panic 恢复（panic 也算任务失败，不让进程崩溃）。
func (s *Scheduler) safeRunDate(ctx context.Context, date string, src Source) (res *RunResult, err error) {
	e := newDateExec(s, ctx, date, src)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("workflow: panic while running date %s: %v", date, r)
		}
	}()
	return e.run(), nil
}

func cloneRunResult(r *RunResult) *RunResult {
	if r == nil {
		return nil
	}
	c := *r
	c.Results = append([]InstanceResult(nil), r.Results...)
	c.SkippedInFlight = append([]InstanceKey(nil), r.SkippedInFlight...)
	c.CyclePath = append([]string(nil), r.CyclePath...)
	return &c
}

func (f *flight) closed() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

func ptrStatus(st Status) *Status { return &st }
