package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestExpansionAlreadyEmittedResumesAcrossRestart
// 上游已展开出后续任务、但后续任务还没执行时进程挂掉；重启后同一天再跑：
// 已展开的任务不能被直接判死，上游重跑再次产出同一批（命中来源去重）
// 也要能继续按正常顺序执行到最终结果，台账仍只算一次。
func TestExpansionAlreadyEmittedResumesAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day = "2026-10-01"

	var scanRuns, childRuns atomic.Int32
	gate1 := make(chan struct{}) // e1 永不放行，模拟崩溃时上游 RUNNING 被挂住

	scanSpec := func(gate <-chan struct{}) TaskSpec {
		return TaskSpec{
			ID: "scan",
			Run: func(ctx context.Context, tc *TaskContext) error {
				scanRuns.Add(1)
				if err := tc.Emit(TaskSpec{
					ID: "child",
					Run: func(context.Context, *TaskContext) error {
						childRuns.Add(1)
						return nil
					},
				}); err != nil {
					return err
				}
				if gate != nil {
					<-gate
				}
				return nil
			},
		}
	}

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(scanSpec(gate1)); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	triggerErr1 := make(chan error, 1)
	go func() { triggerErr1 <- e1.Trigger(context.Background(), day) }()

	deadline := time.After(2 * time.Second)
	for {
		s, sok := e1.Instance("scan", day)
		c, cok := e1.Instance("child", day)
		if sok && cok && s.Status == StatusRunning && c.Status == StatusPending {
			break
		}
		select {
		case <-deadline:
			t.Fatal("never reached scan=RUNNING/child=PENDING before crash")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	t.Log("crash: scan RUNNING, child already emitted PENDING")

	// 直接丢弃 e1（不 Close），新进程从持久化状态恢复。
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	t.Cleanup(func() { _ = e2.Close() })

	s, _ := e2.Instance("scan", day)
	c, _ := e2.Instance("child", day)
	if s.Status != StatusInterrupted {
		t.Fatalf("scan=%s, want INTERRUPTED", s.Status)
	}
	if c.Status != StatusBlocked || !strings.Contains(c.LastError, "scan@"+day) {
		t.Fatalf("child=%s(%q), want BLOCKED naming scan@%s after recovery",
			c.Status, c.LastError, day)
	}
	if c.Origin.EmitterKey != "scan@"+day || c.Origin.Attempt != 1 {
		t.Fatalf("child origin lost across restart: %+v", c.Origin)
	}

	// 重新提交上游定义并再跑同一天：上游重跑、重新产出同一批（命中旧台账去重），
	// 已展开的 child 必须被真正跑起来，而不是永久失败/消失。
	if err := e2.Submit(scanSpec(nil)); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	if err := e2.Trigger(context.Background(), day); err != nil {
		t.Fatalf("re-trigger after restart: %v", err)
	}

	if scanRuns.Load() != 2 {
		t.Fatalf("scan ran %d times, want 2", scanRuns.Load())
	}
	if childRuns.Load() != 1 {
		t.Fatalf("child ran %d times, want exactly 1 (emitted batch ran once)", childRuns.Load())
	}
	s, _ = e2.Instance("scan", day)
	c, _ = e2.Instance("child", day)
	if s.Status != StatusSucceeded || c.Status != StatusSucceeded {
		t.Fatalf("scan=%s child=%s, want both SUCCEEDED", s.Status, c.Status)
	}
	recs := e2.Expansions(day)
	if len(recs) != 1 {
		t.Fatalf("expansion records=%d, want 1 (same source+batch across restart counted once)", len(recs))
	}
	if fmt.Sprint(recs[0].TaskIDs) != "[child]" {
		t.Fatalf("record tasks=%v, want [child]", recs[0].TaskIDs)
	}
	t.Logf("scanRuns=%d childRuns=%d records=%d => 已展开任务重启后继续执行，去重仍只算一次",
		scanRuns.Load(), childRuns.Load(), len(recs))
}

// TestExpandedRetryBudgetSurvivesRestart 展开任务失败后退避等待中重启：
// RETRY_WAIT、已用次数、总预算与原预定重试时刻原样保留；
// 重新绑定定义并重触发后，接着原计划等待、不清零、不立即重试。
func TestExpandedRetryBudgetSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day = "2026-10-01"
	const backoff = 2 * time.Second

	var attempts atomic.Int32
	childRun := func(context.Context, *TaskContext) error {
		n := attempts.Add(1)
		t.Logf("child execution #%d at %s", n, time.Now().Format(time.RFC3339Nano))
		if n < 3 {
			return errors.New("child transient failure")
		}
		return nil
	}

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			return tc.Emit(TaskSpec{
				ID:         "child",
				MaxRetries: 3,
				Backoff:    backoff,
				Run:        childRun,
			})
		},
	}); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	triggerErr := make(chan error, 1)
	go func() { triggerErr <- e1.Trigger(context.Background(), day) }()
	deadline := time.After(2 * time.Second)
	for {
		if c, ok := e1.Instance("child", day); ok && c.Status == StatusRetryWait {
			break
		}
		select {
		case <-deadline:
			t.Fatal("child never entered RETRY_WAIT")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	c1, _ := e1.Instance("child", day)
	planned := c1.NextAttemptAt
	if c1.Attempts != 1 || c1.MaxAttempts != 4 || planned.IsZero() {
		t.Fatalf("unexpected pre-crash retry state: %+v", c1)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("close e1: %v", err)
	}
	// 注：e1 的 Trigger goroutine 仍留在 wait 循环（wait 不监听关闭信号，
	// 与既有重启测试一致）；其 RETRY_WAIT 等待已按落盘状态交还。

	// 重启：展开任务仍是 RETRY_WAIT，预算与计划不丢。
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	t.Cleanup(func() { _ = e2.Close() })
	c2, _ := e2.Instance("child", day)
	if c2.Status != StatusRetryWait {
		t.Fatalf("child=%s, want RETRY_WAIT after restart (not %s)", c2.Status, StatusInterrupted)
	}
	if c2.Attempts != 1 || c2.MaxAttempts != 4 || !c2.NextAttemptAt.Equal(planned) {
		t.Fatalf("budget/plan reset: attempts=%d/%d next=%s, want 1/4 @%s",
			c2.Attempts, c2.MaxAttempts, c2.NextAttemptAt, planned)
	}

	if err := e2.Submit(
		TaskSpec{ID: "scan", Run: func(context.Context, *TaskContext) error { return nil }},
		TaskSpec{ID: "child", MaxRetries: 3, Backoff: backoff, Run: childRun},
	); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	start := time.Now()
	if err := e2.Trigger(context.Background(), day); err != nil {
		t.Fatalf("retrigger: %v", err)
	}
	elapsed := time.Since(start)
	if attempts.Load() != 3 {
		t.Fatalf("child total executions=%d, want 3 (attempt counter continued)", attempts.Load())
	}
	c3, _ := e2.Instance("child", day)
	if c3.Status != StatusSucceeded {
		t.Fatalf("child=%s, want SUCCEEDED", c3.Status)
	}
	if elapsed < backoff-time.Second {
		t.Fatalf("retry fired %s after restart, want to honor remaining plan ~%s", elapsed, backoff)
	}
	t.Logf("child attempts=%d resumed after %s => 展开任务重试预算跨重启延续", attempts.Load(), elapsed)
}

// TestExpansionMultiDayAndDedupAcrossRestart 多天运行 + 同来源重复产出，
// 在重启后互不干扰、只算一次。
func TestExpansionMultiDayAndDedupAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day1 = "2026-09-30"
	const day2 = "2026-10-01"

	var (
		mu       sync.Mutex
		runs     = map[string]int{}
		scanRuns atomic.Int32
	)
	child := func(_ context.Context, tc *TaskContext) error {
		mu.Lock()
		runs[tc.TaskID+"@"+tc.LogicalKey]++
		mu.Unlock()
		return nil
	}
	scanSpec := func(gate <-chan struct{}) TaskSpec {
		return TaskSpec{
			ID: "scan",
			Run: func(ctx context.Context, tc *TaskContext) error {
				scanRuns.Add(1)
				batch := []TaskSpec{{
					ID:  "dyn-" + tc.LogicalKey,
					Run: child,
				}}
				if err := tc.EmitBatch("same-batch", batch); err != nil {
					return err
				}
				// 同来源、同批次重复上报。
				if err := tc.EmitBatch("same-batch", batch); err != nil {
					return err
				}
				if gate != nil {
					<-gate
				}
				return nil
			},
		}
	}

	// day1：展开完成、上游尚未成功时崩溃。
	gate1 := make(chan struct{})
	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(scanSpec(gate1)); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	triggerErr1 := make(chan error, 1)
	go func() { triggerErr1 <- e1.Trigger(context.Background(), day1) }()
	deadline := time.After(2 * time.Second)
	for {
		s, sok := e1.Instance("scan", day1)
		c, cok := e1.Instance("dyn-"+day1, day1)
		if sok && cok && s.Status == StatusRunning && c.Status == StatusPending {
			break
		}
		select {
		case <-deadline:
			t.Fatal("day1 never reached scan RUNNING/child PENDING")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	t.Cleanup(func() { _ = e2.Close() })

	// day1 重启后重跑：上游再次产出同一批（命中旧台账），已展开任务继续跑完。
	if err := e2.Submit(scanSpec(nil)); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	if err := e2.Trigger(context.Background(), day1); err != nil {
		t.Fatalf("retrigger day1 after restart: %v", err)
	}
	// day2 在新进程里全新运行。
	if err := e2.Trigger(context.Background(), day2); err != nil {
		t.Fatalf("trigger day2 after restart: %v", err)
	}

	// 每天只能看到自己那天的静态+展开实例。
	i1 := e2.Instances(day1)
	i2 := e2.Instances(day2)
	if len(i1) != 2 || len(i2) != 2 {
		t.Fatalf("instances day1=%d day2=%d, want 2 each", len(i1), len(i2))
	}
	if _, ok := e2.Instance("dyn-"+day1, day2); ok {
		t.Fatal("day1 expansion leaked into day2")
	}
	if _, ok := e2.Instance("dyn-"+day2, day1); ok {
		t.Fatal("day2 expansion leaked into day1")
	}
	// 展开台账按日期隔离；同来源重复产出（跨重启 + 同次运行重复报）各只算一次。
	r1 := e2.Expansions(day1)
	r2 := e2.Expansions(day2)
	if len(r1) != 1 || len(r2) != 1 {
		t.Fatalf("expansion records day1=%d day2=%d, want 1 each", len(r1), len(r2))
	}
	if r1[0].LogicalKey != day1 || fmt.Sprint(r1[0].TaskIDs) != "[dyn-"+day1+"]" {
		t.Fatalf("day1 record wrong scope/tasks: %+v", r1[0])
	}
	if r2[0].LogicalKey != day2 || fmt.Sprint(r2[0].TaskIDs) != "[dyn-"+day2+"]" {
		t.Fatalf("day2 record wrong scope/tasks: %+v", r2[0])
	}
	mu.Lock()
	defer mu.Unlock()
	if runs["dyn-"+day1+"@"+day1] != 1 || runs["dyn-"+day2+"@"+day2] != 1 {
		t.Fatalf("child run counts=%v, want each exactly 1", runs)
	}
	if n := scanRuns.Load(); n != 3 { // day1 崩溃前 1 + 重跑 1 + day2 1
		t.Fatalf("scan ran %d times, want 3", n)
	}
	t.Logf("runs=%v scanRuns=%d => 多天隔离、重复产出跨重启只算一次", runs, scanRuns.Load())
}

// TestExpansionUpstreamFailsAfterRestart 重启后上游重跑最终失败：
// 已展开任务保持先后关系，被明确标记为 BLOCKED 且点名上游，不抢跑、不挂起。
func TestExpansionUpstreamFailsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day = "2026-10-01"
	gate1 := make(chan struct{})
	var childRan atomic.Bool

	scanSpec := func(gate <-chan struct{}, fail bool) TaskSpec {
		return TaskSpec{
			ID: "scan",
			Run: func(ctx context.Context, tc *TaskContext) error {
				if err := tc.Emit(TaskSpec{
					ID: "child",
					Run: func(context.Context, *TaskContext) error {
						childRan.Store(true)
						return nil
					},
				}); err != nil {
					return err
				}
				if gate != nil {
					<-gate
				}
				if fail {
					return errors.New("scan ultimately failed")
				}
				return nil
			},
		}
	}

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(scanSpec(gate1, false)); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	triggerErr1 := make(chan error, 1)
	go func() { triggerErr1 <- e1.Trigger(context.Background(), day) }()
	deadline := time.After(2 * time.Second)
	for {
		s, sok := e1.Instance("scan", day)
		c, cok := e1.Instance("child", day)
		if sok && cok && s.Status == StatusRunning && c.Status == StatusPending {
			break
		}
		select {
		case <-deadline:
			t.Fatal("never reached pre-crash state")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	t.Cleanup(func() { _ = e2.Close() })
	if err := e2.Submit(scanSpec(nil, true)); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	if err := e2.Trigger(context.Background(), day); !errors.Is(err, ErrUpstreamFailed) {
		t.Fatalf("trigger err=%v, want ErrUpstreamFailed", err)
	}
	s, _ := e2.Instance("scan", day)
	c, _ := e2.Instance("child", day)
	if s.Status != StatusFailed {
		t.Fatalf("scan=%s, want FAILED", s.Status)
	}
	if c.Status != StatusBlocked || !strings.Contains(c.LastError, ErrUpstreamFailed.Error()) ||
		!strings.Contains(c.LastError, "scan@"+day) {
		t.Fatalf("child=%s(%q), want BLOCKED naming scan@%s", c.Status, c.LastError, day)
	}
	if childRan.Load() {
		t.Fatal("child must not run when upstream ultimately fails")
	}
	t.Logf("scan=%s child=%s(%q) => 重启后上游失败，展开任务归属仍明确",
		s.Status, c.Status, c.LastError)
}

// TestExpandedDefinitionRebindViaSubmit 上游已成功、展开任务尚未执行时重启
// （例如崩溃瞬间 child 刚 PENDING）：重启后上游保持 SUCCEEDED 不重跑，
// 应用通过 Submit 用同一 ID 重新提供展开任务定义（重新绑定），child 即可继续执行。
func TestExpandedDefinitionRebindViaSubmit(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day = "2026-10-01"
	proceed := make(chan struct{})
	var scanRuns, childRuns atomic.Int32

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			scanRuns.Add(1)
			if err := tc.Emit(TaskSpec{
				ID: "child",
				Run: func(context.Context, *TaskContext) error {
					childRuns.Add(1)
					return nil
				},
			}); err != nil {
				return err
			}
			<-proceed // 让 child 停在 PENDING（scan 尚未 SUCCEEDED，child 不会抢跑）
			return nil
		},
	}); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	go func() { _ = e1.Trigger(context.Background(), day) }()
	deadline := time.After(2 * time.Second)
	for {
		c, ok := e1.Instance("child", day)
		if ok && c.Status == StatusPending {
			break
		}
		select {
		case <-deadline:
			t.Fatal("child never reached PENDING")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// 崩溃后重启：scan 会被标记 INTERRUPTED，child 被恢复级联标记 BLOCKED。
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	t.Cleanup(func() { _ = e2.Close() })

	// 应用在新进程同时重新提供上游与展开任务定义；只重新 Submit（重绑定），
	// 上游重跑成功后 child 必须真正执行。
	var scanRuns2, childRuns2 atomic.Int32
	if err := e2.Submit(
		TaskSpec{ID: "scan", Run: func(ctx context.Context, tc *TaskContext) error {
			scanRuns2.Add(1)
			return nil // 重跑不再 Emit：定义改由 Submit 提供，台账不新增也能跑
		}},
		TaskSpec{ID: "child", Deps: []string{"scan"}, Run: func(context.Context, *TaskContext) error {
			childRuns2.Add(1)
			return nil
		}},
	); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	if err := e2.Trigger(context.Background(), day); err != nil {
		t.Fatalf("retrigger: %v", err)
	}
	if scanRuns.Load() != 1 || scanRuns2.Load() != 1 {
		t.Fatalf("scan runs: pre=%d post=%d, want 1/1", scanRuns.Load(), scanRuns2.Load())
	}
	if childRuns.Load() != 0 || childRuns2.Load() != 1 {
		t.Fatalf("child runs: pre-crash=%d post-restart=%d, want 0/1",
			childRuns.Load(), childRuns2.Load())
	}
	c, _ := e2.Instance("child", day)
	if c.Status != StatusSucceeded {
		t.Fatalf("child=%s, want SUCCEEDED after Submit rebind", c.Status)
	}
	// 重新绑定不应把动态任务变成静态任务：另一天触发不能凭空实例化 child。
	if err := e2.Trigger(context.Background(), "2026-10-02"); err != nil {
		t.Fatalf("trigger other day: %v", err)
	}
	if _, ok := e2.Instance("child", "2026-10-02"); ok {
		t.Fatal("rebound dynamic task leaked as static instance into another day")
	}
	t.Log("child SUCCEEDED via Submit rebind; scan idempotent; dynamic scope preserved")
}

// TestExpandedUnboundDefinitionFailsClearly 重启后既不重新 Submit 展开定义、
// 上游重跑也不再次产出：重触发时已展开实例得到明确失败（点名未重新注册），
// 不会被误判成“未知任务”后无人接管，也不会挂死 wait。
func TestExpandedUnboundDefinitionFailsClearly(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day = "2026-10-01"
	proceed := make(chan struct{})

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			if err := tc.Emit(TaskSpec{
				ID:  "child",
				Run: func(context.Context, *TaskContext) error { return nil },
			}); err != nil {
				return err
			}
			<-proceed
			return nil
		},
	}); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	go func() { _ = e1.Trigger(context.Background(), day) }()
	deadline := time.After(2 * time.Second)
	for {
		if c, ok := e1.Instance("child", day); ok && c.Status == StatusPending {
			break
		}
		select {
		case <-deadline:
			t.Fatal("child never reached PENDING")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	t.Cleanup(func() { _ = e2.Close() })

	// 只重新提供上游，且上游重跑不再产出 child；不提交 child 定义。
	if err := e2.Submit(TaskSpec{ID: "scan", Run: func(context.Context, *TaskContext) error {
		return nil
	}}); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	if err := e2.Trigger(context.Background(), day); err == nil {
		t.Fatal("trigger with unbound dynamic child should surface failure")
	}
	c, _ := e2.Instance("child", day)
	if c.Status != StatusFailed ||
		!strings.Contains(c.LastError, "not re-registered after restart") {
		t.Fatalf("child=%s(%q), want FAILED naming un-re-registered dynamic definition",
			c.Status, c.LastError)
	}
	s, _ := e2.Instance("scan", day)
	if s.Status != StatusSucceeded {
		t.Fatalf("scan=%s, want SUCCEEDED", s.Status)
	}
	t.Logf("child=%s(%q) => 未绑定定义的展开任务明确失败、不挂死", c.Status, c.LastError)
}
