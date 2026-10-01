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

// waitForStatus 轮询直到指定实例进入目标状态或超时（测试辅助）。
func waitForStatus(t *testing.T, e *Engine, taskID, key string, want Status) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if inst, ok := e.Instance(taskID, key); ok && inst.Status == want {
			return
		}
		select {
		case <-deadline:
			inst, ok := e.Instance(taskID, key)
			t.Fatalf("instance %s@%s status=%v ok=%v, want %s", taskID, key, inst, ok, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestEmittedTasksResumeAfterRestart 重启前已展开但还没执行的任务：
// 重启后同一天重跑（上游被打断后重跑、又把同一批任务报了一遍，命中台账去重），
// 已展开的任务不能被直接判 FAILED 永久死掉，也不能消失，
// 要继续按正常顺序执行到最终结果，且只执行一次、台账只算一次。
func TestEmittedTasksResumeAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day = "2026-10-01"

	var scanRuns, p1Runs, p2Runs atomic.Int32
	holdScan := make(chan struct{})

	scanSpec := func() TaskSpec {
		return TaskSpec{
			ID: "scan",
			Run: func(ctx context.Context, tc *TaskContext) error {
				n := scanRuns.Add(1)
				batch := []TaskSpec{
					{ID: "p1", Run: func(context.Context, *TaskContext) error { p1Runs.Add(1); return nil }},
					{ID: "p2", Run: func(context.Context, *TaskContext) error { p2Runs.Add(1); return nil }},
				}
				// 每次执行都重复上报同一批（模拟“上游重跑成功、又把同一批任务报一遍”）。
				if err := tc.EmitBatch("batch-1", batch); err != nil {
					return err
				}
				if err := tc.EmitBatch("batch-1", batch); err != nil {
					return err
				}
				t.Logf("scan attempt %d emitted p1/p2", n)
				if n == 1 {
					<-holdScan // 第一次执行停在 RUNNING，随后模拟进程崩溃
					return errors.New("scan interrupted by crash")
				}
				return nil
			},
		}
	}

	// ---- 进程 1：上游已展开出 p1/p2（PENDING），但上游还没跑完就崩溃 ----
	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(scanSpec()); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	triggerErr := make(chan error, 1)
	go func() { triggerErr <- e1.Trigger(context.Background(), day) }()

	waitForStatus(t, e1, "p1", day, StatusPending)
	waitForStatus(t, e1, "p2", day, StatusPending)
	scan, _ := e1.Instance("scan", day)
	if scan.Status != StatusRunning {
		t.Fatalf("scan=%s, want RUNNING before crash", scan.Status)
	}
	t.Logf("crash: scan RUNNING, p1/p2 emitted and PENDING")

	// ---- 重启 ----
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	close(holdScan) // 放行旧进程的 scan，避免 goroutine 泄漏
	<-triggerErr

	// 恢复后：上游 INTERRUPTED；展开任务被级联 BLOCKED 并点名上游（失败归属清楚，
	// 不挂起、不消失，也不是旧实现那种 ErrUnknownTask 的永久 FAILED）。
	scan, _ = e2.Instance("scan", day)
	p1, _ := e2.Instance("p1", day)
	p2, _ := e2.Instance("p2", day)
	if scan.Status != StatusInterrupted {
		t.Fatalf("scan=%s, want INTERRUPTED", scan.Status)
	}
	if p1.Status != StatusBlocked || p2.Status != StatusBlocked {
		t.Fatalf("after restart p1=%s p2=%s, want both BLOCKED by interrupted upstream (not vanished)",
			p1.Status, p2.Status)
	}
	if !strings.Contains(p1.LastError, "scan@"+day) {
		t.Fatalf("p1 reason=%q must name scan@%s", p1.LastError, day)
	}
	t.Logf("recovered: scan=%s p1=%s p2=%s (归属上游，等待重跑复活)", scan.Status, p1.Status, p2.Status)

	// ---- 同一天重跑：只重新提交 scan 定义，动态任务定义要由上游重新产出 ----
	if err := e2.Submit(scanSpec()); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	if err := e2.Trigger(context.Background(), day); err != nil {
		t.Fatalf("re-trigger after restart: %v", err)
	}

	// 展开任务按正常顺序跑到 SUCCEEDED，各执行一次（不重复、不消失）。
	p1, _ = e2.Instance("p1", day)
	p2, _ = e2.Instance("p2", day)
	scan, _ = e2.Instance("scan", day)
	if scan.Status != StatusSucceeded || p1.Status != StatusSucceeded || p2.Status != StatusSucceeded {
		t.Fatalf("final scan=%s p1=%s p2=%s, want all SUCCEEDED",
			scan.Status, p1.Status, p2.Status)
	}
	if p1Runs.Load() != 1 || p2Runs.Load() != 1 {
		t.Fatalf("runs p1=%d p2=%d, want exactly 1 each", p1Runs.Load(), p2Runs.Load())
	}
	if scanRuns.Load() != 2 {
		t.Fatalf("scan ran %d times, want 2 (interrupted then re-run)", scanRuns.Load())
	}
	// 来源追溯保留：p1/p2 仍能一眼看出来自 scan@day，台账同来源同批次只算一次。
	if p1.Origin.EmitterKey != "scan@"+day || p1.Origin.Attempt != 1 || p1.Origin.BatchID != "batch-1" {
		t.Fatalf("p1 origin=%+v, want scan@%s attempt=1 batch-1", p1.Origin, day)
	}
	recs := e2.Expansions(day)
	if len(recs) != 1 {
		t.Fatalf("expansion records=%d, want 1 (same source+batch counted once across restart)", len(recs))
	}
	if fmt.Sprint(recs[0].TaskIDs) != "[p1 p2]" {
		t.Fatalf("record tasks=%v, want [p1 p2]", recs[0].TaskIDs)
	}
	if err := e2.Close(); err != nil {
		t.Fatalf("close e2: %v", err)
	}
	t.Logf("after resume: scan runs=%d p1 runs=%d p2 runs=%d records=%d => 展开任务跨重启续跑到成功",
		scanRuns.Load(), p1Runs.Load(), p2Runs.Load(), len(recs))
}

// TestEmittedTaskBlockedByInterruptedUpstreamAfterRestart 重启后若不重跑，
// 上游 INTERRUPTED 与展开任务之间的失败归属也要清楚：已展开的下游级联
// BLOCKED 且点名上游，而不是卡在 PENDING/挂起。
func TestEmittedTaskBlockedByInterruptedUpstreamAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day = "2026-10-02"
	holdScan := make(chan struct{})

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			if err := tc.Emit(TaskSpec{
				ID:  "p1",
				Run: func(context.Context, *TaskContext) error { return nil },
			}); err != nil {
				return err
			}
			<-holdScan
			return nil
		},
	}); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	triggerErr := make(chan error, 1)
	go func() { triggerErr <- e1.Trigger(context.Background(), day) }()
	waitForStatus(t, e1, "p1", day, StatusPending)
	t.Log("crash with scan RUNNING and p1 PENDING")

	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	defer func() { _ = e2.Close() }()
	close(holdScan)
	<-triggerErr

	p1, _ := e2.Instance("p1", day)
	if p1.Status != StatusBlocked {
		t.Fatalf("p1=%s after restart, want BLOCKED (named after interrupted upstream), not hung",
			p1.Status)
	}
	if !strings.Contains(p1.LastError, ErrUpstreamFailed.Error()) ||
		!strings.Contains(p1.LastError, "scan@"+day) {
		t.Fatalf("p1 reason=%q, want it to name %s and scan@%s",
			p1.LastError, ErrUpstreamFailed, day)
	}
	if p1.Origin.EmitterKey != "scan@"+day {
		t.Fatalf("p1 origin=%+v, want scan@%s", p1.Origin, day)
	}
	t.Logf("p1=%s(%q) => 重启后被打断上游的失败归属明确", p1.Status, p1.LastError)
}

// TestEmittedRetryBudgetSurvivesRestart 展开出来的任务失败后正在等下一次重试时
// 重启：剩余重试次数与原定重试时刻要和普通任务一样保留——
// 重启后接着原计划等，不能一重启就清零、立刻重试或判死。
func TestEmittedRetryBudgetSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const day = "2026-10-03"
	const backoff = 2 * time.Second

	var wAttempts atomic.Int32
	workSpec := func() TaskSpec {
		return TaskSpec{
			ID: "work", MaxRetries: 3, Backoff: backoff,
			Run: func(context.Context, *TaskContext) error {
				n := wAttempts.Add(1)
				t.Logf("work execution #%d at %s", n, time.Now().Format(time.RFC3339Nano))
				if n < 3 {
					return errBoom
				}
				return nil
			},
		}
	}
	scanSpec := func() TaskSpec {
		return TaskSpec{
			ID: "scan",
			Run: func(ctx context.Context, tc *TaskContext) error {
				return tc.Emit(workSpec())
			},
		}
	}

	// ---- 进程 1：scan 成功展开 work；work 首次失败进入 RETRY_WAIT 时崩溃 ----
	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(scanSpec()); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	triggerErr := make(chan error, 1)
	go func() { triggerErr <- e1.Trigger(context.Background(), day) }()

	waitForStatus(t, e1, "work", day, StatusRetryWait)
	scan, _ := e1.Instance("scan", day)
	if scan.Status != StatusSucceeded {
		t.Fatalf("scan=%s, want SUCCEEDED (emitter finished before crash)", scan.Status)
	}
	w1, _ := e1.Instance("work", day)
	if w1.Attempts != 1 || w1.MaxAttempts != 4 || w1.NextAttemptAt.IsZero() {
		t.Fatalf("unexpected retry state: %+v", w1)
	}
	planned := w1.NextAttemptAt
	t.Logf("crash during emitted backoff: attempts=%d/%d next=%s origin=%s",
		w1.Attempts, w1.MaxAttempts, planned, w1.Origin.EmitterKey)

	if err := e1.Close(); err != nil {
		t.Fatalf("close e1: %v", err)
	}
	// 注意：Close 只交还退避等待（RETRY_WAIT 已落盘），旧触发的 wait 不再推进，
	// 这与进程退出等价；不等待旧触发返回。

	// ---- 重启：状态必须原样保留（不清零、不 INTERRUPTED、不判死）----
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	defer func() { _ = e2.Close() }()
	w2, _ := e2.Instance("work", day)
	if w2.Status != StatusRetryWait {
		t.Fatalf("recovered status=%s, want RETRY_WAIT (not %s/FAILED)", w2.Status, StatusInterrupted)
	}
	if w2.Attempts != 1 || w2.MaxAttempts != 4 {
		t.Fatalf("recovered budget attempts=%d max=%d, want 1/4 (not reset)",
			w2.Attempts, w2.MaxAttempts)
	}
	if !w2.NextAttemptAt.Equal(planned) {
		t.Fatalf("recovered next=%s, want original plan %s", w2.NextAttemptAt, planned)
	}

	// 重新提交定义（含展开任务自己的定义——闭包无法落盘，由应用在重启后补回）
	// 并触发接管等待。
	if err := e2.Submit(scanSpec(), workSpec()); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	start := time.Now()
	if err := e2.Trigger(context.Background(), day); err != nil {
		t.Fatalf("re-trigger after restart: %v", err)
	}
	elapsed := time.Since(start)

	if wAttempts.Load() != 3 {
		t.Fatalf("work total executions=%d, want 3 (budget continued, not reset)", wAttempts.Load())
	}
	final, _ := e2.Instance("work", day)
	if final.Status != StatusSucceeded {
		t.Fatalf("final status=%s, want SUCCEEDED", final.Status)
	}
	if final.Attempts != 3 {
		t.Fatalf("final attempts=%d, want 3", final.Attempts)
	}
	// 第 2 次执行应等到原计划时刻附近，不能重启后立刻重试。
	if elapsed < backoff-time.Second {
		t.Fatalf("retry fired after %s, want to honor remaining plan ~%s (not immediate)",
			elapsed, backoff)
	}
	t.Logf("emitted task retry #2 after %s of backoff; final=%s attempts=%d => 跨重启重试预算与计划延续",
		elapsed, final.Status, final.Attempts)
}

// TestExpansionScopeAndIdempotencyAcrossRestart 多天运行 + 同来源重复产出，
// 在重启后互不干扰、只算一次：
//   - 每个业务日期只能看到自己那天的展开结果（实例与台账均隔离）；
//   - 重启后即使把动态任务定义重新 Submit，新日期也不会把它当根任务自动实例化，
//     只由当天上游自己产出；
//   - 同一天重跑、同一来源重复报同一批任务，台账仍只算一条、任务只执行一次。
func TestExpansionScopeAndIdempotencyAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"

	var mu sync.Mutex
	runs := map[string]int{}
	work := func() TaskSpec {
		return TaskSpec{
			ID: "work",
			Run: func(ctx context.Context, c *TaskContext) error {
				mu.Lock()
				runs[c.TaskID+"@"+c.LogicalKey]++
				mu.Unlock()
				return nil
			},
		}
	}
	scan := func() TaskSpec {
		return TaskSpec{
			ID: "scan",
			Run: func(ctx context.Context, tc *TaskContext) error {
				batch := []TaskSpec{work()}
				// 同一来源重复报两次同一批：只能算一次。
				if err := tc.EmitBatch("daily", batch); err != nil {
					return err
				}
				return tc.EmitBatch("daily", batch)
			},
		}
	}

	// ---- 进程 1：跑 day1 ----
	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(scan()); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	const day1 = "2026-09-30"
	if err := e1.Trigger(context.Background(), day1); err != nil {
		t.Fatalf("trigger day1: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("close e1: %v", err)
	}

	// ---- 进程 2：重启后跑 day2，并再次提交含动态 work 在内的定义 ----
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	defer func() { _ = e2.Close() }()
	if err := e2.Submit(scan(), work()); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	const day2 = "2026-10-01"
	if err := e2.Trigger(context.Background(), day2); err != nil {
		t.Fatalf("trigger day2: %v", err)
	}

	// day1 的结果原样保留；day2 只有自己的 scan + work，不冒出别的日期的展开。
	i1 := e2.Instances(day1)
	i2 := e2.Instances(day2)
	if len(i1) != 2 || len(i2) != 2 {
		t.Fatalf("day1=%d day2=%d instances, want 2 each (per-day scoping across restart)",
			len(i1), len(i2))
	}
	if _, ok := e2.Instance("work", day1); !ok {
		t.Fatal("day1 work instance lost across restart")
	}
	for _, rec := range e2.Expansions(day1) {
		if rec.LogicalKey != day1 {
			t.Fatalf("day1 record leaked key %q", rec.LogicalKey)
		}
	}
	if len(e2.Expansions(day1)) != 1 || len(e2.Expansions(day2)) != 1 {
		t.Fatalf("records day1=%d day2=%d, want 1 each",
			len(e2.Expansions(day1)), len(e2.Expansions(day2)))
	}

	// 全新日期 day3：即便 work 定义已被 Submit，也不得被当根任务自动实例化，
	// 只出现当天 scan 自己产出的 work。
	const day3 = "2026-10-02"
	if err := e2.Trigger(context.Background(), day3); err != nil {
		t.Fatalf("trigger day3: %v", err)
	}
	i3 := e2.Instances(day3)
	if len(i3) != 2 {
		t.Fatalf("day3=%d instances, want 2 (scan + own emitted work), dynamic def must not auto-root",
			len(i3))
	}

	// 同一天重复产出：每个 work 只执行一次。
	for _, d := range []string{day1, day2, day3} {
		if runs["work@"+d] != 1 {
			t.Fatalf("work@%s ran %d times, want exactly 1", d, runs["work@"+d])
		}
	}
	// 三天各自只有一条台账记录（同来源重复上报 + 跨重玩幂等）。
	for _, d := range []string{day1, day2, day3} {
		if len(e2.Expansions(d)) != 1 {
			t.Fatalf("records for %s=%d, want 1", d, len(e2.Expansions(d)))
		}
	}
	t.Logf("runs=%v => 多天隔离、动态定义不越界生根、重复产出只算一次（跨重启）", runs)
}
