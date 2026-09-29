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

// mkEmitted 构造一个展开任务的 spec（执行体可区分）。
func mkEmitted(id string, ran *atomic.Int32, order *[]string, mu *sync.Mutex) TaskSpec {
	return TaskSpec{
		ID: id,
		Run: func(context.Context, *TaskContext) error {
			if ran != nil {
				ran.Add(1)
			}
			if order != nil {
				mu.Lock()
				*order = append(*order, id)
				mu.Unlock()
			}
			return nil
		},
	}
}

// TestExpansionOriginAndIdempotency 来源追溯 + 同来源重复上报的幂等：
// 事后能查到展开了哪些任务、分别由哪条上游的哪次执行哪批产出带出；
// 同一来源重复报同一批按一次算，任务只登记/执行一次。
func TestExpansionOriginAndIdempotency(t *testing.T) {
	e := newEngine(t)
	var runs atomic.Int32

	if err := e.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			batch := []TaskSpec{
				mkEmitted("p-a", &runs, nil, nil),
				mkEmitted("p-b", &runs, nil, nil),
			}
			// 用显式批次号上报；同一来源、同一批次重复上报两次。
			if err := tc.EmitBatch("batch-1", batch); err != nil {
				return err
			}
			return tc.EmitBatch("batch-1", batch)
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := e.Trigger(context.Background(), "2026-09-30"); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	if runs.Load() != 2 {
		t.Fatalf("emitted tasks ran %d times, want exactly 2", runs.Load())
	}
	recs := e.Expansions("2026-09-30")
	if len(recs) != 1 {
		t.Fatalf("expansion records=%d, want 1 (same source+batch counted once)", len(recs))
	}
	rec := recs[0]
	if rec.EmitterKey != "scan@2026-09-30" || rec.Attempt != 1 || rec.BatchID != "batch-1" {
		t.Fatalf("bad origin: %+v", rec)
	}
	if fmt.Sprint(rec.TaskIDs) != "[p-a p-b]" {
		t.Fatalf("expanded task ids=%v, want [p-a p-b]", rec.TaskIDs)
	}
	for _, id := range []string{"p-a", "p-b"} {
		inst, ok := e.Instance(id, "2026-09-30")
		if !ok {
			t.Fatalf("instance %s missing", id)
		}
		if inst.Origin.EmitterKey != "scan@2026-09-30" ||
			inst.Origin.Attempt != 1 || inst.Origin.BatchID != "batch-1" {
			t.Fatalf("instance %s origin=%+v, want scan attempt=1 batch-1", id, inst.Origin)
		}
	}
	// 静态任务没有来源（零值）。
	if scan, _ := e.Instance("scan", "2026-09-30"); scan.Origin.EmitterKey != "" {
		t.Fatalf("static task must have empty origin, got %+v", scan.Origin)
	}
	t.Logf("origin=%+v runs=%d => 来源可追溯、重复展开幂等", rec, runs.Load())
}

// TestExpansionScopedByLogicalKey 展开作用范围按逻辑键隔离：
// 每次运行只看到自己这次展开的任务，别的业务日期不会冒出；
// 后触发的日期不会把前一日期动态注册的任务自动实例化。
func TestExpansionScopedByLogicalKey(t *testing.T) {
	e := newEngine(t)
	var mu sync.Mutex
	runs := map[string]int{}

	if err := e.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			return tc.Emit(TaskSpec{
				ID: "dyn-" + tc.LogicalKey,
				Run: func(ctx context.Context, c *TaskContext) error {
					mu.Lock()
					runs[c.TaskID+"@"+c.LogicalKey]++
					mu.Unlock()
					return nil
				},
			})
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	for _, day := range []string{"2026-09-29", "2026-09-30"} {
		if err := e.Trigger(context.Background(), day); err != nil {
			t.Fatalf("trigger %s: %v", day, err)
		}
	}

	// 每个日期只看到自己的静态 + 动态任务。
	day1 := e.Instances("2026-09-29")
	day2 := e.Instances("2026-09-30")
	if len(day1) != 2 || len(day2) != 2 {
		t.Fatalf("day1=%d day2=%d instances, want 2 each", len(day1), len(day2))
	}
	if _, ok := e.Instance("dyn-2026-09-29", "2026-09-30"); ok {
		t.Fatal("day1 expansion leaked into day2 key space")
	}
	if _, ok := e.Instance("dyn-2026-09-30", "2026-09-29"); ok {
		t.Fatal("day2 expansion leaked into day1 key space")
	}
	// 展开台账同样按日期隔离。
	if len(e.Expansions("2026-09-29")) != 1 || len(e.Expansions("2026-09-30")) != 1 {
		t.Fatalf("expansion records not scoped per day: %d/%d",
			len(e.Expansions("2026-09-29")), len(e.Expansions("2026-09-30")))
	}
	// 补跑一个全新日期：只应看到该日自己的展开结果，不冒出历史动态任务。
	if err := e.Backfill(context.Background(), []string{"2026-09-28"}); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	day0 := e.Instances("2026-09-28")
	if len(day0) != 2 {
		t.Fatalf("backfilled day instances=%d, want 2 (scan + own dynamic)", len(day0))
	}
	for k, n := range runs {
		if n != 1 {
			t.Fatalf("%s ran %d times, want 1", k, n)
		}
	}
	t.Logf("runs=%v => 不同日期展开互不干扰", runs)
}

// TestEmittedTaskOrderingAndUpstreamFailure 展开任务天然排在上游成功之后；
// 上游最终失败时，已展开的任务标记 BLOCKED（明确是被上游拖垮），不挂起、不消失。
func TestEmittedTaskOrderingAndUpstreamFailure(t *testing.T) {
	e := newEngine(t)
	var mu sync.Mutex
	var order []string
	var childRan atomic.Bool
	var failScan = make(chan struct{})
	var allowScanFail = make(chan struct{})

	err := e.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			// 先产出后续任务（不在 Deps 里手工声明先后）。
			if err := tc.Emit(TaskSpec{
				ID: "child",
				Run: func(context.Context, *TaskContext) error {
					childRan.Store(true)
					mu.Lock()
					order = append(order, "child")
					mu.Unlock()
					return nil
				},
			}); err != nil {
				return err
			}
			<-failScan
			<-allowScanFail
			return errors.New("scan ultimately failed")
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	triggerErr := make(chan error, 1)
	go func() { triggerErr <- e.Trigger(context.Background(), "k") }()

	// 等 child 实例被展开且处于 PENDING；确认它不会在上游未成功前执行。
	deadline := time.After(2 * time.Second)
	for {
		if c, ok := e.Instance("child", "k"); ok && c.Status == StatusPending {
			break
		}
		select {
		case <-deadline:
			t.Fatal("child never emitted as PENDING")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if childRan.Load() {
		t.Fatal("emitted child ran before upstream succeeded")
	}

	// 上游失败：已展开的 child 必须得到 BLOCKED 的明确交代。
	close(failScan)
	close(allowScanFail)
	err = <-triggerErr
	if !errors.Is(err, ErrUpstreamFailed) {
		t.Fatalf("trigger err=%v, want ErrUpstreamFailed", err)
	}
	scan, _ := e.Instance("scan", "k")
	child, _ := e.Instance("child", "k")
	if scan.Status != StatusFailed {
		t.Fatalf("scan=%s, want FAILED", scan.Status)
	}
	if child.Status != StatusBlocked {
		t.Fatalf("child=%s, want BLOCKED (cascaded from upstream), not hung/vanished", child.Status)
	}
	if !strings.Contains(child.LastError, ErrUpstreamFailed.Error()) ||
		!strings.Contains(child.LastError, "scan@k") {
		t.Fatalf("child reason=%q, want it to name ErrUpstreamFailed and upstream scan@k", child.LastError)
	}
	if child.Origin.EmitterKey != "scan@k" {
		t.Fatalf("child origin=%+v, want scan@k", child.Origin)
	}
	if childRan.Load() {
		t.Fatal("child must never run when upstream failed")
	}
	t.Logf("scan=%s child=%s(%q) => 展开任务被上游失败拖垮为 BLOCKED",
		scan.Status, child.Status, child.LastError)
}

// TestEmittedChainFailurePropagates 上游成功后，展开链中某个任务失败，
// 依赖它的后续展开任务级联 BLOCKED，归属原因指明失败任务。
func TestEmittedChainFailurePropagates(t *testing.T) {
	e := newEngine(t)
	err := e.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			if err := tc.Emit(TaskSpec{
				ID:  "bad-child",
				Run: func(context.Context, *TaskContext) error { return errors.New("child boom") },
			}); err != nil {
				return err
			}
			return tc.Emit(TaskSpec{
				ID: "grandchild", Deps: []string{"bad-child"},
				Run: func(context.Context, *TaskContext) error { return nil },
			})
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := e.Trigger(context.Background(), "k"); !errors.Is(err, ErrUpstreamFailed) {
		t.Fatalf("trigger err=%v, want ErrUpstreamFailed", err)
	}
	scan, _ := e.Instance("scan", "k")
	bad, _ := e.Instance("bad-child", "k")
	grand, _ := e.Instance("grandchild", "k")
	if scan.Status != StatusSucceeded {
		t.Fatalf("scan=%s, want SUCCEEDED", scan.Status)
	}
	if bad.Status != StatusFailed {
		t.Fatalf("bad-child=%s, want FAILED", bad.Status)
	}
	if grand.Status != StatusBlocked || !strings.Contains(grand.LastError, "bad-child@k") {
		t.Fatalf("grandchild=%s(%q), want BLOCKED naming bad-child@k", grand.Status, grand.LastError)
	}
	t.Logf("scan=%s bad=%s grand=%s(%q) => 展开链失败级联 BLOCKED 且归属明确",
		scan.Status, bad.Status, grand.Status, grand.LastError)
}
