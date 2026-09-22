package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 打印状态流转与判定依据到测试日志。
func testLogger(t *testing.T) func(string, ...any) {
	t.Helper()
	return func(format string, args ...any) {
		t.Logf("[engine] "+format, args...)
	}
}

func newEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	e.SetLogger(testLogger(t))
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// TestDAGConcurrentOrder 验证依赖顺序与并发执行：
// b、c 依赖 a，d 依赖 b、c；b、c 应并发，d 最后执行。
func TestDAGConcurrentOrder(t *testing.T) {
	e := newEngine(t)
	var mu sync.Mutex
	var order []string
	var bcOverlap atomic.Int32
	var maxOverlap atomic.Int32
	mk := func(id string, deps ...string) TaskSpec {
		return TaskSpec{
			ID: id, Deps: deps,
			Run: func(ctx context.Context, tc *TaskContext) error {
				if id == "b" || id == "c" {
					cur := bcOverlap.Add(1)
					for {
						old := maxOverlap.Load()
						if cur <= old || maxOverlap.CompareAndSwap(old, cur) {
							break
						}
					}
					time.Sleep(50 * time.Millisecond)
					bcOverlap.Add(-1)
				}
				mu.Lock()
				order = append(order, id)
				mu.Unlock()
				t.Logf("task %s finished", id)
				return nil
			},
		}
	}
	if err := e.Submit(mk("a"), mk("b", "a"), mk("c", "a"), mk("d", "b", "c")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := e.Trigger(context.Background(), "2026-09-22"); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if order[0] != "a" || order[3] != "d" {
		t.Fatalf("dependency order violated: %v", order)
	}
	if maxOverlap.Load() < 2 {
		t.Fatalf("b and c did not run concurrently, order=%v", order)
	}
	t.Logf("order=%v maxOverlap=%d => 依赖顺序正确且 b/c 并发", order, maxOverlap.Load())
}

// TestConcurrentTriggerExactlyOnce 并发触发同一逻辑键：任务只能实际执行一次。
func TestConcurrentTriggerExactlyOnce(t *testing.T) {
	e := newEngine(t)
	var runs atomic.Int32
	if err := e.Submit(TaskSpec{
		ID: "once",
		Run: func(ctx context.Context, tc *TaskContext) error {
			runs.Add(1)
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = e.Trigger(context.Background(), "2026-09-22")
		}(i)
	}
	wg.Wait()
	var conflicts int
	for _, err := range errs {
		if err != nil && !errors.Is(err, ErrConflict) {
			t.Fatalf("unexpected error: %v", err)
		}
		if errors.Is(err, ErrConflict) {
			conflicts++
		}
	}
	if runs.Load() != 1 {
		t.Fatalf("task executed %d times, want exactly 1", runs.Load())
	}
	t.Logf("runs=%d conflicts=%d => 并发触发下仅执行一次，其余按冲突拒绝", runs.Load(), conflicts)
}

// TestUpstreamFailedBlocksDownstream 上游失败：下游 BLOCKED，原因可区分。
func TestUpstreamFailedBlocksDownstream(t *testing.T) {
	e := newEngine(t)
	boom := errors.New("boom")
	var downstreamRan atomic.Bool
	err := e.Submit(
		TaskSpec{ID: "bad", Run: func(context.Context, *TaskContext) error { return boom }},
		TaskSpec{ID: "child", Deps: []string{"bad"}, Run: func(context.Context, *TaskContext) error {
			downstreamRan.Store(true)
			return nil
		}},
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	err = e.Trigger(context.Background(), "2026-09-22")
	if !errors.Is(err, ErrUpstreamFailed) {
		t.Fatalf("trigger error = %v, want ErrUpstreamFailed", err)
	}
	bad, _ := e.Instance("bad", "2026-09-22")
	child, _ := e.Instance("child", "2026-09-22")
	t.Logf("bad=%s(%s) child=%s(%s)", bad.Status, bad.LastError, child.Status, child.LastError)
	if bad.Status != StatusFailed || child.Status != StatusBlocked {
		t.Fatalf("bad=%s child=%s, want FAILED/BLOCKED", bad.Status, child.Status)
	}
	if downstreamRan.Load() {
		t.Fatal("downstream must not run when upstream failed")
	}
}

// TestCycleDetected 依赖成环：Submit 返回 ErrCycle，且不会死锁。
func TestCycleDetected(t *testing.T) {
	e := newEngine(t)
	noop := func(context.Context, *TaskContext) error { return nil }
	err := e.Submit(
		TaskSpec{ID: "x", Deps: []string{"z"}, Run: noop},
		TaskSpec{ID: "y", Deps: []string{"x"}, Run: noop},
		TaskSpec{ID: "z", Deps: []string{"y"}, Run: noop},
	)
	if !errors.Is(err, ErrCycle) {
		t.Fatalf("submit error = %v, want ErrCycle", err)
	}
	t.Logf("cycle rejected: %v", err)
}

// TestRetryWithBackoff 失败任务按 MaxRetries 重试后成功。
func TestRetryWithBackoff(t *testing.T) {
	e := newEngine(t)
	var attempts atomic.Int32
	if err := e.Submit(TaskSpec{
		ID: "flaky", MaxRetries: 2, Backoff: 10 * time.Millisecond,
		Run: func(context.Context, *TaskContext) error {
			n := attempts.Add(1)
			if n < 3 {
				return fmt.Errorf("transient failure %d", n)
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := e.Trigger(context.Background(), "k1"); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	inst, _ := e.Instance("flaky", "k1")
	t.Logf("attempts=%d status=%s", inst.Attempts, inst.Status)
	if inst.Attempts != 3 || inst.Status != StatusSucceeded {
		t.Fatalf("attempts=%d status=%s, want 3/SUCCEEDED", inst.Attempts, inst.Status)
	}
}

// TestRetryExhausted 重试耗尽后进入 FAILED 终态。
func TestRetryExhausted(t *testing.T) {
	e := newEngine(t)
	var attempts atomic.Int32
	if err := e.Submit(TaskSpec{
		ID: "always-bad", MaxRetries: 1, Backoff: time.Millisecond,
		Run: func(context.Context, *TaskContext) error {
			attempts.Add(1)
			return errors.New("permanent")
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	err := e.Trigger(context.Background(), "k1")
	if !errors.Is(err, ErrUpstreamFailed) {
		t.Fatalf("trigger error = %v, want ErrUpstreamFailed", err)
	}
	if attempts.Load() != 2 { // 首次 + 1 次重试
		t.Fatalf("attempts=%d, want 2", attempts.Load())
	}
	inst, _ := e.Instance("always-bad", "k1")
	t.Logf("attempts=%d status=%s reason=%s", inst.Attempts, inst.Status, inst.LastError)
	if inst.Status != StatusFailed {
		t.Fatalf("status=%s, want FAILED", inst.Status)
	}
}
