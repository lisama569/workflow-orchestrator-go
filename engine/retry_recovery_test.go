package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// TestRetryBudgetSurvivesRestart 失败后退避途中重启：
//   - RETRY_WAIT 实例恢复后仍是 RETRY_WAIT（与 INTERRUPTED 的“执行到一半被打断”区分开）；
//   - 已用次数 Attempts、剩余预算 MaxAttempts、原预定时刻 NextAttemptAt 原样保留；
//   - 重触发后不会立即重试，也不清零次数：等到原计划时刻附近才做第 2 次尝试。
func TestRetryBudgetSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	const backoff = 2 * time.Second

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))

	var attempts atomic.Int32
	if err := e1.Submit(TaskSpec{
		ID: "flaky", MaxRetries: 3, Backoff: backoff,
		Run: func(context.Context, *TaskContext) error {
			n := attempts.Add(1)
			t.Logf("flaky execution #%d at %s", n, time.Now().Format(time.RFC3339Nano))
			if n < 3 {
				return errBoom
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	triggerErr := make(chan error, 1)
	go func() { triggerErr <- e1.Trigger(context.Background(), "k") }()

	// 等第一次失败、进入 RETRY_WAIT。
	deadline := time.After(2 * time.Second)
	for {
		if i, _ := e1.Instance("flaky", "k"); i != nil && i.Status == StatusRetryWait {
			break
		}
		select {
		case <-deadline:
			t.Fatal("never entered RETRY_WAIT")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	i1, _ := e1.Instance("flaky", "k")
	if i1.Attempts != 1 || i1.MaxAttempts != 4 || i1.NextAttemptAt.IsZero() {
		t.Fatalf("unexpected retry state: %+v", i1)
	}
	planned := i1.NextAttemptAt
	t.Logf("crash during backoff: attempts=%d/%d next=%s", i1.Attempts, i1.MaxAttempts, planned)

	// 模拟退避途中进程退出：Close 会打断本进程的等待 goroutine，但状态按 RETRY_WAIT 落盘。
	if err := e1.Close(); err != nil {
		t.Fatalf("close e1: %v", err)
	}

	// 重启：RETRY_WAIT 不能被当成“执行到一半”（INTERRUPTED），也不能清零成 PENDING。
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	defer func() { _ = e2.Close() }()
	i2, _ := e2.Instance("flaky", "k")
	if i2.Status != StatusRetryWait {
		t.Fatalf("recovered status=%s, want RETRY_WAIT (budget preserved), not %s",
			i2.Status, StatusInterrupted)
	}
	if i2.Attempts != 1 || i2.MaxAttempts != 4 {
		t.Fatalf("recovered budget attempts=%d max=%d, want 1/4 (not reset)",
			i2.Attempts, i2.MaxAttempts)
	}
	if !i2.NextAttemptAt.Equal(planned) {
		t.Fatalf("recovered next=%s, want original plan %s", i2.NextAttemptAt, planned)
	}

	// 重新提交定义并触发以接管等待；剩余等待从原计划时刻继续，不会立即重试。
	start := time.Now()
	if err := e2.Submit(TaskSpec{
		ID: "flaky", MaxRetries: 3, Backoff: backoff,
		Run: func(context.Context, *TaskContext) error {
			n := attempts.Add(1)
			t.Logf("flaky execution #%d at %s", n, time.Now().Format(time.RFC3339Nano))
			if n < 3 {
				return errBoom
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	if err := e2.Trigger(context.Background(), "k"); err != nil {
		t.Fatalf("retrigger: %v", err)
	}
	elapsed := time.Since(start)

	if attempts.Load() != 3 {
		t.Fatalf("total executions=%d, want 3 (attempt counter continued, not reset)", attempts.Load())
	}
	final, _ := e2.Instance("flaky", "k")
	if final.Status != StatusSucceeded {
		t.Fatalf("final status=%s, want SUCCEEDED", final.Status)
	}
	// 第 2 次执行应发生在原计划时刻附近（跨重启保留了绝大部分等待）。
	if elapsed < backoff-time.Second {
		t.Fatalf("retry fired after %s, want to honor remaining plan ~%s (not immediate)",
			elapsed, backoff)
	}
	t.Logf("retry #2 after %s of backoff; final=%s attempts=%d => 预算与等待计划跨重启延续",
		elapsed, final.Status, attempts.Load())
}
