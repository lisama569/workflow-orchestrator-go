package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecoveryAfterRestart 模拟进程重启：
//   - SUCCEEDED 实例不得重复执行；
//   - 崩溃时处于 RUNNING 的实例恢复为 FAILED(ErrInterrupted)，语义确定可解释。
func TestRecoveryAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))

	var doneRuns, stuckRuns atomic.Int32
	release := make(chan struct{})
	err = e1.Submit(
		TaskSpec{ID: "done", Run: func(context.Context, *TaskContext) error {
			doneRuns.Add(1)
			return nil
		}},
		TaskSpec{ID: "stuck", Deps: []string{"done"}, Run: func(context.Context, *TaskContext) error {
			stuckRuns.Add(1)
			<-release // 阻塞，模拟崩溃时仍在执行
			return nil
		}},
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	triggerErr := make(chan error, 1)
	go func() { triggerErr <- e1.Trigger(context.Background(), "2026-09-21") }()
	// 等 stuck 进入 RUNNING 后模拟崩溃：不 Close，直接丢弃 e1。
	deadline := time.After(2 * time.Second)
	for stuckRuns.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("stuck task never started")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	t.Logf("crash simulated: done=%d runs, stuck RUNNING", doneRuns.Load())

	// 重启：新引擎从持久化状态恢复。
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	close(release) // 放行旧 goroutine，避免泄漏
	<-triggerErr
	defer func() { _ = e2.Close() }()

	done, _ := e2.Instance("done", "2026-09-21")
	stuck, _ := e2.Instance("stuck", "2026-09-21")
	t.Logf("recovered: done=%s stuck=%s reason=%q", done.Status, stuck.Status, stuck.LastError)
	if done.Status != StatusSucceeded {
		t.Fatalf("done=%s, want SUCCEEDED", done.Status)
	}
	if stuck.Status != StatusFailed || stuck.LastError != ErrInterrupted.Error() {
		t.Fatalf("stuck=%s reason=%q, want FAILED/%q", stuck.Status, stuck.LastError, ErrInterrupted)
	}

	// 恢复后重新触发同一逻辑键：SUCCEEDED 实例不得重跑，
	// FAILED(interrupted) 实例被重置为 PENDING 并重新执行。
	if err := e2.Submit(
		TaskSpec{ID: "done", Run: func(context.Context, *TaskContext) error {
			doneRuns.Add(1)
			return nil
		}},
		TaskSpec{ID: "stuck", Deps: []string{"done"}, Run: func(context.Context, *TaskContext) error {
			stuckRuns.Add(1)
			return nil
		}},
	); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	if err := e2.Trigger(context.Background(), "2026-09-21"); err != nil {
		t.Fatalf("re-trigger after recovery: %v", err)
	}
	if doneRuns.Load() != 1 {
		t.Fatalf("done executed %d times, want exactly 1 (idempotent)", doneRuns.Load())
	}
	if stuckRuns.Load() != 2 {
		t.Fatalf("stuck executed %d times, want 2 (resumed after interrupt)", stuckRuns.Load())
	}
	stuck, _ = e2.Instance("stuck", "2026-09-21")
	t.Logf("after resume: done runs=%d stuck runs=%d stuck=%s", doneRuns.Load(), stuckRuns.Load(), stuck.Status)
	if stuck.Status != StatusSucceeded {
		t.Fatalf("stuck=%s, want SUCCEEDED after resume", stuck.Status)
	}
}

// TestRecoveryPersistsAcrossCleanRestart 正常关闭后重启：状态完整保留。
func TestRecoveryPersistsAcrossCleanRestart(t *testing.T) {
	path := t.TempDir() + "/state.json"
	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	var runs atomic.Int32
	if err := e1.Submit(TaskSpec{ID: "t", Run: func(context.Context, *TaskContext) error {
		runs.Add(1)
		return nil
	}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := e1.Trigger(context.Background(), "k"); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("close e1: %v", err)
	}

	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	defer func() { _ = e2.Close() }()
	if err := e2.Submit(TaskSpec{ID: "t", Run: func(context.Context, *TaskContext) error {
		runs.Add(1)
		return nil
	}}); err != nil {
		t.Fatalf("submit e2: %v", err)
	}
	// 幂等：已完成的逻辑键再次触发应直接跳过。
	if err := e2.Trigger(context.Background(), "k"); err != nil {
		t.Fatalf("re-trigger after restart: %v", err)
	}
	if runs.Load() != 1 {
		t.Fatalf("task executed %d times across restart, want 1", runs.Load())
	}
	t.Logf("runs=%d => 重启后已完成实例未重复执行", runs.Load())
}
