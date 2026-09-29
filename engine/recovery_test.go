package engine

import (
	"context"
	"errors"
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

// TestRecoveryBackoffBudgetAcrossRestart 退避途中“崩溃”重启：
//   - 失败后处于 WAITING 的任务，剩余重试次数（Attempts 已扣减预算）与
//     原等待计划（NextRetryAt）完整保留：不会清零、不会马上重试；
//   - 恢复后到点只按剩余预算再试一次并耗尽，证明预算延续；
//   - 同一时刻崩溃时正处于 RUNNING 的另一任务恢复为 FAILED(ErrInterrupted)，
//     两种“没跑完”的状态可明确区分；
//   - SUCCEEDED 依旧不重跑（沿用既有恢复语义）。
func TestRecoveryBackoffBudgetAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"

	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))

	var flakyRuns atomic.Int32
	var slowRuns atomic.Int32
	release := make(chan struct{})
	// 退避故意设得很长：重启后绝不能提前触发；真正续跑由缩短后的计划验证。
	longBackoff := time.Hour
	flakySpec := func() TaskSpec {
		return TaskSpec{
			ID: "flaky", MaxRetries: 1, Backoff: longBackoff,
			Run: func(context.Context, *TaskContext) error {
				flakyRuns.Add(1)
				return errFlakyAlways
			},
		}
	}
	slowSpec := func() TaskSpec {
		return TaskSpec{
			ID: "slow",
			Run: func(context.Context, *TaskContext) error {
				slowRuns.Add(1)
				<-release // 崩溃时仍在执行
				return nil
			},
		}
	}
	if err := e1.Submit(flakySpec(), slowSpec()); err != nil {
		t.Fatalf("submit e1: %v", err)
	}
	// 同一逻辑键：flaky 首次失败进入 WAITING 时，slow 恰好仍 RUNNING，
	// 一次崩溃即可同时覆盖“退避中”与“执行到一半被打断”两种状态。
	triggerErr := make(chan error, 1)
	go func() { triggerErr <- e1.Trigger(context.Background(), "k") }()

	// 等到 flaky 首次失败进入 WAITING、slow 进入 RUNNING，再模拟进程崩溃（不 Close）。
	waitFor := func(cond func() bool, msg string) {
		deadline := time.Now().Add(2 * time.Second)
		for {
			if cond() {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("timeout waiting: " + msg)
			}
			time.Sleep(time.Millisecond)
		}
	}
	var planned time.Time
	waitFor(func() bool {
		in, ok := e1.Instance("flaky", "k")
		if ok && in.Status == StatusWaiting {
			planned = in.NextRetryAt
			return true
		}
		return false
	}, "flaky WAITING")
	waitFor(func() bool {
		in, ok := e1.Instance("slow", "k")
		return ok && in.Status == StatusRunning
	}, "slow RUNNING")
	crashAt := time.Now()
	t.Logf("crash: flaky WAITING attempts=1 nextRetryIn=%s; slow RUNNING", time.Until(planned).Round(time.Second))

	// 模拟进程死亡：停止 e1 的退避计时器，使其 1 小时后的等待器不会在测试期间
	// 再写状态文件；slow 执行体仍阻塞，稍后在测试末尾确定性放行并等待落盘结束。
	close(e1.stop)
	e1.mu.Lock()
	e1.closed = true
	e1.mu.Unlock()

	// 重启：e2 从持久化状态恢复。
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	e2.SetLogger(testLogger(t))
	if err := e2.Submit(flakySpec(), slowSpec()); err != nil {
		t.Fatalf("submit e2: %v", err)
	}

	flaky, _ := e2.Instance("flaky", "k")
	slow, _ := e2.Instance("slow", "k")
	if flaky.Status != StatusWaiting {
		t.Fatalf("flaky=%s after restart, want WAITING (backoff in progress)", flaky.Status)
	}
	if flaky.Attempts != 1 {
		t.Fatalf("flaky attempts=%d, want 1 (spent budget preserved, not reset)", flaky.Attempts)
	}
	if !flaky.NextRetryAt.Equal(planned) {
		t.Fatalf("flaky nextRetryAt=%s, want original plan %s", flaky.NextRetryAt, planned)
	}
	if time.Until(flaky.NextRetryAt) <= 0 || time.Until(flaky.NextRetryAt) > longBackoff {
		t.Fatalf("retry plan must remain in the future near original schedule, got %s", time.Until(flaky.NextRetryAt))
	}
	if slow.Status != StatusFailed || slow.LastError != ErrInterrupted.Error() {
		t.Fatalf("slow=%s reason=%q, want FAILED/%q (interrupted mid-run)",
			slow.Status, slow.LastError, ErrInterrupted)
	}
	t.Logf("distinguished: flaky=%s attempts=%d vs slow=%s(%q)",
		flaky.Status, flaky.Attempts, slow.Status, slow.LastError)

	// 退避未到点不得重试：观察一段时间，运行次数不增加、状态仍为 WAITING。
	time.Sleep(300 * time.Millisecond)
	if flakyRuns.Load() != 1 {
		t.Fatalf("flaky ran %d times after restart, must not retry before planned time", flakyRuns.Load())
	}
	flaky, _ = e2.Instance("flaky", "k")
	if flaky == nil {
		t.Fatal("flaky disappeared after restart")
	}
	if flaky.Status != StatusWaiting {
		t.Fatalf("flaky=%s, must remain WAITING until planned time", flaky.Status)
	}
	if time.Since(crashAt) > time.Until(planned) {
		t.Fatal("test design error: original plan arrived during observation")
	}

	// 再次“重启”，但把等待计划缩短到 200ms 后（等价于崩溃点接近计划时间）：
	// e3 必须带着 attempts=1 续跑 —— 只剩一次预算，再败即 FAILED；
	// 若预算被清零，第二次执行后会重新进入 WAITING 而非终态。
	if err := e2.Close(); err != nil {
		t.Fatalf("close e2: %v", err)
	}
	e2.mu.Lock()
	e2.instances["flaky@k"].NextRetryAt = time.Now().Add(200 * time.Millisecond)
	if err := e2.persistLocked(); err != nil {
		e2.mu.Unlock()
		t.Fatalf("persist shortened plan: %v", err)
	}
	e2.mu.Unlock()

	e3, err := Open(path)
	if err != nil {
		t.Fatalf("open e3: %v", err)
	}
	e3.SetLogger(testLogger(t))
	defer func() { _ = e3.Close() }()
	if err := e3.Submit(flakySpec()); err != nil {
		t.Fatalf("submit e3: %v", err)
	}
	if err := e3.Await(context.Background(), "k"); err == nil {
		t.Fatal("Await k: want error (last attempt fails), got nil")
	}
	if flakyRuns.Load() != 2 {
		t.Fatalf("flaky total runs=%d, want 2 (initial + single remaining retry)", flakyRuns.Load())
	}
	flaky, _ = e3.Instance("flaky", "k")
	if flaky.Status != StatusFailed || flaky.Attempts != 2 {
		t.Fatalf("flaky=%s attempts=%d, want FAILED/2 (remaining budget exhausted after restart)",
			flaky.Status, flaky.Attempts)
	}
	if flaky.LastError != errFlakyAlways.Error() {
		t.Fatalf("flaky reason=%q, want the real failure rather than interruption", flaky.LastError)
	}
	t.Logf("resumed at planned time with preserved budget: totalRuns=%d final=%s attempts=%d",
		flakyRuns.Load(), flaky.Status, flaky.Attempts)

	// 确定性收尾：放行仍阻塞的 e1 slow 执行体，并等待 e1 全部在途 goroutine
	// （含其最后的状态落盘）结束，避免与 TempDir 清理产生文件竞态。
	close(release)
	go func() { _ = <-triggerErr }()
	e1.wg.Wait()
}

var errFlakyAlways = errors.New("flaky always fails")
