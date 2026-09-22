package workflow

import (
	"context"
	"sync"
	"testing"
	"time"
)

// gateHandler 进入 Execute 即阻塞，直到 gate 打开；用于制造在途并发窗口。
type gateHandler struct {
	mu      sync.Mutex
	started int
	gate    chan struct{}
}

func (g *gateHandler) Execute(_ context.Context, _ ExecRequest) (ExecResult, error) {
	g.mu.Lock()
	g.started++
	g.mu.Unlock()
	<-g.gate
	return ExecResult{Output: "ok"}, nil
}
func (g *gateHandler) Expand(_ context.Context, _ ExecRequest) (ExpandResult, error) {
	return ExpandResult{}, nil
}

// TestConcurrentTriggerExecutesOnce 并发触发同一日期：同一任务只被实际执行一次。
func TestConcurrentTriggerExecutesOnce(t *testing.T) {
	reg := NewRegistry()
	g := &gateHandler{gate: make(chan struct{})}
	mustReg(t, reg, TaskSpec{Name: "t"}, g)
	store, _ := NewMemoryStore("")
	s, err := NewScheduler(reg, store)
	if err != nil {
		t.Fatal(err)
	}
	const N = 8
	var wg sync.WaitGroup
	resList := make([]*RunResult, N)
	errList := make([]error, N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			resList[i], errList[i] = s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
		}(i)
	}
	close(start)
	// 等待所有 goroutine 已并入在途 Run，再放行执行。
	time.Sleep(20 * time.Millisecond)
	close(g.gate)
	wg.Wait()
	for i, err := range errList {
		if err != nil {
			t.Fatalf("trigger %d err: %v", i, err)
		}
	}
	joined := 0
	for _, r := range resList {
		if r.Joined {
			joined++
		}
	}
	if g.started != 1 {
		t.Fatalf("任务实际执行次数=%d，期望 1", g.started)
	}
	if joined != N-1 {
		t.Fatalf("并入次数=%d，期望 %d", joined, N-1)
	}
	t.Logf("8 个并发触发：1 次实际执行，%d 次并入等待，无重复执行", joined)
}

// TestRestartRecovery 进程重启恢复：
//   - 已成功的任务不重复执行；
//   - 崩溃前处于 running 的任务恢复为 pending 且中断尝试计数+1，下一次触发重试。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/store.json"
	date := "2026-09-22"

	// 第一个进程：a 成功；b 在第 1 次尝试失败后的重试执行中崩溃（手工置 running）。
	{
		reg := NewRegistry()
		h := newScriptHandler()
		h.out[date+"/a"] = "A"
		h.failN[date+"/b"] = 99 // b 在第一进程内必然失败，用于制造带尝试记录的记录
		mustReg(t, reg, TaskSpec{Name: "a"}, h)
		mustReg(t, reg, TaskSpec{Name: "b", DependsOn: []string{"a"}, MaxAttempts: 3, Backoff: time.Millisecond}, h)
		store, err := NewMemoryStore(path)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := NewScheduler(reg, store)
		if _, err := s.Trigger(context.Background(), TriggerRequest{Date: date}); err != nil {
			t.Fatal(err)
		}
		// 把已 failed 的 b 改写成 running 且含 1 次尝试，模拟“重试执行中掉电”。
		rec, _ := store.Get(context.Background(), InstanceKey{Task: "b", Date: date})
		rec.Status = StatusRunning
		rec.Err = ""
		rec.Attempts = []Attempt{{Number: 1, StartedAt: time.Now(), EndedAt: time.Now(), Err: "injected failure"}}
		rec.UpdatedAt = time.Now()
		if ok, err := store.Put(context.Background(), rec, ptrStatus(StatusFailed)); err != nil || !ok {
			t.Fatalf("seed running: ok=%v err=%v", ok, err)
		}
		_ = s.Close()
	}

	// 第二个进程：构造时执行恢复。
	reg := NewRegistry()
	h := newScriptHandler()
	mustReg(t, reg, TaskSpec{Name: "a"}, h)
	mustReg(t, reg, TaskSpec{Name: "b", DependsOn: []string{"a"}, MaxAttempts: 3, Backoff: time.Millisecond}, h)
	store, err := NewMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	log := NewCaptureLogger(nil)
	s, err := NewScheduler(reg, store, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	recB, _ := store.Get(context.Background(), InstanceKey{Task: "b", Date: date})
	if recB.Status != StatusPending {
		t.Fatalf("恢复后 b 状态=%s，期望 pending", recB.Status)
	}
	// 原 1 次失败尝试 + 中断计 1 次 = 2 次
	if len(recB.Attempts) != 2 || recB.Attempts[1].Err == "" {
		t.Fatalf("中断尝试未正确计数: %+v", recB.Attempts)
	}
	t.Log("重启恢复日志:\n" + log.String())

	res, err := s.Trigger(context.Background(), TriggerRequest{Date: date})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]InstanceResult{}
	for _, r := range res.Results {
		got[r.Key.Task] = r
	}
	if got["a"].Reason != ReasonAlreadySucceeded || h.count("a", date) != 0 {
		t.Fatalf("a 不应重复执行: %+v count=%d", got["a"], h.count("a", date))
	}
	if got["b"].Status != StatusSucceeded {
		t.Fatalf("b 应在恢复重试后成功: %+v", got["b"])
	}
	if h.count("b", date) != 1 {
		t.Fatalf("b 恢复后实际执行=%d，期望 1（第 3 次尝试成功）", h.count("b", date))
	}
	t.Logf("恢复后触发: a=%s(不重复执行) b=%s(中断计 1 次失败后重试成功)",
		got["a"].Reason, got["b"].Status)
	_ = s.Close()
}
