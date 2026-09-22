package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// scriptHandler 按 (任务名, 逻辑日期) 控制执行次数、失败与产出，并统计实际执行次数。
type scriptHandler struct {
	mu       sync.Mutex
	counters map[string]int // key -> 实际执行次数
	failN    map[string]int // key -> 前 N 次执行返回失败
	out      map[string]string
	expand   func(name, date string, up map[string]string) []string
}

func newScriptHandler() *scriptHandler {
	return &scriptHandler{
		counters: map[string]int{},
		failN:    map[string]int{},
		out:      map[string]string{},
	}
}

func (h *scriptHandler) execKey(name, date string) string { return date + "/" + name }

func (h *scriptHandler) Execute(_ context.Context, req ExecRequest) (ExecResult, error) {
	k := h.execKey(req.Key.Task, req.Key.Date)
	h.mu.Lock()
	h.counters[k]++
	n := h.counters[k]
	failN := h.failN[k]
	out := h.out[k]
	h.mu.Unlock()
	if n <= failN {
		return ExecResult{}, errors.New("injected failure")
	}
	return ExecResult{Output: out}, nil
}

func (h *scriptHandler) Expand(_ context.Context, req ExecRequest) (ExpandResult, error) {
	if h.expand == nil {
		return ExpandResult{}, nil
	}
	ch := h.expand(req.Key.Task, req.Key.Date, req.UpstreamOut)
	return ExpandResult{Children: ch}, nil
}

func (h *scriptHandler) count(name, date string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counters[h.execKey(name, date)]
}

// TestCycleDetected 依赖成环：必须明确报错且给出环路，不得死锁或静默跳过。
func TestCycleDetected(t *testing.T) {
	reg := NewRegistry()
	h := newScriptHandler()
	mustReg(t, reg, TaskSpec{Name: "a", DependsOn: []string{"c"}}, h)
	mustReg(t, reg, TaskSpec{Name: "b", DependsOn: []string{"a"}}, h)
	mustReg(t, reg, TaskSpec{Name: "c", DependsOn: []string{"b"}}, h)

	store, _ := NewMemoryStore("")
	log := NewCaptureLogger(nil)
	_, err := NewScheduler(reg, store, WithLogger(log))
	if !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("want ErrCycleDetected, got %v", err)
	}
	t.Logf("成环被拒绝: %v", err)
	if !strings.Contains(err.Error(), "a") {
		t.Fatalf("错误中应包含环路路径: %v", err)
	}
	t.Log("\n" + log.String())
}

// TestUpstreamFailure 上游失败必须与“重试耗尽”区分；下游全部 upstream_failed。
func TestUpstreamFailure(t *testing.T) {
	reg := NewRegistry()
	h := newScriptHandler()
	h.failN["2026-09-22/a"] = 1 // a 只尝试 1 次，必然失败
	mustReg(t, reg, TaskSpec{Name: "a"}, h)
	mustReg(t, reg, TaskSpec{Name: "b", DependsOn: []string{"a"}}, h)
	mustReg(t, reg, TaskSpec{Name: "c", DependsOn: []string{"b"}}, h)

	store, _ := NewMemoryStore("")
	log := NewCaptureLogger(nil)
	s, err := NewScheduler(reg, store, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]InstanceResult{}
	for _, r := range res.Results {
		got[r.Key.Task] = r
	}
	if got["a"].Status != StatusFailed || got["a"].Reason != ReasonAttemptsExhausted {
		t.Fatalf("a: got %+v", got["a"])
	}
	for _, n := range []string{"b", "c"} {
		if got[n].Status != StatusSkipped || got[n].Reason != ReasonUpstreamFailed {
			t.Fatalf("%s: got %+v, want skipped/upstream_failed", n, got[n])
		}
	}
	if h.count("a", "2026-09-22") != 1 || h.count("b", "2026-09-22") != 0 {
		t.Fatalf("执行次数错误 a=%d b=%d", h.count("a", "2026-09-22"), h.count("b", "2026-09-22"))
	}
	t.Logf("a=%v b=%v c=%v（失败原因可区分）", got["a"].Reason, got["b"].Reason, got["c"].Reason)
	t.Log("\n" + log.String())
}

// TestRetryWithBackoff 有限重试 + 退避：前两次失败第三次成功，尝试记录完整。
func TestRetryWithBackoff(t *testing.T) {
	reg := NewRegistry()
	h := newScriptHandler()
	h.failN["2026-09-22/t"] = 2
	h.out["2026-09-22/t"] = "done"
	mustReg(t, reg, TaskSpec{Name: "t", MaxAttempts: 3, Backoff: 2 * time.Millisecond}, h)

	store, _ := NewMemoryStore("")
	log := NewCaptureLogger(nil)
	s, _ := NewScheduler(reg, store, WithLogger(log))
	res, err := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	if err != nil {
		t.Fatal(err)
	}
	r := res.Results[0]
	if r.Status != StatusSucceeded || r.Reason != ReasonExecuted {
		t.Fatalf("got %+v", r)
	}
	if h.count("t", "2026-09-22") != 3 {
		t.Fatalf("实际执行次数=%d, 期望 3", h.count("t", "2026-09-22"))
	}
	rec, _ := store.Get(context.Background(), InstanceKey{Task: "t", Date: "2026-09-22"})
	if len(rec.Attempts) != 3 {
		t.Fatalf("尝试记录=%d, 期望 3", len(rec.Attempts))
	}
	if !rec.NextRunAt.IsZero() {
		t.Fatalf("成功后不应残留重试时刻: %v", rec.NextRunAt)
	}
	t.Logf("重试后成功，尝试次数=%d，产出=%q", len(rec.Attempts), rec.Output)
	t.Log("\n" + log.String())
}

// TestAttemptsExhaustedStable 次数耗尽后再次触发，结论保持且不再执行。
func TestAttemptsExhaustedStable(t *testing.T) {
	reg := NewRegistry()
	h := newScriptHandler()
	h.failN["2026-09-22/t"] = 99
	mustReg(t, reg, TaskSpec{Name: "t", MaxAttempts: 2, Backoff: time.Millisecond}, h)
	store, _ := NewMemoryStore("")
	s, _ := NewScheduler(reg, store)

	r1, _ := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	if r1.Results[0].Reason != ReasonAttemptsExhausted {
		t.Fatalf("r1 = %+v", r1.Results[0])
	}
	r2, _ := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	if r2.Results[0].Status != StatusFailed || r2.Results[0].Reason != ReasonAttemptsExhausted {
		t.Fatalf("r2 = %+v", r2.Results[0])
	}
	if h.count("t", "2026-09-22") != 2 {
		t.Fatalf("耗尽后不应再执行, count=%d", h.count("t", "2026-09-22"))
	}
}
