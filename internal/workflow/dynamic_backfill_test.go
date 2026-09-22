package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestDynamicExpansion 父任务根据“上游产出”展开子任务；子任务可读取父产出，
// 未被展开的模板任务给 not_expanded 结论。
func TestDynamicExpansion(t *testing.T) {
	reg := NewRegistry()
	h := newScriptHandler()
	h.out["2026-09-22/root"] = "payload-X"
	h.out["2026-09-22/dep"] = "D"
	// root 展开 child；orphan 永远不被展开。
	h.expand = func(name, date string, up map[string]string) []string {
		if name == "root" {
			if up["dep"] != "D" {
				t.Errorf("root 未收到上游产出 dep=D, got %v", up)
			}
			return []string{"child"}
		}
		return nil
	}
	mustReg(t, reg, TaskSpec{Name: "dep"}, h)
	mustReg(t, reg, TaskSpec{Name: "root", DependsOn: []string{"dep"}}, h)
	mustReg(t, reg, TaskSpec{Name: "child", Dynamic: true}, h)
	mustReg(t, reg, TaskSpec{Name: "orphan", Dynamic: true}, h)

	store, _ := NewMemoryStore("")
	log := NewCaptureLogger(nil)
	s, _ := NewScheduler(reg, store, WithLogger(log))
	res, err := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]InstanceResult{}
	for _, r := range res.Results {
		got[r.Key.Task] = r
	}
	if got["child"].Status != StatusSucceeded {
		t.Fatalf("child = %+v", got["child"])
	}
	if got["orphan"].Status != StatusSkipped || got["orphan"].Reason != ReasonNotExpanded {
		t.Fatalf("orphan = %+v", got["orphan"])
	}
	recChild, _ := store.Get(context.Background(), InstanceKey{Task: "child", Date: "2026-09-22"})
	if recChild.Source != SourceRegular {
		t.Fatalf("child source = %s", recChild.Source)
	}
	// 再次触发：root 与 child 都幂等复用，child 不重复执行。
	res2, _ := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	for _, r := range res2.Results {
		if r.Key.Task == "child" && r.Reason != ReasonAlreadySucceeded {
			t.Fatalf("child 二次触发 = %s, want already_succeeded", r.Reason)
		}
	}
	if h.count("child", "2026-09-22") != 1 {
		t.Fatalf("child 实际执行=%d，期望 1", h.count("child", "2026-09-22"))
	}
	t.Logf("动态展开: child=%s orphan=%s；二次触发 child=%s（幂等）",
		got["child"].Status, got["orphan"].Reason, got["child"].Status)
	t.Log("\n" + log.String())
}

// TestDynamicCycle 展开边引入环时必须失败且原因可区分（dynamic_cycle）。
func TestDynamicCycle(t *testing.T) {
	reg := NewRegistry()
	h := newScriptHandler()
	// x 展开 y（dynamic），y 静态依赖 x：边 x->y 且 y->x（隐式父 x + 静态 dep x）
	// 实际 y 依赖 x 属于静态依赖普通任务合法；环来自“展开一个依赖链能回到父”的结构。
	// 构造：p 展开 d；d 静态依赖 m；m 静态依赖 p。展开边 p->d 后 d 可达 p => 环。
	h.expand = func(name, _ string, _ map[string]string) []string {
		if name == "p" {
			return []string{"d"}
		}
		return nil
	}
	mustReg(t, reg, TaskSpec{Name: "p"}, h)
	mustReg(t, reg, TaskSpec{Name: "m", DependsOn: []string{"p"}}, h)
	mustReg(t, reg, TaskSpec{Name: "d", Dynamic: true, DependsOn: []string{"m"}}, h)

	store, _ := NewMemoryStore("")
	log := NewCaptureLogger(nil)
	s, _ := NewScheduler(reg, store, WithLogger(log))
	res, err := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]InstanceResult{}
	for _, r := range res.Results {
		got[r.Key.Task] = r
	}
	if got["p"].Status != StatusFailed || got["p"].Reason != ReasonDynamicCycle {
		t.Fatalf("p = %+v, want failed/dynamic_cycle", got["p"])
	}
	if h.count("d", "2026-09-22") != 0 {
		t.Fatalf("成环子任务不应被执行 d=%d", h.count("d", "2026-09-22"))
	}
	t.Logf("动态展开成环被拒绝: p=%s", got["p"].Reason)
	t.Log("\n" + log.String())
}

// TestBackfillIdempotent 补跑历史区间：
//   - 与常规触发共享实例，已成功的日期幂等复用，不重复执行；
//   - 未跑过的日期正常执行；
//   - 在途冲突 PolicyReject 明确拒绝；PolicySkipInFlight 标注跳过原因。
func TestBackfillIdempotent(t *testing.T) {
	reg := NewRegistry()
	h := newScriptHandler()
	mustReg(t, reg, TaskSpec{Name: "t"}, h)
	store, _ := NewMemoryStore("")
	s, _ := NewScheduler(reg, store)

	// 常规触发先跑 2026-09-20。
	if _, err := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-20"}); err != nil {
		t.Fatal(err)
	}
	// 补跑闭区间 09-19..09-21。
	res, err := s.Backfill(context.Background(), BackfillRequest{Start: "2026-09-19", End: "2026-09-21"})
	if err != nil {
		t.Fatal(err)
	}
	byDate := map[string]InstanceResult{}
	for _, r := range res.Results {
		byDate[r.Key.Date] = r
	}
	if byDate["2026-09-20"].Reason != ReasonAlreadySucceeded {
		t.Fatalf("09-20 = %s, want already_succeeded", byDate["2026-09-20"].Reason)
	}
	if byDate["2026-09-19"].Reason != ReasonExecuted || byDate["2026-09-21"].Reason != ReasonExecuted {
		t.Fatalf("09-19/09-21 应实际执行: %+v %+v", byDate["2026-09-19"], byDate["2026-09-21"])
	}
	for _, d := range []string{"2026-09-19", "2026-09-20", "2026-09-21"} {
		if h.count("t", d) != 1 {
			t.Fatalf("%s 执行次数=%d，期望恰好 1", d, h.count("t", d))
		}
	}
	t.Logf("补跑结果: 09-19=%s 09-20=%s 09-21=%s（无重复执行）",
		byDate["2026-09-19"].Reason, byDate["2026-09-20"].Reason, byDate["2026-09-21"].Reason)

	// 在途冲突：常规触发在执行中，reject 策略补跑必须被拒绝。
	g := &blockingOnceHandler{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	reg2 := NewRegistry()
	mustReg(t, reg2, TaskSpec{Name: "t"}, g)
	store2, _ := NewMemoryStore("")
	s2, _ := NewScheduler(reg2, store2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = s2.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	}()
	<-g.entered
	_, err = s2.Backfill(context.Background(), BackfillRequest{Start: "2026-09-22", End: "2026-09-22"})
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("在途冲突应 ErrRejected, got %v", err)
	}
	t.Logf("在途冲突被明确拒绝: %v", err)

	// skip_inflight 策略：补跑并入，实例标注 inflight_skipped。
	resSkip, err := s2.Backfill(context.Background(), BackfillRequest{
		Start: "2026-09-22", End: "2026-09-22", Policy: PolicySkipInFlight,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resSkip.Joined || len(resSkip.SkippedInFlight) != 1 ||
		resSkip.Results[0].Reason != ReasonInFlightSkipped {
		t.Fatalf("skip_inflight 结果异常: %+v", resSkip)
	}
	t.Logf("skip_inflight: 立即跳过=%v skipped=%v", resSkip.SkippedInFlight, len(resSkip.SkippedInFlight))

	close(g.gate)
	wg.Wait()
	// 在途 Run 结束后再补跑同一日期：已成功 -> 幂等复用。
	res3, err := s2.Backfill(context.Background(), BackfillRequest{Start: "2026-09-22", End: "2026-09-22"})
	if err != nil {
		t.Fatal(err)
	}
	if res3.Results[0].Reason != ReasonAlreadySucceeded || g.n != 1 {
		t.Fatalf("结束后补跑应幂等复用: reason=%s exec=%d", res3.Results[0].Reason, g.n)
	}
	t.Logf("在途结束后补跑: reason=%s 实际执行次数=%d", res3.Results[0].Reason, g.n)
}

// blockingOnceHandler 进入执行后阻塞直到 gate 关闭，只执行一次。
type blockingOnceHandler struct {
	mu      sync.Mutex
	n       int
	gate    chan struct{}
	entered chan struct{}
}

func (g *blockingOnceHandler) Execute(_ context.Context, _ ExecRequest) (ExecResult, error) {
	g.mu.Lock()
	g.n++
	g.mu.Unlock()
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.gate
	return ExecResult{Output: "z"}, nil
}
func (g *blockingOnceHandler) Expand(_ context.Context, _ ExecRequest) (ExpandResult, error) {
	return ExpandResult{}, nil
}

