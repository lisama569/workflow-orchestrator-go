package workflow

import (
	"context"
	"testing"
)

type staticHandler struct {
	out string
	err error
}

func (h staticHandler) Execute(_ context.Context, _ ExecRequest) (ExecResult, error) {
	return ExecResult{Output: h.out}, h.err
}
func (h staticHandler) Expand(_ context.Context, _ ExecRequest) (ExpandResult, error) {
	return ExpandResult{}, nil
}

func TestSmokeDAG(t *testing.T) {
	reg := NewRegistry()
	mustReg(t, reg, TaskSpec{Name: "a"}, staticHandler{out: "A"})
	mustReg(t, reg, TaskSpec{Name: "b", DependsOn: []string{"a"}}, staticHandler{out: "B"})
	store, _ := NewMemoryStore("")
	log := NewCaptureLogger(nil)
	s, err := NewScheduler(reg, store, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	res, err := s.Trigger(context.Background(), TriggerRequest{Date: "2026-09-22"})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.Results {
		t.Logf("%s -> %s (%s)", r.Key.Task, r.Status, r.Reason)
		if r.Status != StatusSucceeded {
			t.Fatalf("task %s = %s: %s", r.Key.Task, r.Status, r.Err)
		}
	}
	t.Log("\n" + log.String())
}

func mustReg(t *testing.T, reg *Registry, spec TaskSpec, h TaskHandler) {
	t.Helper()
	if err := reg.Register(spec, h); err != nil {
		t.Fatal(err)
	}
}
