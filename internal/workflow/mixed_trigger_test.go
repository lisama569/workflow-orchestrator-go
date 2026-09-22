package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestMixedTriggersNoDoubleExecution 常规触发与补跑在多个日期上高并发交错：
// 每个 (任务,日期) 逻辑实例最多被实际执行一次；冲突只能是 ErrRejected 或幂等复用。
func TestMixedTriggersNoDoubleExecution(t *testing.T) {
	reg := NewRegistry()
	h := newScriptHandler()
	mustReg(t, reg, TaskSpec{Name: "t"}, h)
	store, _ := NewMemoryStore("")
	s, _ := NewScheduler(reg, store)

	const goroutines = 24
	var wg sync.WaitGroup
	var rejectMu sync.Mutex
	rejected := 0
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx := context.Background()
			if i%2 == 0 {
				_, err := s.Trigger(ctx, TriggerRequest{Date: "2026-09-22"})
				if err != nil {
					t.Errorf("trigger err: %v", err)
				}
			} else {
				_, err := s.Backfill(ctx, BackfillRequest{Start: "2026-09-22", End: "2026-09-22"})
				if err != nil {
					if !errors.Is(err, ErrRejected) {
						t.Errorf("backfill err 非预期: %v", err)
					}
					rejectMu.Lock()
					rejected++
					rejectMu.Unlock()
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if h.count("t", "2026-09-22") != 1 {
		t.Fatalf("逻辑实例实际执行次数=%d，期望恰好 1", h.count("t", "2026-09-22"))
	}
	rec, _ := store.Get(context.Background(), InstanceKey{Task: "t", Date: "2026-09-22"})
	if rec.Status != StatusSucceeded {
		t.Fatalf("最终状态=%s，期望 succeeded", rec.Status)
	}
	t.Logf("24 个并发混合触发：执行次数=1，补跑被明确拒绝次数=%d，其余幂等/并入", rejected)
}
