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

// TestBackfillIdempotentWithRegular 补跑与常规触发并存：
// 同一逻辑实例不得重复执行；在途冲突须明确拒绝。
func TestBackfillIdempotentWithRegular(t *testing.T) {
	e := newEngine(t)
	var mu sync.Mutex
	runs := map[string]int{}
	if err := e.Submit(TaskSpec{
		ID: "etl",
		Run: func(ctx context.Context, tc *TaskContext) error {
			mu.Lock()
			runs[tc.LogicalKey]++
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			return nil
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	// 常规触发先跑 09-20。
	if err := e.Trigger(context.Background(), "2026-09-20"); err != nil {
		t.Fatalf("regular trigger: %v", err)
	}
	// 补跑覆盖 09-19 ~ 09-21，其中 09-20 已完成：应幂等跳过而非重跑。
	if err := e.Backfill(context.Background(), []string{"2026-09-19", "2026-09-20", "2026-09-21"}); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	for k, n := range runs {
		if n != 1 {
			t.Fatalf("key=%s executed %d times, want exactly 1", k, n)
		}
	}
	t.Logf("runs=%v => 每个逻辑实例恰好执行一次", runs)

	// 在途冲突：补跑一个正在执行的键，须以 ErrConflict 拒绝。
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := e.Trigger(context.Background(), "2026-09-22"); err != nil {
			t.Errorf("trigger in-flight key: %v", err)
		}
	}()
	// 等实例登记后再发起冲突补跑。
	deadline := time.After(2 * time.Second)
	for {
		e.mu.Lock()
		_, ok := e.instances["etl@2026-09-22"]
		e.mu.Unlock()
		if ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("instance never registered")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	err := e.Backfill(context.Background(), []string{"2026-09-22"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting backfill error = %v, want ErrConflict", err)
	}
	t.Logf("conflicting backfill rejected: %v", err)
	wg.Wait()
	if runs["2026-09-22"] != 1 {
		t.Fatalf("in-flight key executed %d times, want 1", runs["2026-09-22"])
	}
}

// TestDynamicExpansion 运行时根据上游产出展开后续任务。
func TestDynamicExpansion(t *testing.T) {
	e := newEngine(t)
	var produced atomic.Int32
	shards := []string{"s1", "s2", "s3"}
	if err := e.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			for _, shard := range shards {
				s := shard
				if err := tc.Emit(TaskSpec{
					ID:   "process-" + s,
					Deps: []string{"scan"},
					Run: func(context.Context, *TaskContext) error {
						produced.Add(1)
						t.Logf("shard %s processed", s)
						return nil
					},
				}); err != nil {
					return fmt.Errorf("emit %s: %w", s, err)
				}
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := e.Trigger(context.Background(), "2026-09-22"); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if produced.Load() != int32(len(shards)) {
		t.Fatalf("processed %d shards, want %d", produced.Load(), len(shards))
	}
	for _, s := range shards {
		inst, ok := e.Instance("process-"+s, "2026-09-22")
		if !ok || inst.Status != StatusSucceeded {
			t.Fatalf("shard %s: ok=%v status=%v", s, ok, inst)
		}
	}
	t.Logf("dynamic expansion: %d shards all SUCCEEDED", produced.Load())
}

// TestEmitCycleRejected 运行时展开构成环：明确返回 ErrCycle 而非死锁。
func TestEmitCycleRejected(t *testing.T) {
	e := newEngine(t)
	noop := func(context.Context, *TaskContext) error { return nil }
	if err := e.Submit(TaskSpec{ID: "a", Run: func(ctx context.Context, tc *TaskContext) error {
		// a 运行时先展开 c（依赖尚未注册的 d），再展开 d（依赖 c）——构成环，
		// 第二次 Emit 必须返回 ErrCycle 且不生效。
		if err := tc.Emit(TaskSpec{ID: "c", Deps: []string{"d"}, Run: noop}); err != nil {
			return err
		}
		return tc.Emit(TaskSpec{ID: "d", Deps: []string{"c"}, Run: noop})
	}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	err := e.Trigger(context.Background(), "k")
	if !errors.Is(err, ErrUpstreamFailed) {
		t.Fatalf("trigger error = %v, want ErrUpstreamFailed", err)
	}
	inst, _ := e.Instance("a", "k")
	t.Logf("a=%s reason=%s", inst.Status, inst.LastError)
	if inst.Status != StatusFailed {
		t.Fatalf("a=%s, want FAILED", inst.Status)
	}
}
