package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestEmissionSourceTraceAndIdempotency 来源追溯与重复展开幂等：
//   - 每个展开实例能查到来源（上游任务 + 产出批次）；
//   - Engine.Emissions 能列出某逻辑键的全部批次及带出的任务清单；
//   - 同一来源重复报同一批任务只算一次（上游重试重复 Emit 也一样），
//     展开出的任务只执行一次，批次记录不膨胀。
func TestEmissionSourceTraceAndIdempotency(t *testing.T) {
	e := newEngine(t)
	var childRuns atomic.Int32
	var scanAttempt atomic.Int32
	emit := func(tc *TaskContext) {
		// 同一批次同一批任务，重复报两次（模拟上游逻辑被重试/重复触发）。
		for i := 0; i < 2; i++ {
			if err := tc.EmitBatch("shard-s1", TaskSpec{
				ID: "process-s1",
				Run: func(context.Context, *TaskContext) error {
					childRuns.Add(1)
					return nil
				},
			}); err != nil {
				t.Errorf("emit batch: %v", err)
			}
		}
	}
	if err := e.Submit(TaskSpec{
		ID: "scan", MaxRetries: 1, Backoff: time.Millisecond,
		Run: func(ctx context.Context, tc *TaskContext) error {
			n := scanAttempt.Add(1)
			emit(tc)
			if n == 1 {
				return errors.New("scan fails after producing; retry must not duplicate children")
			}
			// 另一个批次带出另一任务。
			return tc.EmitBatch("shard-s2", TaskSpec{
				ID:  "process-s2",
				Run: func(context.Context, *TaskContext) error { return nil },
			})
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := e.Trigger(context.Background(), "2026-09-22"); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	if childRuns.Load() != 1 {
		t.Fatalf("process-s1 executed %d times, want exactly 1 (same source/batch reported twice)", childRuns.Load())
	}
	recs := e.Emissions("2026-09-22")
	if len(recs) != 2 {
		t.Fatalf("emissions=%d, want 2 batches: %+v", len(recs), recs)
	}
	want := map[string][]string{
		"shard-s1": {"scan", "process-s1"},
		"shard-s2": {"scan", "process-s2"},
	}
	for _, m := range recs {
		exp, ok := want[m.Batch]
		if !ok {
			t.Fatalf("unexpected batch %q", m.Batch)
		}
		if m.Emitter != exp[0] || len(m.TaskIDs) != 1 || m.TaskIDs[0] != exp[1] {
			t.Fatalf("batch %q = emitter=%s tasks=%v, want %s/%s", m.Batch, m.Emitter, m.TaskIDs, exp[0], exp[1])
		}
		inst, ok := e.Instance(exp[1], "2026-09-22")
		if !ok || inst.Source == nil {
			t.Fatalf("%s missing source", exp[1])
		}
		if inst.Source.EmitterTaskID != "scan" || inst.Source.Batch != m.Batch {
			t.Fatalf("%s source=%+v, want scan/%s", exp[1], inst.Source, m.Batch)
		}
		t.Logf("traced: %s <- emitter=%s batch=%q scope=%s", exp[1], m.Emitter, m.Batch, m.LogicalKey)
	}
	t.Logf("childRuns=%d scanAttempts=%d => 重复展开按一次算，来源可追溯", childRuns.Load(), scanAttempt.Load())
}

// TestEmissionScopeByLogicalKey 展开作用域隔离：
// 每次运行只看到自己业务日期展开的任务与批次，静态任务不会带出别人的展开。
func TestEmissionScopeByLogicalKey(t *testing.T) {
	e := newEngine(t)
	if err := e.Submit(TaskSpec{
		ID: "scan",
		Run: func(ctx context.Context, tc *TaskContext) error {
			shard := "s-" + tc.LogicalKey
			return tc.EmitBatch("batch-"+tc.LogicalKey, TaskSpec{
				ID:  "process-" + shard,
				Run: func(context.Context, *TaskContext) error { return nil },
			})
		},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// 另一个完全不展开的静态任务，用于验证新日期不会冒出任何动态实例。
	if err := e.Submit(TaskSpec{
		ID:  "report",
		Run: func(context.Context, *TaskContext) error { return nil },
	}); err != nil {
		t.Fatalf("submit report: %v", err)
	}
	if err := e.Backfill(context.Background(), []string{"2026-09-20", "2026-09-21"}); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if err := e.Trigger(context.Background(), "2026-09-22"); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	for _, day := range []string{"2026-09-20", "2026-09-21", "2026-09-22"} {
		insts := e.Instances(day)
		wantTasks := map[string]bool{"scan": true, "report": true, "process-s-" + day: true}
		got := map[string]bool{}
		for _, in := range insts {
			if in.LogicalKey != day {
				t.Fatalf("instance %s leaked into scope %s", in.Key(), day)
			}
			got[in.TaskID] = true
			if in.Status != StatusSucceeded {
				t.Fatalf("%s@%s status=%s, want SUCCEEDED", in.TaskID, day, in.Status)
			}
		}
		if len(got) != len(wantTasks) {
			t.Fatalf("scope %s tasks=%v, want exactly %v", day, got, wantTasks)
		}
		recs := e.Emissions(day)
		if len(recs) != 1 || recs[0].LogicalKey != day || recs[0].TaskIDs[0] != "process-s-"+day {
			t.Fatalf("scope %s emissions=%+v, want only its own batch", day, recs)
		}
	}
	// 未运行过的日期：看不到任何实例或批次。
	if len(e.Instances("2026-09-19")) != 0 || len(e.Emissions("2026-09-19")) != 0 {
		t.Fatal("unrun date must have no instances/emissions")
	}
	t.Log("不同业务日期的展开互不干扰，补跑某天只见该天的展开结果")
}

// TestEmittedOrderingAndUpstreamFailure 展开任务天然排在成功上游之后；
// 上游最终失败时展开任务得 BLOCKED 终态（ErrUpstreamFailed），不悬挂、不消失。
func TestEmittedOrderingAndUpstreamFailure(t *testing.T) {
	t.Run("success ordering under concurrency", func(t *testing.T) {
		e := newEngine(t)
		if err := e.Submit(TaskSpec{
			ID: "scan",
			Run: func(ctx context.Context, tc *TaskContext) error {
				var specs []TaskSpec
				for _, shard := range []string{"s1", "s2", "s3"} {
					s := shard
					specs = append(specs, TaskSpec{
						// 故意不声明 Deps：依赖由引擎按展开来源自动注入。
						ID: "process-" + s,
						Run: func(context.Context, *TaskContext) error {
							up, _ := e.Instance("scan", tc.LogicalKey)
							if up.Status != StatusSucceeded {
								return fmt.Errorf("child started before upstream success: upstream=%s", up.Status)
							}
							return nil
						},
					})
				}
				return tc.EmitBatch("shards", specs...)
			},
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
		if err := e.Trigger(context.Background(), "2026-09-22"); err != nil {
			t.Fatalf("trigger: %v", err)
		}
		for _, s := range []string{"s1", "s2", "s3"} {
			in, _ := e.Instance("process-"+s, "2026-09-22")
			if in.Status != StatusSucceeded {
				t.Fatalf("%s=%s, want SUCCEEDED after upstream", "process-"+s, in.Status)
			}
		}
		t.Log("三个展开任务并发执行时均只在 scan SUCCEEDED 之后启动")
	})

	t.Run("upstream failure blocks emitted", func(t *testing.T) {
		e := newEngine(t)
		var childRan atomic.Bool
		if err := e.Submit(TaskSpec{
			ID: "scan", MaxRetries: 0,
			Run: func(ctx context.Context, tc *TaskContext) error {
				if err := tc.EmitBatch("shards", TaskSpec{
					ID: "process-s1",
					Run: func(context.Context, *TaskContext) error {
						childRan.Store(true)
						return nil
					},
				}); err != nil {
					return err
				}
				return errors.New("scan ultimately fails")
			},
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
		err := e.Trigger(context.Background(), "2026-09-22")
		if !errors.Is(err, ErrUpstreamFailed) {
			t.Fatalf("trigger err=%v, want ErrUpstreamFailed", err)
		}
		scan, _ := e.Instance("scan", "2026-09-22")
		child, _ := e.Instance("process-s1", "2026-09-22")
		t.Logf("scan=%s(%s) child=%s(%s)", scan.Status, scan.LastError, child.Status, child.LastError)
		if scan.Status != StatusFailed {
			t.Fatalf("scan=%s, want FAILED", scan.Status)
		}
		if child.Status != StatusBlocked {
			t.Fatalf("child=%s, want BLOCKED (dragged down by upstream)", child.Status)
		}
		// LastError 是文本描述，须携带可区分原因与具体上游任务。
		if !strings.Contains(child.LastError, ErrUpstreamFailed.Error()) {
			t.Fatalf("child reason=%q must carry ErrUpstreamFailed", child.LastError)
		}
		if !strings.Contains(child.LastError, "scan") {
			t.Fatalf("child reason=%q must name the upstream task scan", child.LastError)
		}
		if childRan.Load() {
			t.Fatal("emitted child must never run when upstream ultimately fails")
		}
	})
}

// TestEmissionsPersistAcrossRestart 展开批次记录与实例来源随状态落盘：
// 干净重启后仍可按业务日期追溯，且别的日期查不到本次批次。
func TestEmissionsPersistAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/state.json"
	e1, err := Open(path)
	if err != nil {
		t.Fatalf("open e1: %v", err)
	}
	e1.SetLogger(testLogger(t))
	if err := e1.Submit(TaskSpec{ID: "scan", Run: func(ctx context.Context, tc *TaskContext) error {
		return tc.EmitBatch("b1", TaskSpec{
			ID: "child", Deps: []string{"scan"},
			Run: func(context.Context, *TaskContext) error { return nil },
		})
	}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := e1.Trigger(context.Background(), "d1"); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e2, err := Open(path)
	if err != nil {
		t.Fatalf("open e2: %v", err)
	}
	defer func() { _ = e2.Close() }()
	recs := e2.Emissions("d1")
	if len(recs) != 1 || recs[0].Emitter != "scan" || recs[0].Batch != "b1" ||
		len(recs[0].TaskIDs) != 1 || recs[0].TaskIDs[0] != "child" {
		t.Fatalf("emissions after restart=%+v", recs)
	}
	if len(e2.Emissions("d2")) != 0 {
		t.Fatal("other date must not see d1 emissions")
	}
	child, ok := e2.Instance("child", "d1")
	if !ok || child.Source == nil || child.Source.EmitterTaskID != "scan" || child.Source.Batch != "b1" {
		t.Fatalf("child source after restart=%+v ok=%v", child, ok)
	}
	t.Logf("restart trace preserved: emitter=%s batch=%s tasks=%v scope=%s",
		recs[0].Emitter, recs[0].Batch, recs[0].TaskIDs, recs[0].LogicalKey)
}
