// 示例：本地工作流调度引擎的最小可运行用法。
//
// 运行：
//
//	go run ./cmd/workflow-demo
//
// 演示内容：
//   - 注册带依赖、有限重试与动态展开的任务；
//   - 常规触发一个日期；
//   - 对历史区间补跑（已成功的逻辑实例幂等复用，不重复执行）。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/lisama569/workflow-orchestrator-go/internal/workflow"
)

type handler struct {
	failUntil int32
	execCount *atomic.Int32
}

func (h handler) Execute(_ context.Context, req workflow.ExecRequest) (workflow.ExecResult, error) {
	n := h.execCount.Add(1)
	switch req.Key.Task {
	case "extract":
		if n <= h.failUntil {
			return workflow.ExecResult{}, errors.New("extract: upstream flaky")
		}
		return workflow.ExecResult{Output: fmt.Sprintf("rows@%s", req.Key.Date)}, nil
	case "fanout":
		return workflow.ExecResult{Output: "fanout-ok"}, nil
	case "shard-0":
		return workflow.ExecResult{Output: "shard-0-ok"}, nil
	default:
		return workflow.ExecResult{Output: "ok"}, nil
	}
}

func (h handler) Expand(_ context.Context, req workflow.ExecRequest) (workflow.ExpandResult, error) {
	if req.Key.Task == "fanout" {
		// 根据上游产出展开后续任务（这里固定展开一个已注册的动态模板任务）。
		return workflow.ExpandResult{Children: []string{"shard-0"}}, nil
	}
	return workflow.ExpandResult{}, nil
}

func main() {
	dir, err := os.MkdirTemp("", "workflow-demo-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	reg := workflow.NewRegistry()
	var execCount atomic.Int32
	h := handler{failUntil: 1, execCount: &execCount} // extract 前 1 次失败
	must(reg.Register(workflow.TaskSpec{Name: "extract", MaxAttempts: 3, Backoff: 10 * time.Millisecond}, h))
	must(reg.Register(workflow.TaskSpec{Name: "fanout", DependsOn: []string{"extract"}}, h))
	must(reg.Register(workflow.TaskSpec{Name: "shard-0", Dynamic: true}, h))

	store, err := workflow.NewMemoryStore(filepath.Join(dir, "state.json"))
	must(err)
	defer store.Close()

	s, err := workflow.NewScheduler(reg, store, workflow.WithLogger(workflow.NewStdLogger()))
	must(err)
	defer s.Close()

	ctx := context.Background()
	res, err := s.Trigger(ctx, workflow.TriggerRequest{Date: "2026-09-22"})
	must(err)
	printResult("常规触发 2026-09-22", res)

	// 再次触发：全部幂等复用。
	res2, err := s.Trigger(ctx, workflow.TriggerRequest{Date: "2026-09-22"})
	must(err)
	printResult("重复触发（应全部 already_succeeded）", res2)

	// 区间补跑：09-20、09-21 实际执行，09-22 幂等复用。
	res3, err := s.Backfill(ctx, workflow.BackfillRequest{Start: "2026-09-20", End: "2026-09-22"})
	must(err)
	printResult("区间补跑 2026-09-20..22", res3)

	fmt.Printf("\n实际执行总次数=%d（每个逻辑实例只执行一次，重试除外）\n", execCount.Load())
}

func printResult(title string, r *workflow.RunResult) {
	fmt.Printf("== %s ==\n", title)
	for _, x := range r.Results {
		fmt.Printf("  %s/%s -> %s (%s) %s\n", x.Key.Date, x.Key.Task, x.Status, x.Reason, x.Err)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
