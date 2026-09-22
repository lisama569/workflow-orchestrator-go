// Package engine 实现本地工作流调度引擎：依赖编排、失败重试、
// 持久化恢复、动态任务展开与补跑幂等。
package engine

import (
	"context"
	"errors"
	"time"
)

// Status 表示任务实例的生命周期状态。
type Status string

const (
	StatusPending   Status = "PENDING"   // 已登记，等待依赖就绪
	StatusRunning   Status = "RUNNING"   // 正在执行
	StatusSucceeded Status = "SUCCEEDED" // 执行成功（终态，幂等）
	StatusFailed    Status = "FAILED"    // 重试耗尽后失败（终态）
	StatusBlocked   Status = "BLOCKED"   // 上游失败导致无法执行（终态）
)

// 可区分的失败/冲突原因，调用方可用 errors.Is 判定。
var (
	ErrCycle          = errors.New("dependency cycle detected")
	ErrUpstreamFailed = errors.New("upstream task failed")
	ErrConflict       = errors.New("trigger conflicts with existing instance")
	ErrUnknownTask    = errors.New("unknown task")
	ErrInterrupted    = errors.New("interrupted by process restart")
)

// TriggerType 区分实例的触发来源。
type TriggerType string

const (
	TriggerRegular  TriggerType = "REGULAR"
	TriggerBackfill TriggerType = "BACKFILL"
)

// TaskSpec 描述一个任务的静态定义。
type TaskSpec struct {
	ID         string
	Deps       []string // 依赖的任务 ID（同一逻辑实例键内）
	MaxRetries int      // 失败后的最大重试次数
	Backoff    time.Duration
	Run        func(ctx context.Context, tc *TaskContext) error
}

// TaskContext 在执行时传给任务，用于动态展开下游任务。
type TaskContext struct {
	LogicalKey string
	Trigger    TriggerType
	engine     *Engine
}

// Instance 是 (TaskID, LogicalKey) 唯一确定的一次逻辑执行。
type Instance struct {
	TaskID     string
	LogicalKey string
	Trigger    TriggerType
	Status     Status
	Attempts   int
	LastError  string
	UpdatedAt  time.Time
}

// Key 返回实例的唯一键。
func (i *Instance) Key() string { return i.TaskID + "@" + i.LogicalKey }
