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
	StatusWaiting   Status = "WAITING"   // 失败后退避中，等待 NextRetryAt 再次重试（预算已扣减）
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
	TaskID     string // 当前正在执行的任务 ID（展开来源）
	LogicalKey string
	Trigger    TriggerType
	engine     *Engine
}

// EmissionSource 记录一个展开实例的来源：
// 由哪条上游任务（同一逻辑键内）的哪次产出（批次标识）带出来。
// 静态登记的实例 Source 为 nil。
type EmissionSource struct {
	EmitterTaskID string // 展开它的上游任务 ID
	Batch         string // 上游一次产出的批次标识（如产出 ID/分片批次）
}

// Emission 记录一次运行中的一次展开动作（按 上游实例+批次 唯一），
// 同一来源重复报同一批任务只记一次、算一次，用于事后追溯。
type Emission struct {
	LogicalKey string   // 作用域：展开只在该逻辑键（业务日期）内可见
	Emitter    string   // 展开来源的上游任务 ID
	Batch      string   // 产出批次标识
	TaskIDs    []string // 该批次展开出的任务 ID（按登记顺序）
	CreatedAt  time.Time
}

// Key 返回展开记录的唯一键（作用域 + 来源）。
func (m *Emission) Key() string { return m.LogicalKey + "\x00" + m.Emitter + "\x00" + m.Batch }

// Instance 是 (TaskID, LogicalKey) 唯一确定的一次逻辑执行。
type Instance struct {
	TaskID      string
	LogicalKey  string
	Trigger     TriggerType
	Status      Status
	Attempts    int // 已发生的尝试次数；WAITING 时即已扣减的重试预算
	LastError   string
	UpdatedAt   time.Time
	NextRetryAt time.Time // WAITING 专用：下次重试的计划时间点（重启后延续）

	// Source 非 nil 表示该实例由运行时展开登记，并记录其来源。
	Source *EmissionSource
}

// IsWaiting 报告实例是否处于失败后的退避等待中（区别于执行到一半被打断）。
func (i *Instance) IsWaiting() bool { return i.Status == StatusWaiting }

// Key 返回实例的唯一键。
func (i *Instance) Key() string { return i.TaskID + "@" + i.LogicalKey }
