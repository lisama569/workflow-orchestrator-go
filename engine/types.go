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
	StatusPending     Status = "PENDING"     // 已登记，等待依赖就绪
	StatusRunning     Status = "RUNNING"     // 正在执行
	StatusRetryWait   Status = "RETRY_WAIT"  // 失败后退避中，等待预定时刻重试（非终态：预算与等待计划已持久化）
	StatusSucceeded   Status = "SUCCEEDED"   // 执行成功（终态，幂等）
	StatusFailed      Status = "FAILED"      // 重试耗尽后失败（终态）
	StatusBlocked     Status = "BLOCKED"     // 上游失败导致无法执行（终态）
	StatusInterrupted Status = "INTERRUPTED" // 重启时仍在 RUNNING：执行到一半被打断（终态，待重触发）
)

// IsTerminal 报告状态是否为终态。
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusBlocked, StatusInterrupted:
		return true
	}
	return false
}

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
	TaskID     string // 当前正在执行的上游任务 ID（展开来源）
	LogicalKey string
	Trigger    TriggerType
	Attempt    int // 当前执行尝试序号（从 1 起），用于标识“哪次产出”
	engine     *Engine
}

// Instance 是 (TaskID, LogicalKey) 唯一确定的一次逻辑执行。
type Instance struct {
	TaskID     string
	LogicalKey string
	Trigger    TriggerType
	Status     Status
	Attempts   int // 已实际执行的次数
	LastError  string
	UpdatedAt  time.Time

	// 重试预算与退避计划（RETRY_WAIT 时持久化，重启后延续）。
	MaxAttempts   int       // 本轮允许的最大执行次数（1 + MaxRetries）
	NextAttemptAt time.Time // 下一次重试的预定时刻（绝对时间，跨重启保留）
	Backoff       time.Duration

	// 运行时展开的来源追溯：该实例由哪条上游任务、哪次执行的哪批产出展开而来。
	// 静态登记的实例为零值。
	Origin ExpansionOrigin
}

// ExpansionOrigin 描述一个展开实例的来源（哪个上游的哪次产出）。
type ExpansionOrigin struct {
	EmitterTaskID string // 发起展开的上游任务 ID
	EmitterKey    string // 上游实例键（TaskID@LogicalKey）
	Attempt       int    // 上游的第几次执行产出
	BatchID       string // 该次产出中的批次标识（同一来源重复报同一批按一次算）
	LogicalKey    string // 展开作用范围：仅在该逻辑键（业务日期）内可见
	EmittedAt     time.Time
}

// DynamicSpec 是运行时展开任务定义中可持久化的部分。
// 执行体 Run 是闭包、无法落盘：重启后一律以 nil-Run 的占位定义恢复，
// 由应用重新 Submit 同一 ID，或上游重跑再次 Emit 时完成重新绑定
// （落盘定义恒为“待重新绑定”状态，不需要额外标志位）。
type DynamicSpec struct {
	Deps       []string      `json:"deps"`
	MaxRetries int           `json:"max_retries"`
	Backoff    time.Duration `json:"backoff"`
}

// ExpansionRecord 是一条“来源 -> 展开结果”的可追溯记录。
// 唯一去重键为 (LogicalKey, EmitterKey, Attempt, BatchID)：
// 同一来源重复上报同一批任务按一次登记，不会越跑越多。
type ExpansionRecord struct {
	EmitterTaskID string    `json:"emitter_task_id"`
	EmitterKey    string    `json:"emitter_key"`
	Attempt       int       `json:"attempt"`
	BatchID       string    `json:"batch_id"`
	LogicalKey    string    `json:"logical_key"`
	EmittedAt     time.Time `json:"emitted_at"`
	TaskIDs       []string  `json:"task_ids"` // 该批展开出的任务 ID（排序）
}

// Key 返回实例的唯一键。
func (i *Instance) Key() string { return i.TaskID + "@" + i.LogicalKey }
