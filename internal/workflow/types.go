// Package workflow 实现一个本地持久化的工作流调度引擎。
//
// 核心概念：
//   - Task：任务定义（DAG 节点），通过 DependsOn 声明静态依赖，
//     Handler 可在运行时根据上游产出动态展开子任务。
//   - Instance：任务的一次“逻辑实例”，由 (任务名, 逻辑日期) 唯一确定。
//     常规触发与历史补跑若指向同一逻辑实例，只允许实际执行一次。
//   - Run：一次触发（常规触发或区间补跑）的执行上下文。
//   - Store：实例状态的持久化存储；进程重启后由 Store 恢复。
package workflow

import (
	"context"
	"errors"
	"time"
)

// Source 标识一次触发的来源。
type Source string

const (
	// SourceRegular 常规触发（通常对应当前调度区间）。
	SourceRegular Source = "regular"
	// SourceBackfill 历史区间补跑。
	SourceBackfill Source = "backfill"
)

// Status 是任务逻辑实例的持久化状态。
type Status string

const (
	// StatusPending 尚未开始执行。
	StatusPending Status = "pending"
	// StatusRunning 已被领取并开始执行（非终态，重启时有确定恢复语义）。
	StatusRunning Status = "running"
	// StatusSucceeded 执行成功（终态，任何触发都不会重复执行）。
	StatusSucceeded Status = "succeeded"
	// StatusFailed 执行失败（终态或可重试耗尽后的状态）。
	StatusFailed Status = "failed"
	// StatusSkipped 因上游失败/跳过而未执行（可区分的终态，并非静默跳过）。
	StatusSkipped Status = "skipped"
)

// Reason 解释实例结果的判定依据。
type Reason string

const (
	ReasonExecuted          Reason = "executed"           // 本次触发实际执行
	ReasonAlreadySucceeded  Reason = "already_succeeded"  // 此前已成功，幂等复用
	ReasonRetryScheduled    Reason = "retry_scheduled"    // 失败后安排退避重试
	ReasonAttemptsExhausted Reason = "attempts_exhausted" // 重试次数耗尽
	ReasonUpstreamFailed    Reason = "upstream_failed"    // 上游失败导致跳过
	ReasonCycleDetected     Reason = "cycle_detected"     // DAG 成环
	ReasonInvalidDAG        Reason = "invalid_dag"        // 依赖未知任务等非法 DAG
	ReasonDynamicCycle      Reason = "dynamic_cycle"      // 动态展开边引入环
	ReasonNotExpanded       Reason = "not_expanded"       // 动态模板任务无人展开
	ReasonInFlightSkipped   Reason = "inflight_skipped"   // 补跑策略：跳过在途实例
)

// ErrCycleDetected DAG 中检测到环。
var ErrCycleDetected = errors.New("workflow: cycle detected in task dependencies")

// ErrInvalidDAG DAG 非法（如依赖了未注册的任务）。
var ErrInvalidDAG = errors.New("workflow: invalid task graph")

// ErrUnknownTask 引用了未注册的任务（含动态展开的子任务）。
var ErrUnknownTask = errors.New("workflow: unknown task")

// ErrRejected 触发因并发冲突被明确拒绝。
var ErrRejected = errors.New("workflow: trigger rejected due to conflicting in-flight instance")

// ErrDynamicCycle 动态展开出的父子边与静态依赖构成环。
var ErrDynamicCycle = errors.New("workflow: dynamic expansion introduces a cycle")

// ErrInvalidRequest 请求参数非法（如日期格式/区间错误）。
var ErrInvalidRequest = errors.New("workflow: invalid trigger request")

// InstanceKey 是任务逻辑实例的唯一标识：任务名 + 逻辑日期。
// 常规触发与补跑对同一逻辑日期的同一任务得到同一个 InstanceKey。
type InstanceKey struct {
	Task string
	Date string // 逻辑日期，格式 YYYY-MM-DD
}

// Attempt 记录一次执行尝试。
type Attempt struct {
	Number    int       `json:"number"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Output    string    `json:"output,omitempty"`
	Err       string    `json:"error,omitempty"`
}

// Record 是一个逻辑实例的持久化状态记录。
type Record struct {
	Key       InstanceKey `json:"key"`
	Status    Status      `json:"status"`
	Attempts  []Attempt   `json:"attempts"`
	Output    string      `json:"output,omitempty"`
	Err       string      `json:"err,omitempty"`
	Source    Source      `json:"source"`           // 首次执行该实例的触发来源
	Parent    string      `json:"parent,omitempty"` // 动态模板任务的展开父任务（隐式依赖）
	UpdatedAt time.Time   `json:"updated_at"`
	NextRunAt time.Time   `json:"next_run_at,omitempty"` // 退避后的最早重试时刻
	Expanded  []string    `json:"expanded,omitempty"`    // 已展开的动态子任务名
}

// Store 持久化接口。所有写操作需保证原子性（崩溃一致性）。
type Store interface {
	// Get 读取实例记录；不存在返回 (nil, nil)。
	Get(ctx context.Context, key InstanceKey) (*Record, error)
	// Put 以 CAS 语义写入：expect 为期望的当前状态（nil 表示记录不存在），
	// 当前状态与 expect 不符时返回 false。
	Put(ctx context.Context, rec *Record, expect *Status) (bool, error)
	// ListByDate 列出某逻辑日期下的全部实例记录（用于恢复）。
	ListByDate(ctx context.Context, date string) ([]*Record, error)
	// List 列出全部实例记录（进程启动恢复时使用）。
	List(ctx context.Context) ([]*Record, error)
	// Close 释放资源。
	Close() error
}

// TaskSpec 任务定义。
type TaskSpec struct {
	Name      string
	DependsOn []string
	// MaxAttempts 最大执行次数（含首次），<=0 表示 1。
	MaxAttempts int
	// Backoff 失败后退避时长；重试不早于 上次结束时间 + Backoff。
	Backoff time.Duration
	// Dynamic 为 true 时该任务是“展开模板”：不作为根任务调度，
	// 只有被某个已完成任务的 Expand 显式列出时才会执行。
	Dynamic bool
}

// ExecRequest 传给任务 Handler 的执行请求。
type ExecRequest struct {
	Key         InstanceKey
	Attempt     int               // 从 1 开始
	UpstreamOut map[string]string // 直接上游任务名 -> 产出
}

// ExecResult Handler 的返回。
type ExecResult struct {
	Output string
}

// ExpandResult 指示运行时动态展开的子任务。
type ExpandResult struct {
	// Children 子任务名，必须已在 Registry 注册。
	Children []string
}

// TaskHandler 任务处理逻辑。Execute 执行业务；Expand 根据上游产出展开子任务。
type TaskHandler interface {
	Execute(ctx context.Context, req ExecRequest) (ExecResult, error)
	Expand(ctx context.Context, req ExecRequest) (ExpandResult, error)
}

// TriggerRequest 一次常规触发请求。
type TriggerRequest struct {
	Date string // 逻辑日期 YYYY-MM-DD
}

// ConflictPolicy 补跑与在途常规触发冲突时的策略。
type ConflictPolicy string

const (
	// PolicyReject 若目标区间内存在在途实例则整体拒绝（默认，冲突明确暴露）。
	PolicyReject ConflictPolicy = "reject"
	// PolicySkipInFlight 跳过在途实例，但对终态实例做幂等复用/原因区分。
	PolicySkipInFlight ConflictPolicy = "skip_inflight"
)

// BackfillRequest 历史区间补跑请求，区间为闭区间 [Start, End]。
type BackfillRequest struct {
	Start  string // YYYY-MM-DD
	End    string // YYYY-MM-DD
	Policy ConflictPolicy
}

// InstanceResult 单个逻辑实例在某次 Run 中的结论。
type InstanceResult struct {
	Key    InstanceKey
	Status Status
	Reason Reason
	Err    string
}

// RunResult 一次触发的汇总结果。
type RunResult struct {
	Source    Source
	Date      string
	End       string // 补跑结束日期；常规触发为空
	Results   []InstanceResult
	CyclePath []string // 成环时的环路节点
	// Joined 为 true 表示本次调用并入了另一个并发在途 Run（未新发起调度）。
	Joined bool
	// SkippedInFlight 为 PolicySkipInFlight 下跳过的在途实例。
	SkippedInFlight []InstanceKey
}
