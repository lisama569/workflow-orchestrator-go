# workflow-orchestrator-go

本地工作流调度引擎：以本地持久化状态模拟任务执行，保证任务依赖、失败重试与
异常恢复在各种触发方式下得到一致且可解释的结论。

## 核心概念

- **TaskSpec**：任务的静态定义（ID、依赖 `Deps`、重试上限 `MaxRetries`、退避
  `Backoff`、执行体 `Run`）。
- **Instance（实例）**：由 `(TaskID, LogicalKey)` 唯一确定的一次逻辑执行，
  是幂等的最小单位。`LogicalKey` 通常为业务日期（如 `2026-09-22`）。
- **TriggerType**：`REGULAR`（常规触发）与 `BACKFILL`（历史补跑）共用同一
  实例键空间，因此天然互斥去重。

## 调度语义

状态机（终态为 `SUCCEEDED` / `FAILED` / `BLOCKED`）：

```
PENDING --依赖就绪--> RUNNING --成功--> SUCCEEDED
                     RUNNING --失败且重试耗尽--> FAILED
PENDING --上游 FAILED/BLOCKED--> BLOCKED
```

- 依赖就绪的实例并发执行；同一实例只会被实际执行一次（`PENDING→RUNNING`
  迁移在锁内完成，并发触发下其余请求按冲突处理）。
- 上游失败时下游标记 `BLOCKED`，原因为 `ErrUpstreamFailed`，不会静默跳过。
- 依赖成环在两个时点被检测并拒绝，绝不死锁：
  1. `Submit` 时静态检测，返回 `ErrCycle` 及环路径；
  2. 运行时 `Emit` 展开构成环时，本次展开不生效并返回 `ErrCycle`；
     调度循环中无法推进且无在途执行的 `PENDING` 实例判定为环并失败。

## 重试与恢复规则

- **有限重试**：失败后按 `Backoff` 退避重试，最多 `MaxRetries` 次；耗尽后
  进入 `FAILED` 终态，`LastError` 记录最后一次错误。
- **持久化**：每次状态迁移后全量落盘（JSON，临时文件 + rename 原子写入）。
- **重启恢复**（`Open` 时）：崩溃时处于 `PENDING`/`RUNNING` 的实例统一判定为
  `FAILED`，原因 `ErrInterrupted` —— 因为其副作用不可知，语义确定且可解释；
  `SUCCEEDED` 实例绝不重复执行。
- **重触发恢复**：对全部处于终态且存在失败的逻辑键再次触发时，仅将
  `FAILED`/`BLOCKED` 实例重置为 `PENDING` 重跑，已成功实例保持不动。

## 补跑与幂等约定

对同一逻辑键的触发按当前实例状态分三种结果，均可区分：

| 已有实例状态            | 结果                                             |
|-------------------------|--------------------------------------------------|
| 全部 `SUCCEEDED`        | 幂等跳过，返回 `nil`                             |
| 存在 `PENDING`/`RUNNING`| 拒绝，返回 `ErrConflict`（含冲突实例与触发来源） |
| 存在 `FAILED`/`BLOCKED` | 仅重置失败实例重跑（恢复语义）                   |

因此补跑区间与常规触发重叠时，同一逻辑实例不会重复执行；冲突被明确拒绝并
携带原因。

## 动态展开

任务执行中可通过 `TaskContext.Emit(spec)` 在同一逻辑键内登记后续任务
（如按上游产出的分片展开）。重复 `Emit` 同一实例为幂等空操作；引入依赖环的
`Emit` 返回 `ErrCycle` 且不生效。

## 验证方法

```bash
go test ./... -race -count=1 -v
```

测试覆盖（日志中打印状态流转与判定依据）：

- `TestDAGConcurrentOrder`：依赖顺序与并发执行；
- `TestConcurrentTriggerExactlyOnce`：并发触发下恰好执行一次，其余按冲突拒绝；
- `TestUpstreamFailedBlocksDownstream`：上游失败 → 下游 `BLOCKED`，原因可区分；
- `TestCycleDetected` / `TestEmitCycleRejected`：静态与运行时成环检测，不死锁；
- `TestRetryWithBackoff` / `TestRetryExhausted`：退避重试与重试耗尽；
- `TestRecoveryAfterRestart` / `TestRecoveryPersistsAcrossCleanRestart`：
  崩溃恢复与干净重启后的幂等；
- `TestBackfillIdempotentWithRegular`：补跑与常规触发并存的幂等与冲突拒绝；
- `TestDynamicExpansion`：运行时按上游产出展开后续任务。
