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
                     RUNNING --失败仍有预算--> WAITING --NextRetryAt 到点--> RUNNING
PENDING --上游 FAILED/BLOCKED--> BLOCKED
```

- 依赖就绪的实例并发执行；同一实例只会被实际执行一次（`PENDING→RUNNING`
  迁移在锁内完成，并发触发下其余请求按冲突处理）。
- 上游失败时下游标记 `BLOCKED`，原因为 `ErrUpstreamFailed`，不会静默跳过。
- 依赖成环在两个时点被检测并拒绝，绝不死锁：
  1. `Submit` 时静态检测，返回 `ErrCycle` 及环路径；
  2. 运行时 `Emit`/`EmitBatch` 展开构成环时，**整批展开不生效**并返回
     `ErrCycle`；调度循环中无法推进且无在途执行的 `PENDING` 实例判定为环
     并失败。

## 运行时展开：来源追溯、作用域、依赖与失败归属

任务执行中可通过 `TaskContext` 动态登记同一逻辑键内的后续任务：

- `Emit(spec)`：展开单个后续任务（归到默认批次）。
- `EmitBatch(batch, specs...)`：按上游的**一次产出批次**展开一或多个任务，
  `batch` 是该次产出的业务标识（如分片 ID、产出主键）。

**来源追溯**：每个展开实例的 `Instance.Source` 记录 `EmitterTaskID`（哪条
上游任务带出）与 `Batch`（哪次产出）；同时引擎持久化展开批次记录，可用
`Engine.Emissions(logicalKey)` 列出某次运行的全部批次、各自的上游来源与带出
的任务清单，事后可完整回答“这次运行展开了哪些任务、分别从哪来”。

**批次幂等**：同一来源 `(上游任务, batch)` 重复报同一批任务（上游失败重试后
再次产出、调用方重复调用）只登记一次、只执行一次，批次清单不追加重复项，
任务不会越跑越多。

**展开作用域**：展开只在当前逻辑键（业务日期）内登记实例。触发一个新日期时
只登记 `Submit` 的静态任务，别的日期运行时展开出的任务绝不会在新日期凭空
冒出；补跑某天时也只会看到该天自己的展开结果与批次记录。

**天然依赖**：展开出的任务自动依赖展开它的上游任务（引擎注入，无需在每个
`Deps` 里手工声明先后；自展开除外），因此一定在上游成功之后才执行，整条链
并发跑时先后关系也由依赖判定保证。

**失败归属**：上游最终 `FAILED`（或被更上游 `BLOCKED`）时，已展开出来的任务
被明确标记为 `BLOCKED`，`LastError` 携带 `ErrUpstreamFailed` 及具体是被哪条
上游拖垮，不会一直挂着，也不会凭空消失。

## 重试与恢复规则

- **有限重试**：失败后按 `Backoff` 退避重试，最多 `MaxRetries` 次；耗尽后
  进入 `FAILED` 终态，`LastError` 记录最后一次错误。
- **退避等待可持久化**：失败且仍有预算时进入 `WAITING` 状态，实例上记录
  `Attempts`（已发生的尝试次数，即已扣减的重试预算）与 `NextRetryAt`
  （下一次重试的计划时间点），并立即落盘。
- **持久化**：每次状态迁移后全量落盘（实例 + 展开批次记录，JSON，
  临时文件 + rename 原子写入；可读取旧版本仅含实例的状态文件）。
- **重启恢复**（`Open` 时）按崩溃时的状态分别处理，语义确定可解释：

  | 崩溃时状态 | 恢复行为 |
  |---|---|
  | `SUCCEEDED` | 保持，绝不重跑 |
  | `RUNNING` | 判为 `FAILED`，原因 `ErrInterrupted`（执行到一半被打断，副作用不可知） |
  | `WAITING` | 保持 `WAITING`，`Attempts` 与 `NextRetryAt` 原样保留；重新挂起倒计时，**到点才按剩余预算续跑**——不清零次数、不立即重试 |
  | `PENDING` | 保持 `PENDING`，待 `Submit`/触发后继续调度 |

  因此“上次执行到一半被打断”（`FAILED/ErrInterrupted`）与“失败后还在等下一次
  重试”（`WAITING` + 未来的 `NextRetryAt`）在恢复后一眼可区分。重启后若不想
  重新触发、只想观察自动续跑，可用 `Engine.Await(ctx, logicalKey)` 等待。
- **重触发恢复**：对全部处于终态且存在失败的逻辑键再次触发时，仅将
  `FAILED`/`BLOCKED` 实例重置为 `PENDING` 重跑（重试预算重新计满），
  已成功实例保持不动。

## 补跑与幂等约定

对同一逻辑键的触发按当前实例状态分三种结果，均可区分：

| 已有实例状态                 | 结果                                             |
|------------------------------|--------------------------------------------------|
| 全部 `SUCCEEDED`             | 幂等跳过，返回 `nil`                             |
| 存在 `PENDING`/`RUNNING`/`WAITING` | 拒绝，返回 `ErrConflict`（含冲突实例与触发来源） |
| 仅 `FAILED`/`BLOCKED` 终态   | 仅重置失败实例重跑（恢复语义）                   |

因此补跑区间与常规触发重叠时，同一逻辑实例不会重复执行；冲突被明确拒绝并
携带原因。

## 动态展开

运行时展开的完整语义（来源追溯、批次幂等、作用域、自动依赖、失败归属）见
上文“运行时展开”一节。简要用法：

```go
err := tc.EmitBatch("shard-17",
    TaskSpec{ID: "process-shard-17", Run: ...}, // 依赖 scan 由引擎自动注入
)
```

`Emit(spec)` 为无批次标识的单任务展开（归默认批次）。重复展开同一实例幂等；
批次以 `(当前任务, batch)` 去重；引入依赖环的展开整批不生效并返回 `ErrCycle`。

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
  崩溃恢复（RUNNING → FAILED/ErrInterrupted）与干净重启后的幂等；
- `TestRecoveryBackoffBudgetAcrossRestart`：退避途中崩溃后 `WAITING` 的
  剩余重试预算（`Attempts`）与等待计划（`NextRetryAt`）延续——不清零、
  不提前重试，到点按剩余预算续跑；并与同次崩溃的 RUNNING 中断状态明确区分；
- `TestBackfillIdempotentWithRegular`：补跑与常规触发并存的幂等与冲突拒绝；
- `TestDynamicExpansion`：运行时按上游产出展开后续任务；
- `TestEmissionSourceTraceAndIdempotency`：来源（上游任务 + 批次）可追溯，
  同一来源重复展开按一次算、不越跑越多（含上游重试重复产出）；
- `TestEmissionScopeByLogicalKey`：不同业务日期的展开互不干扰，补跑只见当天
  自己的批次，未运行日期无任何实例/批次；
- `TestEmittedOrderingAndUpstreamFailure`：展开任务天然排在成功上游之后
  （无需手写 Deps、并发下不乱序），上游最终失败时展开任务 `BLOCKED` 且原因
  指名上游；
- `TestEmissionsPersistAcrossRestart`：展开批次记录与实例来源跨重启落盘可查。
