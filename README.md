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

状态机（终态为 `SUCCEEDED` / `FAILED` / `BLOCKED` / `INTERRUPTED`）：

```
PENDING --依赖就绪--> RUNNING --成功--> SUCCEEDED
                     RUNNING --失败但仍有预算--> RETRY_WAIT --到预定时刻--> RUNNING
                     RUNNING / RETRY_WAIT 中耗尽预算 --> FAILED
PENDING --上游 FAILED/BLOCKED/INTERRUPTED--> BLOCKED
RUNNING --进程重启（执行到一半被打断）--> INTERRUPTED
RETRY_WAIT --进程重启--> RETRY_WAIT（预算与等待计划原样保留，等待接管）
```

- 依赖就绪的实例并发执行；同一实例只会被实际执行一次（`PENDING→RUNNING`
  迁移在锁内完成，并发触发下其余请求按冲突处理）。
- 上游失败时下游标记 `BLOCKED`，原因形如
  `upstream task failed: upstream <上游实例键> failed`，可区分且直接点名
  拖垮它的上游，不会静默跳过或一直挂起。
- 依赖成环在两个时点被检测并拒绝，绝不死锁：
  1. `Submit` 时静态检测，返回 `ErrCycle` 及环路径；
  2. 运行时 `Emit`/`EmitBatch` 展开构成环时，本次展开不生效并返回 `ErrCycle`；
     调度循环中无法推进且无在途执行（含退避等待）的 `PENDING` 实例判定为环并失败。

## 重试与恢复规则

- **有限重试**：失败后进入 `RETRY_WAIT`，按绝对的预定时刻 `NextAttemptAt`
  退避重试，最多 `MaxRetries` 次（总执行次数 `MaxAttempts = 1 + MaxRetries`）；
  耗尽后进入 `FAILED` 终态，`LastError` 记录最后一次错误。
- **持久化**：每次状态迁移后全量落盘（JSON，临时文件 + rename 原子写入）。
  `RETRY_WAIT` 落盘内容包含 `Attempts`（已用次数）、`MaxAttempts`（总预算）、
  `NextAttemptAt`（下一次重试的绝对时刻）与 `Backoff`。运行时展开的任务**定义**
  （依赖、重试参数）同样随状态落盘（`dynamic_specs`）；执行体 `Run` 是闭包、
  无法持久化，重启后先以 `Run == nil` 的占位定义恢复，等待重新绑定（见下文）。
- **重启恢复**（`Open` 时），两类“没跑完”的状态严格区分：
  - 崩溃时处于 `RUNNING`：副作用不可知，标记 **`INTERRUPTED`**
    （原因 `ErrInterrupted`），表示“上次执行到一半被打断”；
  - 崩溃时处于 `RETRY_WAIT`：**保持 `RETRY_WAIT`**，已用/剩余重试次数和
    原等待计划原样保留。重触发时由新进程接管等待，剩余时间从原计划时刻
    继续计算——不会清零次数，也不会重启后立刻重试；
  - `PENDING` 保持不变；`SUCCEEDED` 实例绝不重复执行；
  - 恢复时来源上游已处于失败终态（含刚打出的 `INTERRUPTED`）的展开任务，
    立即标记 `BLOCKED` 并点名上游（多级展开链逐级级联），重启后第一眼就能
    看出归属，不会卡在 `PENDING` 或被误判为 `FAILED`。
- **重触发恢复**：对全部处于终态且存在失败的逻辑键再次触发时，仅将
  `FAILED`/`BLOCKED`/`INTERRUPTED` 实例重置为 `PENDING` 重跑（重试计数清零，
  视为新一轮执行）；重启遗留的 `RETRY_WAIT` 则按上面的接管逻辑延续等待；
  已成功实例始终保持不动。

## 补跑与幂等约定

对同一逻辑键的触发按当前实例状态分三种结果，均可区分：

| 已有实例状态                          | 结果                                                          |
|---------------------------------------|---------------------------------------------------------------|
| 全部 `SUCCEEDED`                      | 幂等跳过，返回 `nil`                                          |
| 存在本进程在途的 `RUNNING`/`RETRY_WAIT`| 拒绝，返回 `ErrConflict`（含冲突实例与触发来源）              |
| 存在重启遗留的 `RETRY_WAIT`            | 接管等待：保留预算与原计划时刻，到点继续重试                  |
| 存在 `PENDING`                        | 继续调度（与失败重置路径相同，成功实例不重跑）                |
| 存在 `FAILED`/`BLOCKED`/`INTERRUPTED`  | 仅重置失败终态实例重跑（恢复语义），成功实例不重跑            |

因此补跑区间与常规触发重叠时，同一逻辑实例不会重复执行；冲突被明确拒绝并
携带原因。

## 运行时动态展开（来源追溯 / 作用范围 / 依赖与失败归属）

任务执行中可通过 `TaskContext.Emit(spec)` 展开单个后续任务，或用
`TaskContext.EmitBatch(batchID, specs)` 一次展开一批：

- **来源追溯**：每个展开实例都记录来源 `Origin`——由哪条上游任务
  （`EmitterTaskID`/`EmitterKey` = 任务@逻辑键）、**哪一次执行**
  （`Attempt`，重试会区分不同产出）、哪一批（`BatchID`）带出，以及所属逻辑键。
  通过 `Engine.Expansions(logicalKey)` 可查某次运行“展开了哪些任务、各自来源”，
  每条记录含排序后的任务 ID 列表。
- **同来源重复上报幂等**：去重键为
  `(逻辑键, 上游实例键, 上游执行序号, BatchID)`。同一来源重复上报同一批任务
  只登记一次（`Emit` 未给批次号时以任务 ID 集合的稳定指纹作为批次），
  不会越跑越多。上游在不同重试 attempt 中的产出按不同来源分别记录。
- **作用范围限定在本次运行**：展开实例只属于当前逻辑键（业务日期）。
  其他日期的运行看不到、也不会自动实例化本次动态注册的任务；补跑某天时只看到
  那天自己的展开结果（新触发只实例化 `Submit` 注册的静态任务）。
- **天然依赖上游、失败归属明确**：展开出的任务自动依赖发起它的上游
  （无需在 `Deps` 里手工声明先后；调度时同时参考实例来源），所以只会在上游
  成功后执行；上游最终失败（或被打断）时，已经展开的任务标记 `BLOCKED`，
  原因直接点名上游实例（如 `upstream scan@k failed`），一眼可看出是被上游
  拖垮，而不是一直挂起或凭空消失。展开链中任一任务失败，其后续任务级联
  `BLOCKED`。整个链路并发运行时该先后关系由调度状态机统一保证。

### 展开任务的重启恢复

上游已展开出一批后续任务、但这些任务尚未执行完时进程退出，重启后同一天再跑：

- **定义恢复与重新绑定**：展开任务的定义（依赖、重试上限、退避）已随状态
  落盘，重启后先以占位定义恢复。执行体需要在新进程重新提供，有两条途径：
  1. 上游重跑并再次 `Emit`/`EmitBatch` 同一批时自动重新绑定——**即使该来源
     已在台账中（命中去重）也会先完成绑定**，随后已存在的展开实例照常被
     调度执行；
  2. 应用在重触发前用同一 ID 调用 `Submit` 提供定义（视为重新绑定，不会把
     该任务变成静态任务，其他业务日期仍不会实例化它）。
  若重触发时定义仍未绑定，相关实例以明确原因失败（dynamic task ... definition
  not re-registered after restart），而不会被误判成“未知任务”后永久搁置；
  补提交定义后再次触发即可恢复。
- **已展开任务继续跑完**：重触发只重置失败终态实例；上游 `INTERRUPTED` 后
  重跑成功、又把同一批任务报一遍时，去重台账保证只算一次、实例不重复创建，
  但已展开的任务会按正常先后顺序真正执行到最终结果——不会永久失败，
  也不会凭空消失。上游重跑最终失败（或再次被打断）时，已展开任务仍是
  `BLOCKED` 且点名上游，不抢跑、不挂起。
- **重试预算跨重启延续**：展开任务失败后处于 `RETRY_WAIT` 时重启，恢复后
  仍是 `RETRY_WAIT`（区别于 `INTERRUPTED`），`Attempts`/`MaxAttempts` 与
  原定 `NextAttemptAt` 原样保留；重新绑定定义并重触发后，接着原计划时刻
  等待并执行下一次尝试，不清零次数、不立刻重试、也不会直接判死。
- **来源与作用范围不丢**：实例的 `Origin`（上游任务/实例键/执行序号/批次/
  逻辑键）随实例持久化；`Expansions(day)` 多天运行时仍各自只见当天记录，
  同一来源（逻辑键 + 上游实例键 + 上游执行序号 + 批次）跨重启重复产出
  仍然只登记一次。

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
  崩溃恢复（`RUNNING→INTERRUPTED`）与干净重启后的幂等；
- `TestRetryBudgetSurvivesRestart`：退避途中重启后 `RETRY_WAIT` 状态、
  已用/剩余重试次数与原等待计划延续（不立即重试、不清零）；
- `TestBackfillIdempotentWithRegular`：补跑与常规触发并存的幂等与冲突拒绝；
- `TestDynamicExpansion`：运行时按上游产出展开后续任务；
- `TestExpansionOriginAndIdempotency`：展开来源追溯（上游/执行序号/批次）与
  同来源重复上报的幂等；
- `TestExpansionScopedByLogicalKey`：不同业务日期的展开互不干扰、补跑隔离；
- `TestEmittedTaskOrderingAndUpstreamFailure`：展开任务天然排在上游成功之后，
  上游失败时已展开任务 `BLOCKED`（点名上游）；
- `TestEmittedChainFailurePropagates`：展开链中任务失败的级联 `BLOCKED` 归属；
- `TestExpansionAlreadyEmittedResumesAcrossRestart`：重启前已展开但未执行的
  任务在重启后上游重新产出（命中来源去重）时继续按顺序执行到成功，台账只算一次；
- `TestExpandedRetryBudgetSurvivesRestart`：展开任务退避等待中重启后，
  `RETRY_WAIT`、已用/剩余重试次数与原定重试时刻延续，不立即重试、不清零；
- `TestExpansionMultiDayAndDedupAcrossRestart`：重启后多天展开结果按日期隔离、
  同来源重复产出（含跨重启）只登记一次、各任务只执行一次；
- `TestExpansionUpstreamFailsAfterRestart`：重启后上游重跑最终失败时，
  已展开任务仍被明确标记为 `BLOCKED` 并点名上游，不抢跑、不挂起；
- `TestExpandedDefinitionRebindViaSubmit`：重启后通过 `Submit` 重新提供展开
  任务定义（重新绑定）即可继续执行，且该任务仍是动态任务、不泄漏到其他日期；
- `TestExpandedUnboundDefinitionFailsClearly`：定义未重新绑定时展开任务得到
  明确失败（not re-registered after restart），不被误判未知任务、不挂死。
