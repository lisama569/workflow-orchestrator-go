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
BLOCKED/INTERRUPTED/FAILED --同日重触发（展开任务且上游重报同批）--> PENDING（成功实例不重跑）
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
  `NextAttemptAt`（下一次重试的绝对时刻）与 `Backoff`。
- **重启恢复**（`Open` 时），两类“没跑完”的状态严格区分：
  - 崩溃时处于 `RUNNING`：副作用不可知，标记 **`INTERRUPTED`**
    （原因 `ErrInterrupted`），表示“上次执行到一半被打断”；
  - 崩溃时处于 `RETRY_WAIT`：**保持 `RETRY_WAIT`**，已用/剩余重试次数和
    原等待计划原样保留。重触发时由新进程接管等待，剩余时间从原计划时刻
    继续计算——不会清零次数，也不会重启后立刻重试；
  - `PENDING` 保持不变；`SUCCEEDED` 实例绝不重复执行。
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
  那天自己的展开结果（新触发只实例化 `Submit` 注册的静态任务；曾作为展开结果
  出现过的 ID 即使在重启后被重新 `Submit`，也只在其来源逻辑键内由上游产出，
  不会在别的业务日期被当成根任务凭空实例化）。
- **天然依赖上游、失败归属明确**：展开出的任务自动依赖发起它的上游
  （无需在 `Deps` 里手工声明先后；调度时同时参考实例来源），所以只会在上游
  成功后执行；上游最终失败（或被打断）时，已经展开的任务标记 `BLOCKED`，
  原因直接点名上游实例（如 `upstream scan@k failed`），一眼可看出是被上游
  拖垮，而不是一直挂起或凭空消失。展开链中任一任务失败，其后续任务级联
  `BLOCKED`。整个链路并发运行时该先后关系由调度状态机统一保证。

### 展开任务的重启恢复

动态展开的 `TaskSpec` 含执行体闭包，无法随状态落盘，因此重启恢复时展开任务
与静态任务的处理有专门约定（应用重启后应先重新 `Submit` 全部任务定义，再
触发，这与普通任务恢复一致）：

- **上游执行到一半崩溃，已展开任务尚未执行**：上游恢复为 `INTERRUPTED`，
  已展开的下游立刻被级联标记为 `BLOCKED` 并点名上游（`Open` 时即可见，
  失败归属清楚，不挂起、不消失，也不是旧实现中 `ErrUnknownTask` 的永久
  `FAILED`）。同一天再次触发时，上游与这些下游一起复位为 `PENDING` 重跑：
  - 上游重跑成功、又把同一批任务报一遍时命中台账去重（**不新增记录、不新增
    实例**），但引擎会借这次上报把丢失的动态定义补回；若下游此前因重启空档
    停在 `BLOCKED`/`FAILED`，会被复位为 `PENDING` 并按正常顺序执行到终态，
    每个任务仍只执行一次、台账仍只算一条；
  - 若应用已在重启后自行重新 `Submit` 了动态定义，则下游无需等待上游重报，
    直接按依赖顺序恢复执行。
- **定义空档不会判死**：展开任务的定义尚未补回时，只要其来源上游仍会重跑
  （非终态），任务保持 `PENDING` 等待定义，绝不直接 `FAILED`；仅当来源上游
  已不可能再产出（终态）且定义确实缺失时，才给出点名原因的 `FAILED`。
- **退避途中崩溃，重试预算跨重启延续**：展开任务处于 `RETRY_WAIT` 时崩溃，
  与普通任务完全一致——恢复后仍是 `RETRY_WAIT`，`Attempts`（已用次数）、
  `MaxAttempts`（总预算）、`NextAttemptAt`（原计划重试绝对时刻）原样保留，
  既不清零次数、不重启后立刻重试，也不判死；重新触发后由新进程接管等待，
  剩余时间从原计划时刻继续计算。到点执行所需的动态定义在启动时由应用
  重新 `Submit` 补回。
- **恢复状态区分**：上游 `RUNNING→INTERRUPTED`（执行到一半被打断）与其下游
  `PENDING→BLOCKED`（被上游拖垮）严格区分于失败退避中的
  `RETRY_WAIT`（原样保留、接管等待）；三者在日志与 `LastError` 中均可直接辨认。
- **多天与重复产出在重启后依旧互不干扰**：展开台账按 `(逻辑键, 上游实例键,
  执行序号, BatchID)` 持久化去重。重启后不同业务日期各自只能看到自己的展开
  记录与实例；同来源重复报同一批，无论发生在同一次执行、上游重跑还是重启
  之后，都只登记一次、任务只执行一次。

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
- `TestEmittedTasksResumeAfterRestart`：上游展开一批任务后崩溃，重启后同一天
  重跑、上游重报同一批（命中去重）时，已展开任务不永久失败、不消失，
  按正常顺序各执行一次到成功，台账跨重启只算一条；
- `TestEmittedTaskBlockedByInterruptedUpstreamAfterRestart`：重启后不重跑时，
  被打断上游的已展开任务级联 `BLOCKED` 且点名上游，不挂起；
- `TestEmittedRetryBudgetSurvivesRestart`：展开任务退避途中重启后 `RETRY_WAIT`
  状态、已用/剩余次数与原等待计划延续（不立即重试、不清零、不判死）；
- `TestExpansionScopeAndIdempotencyAcrossRestart`：重启后多天运行各自只看到
  自己的展开结果、动态定义不在别的日期生根，同来源重复产出只算一次。
