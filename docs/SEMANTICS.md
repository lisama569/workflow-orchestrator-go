# 本地工作流调度引擎：调度语义说明

本引擎在单机本地持久化状态（JSON 快照 + 文件锁）上模拟任务执行，目标是让
**任务依赖、失败重试、异常恢复在各种触发方式（常规触发 / 并发触发 / 历史补跑 / 进程重启）下
得到一致且可解释的结论**。

代码包：`internal/workflow`；可运行示例：`cmd/workflow-demo`。

---

## 1. 核心模型

| 概念 | 含义 |
| --- | --- |
| Task | 任务定义。`DependsOn` 声明静态依赖；`Dynamic=true` 表示“展开模板”任务 |
| 逻辑实例 | 由 `(任务名, 逻辑日期 YYYY-MM-DD)` 唯一确定（`InstanceKey`） |
| 常规触发 | `Trigger{Date}`，对某一逻辑日期跑一遍 DAG |
| 区间补跑 | `Backfill{Start,End}`，对闭区间内每个日期按日历顺序触发 |
| Record | 逻辑实例的持久化记录（状态、尝试历史、产出、展开结果、父任务） |

**关键约定：常规触发与补跑只是“到达同一逻辑实例的不同入口”，它们读写同一份
`(任务,日期)` 记录。** 因此“同一逻辑实例只执行一次”由持久化状态保证，而不是由调用方自觉保证。

## 2. 调度语义（DAG + 并发）

1. 启动时静态校验任务图：
   - 依赖了未注册任务、静态依赖指向 `Dynamic` 模板、任务重名 → `ErrInvalidDAG`；
   - 存在依赖环 → `ErrCycleDetected`，错误中带具体环路（如 `[a c b a]`）。
   - 校验失败则拒绝启动：**不死锁、不静默跳过任何任务**。
2. 通过校验后，无依赖关系的任务**并发执行**；任务等待全部直接依赖结束后再判定。
3. 任一依赖未成功（`failed`/`skipped`），该任务确定性跳过，状态 `skipped`，
   原因 `upstream_failed`，错误信息指出是哪个上游、以什么状态结束的
   （例：`dependency "a" ended with failed(attempts_exhausted)`）。
   跳过会沿依赖链传播，且与“真正执行失败”明确区分。
4. 进程内同一逻辑日期只允许一个在途 Run：
   - 常规触发并发到达时，只有第一个调用真正调度，其余**并入等待**（结果 `Joined=true`）；
   - 即使绕过在途合并（如跨进程），实例领取仍走持久化 CAS（见 §3），并发安全。
5. 任务 panic 会被捕获为 Run 错误，不会导致进程崩溃或永久挂起。

## 3. 状态机与“只执行一次”

每个实例在持久化存储中按 CAS（compare-and-set）推进状态：

```
pending ──领取(CAS pending→running)──▶ running
  ▲                                     ├─ 成功 ─────────────▶ succeeded(终态)
  │                                     ├─ 失败且仍有次数 ───▶ pending(NextRunAt=now+Backoff)
  └─────────────────────────────────────┤
                                        └─ 失败且次数耗尽 ───▶ failed(终态)
```

- 领取动作是 `Put(rec, expect=pending)`：只有当前状态恰为 `pending` 才成功。
  两个执行者并发领取同一实例时，只有一个能成功 → **同一任务实际执行一次**。
- 终态短路：
  - `succeeded`：任何触发都直接复用，原因 `already_succeeded`，**绝不重复执行**；
  - `failed`：结论保持为 `attempts_exhausted`（或 `dynamic_cycle`），不会因为再次触发而重跑；
  - `skipped`：结论保持为 `upstream_failed`。
- 每次成功的 `Put` 都立即把全量快照 fsync + 原子 rename 落盘。

## 4. 失败重试与退避

- `TaskSpec.MaxAttempts` 为总尝试次数（含首次，<=0 视为 1）；`Backoff` 为退避时长。
- 第 n 次失败后若 `n < MaxAttempts`：状态回 `pending`，记录 `NextRunAt=now+Backoff`，
  本次 Run 内睡过退避窗口后自动重试；每次尝试的起止时间、产出、错误都持久化在 `Attempts` 中。
- 重试期间进程退出：`NextRunAt` 已落盘，下次触发时若退避窗口已过则立即重试，
  未到则等待至该时刻——**“何时可以重试”由持久化状态决定，不依赖内存定时器**。
- 次数耗尽：状态 `failed`，原因 `attempts_exhausted`，此后结论稳定（§3）。
- 退避时钟可通过 `WithClock` 注入，测试中可精确控制等待。

## 5. 进程重启恢复规则

`NewScheduler` 时先读图，再扫描全部持久化记录执行恢复，规则确定且可解释：

| 重启前状态 | 恢复动作 |
| --- | --- |
| `succeeded` / `failed` / `skipped` | 原样保留（终态稳定，已完成任务不重复执行） |
| `pending`（含等待退避） | 原样保留；下次触发按 `NextRunAt` 决定立即重试还是等待 |
| `running` | 该次尝试结局未知（可能成功也可能失败）。**记为一次失败尝试**，状态回收为 `pending`，尝试记录写明 `interrupted by process restart; attempt outcome unknown` |

`running→pending` 的恢复同样用 CAS（期望恰为 `running`），中断尝试计入总次数，
因此“崩溃-重启”不会让一个任务获得无限次重试机会。恢复过程逐条打印日志。

持久化采用“临时文件 fsync 后原子 rename”，崩溃边界清晰：要么看到上一份完整快照，
要么看到新一份完整快照，不会出现半截文件。快照文件还有 `flock` 进程级排他锁，
同一时刻只有一个调度进程能打开同一状态文件。

### 动态展开的恢复

父任务展开子任务时，会先把 `子任务→父任务` 的隐式依赖边和子任务名（`Expanded`）、
父任务名（记录中的 `Parent`）持久化，再执行业务逻辑。重启后：

- 父在 `running`：按上面的规则回收，已展开的子任务按 `Expanded` 重新激活；
- 子在 `running`：按一般 `running` 规则回收，按 `Parent` 重新挂上隐式父依赖；
- 父已成功、子未完成：下次触发时子任务从持久化记录恢复调度（不会误判为“未展开”）。

## 6. 运行时动态展开

- 普通任务在 Run 开始即可调度；`Dynamic` 模板任务**只能**由某个任务的
  `Expand(ctx, ExecRequest)` 返回其名字后启动（`Expand` 能读到直接上游产出，
  从而“根据上游产出展开后续任务”）。
- 动态子任务**隐式依赖展开它的父任务**：父成功才执行；父失败/跳过则子 `upstream_failed`。
- 展开时做强校验，违规明确报错而不是含糊处理：
  - 子任务未注册 → 包装 `ErrUnknownTask`；
  - 展开了一个非 `Dynamic` 的普通任务 → `ErrInvalidDAG`；
  - 同一动态任务被两个不同父任务展开 → `ErrRejected`（身份必须确定）；
  - 新的父子边与“静态边 ∪ 已有隐式边”构成环 → 父任务落 `failed`，
    原因 `dynamic_cycle`，记录稳定标记，结果中可看到具体环路（如 `[d m p d]`）。
- 始终没有被任何父任务展开的动态模板：结果为 `skipped / not_expanded`
 ——明确的“未被需要”，而不是静默消失。

## 7. 补跑与幂等约定

- `Backfill{Start,End,Policy}` 对闭区间（真实日历日期校验）逐日触发；
  补跑与常规触发共享同一套实例记录。
- 幂等：补跑到已经 `succeeded` 的日期时，原因是 `already_succeeded`，任务不执行；
  `failed`/`skipped` 同样按稳定结论返回。**补跑与常规触发并存不会重复执行同一逻辑实例。**
- 与在途 Run 的冲突必须显式选择策略，不允许含糊覆盖：
  - `PolicyReject`（默认）：区间内任一日期存在在途 Run，整个补跑立即返回 `ErrRejected`，
    错误指出冲突日期；
  - `PolicySkipInFlight`：不等待在途 Run，冲突日期的实例立即返回
    `pending / inflight_skipped` 并列入 `RunResult.SkippedInFlight`，在途 Run 结束后
    再次补跑则得到正常的 `already_succeeded` 等幂等结论。
- 常规触发遇到同日期在途 Run：并入等待（`Joined=true`），拿到的是同一份结论。

## 8. 结果原因码（判定依据）

| Reason | 含义 |
| --- | --- |
| `executed` | 本次实际执行并成功 |
| `already_succeeded` | 此前已成功，幂等复用，未执行 |
| `retry_scheduled` | 失败后已安排退避重试（如触发被 ctx 取消） |
| `attempts_exhausted` | 重试次数耗尽，任务失败 |
| `upstream_failed` | 依赖未成功而确定性跳过 |
| `cycle_detected` | 静态 DAG 成环（启动即拒绝） |
| `invalid_dag` | 非法任务图（未知依赖、静态依赖动态模板等） |
| `dynamic_cycle` | 动态展开边引入环 |
| `not_expanded` | 动态模板任务没有被任何父任务展开 |
| `inflight_skipped` | 补跑按 skip_inflight 策略跳过在途实例 |

每次状态流转、跳过判定、退避等待、恢复动作都通过 `Logger` 打印
（`触发/状态流转/判定/等待退避/恢复/动态展开` 前缀），日志中包含任务、日期、
尝试序号与判定依据。测试使用 `CaptureLogger` 捕获并在 `t.Log` 中打印这些流转。

## 9. 验证方法

```bash
# 全量单测（含 -race 竞态检测）
go test ./... -race -v

# 只跑关键场景
go test ./... -v -run 'TestCycleDetected|TestUpstreamFailure|TestRetryWithBackoff'
go test ./... -v -run 'TestRestartRecovery|TestConcurrentTriggerExecutesOnce'
go test ./... -v -run 'TestDynamicExpansion|TestDynamicCycle|TestBackfillIdempotent'

# 可运行示例（重试、重复触发、区间补跑）
go run ./cmd/workflow-demo
```

测试覆盖矩阵：

| 场景 | 测试 | 断言要点 |
| --- | --- | --- |
| 依赖成环 | `TestCycleDetected` | 返回 `ErrCycleDetected` 且带环路；拒绝启动、无死锁 |
| 上游失败 | `TestUpstreamFailure` | 根因 `attempts_exhausted`，下游全部 `upstream_failed`，下游零执行 |
| 失败重试/退避 | `TestRetryWithBackoff` | 恰好 3 次尝试、尝试记录完整、成功后清理重试时刻 |
| 耗尽后稳定 | `TestAttemptsExhaustedStable` | 二次触发结论不变、不再执行 |
| 重启恢复 | `TestRestartRecovery` | running 回收为 pending 且中断计次；succeeded 不重复执行 |
| 并发重复触发 | `TestConcurrentTriggerExecutesOnce` | 8 并发触发仅 1 次实际执行，其余并入 |
| 动态展开 | `TestDynamicExpansion` | 按上游产出展开；子读父产出；未展开模板 `not_expanded`；二次触发幂等 |
| 动态成环 | `TestDynamicCycle` | `dynamic_cycle`，环内任务不执行 |
| 补跑幂等/冲突 | `TestBackfillIdempotent` | 区间补跑不重复执行；在途冲突 reject / skip_inflight 两种结论可区分 |
