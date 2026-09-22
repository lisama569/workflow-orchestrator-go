# workflow-orchestrator-go

本地工作流调度引擎：在本地持久化状态上模拟任务执行，使任务依赖、失败重试与
异常恢复在常规触发、并发触发、历史补跑和进程重启下得到**一致且可解释**的结论。

## 特性

- **DAG 并发调度**：无依赖任务并发执行；成环/非法图启动即拒绝并给出具体环路，
  不死锁、不静默跳过；上游失败与自身失败原因可区分。
- **一次执行保证**：实例按 `(任务, 逻辑日期)` 唯一标识，经持久化 CAS 领取；
  并发触发同一实例只执行一次（同日期触发并入在途 Run）。
- **有限重试 + 退避**：`MaxAttempts` 与 `Backoff` 可配；尝试历史、最早重试时刻持久化。
- **崩溃恢复语义确定**：`succeeded/failed/skipped` 原样保留；`running` 回收为
  `pending` 且中断尝试计为一次失败；`pending` 按持久化的重试时刻决定是否等待。
- **动态展开**：任务可依据上游产出 `Expand` 出后续模板任务；展开边成环、
  重复父任务等非法情形明确拒绝并给原因。
- **历史补跑**：与常规触发共享实例记录，终态实例幂等复用；在途冲突可选
  `reject`（默认，明确拒绝）或 `skip_inflight`（标注跳过原因）。
- **可解释日志**：每次触发、状态流转、判定依据、退避、恢复与展开均打印日志。

## 快速开始

```bash
# 运行可工作端到端示例（含重试、重复触发与区间补跑）
go run ./cmd/workflow-demo

# 全量测试（竞态检测）
go test ./... -race -v
```

## 语义文档

- [docs/SEMANTICS.md](docs/SEMANTICS.md) —— 调度语义、重试与恢复规则、
  补跑与幂等约定、原因码表与验证方法。

## 代码结构

```
internal/workflow/
  types.go            # 核心类型、状态/原因枚举、Store 接口
  registry.go         # 任务注册表
  graph.go            # 静态 DAG 校验与成环定位
  store_memory.go     # JSON 快照 + CAS + 原子落盘的本地持久化存储
  filelock_*.go       # 快照文件进程级 flock
  scheduler.go        # 触发/补跑入口、在途合并、重启恢复
  engine.go           # 单日期 DAG 并发执行、动态任务激活
  execute.go          # 实例状态机：领取、重试退避、展开校验
  logger.go           # 日志接口与测试用捕获日志器
  date.go             # 逻辑日期工具
cmd/workflow-demo/    # 端到端示例
```
