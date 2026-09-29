package engine

// Emit 在任务执行期间动态展开一个后续任务（同一逻辑键内）。
// 展开出的任务自动依赖当前上游任务，无需手工声明先后；重复展开同一实例幂等。
// 若展开会引入依赖环，返回 ErrCycle 且本次批次整体不生效。
//
// 注意：Emit 不携带批次标识，追溯时统一归到默认批次；需要按上游产出
// （如分片 ID）精确追溯与批次幂等时，请用 EmitBatch。
func (tc *TaskContext) Emit(spec TaskSpec) error {
	return tc.engine.emitBatch(tc, "", []TaskSpec{spec})
}

// EmitBatch 按上游的一次产出批次展开一或多个后续任务（同一逻辑键内）。
//
//   - 批次由 (当前任务, batch) 标识一次上游产出：同一来源重复报同一批任务
//     只登记一次、只执行一次，批次清单可事后经 Engine.Emissions 追溯；
//   - 展开出的任务自动依赖当前上游任务，上游成功后才能执行；
//     上游最终失败（或被更上游 BLOCKED）时，这些任务得到 BLOCKED 终态，
//     原因为 ErrUpstreamFailed，可一眼看出是被上游拖垮，不会悬挂或消失；
//   - 展开作用域为当前逻辑键（业务日期）：别的日期的运行看不到本次展开，
//     补跑某天时也只会看到该天自己的展开结果；
//   - 批次内任一规格与现有规格成环时整批不生效并返回 ErrCycle。
func (tc *TaskContext) EmitBatch(batch string, specs ...TaskSpec) error {
	return tc.engine.emitBatch(tc, batch, specs)
}
