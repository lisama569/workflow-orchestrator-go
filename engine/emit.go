package engine

// Emit 在任务执行期间动态展开一个后续任务（同一逻辑键内）。
//
// 展开来源自动记录为“当前任务的当前这次执行产出”。重复上报同一批任务
// （同来源 + 同批次指纹）按一次登记，不会越跑越多。
// 展开出的任务天然依赖当前上游，无需在 Deps 中手工声明先后。
// 若展开会引入依赖环，返回 ErrCycle 且本次展开不生效。
func (tc *TaskContext) Emit(spec TaskSpec) error {
	return tc.engine.emitBatch(tc, []TaskSpec{spec}, "")
}

// EmitBatch 一次展开一批后续任务，batchID 显式标识上游本次产出的“哪一批”。
// 同一 (上游实例, 上游执行序号, batchID) 重复上报整批任务按一次算（幂等）。
func (tc *TaskContext) EmitBatch(batchID string, specs []TaskSpec) error {
	return tc.engine.emitBatch(tc, specs, batchID)
}
