package engine

// Emit 在任务执行期间动态展开后续任务（同一逻辑键内）。
// 若展开会引入依赖环，返回 ErrCycle 且本次展开不生效。
func (tc *TaskContext) Emit(spec TaskSpec) error {
	return tc.engine.emit(tc, spec)
}
