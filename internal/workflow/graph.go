package workflow

import "fmt"

// validateGraph 校验注册表构成的静态任务图：
//  1. 所有 DependsOn 必须指向已注册任务，否则返回 ErrInvalidDAG；
//  2. 不允许成环，否则返回 ErrCycleDetected 并附带一条具体环路（含首尾重复节点）。
//
// 动态展开只“显式化”已注册节点，不引入新边，因此运行前对全量
// 静态图做一次校验即可保证任何展开序列都不会产生环。
//
// 返回值 cycle 为成环路径（仅在 err 包装 ErrCycleDetected 时非空）。
func validateGraph(reg *Registry) (cycle []string, err error) {
	const (
		white = 0
		gray  = 1 // 在当前 DFS 栈中
		black = 2
	)
	color := make(map[string]int)
	var stack []string

	var dfs func(name string) error
	dfs = func(name string) error {
		color[name] = gray
		stack = append(stack, name)
		spec, _, ok := reg.Get(name)
		if !ok {
			return fmt.Errorf("%w: task %q", ErrUnknownTask, name)
		}
		for _, dep := range spec.DependsOn {
			depSpec, _, depOK := reg.Get(dep)
			if !depOK {
				return fmt.Errorf("%w: task %q depends on unregistered task %q",
					ErrInvalidDAG, name, dep)
			}
			if depSpec.Dynamic {
				return fmt.Errorf("%w: task %q has static dependency on dynamic template %q; dynamic tasks must be reached via Expand",
					ErrInvalidDAG, name, dep)
			}
			switch color[dep] {
			case white:
				if err := dfs(dep); err != nil {
					return err
				}
			case gray:
				// 从栈中 dep 首次出现的位置截取具体环路
				for i := len(stack) - 1; i >= 0; i-- {
					if stack[i] == dep {
						cycle = append(append([]string{}, stack[i:]...), dep)
						break
					}
				}
				return fmt.Errorf("%w: %v", ErrCycleDetected, cycle)
			}
		}
		color[name] = black
		stack = stack[:len(stack)-1]
		return nil
	}

	for _, name := range reg.Names() {
		if color[name] == white {
			if err := dfs(name); err != nil {
				return cycle, err
			}
		}
	}
	return nil, nil
}
