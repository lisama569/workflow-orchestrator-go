package workflow

import (
	"fmt"
	"sort"
	"sync"
)

// Registry 保存任务名 -> 定义/处理器 的注册表。
type Registry struct {
	mu    sync.RWMutex
	tasks map[string]*taskEntry
}

type taskEntry struct {
	spec    TaskSpec
	handler TaskHandler
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{tasks: make(map[string]*taskEntry)}
}

// Register 注册一个任务；重复注册返回错误。
func (r *Registry) Register(spec TaskSpec, handler TaskHandler) error {
	if spec.Name == "" {
		return fmt.Errorf("%w: empty task name", ErrInvalidDAG)
	}
	if handler == nil {
		return fmt.Errorf("%w: task %q has nil handler", ErrInvalidDAG, spec.Name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tasks[spec.Name]; ok {
		return fmt.Errorf("%w: duplicate task %q", ErrInvalidDAG, spec.Name)
	}
	deps := make([]string, len(spec.DependsOn))
	copy(deps, spec.DependsOn)
	if spec.MaxAttempts < 1 {
		spec.MaxAttempts = 1
	}
	spec.DependsOn = dedupSorted(deps)
	r.tasks[spec.Name] = &taskEntry{spec: spec, handler: handler}
	return nil
}

// Get 查询任务定义与处理器。
func (r *Registry) Get(name string) (TaskSpec, TaskHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.tasks[name]
	if !ok {
		return TaskSpec{}, nil, false
	}
	return e.spec, e.handler, true
}

// Names 返回全部已注册任务名（排序，保证可解释的稳定输出）。
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tasks))
	for n := range r.tasks {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func dedupSorted(in []string) []string {
	if len(in) == 0 {
		return in
	}
	sort.Strings(in)
	out := in[:0]
	for _, v := range in {
		if len(out) > 0 && out[len(out)-1] == v {
			continue
		}
		out = append(out, v)
	}
	return out
}
