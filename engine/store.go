package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 负责实例状态的本地持久化（JSON 文件）。
// 写操作采用“临时文件 + rename”保证原子性，避免崩溃留下半截文件。
type Store struct {
	path string
}

// OpenStore 打开（不存在则延迟创建）持久化文件。
func OpenStore(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("store path is empty")
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create store dir: %w", err)
		}
	}
	return &Store{path: path}, nil
}

// State 是持久化的完整状态：实例表 + 运行时展开台账 + 展开任务定义。
type State struct {
	Instances    map[string]*Instance
	Expansions   map[string]*ExpansionRecord
	DynamicSpecs map[string]*DynamicSpec
}

// persistedState 是磁盘上的 JSON 结构。
type persistedState struct {
	Instances    map[string]*Instance        `json:"instances"`
	Expansions   map[string]*ExpansionRecord `json:"expansions"`
	DynamicSpecs map[string]*DynamicSpec     `json:"dynamic_specs,omitempty"`
}

// Load 读取全部持久化状态；文件不存在时返回空集合。
// 兼容旧版本“仅实例表”的 JSON 布局。
func (s *Store) Load() (*State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return &State{Instances: map[string]*Instance{}, Expansions: map[string]*ExpansionRecord{}, DynamicSpecs: map[string]*DynamicSpec{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store: %w", err)
	}
	if len(data) == 0 {
		return &State{Instances: map[string]*Instance{}, Expansions: map[string]*ExpansionRecord{}, DynamicSpecs: map[string]*DynamicSpec{}}, nil
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err == nil && (st.Instances != nil || st.Expansions != nil) {
		if st.Instances == nil {
			st.Instances = map[string]*Instance{}
		}
		if st.Expansions == nil {
			st.Expansions = map[string]*ExpansionRecord{}
		}
		if st.DynamicSpecs == nil {
			st.DynamicSpecs = map[string]*DynamicSpec{}
		}
		return &State{Instances: st.Instances, Expansions: st.Expansions, DynamicSpecs: st.DynamicSpecs}, nil
	}
	// 旧版本布局：顶层直接是 map[实例键]*Instance。
	legacy := map[string]*Instance{}
	if err := json.Unmarshal(data, &legacy); err != nil {
		return nil, fmt.Errorf("decode store: %w", err)
	}
	return &State{Instances: legacy, Expansions: map[string]*ExpansionRecord{}, DynamicSpecs: map[string]*DynamicSpec{}}, nil
}

// Save 全量写入状态（先写临时文件再原子替换）。
func (s *Store) Save(st *State) error {
	if st.Instances == nil {
		st.Instances = map[string]*Instance{}
	}
	if st.Expansions == nil {
		st.Expansions = map[string]*ExpansionRecord{}
	}
	if st.DynamicSpecs == nil {
		st.DynamicSpecs = map[string]*DynamicSpec{}
	}
	data, err := json.MarshalIndent(persistedState{Instances: st.Instances, Expansions: st.Expansions, DynamicSpecs: st.DynamicSpecs}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode store: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write tmp store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename store: %w", err)
	}
	return nil
}
