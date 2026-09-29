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

// State 是持久化文件的完整内容：实例状态 + 运行时展开批次记录。
type State struct {
	Instances map[string]*Instance
	Emissions map[string]*Emission
}

// Load 读取全部持久化状态；文件不存在时返回空集合。
func (s *Store) Load() (*State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return &State{Instances: map[string]*Instance{}, Emissions: map[string]*Emission{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store: %w", err)
	}
	st := &State{Instances: map[string]*Instance{}, Emissions: map[string]*Emission{}}
	if len(data) == 0 {
		return st, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, fmt.Errorf("decode store: %w", err)
	}
	if raw, ok := top["Instances"]; ok {
		if err := json.Unmarshal(raw, &st.Instances); err != nil {
			return nil, fmt.Errorf("decode instances: %w", err)
		}
		if raw, ok := top["Emissions"]; ok {
			if err := json.Unmarshal(raw, &st.Emissions); err != nil {
				return nil, fmt.Errorf("decode emissions: %w", err)
			}
		}
	} else {
		// 兼容旧版本格式：文件内容直接是 map[instanceKey]*Instance。
		if err := json.Unmarshal(data, &st.Instances); err != nil {
			return nil, fmt.Errorf("decode legacy store: %w", err)
		}
	}
	if st.Instances == nil {
		st.Instances = map[string]*Instance{}
	}
	if st.Emissions == nil {
		st.Emissions = map[string]*Emission{}
	}
	return st, nil
}

// Save 全量写入状态（先写临时文件再原子替换）。
func (s *Store) Save(st *State) error {
	data, err := json.MarshalIndent(st, "", "  ")
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
