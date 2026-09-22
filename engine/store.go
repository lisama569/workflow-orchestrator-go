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

// Load 读取全部实例状态；文件不存在时返回空集合。
func (s *Store) Load() (map[string]*Instance, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]*Instance{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store: %w", err)
	}
	out := map[string]*Instance{}
	if len(data) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode store: %w", err)
	}
	return out, nil
}

// Save 全量写入实例状态（先写临时文件再原子替换）。
func (s *Store) Save(instances map[string]*Instance) error {
	data, err := json.MarshalIndent(instances, "", "  ")
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
