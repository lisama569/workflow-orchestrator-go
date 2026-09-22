package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// MemoryStore 基于 map + JSON 文件快照的本地持久化存储。
//
// 持久化规则：每次成功的 Put 都把全量快照写入临时文件，fsync 后原子 rename，
// 因此崩溃只会丢失“最后一次未完成的 Put”，不会产生半截文件——这正是
// Running 恢复语义（见 Scheduler.recover）所依赖的崩溃边界。
type MemoryStore struct {
	mu     sync.Mutex
	path   string
	lockFd *os.File
	recs   map[string]*Record
}

type snapshot struct {
	Records []*Record `json:"records"`
}

// NewMemoryStore 创建存储；若 path 已存在则从中恢复全部记录。
// path 非空时取得进程级文件锁，同一快照同时只允许一个调度进程打开。
// path 为空时退化为纯内存存储（不跨进程恢复）。
func NewMemoryStore(path string) (*MemoryStore, error) {
	s := &MemoryStore{path: path, recs: make(map[string]*Record)}
	if path == "" {
		return s, nil
	}
	lockFd, err := acquireFileLock(path)
	if err != nil {
		return nil, err
	}
	s.lockFd = lockFd
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("workflow: read store %q: %w", path, err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return s, nil
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("workflow: corrupt store %q: %w", path, err)
	}
	for _, r := range snap.Records {
		s.recs[keyString(r.Key)] = r
	}
	return s, nil
}

func keyString(k InstanceKey) string { return k.Date + "/" + k.Task }

// Get 读取实例记录；不存在返回 (nil, nil)。
func (s *MemoryStore) Get(_ context.Context, key InstanceKey) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.recs[keyString(key)]
	return cloneRecord(r), nil
}

// Put 以 CAS 语义写入：expect 为期望的当前状态（nil 表示记录当前不存在），
// 状态不符返回 (false, nil)。写入与落盘在同一临界区内完成。
func (s *MemoryStore) Put(ctx context.Context, rec *Record, expect *Status) (bool, error) {
	if rec == nil {
		return false, fmt.Errorf("workflow: nil record")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.recs[keyString(rec.Key)]
	if !statusEqual(statusOf(cur), expect) {
		return false, nil
	}
	stored := cloneRecord(rec)
	s.recs[keyString(rec.Key)] = stored
	if s.path != "" {
		if err := s.persistLocked(ctx); err != nil {
			// 回滚内存态，保持内存与磁盘一致。
			if cur == nil {
				delete(s.recs, keyString(rec.Key))
			} else {
				s.recs[keyString(rec.Key)] = cur
			}
			return false, err
		}
	}
	return true, nil
}

// ListByDate 列出某逻辑日期下的全部实例记录（按任务名排序）。
func (s *MemoryStore) ListByDate(_ context.Context, date string) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := date + "/"
	var out []*Record
	for k, r := range s.recs {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, cloneRecord(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key.Task < out[j].Key.Task })
	return out, nil
}

// List 列出全部实例记录（按日期、任务名排序）。
func (s *MemoryStore) List(_ context.Context) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Record, 0, len(s.recs))
	for _, r := range s.recs {
		out = append(out, cloneRecord(r))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key.Date != out[j].Key.Date {
			return out[i].Key.Date < out[j].Key.Date
		}
		return out[i].Key.Task < out[j].Key.Task
	})
	return out, nil
}

// Close 释放进程级文件锁（快照无需额外刷盘，每次 Put 已落盘）。
func (s *MemoryStore) Close() error {
	if s.lockFd != nil {
		releaseFileLock(s.lockFd)
		s.lockFd = nil
	}
	return nil
}

func (s *MemoryStore) persistLocked(_ context.Context) error {
	recs := make([]*Record, 0, len(s.recs))
	for _, r := range s.recs {
		recs = append(recs, cloneRecord(r))
	}
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].Key.Date != recs[j].Key.Date {
			return recs[i].Key.Date < recs[j].Key.Date
		}
		return recs[i].Key.Task < recs[j].Key.Task
	})
	data, err := json.MarshalIndent(snapshot{Records: recs}, "", "  ")
	if err != nil {
		return fmt.Errorf("workflow: marshal store: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("workflow: mkdir store: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("workflow: write store: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("workflow: write store: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("workflow: fsync store: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("workflow: close store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("workflow: rename store: %w", err)
	}
	return nil
}

func statusOf(r *Record) *Status {
	if r == nil {
		return nil
	}
	st := r.Status
	return &st
}

func statusEqual(a, b *Status) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func cloneRecord(r *Record) *Record {
	if r == nil {
		return nil
	}
	c := *r
	if r.Attempts != nil {
		c.Attempts = append([]Attempt(nil), r.Attempts...)
	}
	if r.Expanded != nil {
		c.Expanded = append([]string(nil), r.Expanded...)
	}
	return &c
}
