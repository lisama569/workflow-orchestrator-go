//go:build !windows

package workflow

import (
	"fmt"
	"os"
	"syscall"
)

// acquireFileLock 对快照文件取得进程级排他建议锁（flock）。
// 同一文件同时只允许一个调度进程打开；第二个进程在 NewMemoryStore 处阻塞，
// 直到前一进程 Close 释放。这样“本地单机多进程”下也不会互相覆盖状态。
func acquireFileLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("workflow: open lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("workflow: acquire lock: %w", err)
	}
	return f, nil
}

func releaseFileLock(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}
