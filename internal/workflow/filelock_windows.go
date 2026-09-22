//go:build windows

package workflow

import "os"

func acquireFileLock(path string) (*os.File, error) {
	// Windows 平台退化为无锁（本引擎目标环境为本地类 Unix）。
	return nil, nil
}

func releaseFileLock(f *os.File) {}
