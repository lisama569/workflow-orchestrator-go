package workflow

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

// Logger 状态流转日志接口；测试中可捕获输出以打印判定依据。
type Logger interface {
	Logf(ctx context.Context, format string, args ...any)
}

type stdLogger struct{ l *log.Logger }

// NewStdLogger 输出到标准错误的默认日志器。
func NewStdLogger() Logger {
	return stdLogger{l: log.New(os.Stderr, "[workflow] ", log.LstdFlags|log.Lmicroseconds)}
}

func (s stdLogger) Logf(_ context.Context, format string, args ...any) {
	s.l.Output(2, fmt.Sprintf(format, args...))
}

// CaptureLogger 同时写入底层 Writer 与内存缓冲（测试断言与日志打印用）。
type CaptureLogger struct {
	mu  sync.Mutex
	w   io.Writer
	buf []byte
}

// NewCaptureLogger 创建可捕获日志器；w 为 nil 时仅保存在内存。
func NewCaptureLogger(w io.Writer) *CaptureLogger {
	return &CaptureLogger{w: w}
}

func (c *CaptureLogger) Logf(_ context.Context, format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	c.mu.Lock()
	c.buf = append(c.buf, line...)
	if len(line) == 0 || line[len(line)-1] != '\n' {
		c.buf = append(c.buf, '\n')
	}
	c.mu.Unlock()
	if c.w != nil {
		fmt.Fprintln(c.w, line)
	}
}

// String 返回已捕获的日志内容。
func (c *CaptureLogger) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return string(c.buf)
}
