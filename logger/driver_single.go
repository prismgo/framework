package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/prismgo/framework/support"
)

// singleDriver 把所有日志追加写入单个文件，不做任何轮转。
// 适用于开发环境、容器内 stdout 挂载、或外部已有 logrotate 介入的部署。
type singleDriver struct {
	mu     sync.Mutex
	file   *os.File
	closed bool
}

// newSingleDriver 打开（或创建）目标文件并返回驱动实例。
func newSingleDriver(opts ChannelOptions) (*singleDriver, error) {
	path := strings.TrimSpace(opts.Path)
	if path == "" {
		return nil, fmt.Errorf("logger: single driver requires path")
	}
	path = support.BasePath(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &singleDriver{file: file}, nil
}

// Write 把日志字节写入文件，持锁以避免并发交叉。
func (d *singleDriver) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// 关闭后保持终态，避免 Manager.Close 之后的异步误写重新触碰无效文件句柄。
	if d.closed {
		return 0, os.ErrClosed
	}
	return d.file.Write(p)
}

// Close 关闭底层文件句柄。
func (d *singleDriver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		d.closed = true
		return nil
	}
	err := d.file.Close()
	d.file = nil
	d.closed = true
	return err
}
