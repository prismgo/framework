package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/prismgo/framework/support"
)

// dailyDriver 按天切换文件的日志写入器。
// 行为沿袭旧实现 dailyFileWriter：base-YYYY-MM-DD.log，Write 前惰性切换。
type dailyDriver struct {
	mu          sync.Mutex
	filePath    string
	now         func() time.Time
	currentDate string
	file        *os.File
	closed      bool
}

// newDailyDriver 创建按天切分的驱动，并立即准备好当日文件。
func newDailyDriver(opts ChannelOptions) (*dailyDriver, error) {
	path := strings.TrimSpace(opts.Path)
	if path == "" {
		return nil, fmt.Errorf("logger: daily driver requires path")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	d := &dailyDriver{
		filePath: support.BasePath(path),
		now:      now,
	}
	if err := d.rotateLocked(now()); err != nil {
		return nil, err
	}
	return d, nil
}

// Write 在真正写入前检查日期是否变化，必要时完成日志轮转。
func (d *dailyDriver) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// 关闭后不再轮转，避免 late write 在次日重新创建日期文件。
	if d.closed {
		return 0, os.ErrClosed
	}
	if err := d.rotateLocked(d.now()); err != nil {
		return 0, err
	}
	return d.file.Write(p)
}

// Close 关闭当前日志文件句柄。
func (d *dailyDriver) Close() error {
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

// rotateLocked 在持锁状态下切换到指定日期对应的日志文件。
func (d *dailyDriver) rotateLocked(now time.Time) error {
	date := now.Format("2006-01-02")
	if d.currentDate == date && d.file != nil {
		return nil
	}

	path := datedLogPath(d.filePath, date)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	if d.file != nil {
		if err := d.file.Close(); err != nil {
			_ = file.Close()
			return err
		}
	}

	d.file = file
	d.currentDate = date
	return nil
}

// datedLogPath 根据基础文件路径生成当天实际写入的日志文件路径。
func datedLogPath(filePath, date string) string {
	ext := filepath.Ext(filePath)
	base := strings.TrimSuffix(filepath.Base(filePath), ext)
	dir := filepath.Dir(filePath)
	if ext == "" {
		return filepath.Join(dir, fmt.Sprintf("%s-%s.log", base, date))
	}
	return filepath.Join(dir, fmt.Sprintf("%s-%s%s", base, date, ext))
}
