package logger

import (
	"context"

	"github.com/sirupsen/logrus"
)

// entry 包装 logrus.Entry，使 WithField / WithFields / WithError 的返回值仍实现 Logger 接口。
// 保留 manager 引用以便链式 Channel(name) 切换到其它通道。
type entry struct {
	entry   *logrus.Entry
	manager *Manager
	extract ContextExtractor
}

// --- Logger 接口实现 ---

func (e *entry) Debug(args ...any)                 { e.entry.Debug(args...) }
func (e *entry) Debugf(format string, args ...any) { e.entry.Debugf(format, args...) }
func (e *entry) Info(args ...any)                  { e.entry.Info(args...) }
func (e *entry) Infof(format string, args ...any)  { e.entry.Infof(format, args...) }
func (e *entry) Warn(args ...any)                  { e.entry.Warn(args...) }
func (e *entry) Warnf(format string, args ...any)  { e.entry.Warnf(format, args...) }
func (e *entry) Error(args ...any)                 { e.entry.Error(args...) }
func (e *entry) Errorf(format string, args ...any) { e.entry.Errorf(format, args...) }
func (e *entry) Fatal(args ...any)                 { e.entry.Fatal(args...) }
func (e *entry) Fatalf(format string, args ...any) { e.entry.Fatalf(format, args...) }

func (e *entry) WithField(key string, value any) Logger {
	return &entry{entry: e.entry.WithField(key, value), manager: e.manager, extract: e.extract}
}

func (e *entry) WithFields(fields map[string]any) Logger {
	return &entry{entry: e.entry.WithFields(toLogrusFields(fields)), manager: e.manager, extract: e.extract}
}

func (e *entry) WithError(err error) Logger {
	return &entry{entry: e.entry.WithError(err), manager: e.manager, extract: e.extract}
}

func (e *entry) WithContext(ctx context.Context) Logger {
	logEntry := e.entry.WithContext(ctx)
	if fields := extractContextFields(e.extract, ctx); len(fields) > 0 {
		logEntry = logEntry.WithFields(fields)
	}
	return &entry{entry: logEntry, manager: e.manager, extract: e.extract}
}

// Channel 在已附加字段的 entry 上切换通道。
// 字段仅随原 logger 实例保留；切换后的新 logger 不会自动携带旧字段
// （符合 Laravel 行为：withContext 是调用方显式管理的上下文）。
func (e *entry) Channel(name string) Logger {
	if e.manager == nil {
		return e
	}
	return e.manager.Channel(name)
}
