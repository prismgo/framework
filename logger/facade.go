package logger

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/facade"
	"github.com/sirupsen/logrus"
)

var (
	fallbackLg          Logger = newFallbackLogger()
	standardHook               = &standardLogrusHook{}
	standardHookInstall sync.Once
)

const serviceKey = "logger.manager"

// Resolve 从当前 Application 容器解析日志 Manager。
func Resolve() *Manager {
	return facade.Resolve[*Manager](serviceKey)
}

// DefaultName 返回当前全局 Manager 的默认通道名称。
func DefaultName() string {
	m := Resolve()
	if m == nil {
		return ""
	}
	return m.DefaultName()
}

// Close 释放当前全局 Manager 已构造通道的底层驱动资源。
func Close() error {
	m := Resolve()
	if m == nil {
		return nil
	}
	return m.Close()
}

func defaultLogger() Logger {
	m := Resolve()
	if m == nil {
		return fallbackLg
	}
	return m.Default()
}

// Channel 按名称获取通道 Logger；未 Use 过 Manager 时返回 fallback logger。
func Channel(name string) Logger {
	m := Resolve()
	if m == nil {
		return fallbackLg
	}
	return m.Channel(name)
}

// Debug 通过默认通道记录 debug 日志。
func Debug(args ...any) { defaultLogger().Debug(args...) }

// Debugf 通过默认通道按格式记录 debug 日志。
func Debugf(format string, args ...any) { defaultLogger().Debugf(format, args...) }

// Info 通过默认通道记录 info 日志。
func Info(args ...any) { defaultLogger().Info(args...) }

// Infof 通过默认通道按格式记录 info 日志。
func Infof(format string, args ...any) { defaultLogger().Infof(format, args...) }

// Warn 通过默认通道记录 warn 日志。
func Warn(args ...any) { defaultLogger().Warn(args...) }

// Warnf 通过默认通道按格式记录 warn 日志。
func Warnf(format string, args ...any) { defaultLogger().Warnf(format, args...) }

// Error 通过默认通道记录 error 日志。
func Error(args ...any) { defaultLogger().Error(args...) }

// Errorf 通过默认通道按格式记录 error 日志。
func Errorf(format string, args ...any) { defaultLogger().Errorf(format, args...) }

// Fatal 通过默认通道记录 fatal 日志。
func Fatal(args ...any) { defaultLogger().Fatal(args...) }

// Fatalf 通过默认通道按格式记录 fatal 日志。
func Fatalf(format string, args ...any) { defaultLogger().Fatalf(format, args...) }

// WithField 在默认通道上附加单个上下文字段。
func WithField(key string, value any) Logger {
	return defaultLogger().WithField(key, value)
}

// WithFields 在默认通道上附加多个上下文字段。
func WithFields(fields map[string]any) Logger {
	return defaultLogger().WithFields(fields)
}

// WithError 在默认通道上附加错误上下文。
func WithError(err error) Logger {
	return defaultLogger().WithError(err)
}

// WithContext 在默认通道上附加 context 关联字段。
func WithContext(ctx context.Context) Logger {
	return defaultLogger().WithContext(ctx)
}

// newFallbackLogger 返回一个基于 stderr 的最小 Logger，用于 Use 之前的兜底。
func newFallbackLogger() Logger {
	lg := logrus.New()
	lg.SetOutput(os.Stderr)
	lg.SetLevel(logrus.InfoLevel)
	lg.SetFormatter(defaultFormatter())
	return &channel{name: "__fallback__", driver: newStderrDriver(), logger: lg}
}

func managerCloseOption() containercontract.BindingOption {
	return container.WithCloser(func(m *Manager) error {
		return m.Close()
	})
}

// syncLogrusStandard 把默认通道的配置写回全局 logrus。
// 迁移期确保 logrus.Info 等旧调用与 logger.Info 行为一致。
func syncLogrusStandard(m *Manager) {
	if m == nil {
		setStandardLogrusTarget(nil, logrus.InfoLevel)
		return
	}
	lg := m.Default()
	setStandardLogrusTarget(lg, maxLoggerLevel(lg))
}

// standardLogrusHook 把全局 logrus entry 分发回当前默认 Logger。
// 需求背景：stack 默认通道必须保留子通道自己的 level/formatter/driver 逻辑，不能把底层 writer 拼成 MultiWriter。
type standardLogrusHook struct {
	mu     sync.RWMutex
	target Logger
}

func (h *standardLogrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *standardLogrusHook) Fire(entry *logrus.Entry) error {
	h.mu.RLock()
	target := h.target
	h.mu.RUnlock()
	if target == nil {
		return nil
	}
	return writeLogrusEntry(target, entry)
}

func (h *standardLogrusHook) setTarget(target Logger) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.target = target
}

// setStandardLogrusTarget 安装包内 hook，并配置全局 logrus 的输出策略。
// target 非空时输出设为 Discard，避免 hook 分发后标准 logger 再写一份。
func setStandardLogrusTarget(target Logger, level logrus.Level) {
	standardHookInstall.Do(func() {
		logrus.StandardLogger().AddHook(standardHook)
	})
	standardHook.setTarget(target)
	if target == nil {
		logrus.SetOutput(os.Stderr)
	} else {
		logrus.SetOutput(io.Discard)
	}
	logrus.SetLevel(level)
	logrus.SetFormatter(defaultFormatter())
	logrus.SetReportCaller(true)
}

// writeLogrusEntry 按原始 logrus 级别调用 Logger 接口，让目标通道自行完成级别过滤。
func writeLogrusEntry(target Logger, entry *logrus.Entry) error {
	if len(entry.Data) > 0 {
		target = target.WithFields(map[string]any(entry.Data))
	}
	switch entry.Level {
	case logrus.TraceLevel, logrus.DebugLevel:
		target.Debug(entry.Message)
	case logrus.InfoLevel:
		target.Info(entry.Message)
	case logrus.WarnLevel:
		target.Warn(entry.Message)
	case logrus.ErrorLevel:
		target.Error(entry.Message)
	case logrus.FatalLevel:
		target.Fatal(entry.Message)
	case logrus.PanicLevel:
		target.Error(entry.Message)
	}
	return nil
}

// maxLoggerLevel 返回默认 Logger 树中最宽松的级别，确保全局 logrus entry 能先进入 hook。
func maxLoggerLevel(lg Logger) logrus.Level {
	switch v := lg.(type) {
	case *channel:
		return v.logger.Level
	case *stackLogger:
		level := logrus.PanicLevel
		for _, child := range v.children {
			if childLevel := maxLoggerLevel(child); childLevel > level {
				level = childLevel
			}
		}
		return level
	default:
		return logrus.InfoLevel
	}
}

func errWriter() io.Writer { return os.Stderr }
