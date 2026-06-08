package logger

import "context"

// stackLogger 把一次写入扇出到多个子通道。
// 采用扇出而非 io.MultiWriter 的理由：每个子通道拥有独立的 level 过滤与 formatter，
// 直接共享底层 writer 会丢失这些能力（例如 error 通道只想收 warn+ 级别日志）。
// 对应 Laravel 的 stack 驱动（Monolog GroupHandler）。
type stackLogger struct {
	children []Logger
	manager  *Manager
}

// newStackLogger 构造 stack 通道；要求子通道列表非空。
func newStackLogger(children []Logger, manager *Manager) *stackLogger {
	return &stackLogger{children: children, manager: manager}
}

// --- Logger 接口实现：遍历子通道广播 ---

func (s *stackLogger) Debug(args ...any) {
	for _, c := range s.children {
		c.Debug(args...)
	}
}

func (s *stackLogger) Debugf(format string, args ...any) {
	for _, c := range s.children {
		c.Debugf(format, args...)
	}
}

func (s *stackLogger) Info(args ...any) {
	for _, c := range s.children {
		c.Info(args...)
	}
}

func (s *stackLogger) Infof(format string, args ...any) {
	for _, c := range s.children {
		c.Infof(format, args...)
	}
}

func (s *stackLogger) Warn(args ...any) {
	for _, c := range s.children {
		c.Warn(args...)
	}
}

func (s *stackLogger) Warnf(format string, args ...any) {
	for _, c := range s.children {
		c.Warnf(format, args...)
	}
}

func (s *stackLogger) Error(args ...any) {
	for _, c := range s.children {
		c.Error(args...)
	}
}

func (s *stackLogger) Errorf(format string, args ...any) {
	for _, c := range s.children {
		c.Errorf(format, args...)
	}
}

// Fatal 广播到所有子通道后由最后一个通道触发进程退出。
// 语义上与 logrus.Fatal 一致（最后写入的 logger 负责 os.Exit(1)）。
func (s *stackLogger) Fatal(args ...any) {
	if len(s.children) == 0 {
		return
	}
	for _, c := range s.children[:len(s.children)-1] {
		c.Error(args...)
	}
	s.children[len(s.children)-1].Fatal(args...)
}

// Fatalf 同 Fatal。
func (s *stackLogger) Fatalf(format string, args ...any) {
	if len(s.children) == 0 {
		return
	}
	for _, c := range s.children[:len(s.children)-1] {
		c.Errorf(format, args...)
	}
	s.children[len(s.children)-1].Fatalf(format, args...)
}

// WithField 把字段附加到每个子通道，返回仍实现 Logger 的 stack。
func (s *stackLogger) WithField(key string, value any) Logger {
	next := make([]Logger, len(s.children))
	for i, c := range s.children {
		next[i] = c.WithField(key, value)
	}
	return &stackLogger{children: next, manager: s.manager}
}

// WithFields 同 WithField。
func (s *stackLogger) WithFields(fields map[string]any) Logger {
	next := make([]Logger, len(s.children))
	for i, c := range s.children {
		next[i] = c.WithFields(fields)
	}
	return &stackLogger{children: next, manager: s.manager}
}

// WithError 同 WithField。
func (s *stackLogger) WithError(err error) Logger {
	next := make([]Logger, len(s.children))
	for i, c := range s.children {
		next[i] = c.WithError(err)
	}
	return &stackLogger{children: next, manager: s.manager}
}

// WithContext 把 context 关联字段附加到每个子通道，返回仍实现 Logger 的 stack。
func (s *stackLogger) WithContext(ctx context.Context) Logger {
	next := make([]Logger, len(s.children))
	for i, c := range s.children {
		next[i] = c.WithContext(ctx)
	}
	return &stackLogger{children: next, manager: s.manager}
}

// Channel 在 stack 上切换通道：回到 Manager 解析。
func (s *stackLogger) Channel(name string) Logger {
	if s.manager == nil {
		return s
	}
	return s.manager.Channel(name)
}
