package logger

// nullDriver 丢弃所有日志写入，等价于 Laravel 的 null 通道。
// 适合测试场景下屏蔽某条通道的输出，或临时关闭某个通道但保留调用链路。
type nullDriver struct{}

// newNullDriver 返回共享实例。
func newNullDriver() *nullDriver { return &nullDriver{} }

// Write 报告“写入成功”，但实际不做任何事。
func (*nullDriver) Write(p []byte) (int, error) { return len(p), nil }

// Close 是 no-op。
func (*nullDriver) Close() error { return nil }
