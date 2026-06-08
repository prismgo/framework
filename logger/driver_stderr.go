package logger

import "os"

// stderrDriver 把日志写到进程 stderr。
// 典型用途：容器化部署由平台收集；或开发环境直接观察终端。
// Close 是 no-op：stderr 的生命周期归 runtime 管，不应由 logger 关闭。
type stderrDriver struct{}

// newStderrDriver 返回一个共享的 stderr 驱动实例。
func newStderrDriver() *stderrDriver { return &stderrDriver{} }

// Write 直接透传到 os.Stderr。
func (*stderrDriver) Write(p []byte) (int, error) { return os.Stderr.Write(p) }

// Close 不做任何事，仅为满足 Driver 接口。
func (*stderrDriver) Close() error { return nil }
