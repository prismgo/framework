// Package logger 定义日志系统的公共契约。
//
// 本包声明日志记录器接口和日志驱动接口。
// 具体的文件、stderr、null 驱动和 Manager 编排逻辑由 prismgo/logger 实现包提供。
package logger

import (
	"context"
	"io"
)

// Driver 是日志输出驱动的契约。
//
// 用途：自定义日志输出目标（如 syslog、Kafka、云日志服务）必须实现此接口。
// 驱动只负责写入已格式化的日志字节流；格式化与级别过滤由上层 Channel 完成。
//
// 使用方式：
//
//	logger.Extend("syslog", func(opts logger.ChannelOptions) (logger.Driver, error) {
//	    return newSyslogDriver(opts)
//	})
type Driver interface {
	io.Writer
	// Close 释放驱动持有的外部资源（如文件句柄、网络连接）。
	Close() error
}

// ContextExtractor 从 context 中提取可安全写入日志的结构化字段。
//
// 需求背景：业务请求链路通常会把 request_id、trace_id、tenant_id、user_id
// 等关联信息放入 context。日志系统只通过显式配置的 extractor 提取白名单字段，
// 默认不读取任何 context 内容，避免敏感信息被隐式写入日志。
type ContextExtractor func(context.Context) map[string]any

// Logger 是日志记录的完整契约。
//
// 用途：这是所有业务代码面向的日志接口。支持分级输出、上下文字段和通道切换。
// 通过 logger.Default() 获取默认通道的 Logger，通过 Channel(name) 切换到其他通道。
//
// 使用方式：
//
//	logger.Default().Info("服务启动")
//	logger.WithField("user_id", 123).WithError(err).Warn("操作异常")
//	logger.Default().Channel("app").Error("业务逻辑失败")
type Logger interface {
	// Debug 输出调试级别日志。
	Debug(args ...any)
	Debugf(format string, args ...any)

	// Info 输出信息级别日志。
	Info(args ...any)
	Infof(format string, args ...any)

	// Warn 输出警告级别日志。
	Warn(args ...any)
	Warnf(format string, args ...any)

	// Error 输出错误级别日志。
	Error(args ...any)
	Errorf(format string, args ...any)

	// Fatal 输出致命级别日志并退出进程。
	Fatal(args ...any)
	Fatalf(format string, args ...any)

	// WithField 派生一个附加单个上下文字段的 Logger。
	//
	// 使用方式：
	//
	//	log := logger.Default().WithField("tenant_id", 10)
	//	log.Info("租户操作日志")
	WithField(key string, value any) Logger

	// WithFields 派生一个附加多个上下文字段的 Logger。
	//
	// 使用方式：
	//
	//	log := logger.Default().WithFields(map[string]any{
	//	    "tenant_id": 10,
	//	    "user_id":   42,
	//	})
	WithFields(fields map[string]any) Logger

	// WithError 派生一个附加错误信息的 Logger。
	WithError(err error) Logger

	// WithContext 派生一个附加 context 关联字段的 Logger。
	//
	// 具体会读取哪些字段由实现包配置的 ContextExtractor 决定；默认不读取任何字段。
	WithContext(ctx context.Context) Logger

	// Channel 切换到指定名称的日志通道。
	//
	// 默认通道通过 logger.Default() 获取；切换后的 Logger 不会自动携带旧通道的字段。
	// 未知通道回退到默认通道。
	Channel(name string) Logger
}
