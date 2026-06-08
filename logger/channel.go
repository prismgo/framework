package logger

import (
	"context"
	"strings"

	"github.com/sirupsen/logrus"

	loggercontract "github.com/prismgo/framework/contracts/logger"
)

// Logger 是所有日志记录对象对外暴露的统一接口。
// 每个 Channel、stack 和 entry（WithField 等派生出的上下文）都实现它。
// 采用 any 替代 logrus.Fields 以避免上层代码污染 logrus 依赖。
type Logger = loggercontract.Logger

// channel 是单个通道的 Logger 实现，封装 *logrus.Logger + Driver。
// 每个通道独立配置 level、output、formatter，互不干扰。
type channel struct {
	name    string
	driver  Driver
	logger  *logrus.Logger
	manager *Manager
	extract ContextExtractor
}

// newChannel 按 ChannelOptions 构造一个普通通道。
// Level / Formatter 解析失败返回 error，由 Manager 决定是否回退。
func newChannel(name string, opts ChannelOptions, manager *Manager) (*channel, error) {
	driver, err := buildDriver(opts)
	if err != nil {
		return nil, err
	}
	level, err := logrus.ParseLevel(strings.ToLower(strings.TrimSpace(opts.Level)))
	if err != nil {
		_ = driver.Close()
		return nil, err
	}
	formatterParams := make(map[string]any, len(opts.FormatterParams)+1)
	for key, value := range opts.FormatterParams {
		formatterParams[key] = value
	}
	formatterParams["channel"] = name
	formatter, err := buildFormatter(opts.Formatter, formatterParams)
	if err != nil {
		_ = driver.Close()
		return nil, err
	}
	lg := logrus.New()
	lg.SetOutput(driver)
	lg.SetLevel(level)
	lg.SetFormatter(formatter)
	lg.SetReportCaller(true)
	return &channel{name: name, driver: driver, logger: lg, manager: manager, extract: opts.ContextExtractor}, nil
}

// defaultFormatter 返回全局 fallback 使用的格式（line）。
// line formatter 通过 params 注入 channel 名；fallback 场景允许缺省为空。
func defaultFormatter() logrus.Formatter {
	f, _ := buildFormatter("line", nil)
	return f
}

// Close 释放底层驱动资源（文件句柄等）。Manager.Close 会统一调用。
func (c *channel) Close() error {
	if c == nil || c.driver == nil {
		return nil
	}
	return c.driver.Close()
}

// --- Logger 接口实现 ---

func (c *channel) Debug(args ...any)                 { c.logger.Debug(args...) }
func (c *channel) Debugf(format string, args ...any) { c.logger.Debugf(format, args...) }
func (c *channel) Info(args ...any)                  { c.logger.Info(args...) }
func (c *channel) Infof(format string, args ...any)  { c.logger.Infof(format, args...) }
func (c *channel) Warn(args ...any)                  { c.logger.Warn(args...) }
func (c *channel) Warnf(format string, args ...any)  { c.logger.Warnf(format, args...) }
func (c *channel) Error(args ...any)                 { c.logger.Error(args...) }
func (c *channel) Errorf(format string, args ...any) { c.logger.Errorf(format, args...) }
func (c *channel) Fatal(args ...any)                 { c.logger.Fatal(args...) }
func (c *channel) Fatalf(format string, args ...any) { c.logger.Fatalf(format, args...) }

func (c *channel) WithField(key string, value any) Logger {
	return &entry{entry: c.logger.WithField(key, value), manager: c.manager, extract: c.extract}
}

func (c *channel) WithFields(fields map[string]any) Logger {
	return &entry{entry: c.logger.WithFields(toLogrusFields(fields)), manager: c.manager, extract: c.extract}
}

func (c *channel) WithError(err error) Logger {
	return &entry{entry: c.logger.WithError(err), manager: c.manager, extract: c.extract}
}

func (c *channel) WithContext(ctx context.Context) Logger {
	logEntry := c.logger.WithContext(ctx)
	if fields := extractContextFields(c.extract, ctx); len(fields) > 0 {
		logEntry = logEntry.WithFields(fields)
	}
	return &entry{entry: logEntry, manager: c.manager, extract: c.extract}
}

// Channel 让调用方链式切换到其它通道，如 logger.Default().Channel("app").Info(...)。
// 未知通道回退到默认通道，由 Manager 决定。
func (c *channel) Channel(name string) Logger {
	if c.manager == nil {
		return c
	}
	return c.manager.Channel(name)
}

// toLogrusFields 把通用 map 转成 logrus.Fields，避免业务代码显式依赖 logrus 类型。
func toLogrusFields(fields map[string]any) logrus.Fields {
	if len(fields) == 0 {
		return logrus.Fields{}
	}
	out := make(logrus.Fields, len(fields))
	for k, v := range fields {
		out[k] = v
	}
	return out
}

func extractContextFields(extractor ContextExtractor, ctx context.Context) logrus.Fields {
	if extractor == nil || ctx == nil {
		return logrus.Fields{}
	}
	return toLogrusFields(extractor(ctx))
}
