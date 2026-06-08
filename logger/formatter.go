package logger

import (
	"fmt"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

// Formatter 抽象日志行的格式化策略，对应 Laravel/Monolog 的 Formatter 概念。
// 与 Driver（输出载体）解耦：同一 driver（如 daily）可以搭配不同 formatter（text/json）。
// 当前复用 logrus.Formatter 接口作为底层实现，以便 Channel 直接设给 *logrus.Logger。
type Formatter = logrus.Formatter

// FormatterFactory 构造 Formatter 的工厂。
// params 透传 ChannelOptions 中 formatter 相关的零散配置（预留扩展），
// 未使用的 formatter 可以直接忽略。
type FormatterFactory func(params map[string]any) (Formatter, error)

var (
	formatterRegistryMu sync.RWMutex
	formatterRegistry   = map[string]FormatterFactory{
		// text：logrus 原生文本格式，兼容历史 key=value 输出。
		"text": func(params map[string]any) (Formatter, error) {
			return &logrus.TextFormatter{
				FullTimestamp:   true,
				TimestampFormat: "2006-01-02 15:04:05",
				DisableColors:   true,
			}, nil
		},
		// json：Laravel 的 JsonFormatter，便于被 ELK/Loki 等管道消费。
		"json": func(params map[string]any) (Formatter, error) {
			return &logrus.JSONFormatter{
				TimestampFormat: "2006-01-02 15:04:05",
			}, nil
		},
		// line：Laravel 默认格式，等价于 Monolog LineFormatter。
		"line": newLineFormatter,
	}
)

// RegisterFormatter 注册自定义 formatter 工厂，供业务侧按需扩展
// （例如 syslog 风格、带颜色的终端 formatter、对接内部日志规范的自定义实现）。
func RegisterFormatter(name string, factory FormatterFactory) {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" || factory == nil {
		return
	}
	formatterRegistryMu.Lock()
	defer formatterRegistryMu.Unlock()
	formatterRegistry[name] = factory
}

// buildFormatter 按名字解析 formatter；空名时使用 line 作为默认。
func buildFormatter(name string, params map[string]any) (Formatter, error) {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		name = "line"
	}
	formatterRegistryMu.RLock()
	factory, ok := formatterRegistry[name]
	formatterRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("logger: unknown formatter %q", name)
	}
	return factory(params)
}
