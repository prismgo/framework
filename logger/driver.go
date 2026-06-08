package logger

import (
	"fmt"
	"strings"
	"sync"
	"time"

	loggercontract "github.com/prismgo/framework/contracts/logger"
)

// Driver 是所有日志输出驱动的统一接口。
// 不同驱动只负责把一条已经序列化好的日志字节流写入目标（文件、终端、黑洞等），
// 其格式化与级别过滤由上层 Channel 持有的 *logrus.Logger 完成。
type Driver = loggercontract.Driver

// ContextExtractor 是 logger contract 暴露的 context 字段提取器类型。
// 在实现包中保留类型别名，方便调用方继续通过 logger.ContextExtractor 配置 Manager。
type ContextExtractor = loggercontract.ContextExtractor

// ChannelOptions 描述单个通道的配置。
// 类比 Laravel/Monolog：Driver 决定 Handler（输出载体），Formatter 决定行的格式。
// 未使用的字段在当前 driver/formatter 下会被安全忽略。
type ChannelOptions struct {
	// Driver 驱动名（daily / single / stack / stderr / null）。对应 Laravel 的 handler 驱动。
	Driver string
	// Formatter 格式化器名（line / text / json / 自定义）。为空时使用 line。
	Formatter string
	// FormatterParams 透传给 FormatterFactory 的自定义参数，供扩展使用（如 JSON 字段重命名）。
	FormatterParams map[string]any
	// Level logrus 可识别的级别名称（debug/info/warn/error/fatal/panic）。
	Level string
	// Path 文件型驱动的输出路径。daily 会按日期扩展文件名。
	Path string
	// Channels stack 驱动聚合的子通道名。
	Channels []string
	// ContextExtractor 从 context 中提取结构化字段；未设置时继承 Manager 的默认 extractor。
	ContextExtractor ContextExtractor
	// Now 可注入的时间函数，仅用于 daily 驱动的跨日切换测试。
	Now func() time.Time
}

// driverFactory 用于注册内置和自定义日志驱动工厂，由 Extend 维护。
type driverFactory func(opts ChannelOptions) (Driver, error)

var (
	driverRegistryMu sync.RWMutex
	driverRegistry   = map[string]driverFactory{
		"daily":  func(opts ChannelOptions) (Driver, error) { return newDailyDriver(opts) },
		"single": func(opts ChannelOptions) (Driver, error) { return newSingleDriver(opts) },
		"stderr": func(opts ChannelOptions) (Driver, error) { return newStderrDriver(), nil },
		"null":   func(opts ChannelOptions) (Driver, error) { return newNullDriver(), nil },
	}
)

// Extend 注册自定义日志驱动工厂，方便业务侧按需扩展（如 syslog、errorlog）。
//
// 注册后可在 ChannelOptions.Driver 中直接引用。空名称或 nil 工厂会被忽略；
// 同名注册会覆盖先前工厂，保持和 Laravel LogManager::extend 一致的后注册生效语义。
func Extend(name string, factory func(opts ChannelOptions) (Driver, error)) {
	name = strings.TrimSpace(name)
	if name == "" || factory == nil {
		return
	}
	driverRegistryMu.Lock()
	defer driverRegistryMu.Unlock()
	driverRegistry[name] = factory
}

// buildDriver 按 ChannelOptions.Driver 字段查找并构造驱动。
// 注意：stack 驱动由 Manager 统一解析为 stackLogger，此处不会被调用。
func buildDriver(opts ChannelOptions) (Driver, error) {
	name := strings.TrimSpace(strings.ToLower(opts.Driver))
	if name == "" {
		return nil, fmt.Errorf("logger: driver is required")
	}
	driverRegistryMu.RLock()
	factory, ok := driverRegistry[name]
	driverRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("logger: unknown driver %q", name)
	}
	return factory(opts)
}
