package rabbitmq

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/prismgo/framework/queue/internal/helper"
	"github.com/prismgo/framework/queue/payload"
)

// 本文件负责 RabbitMQ 配置默认值、连接 URL 组装和队列名归一化。
// 配置修正集中在这里，保证构造连接和测试 fake 连接使用同一套边界规则。

// defaultRabbitMQOptions 返回 RabbitMQ driver 的完整默认配置。
//
// 默认值对应 issue 07 的运行期重连策略：初始化失败立即返回，初始化成功后的断线才进入有界退避重连。
func defaultRabbitMQOptions() Options {
	return Options{
		Scheme:              defaultRabbitMQScheme,
		Host:                defaultRabbitMQHost,
		Port:                defaultRabbitMQPort,
		VHost:               defaultRabbitMQVHost,
		Exchange:            defaultRabbitMQExchange,
		ExchangeType:        defaultRabbitMQExchangeType,
		Declare:             Bool(defaultRabbitMQDeclare),
		ExchangeDurable:     Bool(defaultRabbitMQExchangeDurable),
		QueueDurable:        Bool(defaultRabbitMQQueueDurable),
		MessagePersistent:   Bool(defaultRabbitMQPersistent),
		AutoDelete:          Bool(false),
		Exclusive:           Bool(false),
		NoWait:              Bool(false),
		Confirm:             Bool(defaultRabbitMQConfirm),
		DelayMode:           defaultRabbitMQDelayMode,
		DelayBuckets:        append([]time.Duration(nil), defaultRabbitMQDelayBuckets...),
		Prefetch:            defaultRabbitMQPrefetch,
		Heartbeat:           defaultRabbitMQHeartbeat,
		PublishTimeout:      defaultRabbitMQPublishTimeout,
		PublishChannels:     defaultRabbitMQPublishChannels,
		ReconnectMinDelay:   defaultRabbitMQReconnectMin,
		ReconnectMaxDelay:   defaultRabbitMQReconnectMax,
		RestartQueue:        defaultRabbitMQRestartQueue,
		RestartEnabled:      Bool(defaultRabbitMQRestartEnabled),
		RestartPollInterval: defaultRabbitMQRestartPoll,
	}
}

func DefaultOptions() Options {
	return defaultRabbitMQOptions()
}

// resolveRabbitMQOptions 合并调用方配置并修正非法值。
//
// 设计说明：公开 Options 使用三态布尔值表达“未设置/显式 true/显式 false”；
// 运行期只保存 resolvedOptions，避免连接、拓扑和发布路径反复处理默认值。
func resolveRabbitMQOptions(options Options) resolvedOptions {
	defaults := defaultRabbitMQOptions()
	resolved := resolvedOptions{
		URL:                     options.URL,
		Dialer:                  options.Dialer,
		Scheme:                  firstNonEmpty(options.Scheme, defaults.Scheme),
		Host:                    firstNonEmpty(options.Host, defaults.Host),
		Port:                    firstNonEmpty(options.Port, defaults.Port),
		Username:                options.Username,
		Password:                options.Password,
		VHost:                   firstNonEmpty(options.VHost, defaults.VHost),
		Exchange:                firstNonEmpty(options.Exchange, defaults.Exchange),
		ExchangeType:            firstNonEmpty(options.ExchangeType, defaults.ExchangeType),
		Declare:                 options.Declare.Or(defaults.Declare.Or(defaultRabbitMQDeclare)),
		ExchangeDurable:         options.ExchangeDurable.Or(defaults.ExchangeDurable.Or(defaultRabbitMQExchangeDurable)),
		QueueDurable:            options.QueueDurable.Or(defaults.QueueDurable.Or(defaultRabbitMQQueueDurable)),
		QueueMaxPriority:        options.QueueMaxPriority,
		MessagePersistent:       options.MessagePersistent.Or(defaults.MessagePersistent.Or(defaultRabbitMQPersistent)),
		AutoDelete:              options.AutoDelete.Or(defaults.AutoDelete.Or(false)),
		Exclusive:               options.Exclusive.Or(defaults.Exclusive.Or(false)),
		NoWait:                  options.NoWait.Or(defaults.NoWait.Or(false)),
		Confirm:                 options.Confirm.Or(defaults.Confirm.Or(defaultRabbitMQConfirm)),
		DelayMode:               firstNonEmpty(options.DelayMode, defaults.DelayMode),
		DelayBuckets:            sanitizeRabbitMQDelayBuckets(options.DelayBuckets),
		Prefetch:                options.Prefetch,
		Heartbeat:               options.Heartbeat,
		PublishTimeout:          options.PublishTimeout,
		PublishChannels:         normalizeRabbitMQPublishChannels(options.PublishChannels),
		Codec:                   options.Codec,
		ReconnectMinDelay:       options.ReconnectMinDelay,
		ReconnectMaxDelay:       options.ReconnectMaxDelay,
		RestartQueue:            firstNonEmpty(options.RestartQueue, defaults.RestartQueue),
		RestartEnabled:          options.RestartEnabled.Or(defaults.RestartEnabled.Or(defaultRabbitMQRestartEnabled)),
		RestartPollInterval:     options.RestartPollInterval,
		TopologyCacheTTL:        options.TopologyCacheTTL,
		TopologyCacheMaxEntries: options.TopologyCacheMaxEntries,
	}
	if resolved.Prefetch <= 0 {
		resolved.Prefetch = defaults.Prefetch
	}
	if resolved.Heartbeat <= 0 {
		resolved.Heartbeat = defaults.Heartbeat
	}
	if resolved.PublishTimeout <= 0 {
		resolved.PublishTimeout = defaults.PublishTimeout
	}
	if resolved.ReconnectMinDelay <= 0 {
		resolved.ReconnectMinDelay = defaults.ReconnectMinDelay
	}
	if resolved.ReconnectMaxDelay <= 0 {
		resolved.ReconnectMaxDelay = defaults.ReconnectMaxDelay
	}
	if resolved.ReconnectMaxDelay < resolved.ReconnectMinDelay {
		resolved.ReconnectMaxDelay = defaults.ReconnectMaxDelay
		if resolved.ReconnectMaxDelay < resolved.ReconnectMinDelay {
			resolved.ReconnectMaxDelay = resolved.ReconnectMinDelay
		}
	}
	if resolved.RestartPollInterval <= 0 {
		resolved.RestartPollInterval = defaults.RestartPollInterval
	}
	if resolved.TopologyCacheTTL < 0 {
		resolved.TopologyCacheTTL = 0
	}
	if resolved.TopologyCacheMaxEntries < 0 {
		resolved.TopologyCacheMaxEntries = 0
	}
	return resolved
}

// normalizeRabbitMQPublishChannels 修正发布 channel 池大小。
//
// 参数说明：
// - value：来自 Options.PublishChannels、配置文件或环境变量的原始值。
//
// 设计原因：
// 默认值 1 保持历史串行发布行为；上限 128 用来防止误配置在单个 AMQP connection 上创建过多 channel。
// 该函数集中处理边界，确保连接初始化、测试 fake 和运行期 slot 选择使用同一套规则。
func normalizeRabbitMQPublishChannels(value int) int {
	if value <= 0 {
		return defaultRabbitMQPublishChannels
	}
	if value > maxRabbitMQPublishChannels {
		// 截断警告：配置值超过上限时静默截断可能导致运维困惑，输出告警便于排查。
		fmt.Printf("[rabbitmq] warning: PublishChannels %d exceeds max %d, clamped\n", value, maxRabbitMQPublishChannels)
		return maxRabbitMQPublishChannels
	}
	return value
}

// sanitizeRabbitMQDelayBuckets 过滤非法 bucket；空配置或全非法配置回退默认值。
func sanitizeRabbitMQDelayBuckets(values []time.Duration) []time.Duration {
	if len(values) == 0 {
		return append([]time.Duration(nil), defaultRabbitMQDelayBuckets...)
	}
	out := make([]time.Duration, 0, len(values))
	for _, value := range values {
		if value > 0 {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		return append([]time.Duration(nil), defaultRabbitMQDelayBuckets...)
	}
	return out
}

func SanitizeDelayBuckets(values []time.Duration) []time.Duration {
	return sanitizeRabbitMQDelayBuckets(values)
}

func (o resolvedOptions) connectionURL() string {
	if strings.TrimSpace(o.URL) != "" {
		return strings.TrimSpace(o.URL)
	}

	u := &url.URL{
		Scheme: firstNonEmpty(o.Scheme, defaultRabbitMQScheme),
		Host:   rabbitMQJoinHostPort(firstNonEmpty(o.Host, defaultRabbitMQHost), firstNonEmpty(o.Port, defaultRabbitMQPort)),
		Path:   rabbitMQVHostPath(firstNonEmpty(o.VHost, defaultRabbitMQVHost)),
	}
	if o.Username != "" {
		if o.Password != "" {
			u.User = url.UserPassword(o.Username, o.Password)
		} else {
			u.User = url.User(o.Username)
		}
	}
	return u.String()
}

// normalizeRabbitMQQueues 统一 worker 输入队列列表，空列表映射到 default。
func normalizeRabbitMQQueues(queues []string) []string {
	return helper.NormalizeQueues(queues, "default")
}

func NormalizeQueues(queues []string) []string {
	return normalizeRabbitMQQueues(queues)
}

// normalizeRabbitMQQueueName 统一收敛 Push/Pop 使用的队列名来源。
//
// 优先级：
// 1. 调用方显式传入的 queue
// 2. Envelope 自身携带的 Queue
// 3. 默认队列 default
func normalizeRabbitMQQueueName(queue string, env *payload.Envelope) string {
	queue = strings.TrimSpace(queue)
	if queue != "" {
		return queue
	}
	if env != nil && strings.TrimSpace(env.Queue) != "" {
		return strings.TrimSpace(env.Queue)
	}
	return "default"
}

func NormalizeQueueName(queue string, env *payload.Envelope) string {
	return normalizeRabbitMQQueueName(queue, env)
}

func rabbitMQJoinHostPort(host, port string) string {
	if port == "" {
		return host
	}
	return net.JoinHostPort(host, port)
}

func JoinHostPort(host, port string) string {
	return rabbitMQJoinHostPort(host, port)
}

// rabbitMQVHostPath 把 RabbitMQ vhost 转成 AMQP URL path。
//
// 根 vhost 保持为 "/"；非根 vhost 需要 URL escape，但保留路径分隔语义以兼容层级化命名。
func rabbitMQVHostPath(vhost string) string {
	trimmed := strings.TrimSpace(vhost)
	if trimmed == "" || trimmed == "/" {
		return "/"
	}
	trimmed = strings.TrimPrefix(trimmed, "/")
	return "/" + strings.ReplaceAll(url.PathEscape(trimmed), "%2F", "/")
}

func VHostPath(vhost string) string {
	return rabbitMQVHostPath(vhost)
}

// redactedRabbitMQURL 只用于错误信息，避免把密码写入日志或测试输出。
func redactedRabbitMQURL(address string) string {
	parsed, err := url.Parse(address)
	if err != nil {
		return address
	}
	return parsed.Redacted()
}

func RedactedURL(address string) string {
	return redactedRabbitMQURL(address)
}

func DefaultDelayBuckets() []time.Duration {
	return append([]time.Duration(nil), defaultRabbitMQDelayBuckets...)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
