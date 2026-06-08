// Package queue 提供 Laravel 风格的队列基础设施。
//
// 队列包只依赖通用基础包和 Redis 客户端，不反向依赖业务代码。任务通过 Go
// 类型名和当前 queue Payload Encoding 编码后的 payload 持久化；worker 通过 RegisterType
// 注册的类型表恢复任务实例。
package queue

import (
	"errors"
	"strings"
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	queueerrors "github.com/prismgo/framework/queue/internal/errors"
	"github.com/prismgo/framework/queue/payload"
	rabbitmqdriver "github.com/prismgo/framework/queue/rabbitmq"
)

var (
	// ErrEmpty 表示当前队列没有可消费任务。
	ErrEmpty = queueerrors.ErrEmpty
	// ErrJobNotRegistered 表示 worker 找不到任务类型对应的反序列化工厂。
	ErrJobNotRegistered = errors.New("queue: job not registered")
	// ErrDuplicate 表示唯一任务锁已存在，本次投递被跳过。
	ErrDuplicate = errors.New("queue: duplicate unique job")
	// ErrSkipped 表示任务被 middleware 主动跳过，worker 会按成功处理并删除任务。
	ErrSkipped = errors.New("queue: job skipped")
	// ErrBatchCancelled 表示任务所属批次已取消。
	ErrBatchCancelled = errors.New("queue: batch cancelled")
	// ErrManagerClosed 表示队列管理器已经关闭，不能再解析或返回连接。
	ErrManagerClosed = errors.New("queue: manager closed")
	// ErrConnectionClosed 表示连接已经关闭，后续操作不能继续执行。
	ErrConnectionClosed = rabbitmqdriver.ErrConnectionClosed
	// ErrUnsupportedOperation 表示当前 driver 尚未支持对应语义或操作。
	ErrUnsupportedOperation = rabbitmqdriver.ErrUnsupportedOperation
	// ErrPoisonEnvelope 表示 driver 已取到原始消息，但消息体无法按当前 Payload Encoding 解码为 Prismgo Envelope。
	ErrPoisonEnvelope = rabbitmqdriver.ErrPoisonEnvelope
	// ErrUnsupportedRetryAfter 表示 RabbitMQ driver 不支持 Redis 风格的 retry_after visibility timeout。
	ErrUnsupportedRetryAfter = errors.New("queue: rabbitmq does not support retry_after visibility timeout")
	// ErrRabbitMQDialFailed 表示 RabbitMQ driver 初始化阶段建立 AMQP 连接失败。
	ErrRabbitMQDialFailed = rabbitmqdriver.ErrRabbitMQDialFailed
	// ErrRabbitMQTopologyMissing 表示关闭自动声明后，RabbitMQ 目标 exchange 或 queue 不存在。
	ErrRabbitMQTopologyMissing = rabbitmqdriver.ErrRabbitMQTopologyMissing
	// ErrRabbitMQPublishNacked 表示 RabbitMQ broker 明确拒绝了本次 publisher confirm。
	ErrRabbitMQPublishNacked = rabbitmqdriver.ErrRabbitMQPublishNacked
	// ErrRabbitMQPublishTimeout 表示等待 RabbitMQ publisher confirm 超时。
	ErrRabbitMQPublishTimeout = rabbitmqdriver.ErrRabbitMQPublishTimeout
	// ErrRabbitMQPublishConfirmClosed 表示等待 RabbitMQ publisher confirm 时确认通道已经关闭。
	ErrRabbitMQPublishConfirmClosed = rabbitmqdriver.ErrRabbitMQPublishConfirmClosed
	// ErrRabbitMQPublishUnrouted 表示 mandatory RabbitMQ 发布没有路由到任何队列。
	// 调用方可通过 errors.Is 判断该错误，并检查 exchange、routing key、binding 或 declare=false 预建 topology。
	ErrRabbitMQPublishUnrouted = rabbitmqdriver.ErrRabbitMQPublishUnrouted
	// ErrRabbitMQReleaseRepublishFailed 表示 RabbitMQ release 已 ack 原 delivery，但替换发布或 confirm 失败。
	ErrRabbitMQReleaseRepublishFailed = rabbitmqdriver.ErrRabbitMQReleaseRepublishFailed
)

// Job 是可投递任务的最小契约。
//
// 该类型别名指向 contracts/queue.Job。
type Job = queuecontract.Job

// ---- 任务行为提供者接口 ----
// 以下接口由 Job 可选实现，供 dispatch runtime 通过类型断言读取默认配置。

// ConnectionProvider 允许任务声明默认队列连接，对应 Laravel job 的 $connection。
type ConnectionProvider = queuecontract.ConnectionProvider

// QueueProvider 允许任务声明默认队列名称，对应 Laravel job 的 $queue。
type QueueProvider = queuecontract.QueueProvider

// DelayProvider 允许任务声明默认延迟，对应 Laravel 的 delay 语义。
type DelayProvider = queuecontract.DelayProvider

// ConsumerIntentLeaser 是 RabbitMQ 等 push-consumer driver 可选实现的 worker 生命周期接口。
type ConsumerIntentLeaser = queuecontract.ConsumerIntentLeaser

// TriesProvider 允许任务声明最大尝试次数，对应 Laravel job 的 $tries。
type TriesProvider = queuecontract.TriesProvider

// TimeoutProvider 允许任务声明单次执行超时，对应 Laravel job 的 $timeout。
type TimeoutProvider = queuecontract.TimeoutProvider

// BackoffProvider 允许任务声明重试退避序列，对应 Laravel 的 backoff 方法。
type BackoffProvider = queuecontract.BackoffProvider

// RetryUntilProvider 允许任务声明重试截止时间，对应 Laravel 的 retryUntil 方法。
type RetryUntilProvider = queuecontract.RetryUntilProvider

// MaxExceptionsProvider 允许任务声明最大异常次数，对应 Laravel job 的 $maxExceptions。
type MaxExceptionsProvider = queuecontract.MaxExceptionsProvider

// FailOnTimeoutProvider 允许任务声明超时后直接失败，对应 Laravel job 的 $failOnTimeout。
type FailOnTimeoutProvider = queuecontract.FailOnTimeoutProvider

// EncryptedProvider 允许任务声明 payload 需要加密保存，对应 Laravel 的 ShouldBeEncrypted。
type EncryptedProvider = queuecontract.EncryptedProvider

// TagsProvider 允许任务显式声明 Horizon 可展示的安全标签。
type TagsProvider = queuecontract.TagsProvider

// SilencedProvider 允许任务显式声明 Horizon 默认展示时静默。
type SilencedProvider = queuecontract.SilencedProvider

// MiddlewareProvider 允许任务声明自己的 middleware 链，对应 Laravel 的 middleware 方法。
type MiddlewareProvider = queuecontract.MiddlewareProvider

// UniqueIDProvider 允许任务声明唯一任务 ID，对应 Laravel 的 uniqueId 方法。
type UniqueIDProvider = queuecontract.UniqueIDProvider

// UniqueForProvider 允许任务声明唯一锁保留时间，对应 Laravel job 的 $uniqueFor。
type UniqueForProvider = queuecontract.UniqueForProvider

// UniqueViaProvider 允许任务声明唯一锁使用的缓存 store，对应 Laravel 的 uniqueVia 方法。
type UniqueViaProvider = queuecontract.UniqueViaProvider

// UniqueUntilProcessingProvider 允许任务在开始执行前释放唯一锁。
type UniqueUntilProcessingProvider = queuecontract.UniqueUntilProcessingProvider

// DebounceIDProvider 允许任务声明防抖 ID。
type DebounceIDProvider = queuecontract.DebounceIDProvider

// DebounceForProvider 允许任务声明防抖窗口。
type DebounceForProvider = queuecontract.DebounceForProvider

// DebounceViaProvider 允许任务声明防抖状态使用的缓存 store。
type DebounceViaProvider = queuecontract.DebounceViaProvider

// FailedProvider 允许任务在最终失败时接收回调，对应 Laravel 的 failed 方法。
type FailedProvider = queuecontract.FailedProvider

// DispatchOptions 控制一次投递的连接、队列、延迟和高级语义。
// DispatchOptions 控制一次投递的连接、队列、延迟和高级语义。
type DispatchOptions struct {
	// Connection 是目标队列连接名称。
	Connection string
	// Queue 是目标队列名称。
	Queue string
	// Delay 是延迟投递时间。
	Delay time.Duration
	// Tries 是最大尝试次数。
	Tries int
	// MaxExceptions 是最大异常次数，超出后直接标记失败。
	MaxExceptions int
	// Timeout 是单次执行超时时间。
	Timeout time.Duration
	// FailOnTimeout 表示超时后是否直接失败跳过重试。
	FailOnTimeout bool
	// Encrypted 表示 payload 是否需要加密存储。
	Encrypted bool
	// Backoff 是重试退避时间序列。
	Backoff []time.Duration
	// RetryUntil 是允许重试的最后时间。
	RetryUntil time.Time
	// Chain 是链式后续任务列表，当前任务成功后依次投递。
	Chain []payload.PendingJob
	// BatchID 是所属批次 ID。
	BatchID string
	// UniqueKey 是唯一任务锁标识。
	UniqueKey string
	// UniqueFor 是唯一锁保留时间。
	UniqueFor time.Duration
	// UniqueUntil 表示是否在开始处理前释放唯一锁。
	UniqueUntil bool
	// DebounceKey 是防抖标识，同 Key 的任务在窗口中只保留最后一个。
	DebounceKey string
	// DebounceFor 是防抖窗口时间。
	DebounceFor time.Duration
	// Tags 是本次投递显式传入的 Horizon 展示安全标签。
	Tags []string
	// Silenced 表示本次投递在 Horizon 默认视图中静默。
	Silenced bool

	// uniqueVia 是唯一锁使用的缓存 store（内部字段，通过 UniqueVia 选项设置）。
	uniqueVia cachecontract.Repository
	// debounceVia 是防抖状态使用的缓存 store（内部字段，通过 DebounceVia 选项设置）。
	debounceVia cachecontract.Repository
}

// DispatchOption 是 Dispatch 的函数式选项。
type DispatchOption func(*DispatchOptions)

// OnConnection 指定任务投递到哪个连接。
func OnConnection(name string) DispatchOption {
	return func(o *DispatchOptions) { o.Connection = name }
}

// OnQueue 指定任务投递到哪个队列名称。
func OnQueue(name string) DispatchOption {
	return func(o *DispatchOptions) { o.Queue = name }
}

// Delay 指定任务最早可执行时间。
func Delay(d time.Duration) DispatchOption {
	return func(o *DispatchOptions) { o.Delay = d }
}

// Tries 指定任务最大尝试次数。
func Tries(n int) DispatchOption {
	return func(o *DispatchOptions) { o.Tries = n }
}

// MaxExceptions 指定任务最多允许失败错误次数。
func MaxExceptions(n int) DispatchOption {
	return func(o *DispatchOptions) { o.MaxExceptions = n }
}

// Timeout 指定单次任务执行超时时间。
func Timeout(d time.Duration) DispatchOption {
	return func(o *DispatchOptions) { o.Timeout = d }
}

// FailOnTimeout 指定任务超时时是否直接失败。
func FailOnTimeout() DispatchOption {
	return func(o *DispatchOptions) { o.FailOnTimeout = true }
}

// Encrypt 声明本次投递的 Job payload 需要加密保存。
func Encrypt() DispatchOption {
	return func(o *DispatchOptions) { o.Encrypted = true }
}

// Tags 显式传入 Horizon 可展示的安全标签。
//
// 参数说明：values 是调用方传入的标签列表，会在写入 DispatchOptions 前清理空白项并去重。
func Tags(values ...string) DispatchOption {
	return func(o *DispatchOptions) { o.Tags = normalizeOptionStrings(values) }
}

// Silenced 显式声明本次投递在 Horizon 默认视图中静默。
//
// 使用方式：适合低价值、高频或只在专门 Silenced Jobs 页面查看的任务；该选项不会改变
// worker 执行、失败归档或 metrics 聚合行为。
func Silenced() DispatchOption {
	return func(o *DispatchOptions) { o.Silenced = true }
}

// Backoff 指定任务失败后的退避时间序列。
func Backoff(values ...time.Duration) DispatchOption {
	return func(o *DispatchOptions) { o.Backoff = append([]time.Duration(nil), values...) }
}

// RetryUntil 指定任务可继续重试的最后时间。
func RetryUntil(t time.Time) DispatchOption {
	return func(o *DispatchOptions) { o.RetryUntil = t }
}

// Unique 为本次投递声明唯一任务锁。
func Unique(key string, ttl time.Duration) DispatchOption {
	return func(o *DispatchOptions) {
		o.UniqueKey = key
		o.UniqueFor = ttl
	}
}

// UniqueVia 指定本次投递的唯一任务状态使用哪个缓存 store。
func UniqueVia(store cachecontract.Repository) DispatchOption {
	return func(o *DispatchOptions) { o.uniqueVia = store }
}

// UniqueUntilProcessing 声明唯一锁在任务开始执行前释放。
func UniqueUntilProcessing() DispatchOption {
	return func(o *DispatchOptions) { o.UniqueUntil = true }
}

// Debounce 为本次投递声明防抖窗口。
func Debounce(key string, ttl time.Duration) DispatchOption {
	return func(o *DispatchOptions) {
		o.DebounceKey = key
		o.DebounceFor = ttl
	}
}

// DebounceVia 指定本次投递的防抖状态使用哪个缓存 store。
func DebounceVia(store cachecontract.Repository) DispatchOption {
	return func(o *DispatchOptions) { o.debounceVia = store }
}

func withChain(chain []payload.PendingJob) DispatchOption {
	return func(o *DispatchOptions) { o.Chain = append([]payload.PendingJob(nil), chain...) }
}

func withBatch(id string) DispatchOption {
	return func(o *DispatchOptions) { o.BatchID = id }
}

func applyOptions(options ...DispatchOption) DispatchOptions {
	var result DispatchOptions
	for _, option := range options {
		if option != nil {
			option(&result)
		}
	}
	return result
}

// normalizeOptionStrings 清理 DispatchOptions 中的展示安全字符串列表。
//
// 参数说明：values 是调用方或 Job provider 返回的原始标签列表；返回值会去掉空白项并按
// 首次出现顺序去重，保证同一 Envelope 上的 tags 稳定且不会无限膨胀。
func normalizeOptionStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func seconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d.Seconds())
}

func unixSeconds(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func durations(values []int) []time.Duration {
	result := make([]time.Duration, 0, len(values))
	for _, value := range values {
		if value > 0 {
			result = append(result, time.Duration(value)*time.Second)
		}
	}
	return result
}

func secondsList(values []time.Duration) []int {
	result := make([]int, 0, len(values))
	for _, value := range values {
		if value > 0 {
			result = append(result, seconds(value))
		}
	}
	return result
}
