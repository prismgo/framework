package queue

import (
	"context"
	"time"

	"github.com/prismgo/framework/contracts/cache"
)

// ConnectionProvider 允许任务声明默认队列连接。
//
// 用途：任务实现此接口后，Dispatch 时若未显式指定连接则使用此处返回的值。
//
// 使用方式：
//
//	func (j *EmailJob) QueueConnection() string { return "redis" }
type ConnectionProvider interface {
	QueueConnection() string
}

// QueueProvider 允许任务声明默认队列名称。
//
// 用途：任务实现此接口后，Dispatch 时若未显式指定队列则使用此处返回的值。
//
// 使用方式：
//
//	func (j *EmailJob) QueueName() string { return "emails" }
type QueueProvider interface {
	QueueName() string
}

// DelayProvider 允许任务声明默认延迟时间。
//
// 用途：任务入队后在指定时间后才能被 worker 消费。
type DelayProvider interface {
	QueueDelay() time.Duration
}

// TriesProvider 允许任务声明最大尝试次数。
//
// 用途：任务失败后最多重试的次数限制。
type TriesProvider interface {
	Tries() int
}

// TimeoutProvider 允许任务声明单次执行超时。
//
// 用途：worker 在超时后取消任务上下文。
type TimeoutProvider interface {
	Timeout() time.Duration
}

// BackoffProvider 允许任务声明重试退避序列。
//
// 用途：定义每次重试之间的等待时间序列，如 [10s, 30s, 1m]。
//
// 使用方式：
//
//	func (j *EmailJob) Backoff() []time.Duration {
//	    return []time.Duration{10*time.Second, 30*time.Second, time.Minute}
//	}
type BackoffProvider interface {
	Backoff() []time.Duration
}

// RetryUntilProvider 允许任务声明重试截止时间。
//
// 用途：超过截止时间后不再重试，直接标记失败。
type RetryUntilProvider interface {
	RetryUntil() time.Time
}

// MaxExceptionsProvider 允许任务声明最大异常次数。
//
// 用途：任务抛出的异常（非预期错误）达到上限后标记失败，不等待尝试次数耗尽。
type MaxExceptionsProvider interface {
	MaxExceptions() int
}

// FailOnTimeoutProvider 允许任务声明超时后直接失败。
//
// 用途：返回 true 时任务超时直接标记失败并跳过重试。
type FailOnTimeoutProvider interface {
	FailOnTimeout() bool
}

// EncryptedProvider 允许任务声明 payload 需要加密保存。
//
// 用途：返回 true 时队列将任务数据加密后存储，防止敏感信息泄露。
//
// 使用方式：
//
//	func (j *ExportJob) ShouldEncrypt() bool { return true }
type EncryptedProvider interface {
	ShouldEncrypt() bool
}

// TagsProvider 允许任务声明 Horizon 展示的标签。
//
// 用途：返回一组稳定元数据标签，Horizon 界面据此过滤和分类任务。
// 不应返回敏感信息（凭证、完整 payload 等）。
type TagsProvider interface {
	Tags() []string
}

// SilencedProvider 允许任务在 Horizon 默认视图中静默。
//
// 用途：返回 true 时任务不出现在 Horizon 的普通 recent/failed 列表中，
// 适合低频或高敏感的辅助任务。
type SilencedProvider interface {
	Silenced() bool
}

// MiddlewareProvider 允许任务声明自己的 middleware 链。
//
// 用途：返回的 Middleware 列表会在任务执行前后包装调用。
type MiddlewareProvider interface {
	Middleware() []Middleware
}

// UniqueIDProvider 允许任务声明唯一任务 ID。
//
// 用途：同 ID 的任务在唯一锁有效期内不会重复入队。
type UniqueIDProvider interface {
	UniqueID() string
}

// UniqueForProvider 允许任务声明唯一锁保留时间。
type UniqueForProvider interface {
	UniqueFor() time.Duration
}

// UniqueViaProvider 允许任务声明唯一锁使用的缓存 store。
type UniqueViaProvider interface {
	UniqueVia() cache.Repository
}

// UniqueUntilProcessingProvider 允许任务在执行开始前释放唯一锁。
//
// 用途：返回 true 时，防重复入队只保护到任务被 worker 取出为止。
type UniqueUntilProcessingProvider interface {
	UniqueUntilProcessing() bool
}

// DebounceIDProvider 允许任务声明防抖 ID。
//
// 用途：同 ID 的任务在防抖窗口内重复投递时只保留最后一个。
type DebounceIDProvider interface {
	DebounceID() string
}

// DebounceForProvider 允许任务声明防抖窗口。
type DebounceForProvider interface {
	DebounceFor() time.Duration
}

// DebounceViaProvider 允许任务声明防抖状态使用的缓存 store。
type DebounceViaProvider interface {
	DebounceVia() cache.Repository
}

// FailedProvider 允许任务在最终失败时接收回调。
//
// 用途：任务耗尽所有重试后，框架调用此方法通知任务执行清理或告警。
//
// 使用方式：
//
//	func (j *ExportJob) Failed(ctx context.Context, err error) {
//	    notifyService.Alert(ctx, "导出任务最终失败", err)
//	}
type FailedProvider interface {
	Failed(context.Context, error)
}
