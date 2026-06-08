package queue

import "time"

// DispatchOptions 是跨包投递任务时可读取的队列选项契约。
//
// 用途：事件系统等跨包组件通过只读方法了解任务的连接配置、队列名称、
// 延迟、重试和超时设置，而不直接依赖实现包的配置 DSL。
type DispatchOptions interface {
	// QueueConnection 返回目标队列连接名称。
	QueueConnection() string

	// QueueName 返回目标队列名称。
	QueueName() string

	// QueueDelay 返回延迟投递时间。
	QueueDelay() time.Duration

	// QueueTries 返回最大尝试次数。
	QueueTries() int

	// QueueMaxExceptions 返回最大异常次数。
	QueueMaxExceptions() int

	// QueueTimeout 返回单次执行超时时间。
	QueueTimeout() time.Duration

	// QueueFailOnTimeout 返回超时后是否直接失败。
	QueueFailOnTimeout() bool

	// QueueEncrypted 返回 payload 是否需要加密。
	QueueEncrypted() bool

	// QueueBackoff 返回重试退避时间序列。
	QueueBackoff() []time.Duration

	// QueueRetryUntil 返回允许重试的最后时间。
	QueueRetryUntil() time.Time

	// QueueBatchID 返回所属批次 ID。
	QueueBatchID() string

	// QueueUniqueKey 返回唯一任务锁标识。
	QueueUniqueKey() string

	// QueueUniqueFor 返回唯一锁保留时间。
	QueueUniqueFor() time.Duration

	// QueueUniqueUntil 返回是否在开始处理前释放唯一锁。
	QueueUniqueUntil() bool

	// QueueDebounceKey 返回防抖标识。
	QueueDebounceKey() string

	// QueueDebounceFor 返回防抖窗口时间。
	QueueDebounceFor() time.Duration

	// QueueTags 返回 Horizon 展示安全标签。
	QueueTags() []string

	// QueueSilenced 返回是否在 Horizon 默认视图中静默。
	QueueSilenced() bool
}
