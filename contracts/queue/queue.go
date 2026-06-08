package queue

import (
	"context"
	"time"
)

// Payload 是 driver 层传输的已编码 Job payload。
//
// 设计说明：contracts 只描述抽象能力，不承载 envelope、batch 等 durable DTO；具体
// 存储模型位于 prismgo/queue/payload。
type Payload []byte

// BulkResult 描述批量投递被 transport 接收的 payload 数量。
//
// 设计背景：批量投递可能出现部分成功，例如 RabbitMQ publisher confirm 中前几条已 ack、
// 后续条目 nack、unrouted 或超时。调用方需要知道已进入 transport 的数量，才能保留
// batch metadata，并把未接收的任务计入失败，避免已入队任务执行时找不到 batch。
type BulkResult struct {
	// Accepted 是本次 bulk 中已经被 transport 接收的 payload 数量；成功时必须等于 len(bodies)。
	Accepted int
}

// Queue 是 Laravel 风格队列 connection 的窄契约。
type Queue interface {
	// Push 将已编码 payload 推入指定队列。
	Push(ctx context.Context, queue string, body Payload) error

	// Later 将已编码 payload 延迟推入指定队列。
	Later(ctx context.Context, queue string, body Payload, delay time.Duration) error

	// Bulk 批量推入已编码 payload，并返回 transport 已接收数量。
	Bulk(ctx context.Context, queue string, bodies []Payload) (BulkResult, error)

	// Pop 从一个或多个队列中拉取一个 reserved job；无任务时返回实现层的 empty 错误。
	//
	// 参数 queues 是按优先级排列的队列名列表。调用方应优先传入已归一化队列名；
	// driver 保留空列表 fallback 只用于防御直接调用或自定义 driver 场景。
	//
	// 参数 wait 省略时默认 PopWaitAvailable；多队列 worker 的非阻塞扫描必须显式传
	// PopNoWait，避免通过下标或负数协议隐式表达等待策略。
	Pop(ctx context.Context, queues []string, wait ...PopWaitMode) (ReservedJob, error)

	// Size 返回指定队列待处理任务数。
	Size(ctx context.Context, queue string) (int64, error)

	// Clear 清空指定队列。
	Clear(ctx context.Context, queue string) error

	// Close 释放连接资源。
	Close() error
}

// PopWaitMode 表示一次 Pop 调用是否允许等待可用任务。
//
// 设计原因：等待行为属于 Pop 调用语义，不应通过队列下标、负数标记或 driver 私有状态隐式表达。
type PopWaitMode uint8

const (
	// PopNoWait 表示只执行一次非阻塞拉取；当前没有可用任务时立即返回 empty 错误。
	PopNoWait PopWaitMode = iota

	// PopWaitAvailable 表示允许 driver 按连接配置等待可用任务，例如 block_for。
	PopWaitAvailable
)

// PopSessionProvider creates a worker-local Queue view for Pop state.
type PopSessionProvider interface {
	NewPopSession() Queue
}

// WorkerSession 表示一个 worker 生命周期内持有的队列消费资源。
//
// 设计原因：Horizon 等外层运行器需要多轮执行单次 worker 消费，但不应直接管理
// PopSessionProvider、ConsumerIntentLeaser 或底层 queue connection。
type WorkerSession interface {
	// Activate 开始底层消费意图。
	//
	// 调用方可以在监控链路启动后再调用该方法，使 consumer_started 等事件进入采集链路。
	// 多次调用必须幂等。
	Activate(context.Context) error

	// Work 执行一轮 worker 消费。
	//
	// 实现方负责确保每轮只消费一个任务，并复用同一个 worker-local queue view。
	Work(context.Context) error

	// Close 释放 worker 生命周期资源。
	//
	// Close 必须释放 pop session 和 consumer intent；多次调用应保持安全。
	Close() error
}

// ReservedJob 是 worker 已保留、等待执行或释放的任务。
type ReservedJob interface {
	// ID 返回 payload envelope 中的任务 ID。
	ID() string

	// Name 返回 payload envelope 中的任务名称。
	Name() string

	// Payload 返回原始 Job payload 字节。
	Payload() Payload

	// Attempts 返回当前任务已尝试次数。
	Attempts() int

	// Delete 确认并删除已完成任务。
	Delete(ctx context.Context) error

	// Release 将任务释放回队列，并可指定延迟重试时间。
	Release(ctx context.Context, delay time.Duration) error
}

// Connector 按连接名和配置创建 Queue，供 Manager 注册自定义 driver。
type Connector interface {
	Connect(ctx context.Context, name string, config map[string]any) (Queue, error)
}

// Factory 解析默认连接、命名连接和自定义 connector。
type Factory interface {
	Queue(name string) (Queue, error)
}

// RestartStore 是 worker 重启信号存储的契约。
type RestartStore interface {
	RequestRestart(ctx context.Context, at time.Time) error
	RestartRequestedAt(ctx context.Context) (time.Time, error)
}

// ConsumerIntentLeaser 是 push-consumer 驱动 worker 生命周期管理的可选契约。
type ConsumerIntentLeaser interface {
	AcquireConsumerIntent(queues []string) (func() error, error)
}

// Next 是 middleware 调用下一个处理器的函数。
type Next func(context.Context) error

// Middleware 是队列任务中间件的接口。
type Middleware interface {
	Handle(ctx context.Context, job Job, next Next) error
}
