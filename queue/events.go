package queue

import (
	"context"
	"sync"
	"time"

	queueevents "github.com/prismgo/framework/queue/internal/events"
	"github.com/prismgo/framework/queue/payload"
)

const (
	EventJobQueued      = "queue.job_queued"
	EventJobProcessing  = "queue.job_processing"
	EventJobProcessed   = "queue.job_processed"
	EventJobReleased    = "queue.job_released"
	EventJobFailed      = "queue.job_failed"
	EventBatchCreated   = "queue.batch_created"
	EventBatchUpdated   = "queue.batch_updated"
	EventBatchCancelled = "queue.batch_cancelled"
	EventBatchFinished  = "queue.batch_finished"

	// 队列基础设施生命周期事件由 driver 触发，并通过与 job/batch 相同的 UseEventSink 出口观察。
	EventConnectionConnecting      = queueevents.EventConnectionConnecting
	EventConnectionConnected       = queueevents.EventConnectionConnected
	EventConnectionDisconnected    = queueevents.EventConnectionDisconnected
	EventConnectionReconnecting    = queueevents.EventConnectionReconnecting
	EventConnectionReconnected     = queueevents.EventConnectionReconnected
	EventConnectionReconnectFailed = queueevents.EventConnectionReconnectFailed
	EventTopologyDeclared          = queueevents.EventTopologyDeclared
	EventTopologyDeclareFailed     = queueevents.EventTopologyDeclareFailed
	EventConsumerStarted           = queueevents.EventConsumerStarted
	EventConsumerStopped           = queueevents.EventConsumerStopped
	EventConsumerStopFailed        = queueevents.EventConsumerStopFailed
	EventPublishFailed             = queueevents.EventPublishFailed
	EventPoisonEnvelope            = queueevents.EventPoisonEnvelope
	// EventReleaseRepublishFailed 是 RabbitMQ release 替换发布失败的公开事件名。
	//
	// 业务侧监听该事件时，应按“原 delivery 已 ack，替换消息可能丢失”的运维告警处理。
	EventReleaseRepublishFailed = queueevents.EventReleaseRepublishFailed

	PoisonEnvelopeActionReject       = queueevents.PoisonEnvelopeActionReject
	PoisonEnvelopeActionRejectFailed = queueevents.PoisonEnvelopeActionRejectFailed
	PoisonEnvelopeActionDiscard      = queueevents.PoisonEnvelopeActionDiscard
)

// Event 是队列生命周期事件的最小契约。
//
// 该接口刻意不依赖 prismgo/event，避免 queue 与 event 形成循环依赖。foundation
// 会把这里的事件 sink 接到 prismgo/event.Dispatch。
type Event interface {
	Name() string
}

// JobFailed 是 queue.job_failed 生命周期事件载荷。
//
// 设计思路：payload.FailedJob 是 failed store 使用的持久化 DTO，不能直接承担
// 事件契约；这里通过嵌入复用归档字段，让监听器读取字段时保持简洁，同时把
// “事件命名”和“失败归档结构”两个职责拆开。
type JobFailed struct {
	payload.FailedJob
}

// Name 返回队列失败事件的稳定事件名，供 queue.Event sink 和 prismgo/event 订阅匹配使用。
func (JobFailed) Name() string {
	return EventJobFailed
}

type eventSink func(context.Context, Event)

type eventObserverContextKey struct{}

var (
	sinkMu sync.RWMutex
	sink   eventSink
)

// UseEventSink 注册队列生命周期事件出口。
func UseEventSink(fn func(context.Context, Event)) {
	sinkMu.Lock()
	defer sinkMu.Unlock()
	sink = fn
	if fn == nil {
		queueevents.UseSink(nil)
		return
	}
	// 子包事件通过 internal bridge 回流到父包 sink，避免 rabbitmq 子包直接 import prismgo/queue
	// 形成循环依赖。
	queueevents.UseSink(func(ctx context.Context, ev queueevents.Event) {
		fire(ctx, ev)
	})
}

// CurrentEventSink 返回当前队列生命周期事件出口。
//
// 用途：提供当前全局事件出口的只读查询能力，便于诊断或高级集成确认事件桥是否已经安装。
// 设计思路：返回当前函数值的快照；并发安全由 sinkMu 保护读取。worker 级观测应通过
// WorkerOptions.EventObserver 注入，不应通过临时替换进程级 sink 承载。
func CurrentEventSink() func(context.Context, Event) {
	sinkMu.RLock()
	defer sinkMu.RUnlock()
	return sink
}

func fire(ctx context.Context, ev Event) {
	if observer := eventObserverFromContext(ctx); observer != nil && ev != nil {
		if observedCtx := observer(ctx, ev); observedCtx != nil {
			ctx = observedCtx
		}
	}
	sinkMu.RLock()
	fn := sink
	sinkMu.RUnlock()
	if fn != nil && ev != nil {
		fn(ctx, ev)
	}
}

func contextWithEventObserver(ctx context.Context, observer func(context.Context, Event) context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, eventObserverContextKey{}, observer)
}

func eventObserverFromContext(ctx context.Context) func(context.Context, Event) context.Context {
	if ctx == nil {
		return nil
	}
	observer, _ := ctx.Value(eventObserverContextKey{}).(func(context.Context, Event) context.Context)
	return observer
}

// JobQueued 表示任务已经进入队列连接。
type JobQueued struct {
	Connection string
	Queue      string
	JobID      string
	JobName    string
	Delay      time.Duration
	// Tags 是入队时已经确定的展示安全标签，供 Horizon 直接消费。
	Tags []string
	// Silenced 表示该任务默认不出现在 Horizon 普通任务列表。
	Silenced bool
	// QueuedAt 是任务写入队列的时间；driver 缺失该值时 Horizon 必须显示 unknown。
	QueuedAt time.Time
}

func (JobQueued) Name() string { return EventJobQueued }

// JobProcessing 表示 worker 即将执行任务。
type JobProcessing struct {
	Connection string
	Queue      string
	JobID      string
	JobName    string
	Attempts   int
	// Tags 延续 payload.Envelope 中的展示安全标签，不从 payload 反推。
	Tags []string
	// Silenced 延续 payload.Envelope 中的静默标记，展示层只消费该布尔值。
	Silenced bool
	// QueuedAt 延续 payload.Envelope 创建时间，用于需要时计算等待时间。
	QueuedAt time.Time
}

func (JobProcessing) Name() string { return EventJobProcessing }

// JobProcessed 表示任务执行成功。
type JobProcessed struct {
	Connection string
	Queue      string
	JobID      string
	JobName    string
	Duration   time.Duration
	// Tags 是任务完成事件携带的展示安全标签。
	Tags []string
	// Silenced 表示该任务完成摘要是否从默认 Horizon 列表中过滤。
	Silenced bool
}

func (JobProcessed) Name() string { return EventJobProcessed }

// JobReleased 表示任务失败后被释放回队列等待下一次重试。
type JobReleased struct {
	Connection string
	Queue      string
	JobID      string
	JobName    string
	Delay      time.Duration
	Err        string
	// Tags 是任务释放重试事件携带的展示安全标签。
	Tags []string
	// Silenced 表示该释放摘要是否从默认 Horizon 列表中过滤。
	Silenced bool
}

func (JobReleased) Name() string { return EventJobReleased }

// BatchEvent 表示批次状态发生变化。
type BatchEvent struct {
	EventName string
	Batch     payload.BatchStatus
}

func (e BatchEvent) Name() string { return e.EventName }

// InfrastructureEvent 是队列 driver 生命周期可观测事件的公开 payload。
type InfrastructureEvent = queueevents.InfrastructureEvent

// PoisonEnvelope 是 driver 按当前 payload.Payload Encoding 解码 payload.Envelope 失败时发出的公开事件 payload。
//
// 事件中的 BodyBase64 可能包含敏感原始消息片段，监听方需要按安全事件处理，避免直接写入公开日志。
type PoisonEnvelope = queueevents.PoisonEnvelope
