package events

import (
	"context"
	"sync"
	"time"
)

// 基础设施事件名称由 queue 父包和 RabbitMQ 等子包共享。
//
// 需求背景：
// RabbitMQ driver 位于 prismgo/queue/rabbitmq 子包，不能直接 import 父包 prismgo/queue，
// 否则会形成循环依赖。因此事件名称和最小事件桥放在 internal 包内，再由父包公开常量别名。
const (
	EventConnectionConnecting      = "queue.connection_connecting"
	EventConnectionConnected       = "queue.connection_connected"
	EventConnectionDisconnected    = "queue.connection_disconnected"
	EventConnectionReconnecting    = "queue.connection_reconnecting"
	EventConnectionReconnected     = "queue.connection_reconnected"
	EventConnectionReconnectFailed = "queue.connection_reconnect_failed"
	EventTopologyDeclared          = "queue.topology_declared"
	EventTopologyDeclareFailed     = "queue.topology_declare_failed"
	EventConsumerStarted           = "queue.consumer_started"
	EventConsumerStopped           = "queue.consumer_stopped"
	EventConsumerStopFailed        = "queue.consumer_stop_failed"
	EventPublishFailed             = "queue.publish_failed"
	EventPoisonEnvelope            = "queue.poison_envelope"
	// EventReleaseRepublishFailed 表示 RabbitMQ release 已 ack 原 delivery，但替换发布或 confirm 失败。
	//
	// 该事件用于运维告警：原 delivery 已经结束，替换消息可能没有进入 broker，调用方需要人工补偿或业务幂等重建。
	EventReleaseRepublishFailed = "queue.release_republish_failed"
)

const (
	PoisonEnvelopeActionReject       = "reject"
	PoisonEnvelopeActionRejectFailed = "reject_failed"
	PoisonEnvelopeActionDiscard      = "discard"
)

// Event 是 internal 事件桥使用的最小事件契约。
//
// 设计思路：
// 只要求 Name 方法，避免 internal 包依赖 prismgo/event 或 queue 父包中的具体事件结构。
type Event interface {
	Name() string
}

// InfrastructureEvent 描述队列 driver 的基础设施生命周期状态。
//
// 用途：
// 该结构用于暴露连接、重连、拓扑声明、consumer 启停和发布失败等运维可观测事件。
//
// 设计原因：
// payload 只保存连接名、driver、queue/exchange、重连次数、错误文本和时间戳，避免泄露
// RabbitMQ 原始连接对象、channel 句柄、凭据或 raw error 对象。
type InfrastructureEvent struct {
	// EventName 表示事件名称，取值应来自本文件中的基础设施事件常量。
	EventName string
	// Connection 表示 queue 配置里的连接名称。
	Connection string
	// Driver 表示队列后端类型，例如 rabbitmq。
	Driver string
	// Queue 表示事件关联的队列名称；连接级事件没有明确队列时为空。
	Queue string
	// Exchange 表示事件关联的 RabbitMQ exchange；非 RabbitMQ driver 可按需留空。
	Exchange string
	// Attempt 表示重连尝试次数；非重连事件为 0。
	Attempt int
	// Error 表示错误文本；成功事件为空字符串，不保存原始 error 对象。
	Error string
	// Timestamp 表示 driver 发出该事件的时间。
	Timestamp time.Time
}

func (e InfrastructureEvent) Name() string { return e.EventName }

// PoisonEnvelope 描述 driver 在 Envelope Payload Encoding 边界识别出的坏消息。
//
// 用途：
// 该事件只用于 raw message 已经无法恢复为可信 Envelope 的场景。它不同于 queue.job_failed，
// 因为此时没有可信 job id、job name 或 payload 可以写入 FailedStore。
//
// 安全边界：
// BodyBase64 会包含原始消息体前缀，可能带有敏感数据。监听方不得默认写入公开日志或低权限事件流。
type PoisonEnvelope struct {
	// Connection 是 queue 配置中的连接名称。
	Connection string
	// Driver 是触发事件的队列后端类型，RabbitMQ 当前固定为 rabbitmq。
	Driver string
	// Queue 是收到坏消息的业务队列名称。
	Queue string
	// Action 描述 driver 对坏消息采取的终结动作。
	Action string
	// Error 保存 Envelope 解码或终结动作失败的错误文本。
	Error string
	// Encoding 是本次 driver 尝试恢复 Envelope 时使用的 queue Payload Encoding。
	Encoding string
	// BodyBase64 保存原始 body 前缀的 base64 编码。
	BodyBase64 string
	// BodyEncoding 固定为 base64，便于监听方显式解码。
	BodyEncoding string
	// BodySize 是原始 body 的总字节数。
	BodySize int
	// BodyTruncated 表示 BodyBase64 是否只包含原始 body 前缀。
	BodyTruncated bool
	// Timestamp 是 driver 识别并处理坏消息的时间。
	Timestamp time.Time
}

func (e PoisonEnvelope) Name() string { return EventPoisonEnvelope }

// Sink 接收子包发出的 queue 事件，并由父包 queue.UseEventSink 注册。
//
// 参数说明：
// context.Context 用于沿用调用链上下文；Event 为具体事件 payload。
type Sink func(context.Context, Event)

var (
	sinkMu sync.RWMutex
	sink   Sink
)

// UseSink 安装 internal 事件桥的目标 sink。
//
// 参数说明：
// fn 为父包 queue.UseEventSink 传入的转发函数；传 nil 表示清空事件出口。
func UseSink(fn Sink) {
	sinkMu.Lock()
	defer sinkMu.Unlock()
	sink = fn
}

// Fire 发布 internal 事件。
//
// 逻辑说明：
// 如果父包尚未注册 sink，或传入事件为空，则直接 no-op，保证 driver 可在没有监听器时零侵入运行。
//
// 参数说明：
// ctx 为事件上下文；ev 为要发布的基础设施事件或其他 internal queue 事件。
func Fire(ctx context.Context, ev Event) {
	sinkMu.RLock()
	fn := sink
	sinkMu.RUnlock()
	if fn != nil && ev != nil {
		fn(ctx, ev)
	}
}
