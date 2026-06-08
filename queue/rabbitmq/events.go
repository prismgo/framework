package rabbitmq

import (
	"context"

	queueevents "github.com/prismgo/framework/queue/internal/events"
	"github.com/prismgo/framework/queue/internal/observability"
)

const rabbitMQEventDriver = "rabbitmq"

// emitRabbitMQInfrastructureEvent 统一组装 RabbitMQ 基础设施事件 payload。
//
// 需求背景：
// issue 08 要求 RabbitMQ 生命周期事件只能通过 queue.UseEventSink 对外暴露，不能新增
// RabbitMQ 专用 logger 或应用侧观测 API。
//
// 参数说明：
// ctx 为事件上下文；eventName 为事件名称；connection 为 queue 连接名；queue 为队列名；
// exchange 为 exchange 名；attempt 为重连尝试次数；err 为需要转成文本的错误。
func emitRabbitMQInfrastructureEvent(ctx context.Context, eventName, connection, queue, exchange string, attempt int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	queueevents.Fire(ctx, observability.InfrastructureEvent(observability.InfrastructureFacts{
		EventName:  eventName,
		Connection: connection,
		Driver:     rabbitMQEventDriver,
		Queue:      queue,
		Exchange:   exchange,
		Attempt:    attempt,
		Err:        err,
	}))
}

// emitInfrastructureEvent 为当前 RabbitMQ connection 发出基础设施事件。
//
// 设计思路：
// connection 名称由 Connection 持有，调用点只需要传当前事件涉及的 queue、exchange、
// attempt 和 error，减少各生命周期路径重复拼装 payload 的机会。
//
// 参数说明：
// ctx 为事件上下文；eventName 为事件名称；queue/exchange 为事件关联的拓扑信息；
// attempt 为重连尝试次数；err 会被转换为错误文本写入事件。
func (c *Connection) emitInfrastructureEvent(ctx context.Context, eventName, queue, exchange string, attempt int, err error) {
	if c == nil {
		return
	}
	emitRabbitMQInfrastructureEvent(ctx, eventName, c.name, queue, exchange, attempt, err)
}

// emitPoisonEnvelopeEvent 统一组装 RabbitMQ 坏消息事件。
//
// 需求背景：
// poison envelope 发生在 Envelope 解码之前，不能伪造成 job_failed，也没有可信 job 元数据可以归档。
// 因此这里只暴露当前 Payload Encoding、原始 body 的有限前缀和处理动作，方便运维定位坏消息，
// 同时避免无限制携带敏感 payload。
//
// 参数说明：
// ctx 为 Pop 调用上下文；queue 是收到坏消息的业务队列；action 是 reject 或 reject_failed；
// body 是 broker 投递的原始消息体；err 是需要写入事件的解码或 reject 失败上下文。
func (c *Connection) emitPoisonEnvelopeEvent(ctx context.Context, queue, action string, body []byte, err error) {
	if c == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queueevents.Fire(ctx, observability.PoisonEnvelope(observability.PoisonEnvelopeFacts{
		Connection: c.name,
		Driver:     rabbitMQEventDriver,
		Queue:      queue,
		Action:     action,
		Encoding:   c.codecOrDefault().Name(),
		Body:       body,
		Err:        err,
	}))
}
