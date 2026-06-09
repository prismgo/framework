package rabbitmq

import (
	"context"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
)

// RabbitMQReservedJob 持有 RabbitMQ delivery 的确认状态。
//
// Delete 映射 AMQP ack；Release 保持既有语义：ack 原 delivery 后重新 publish 一条
// 替换消息，避免父包 worker 了解 AMQP delivery 细节。
type RabbitMQReservedJob struct {
	queue *RabbitMQQueue
	env   *payload.Envelope
	body  queuecontract.Payload
}

func (j *RabbitMQReservedJob) ID() string {
	if j == nil || j.env == nil {
		return ""
	}
	return j.env.ID
}

func (j *RabbitMQReservedJob) Name() string {
	if j == nil || j.env == nil {
		return ""
	}
	return j.env.Name
}

func (j *RabbitMQReservedJob) Payload() queuecontract.Payload {
	if j == nil {
		return nil
	}
	return append(queuecontract.Payload(nil), j.body...)
}

func (j *RabbitMQReservedJob) Attempts() int {
	if j == nil || j.env == nil {
		return 0
	}
	return j.env.Attempts
}

func (j *RabbitMQReservedJob) Delete(ctx context.Context) error {
	if j == nil || j.queue == nil {
		return nil
	}
	return j.queue.inner.Delete(ctx, j.env)
}

func (j *RabbitMQReservedJob) Release(ctx context.Context, delay time.Duration) error {
	if j == nil || j.queue == nil {
		return nil
	}
	return j.queue.inner.Release(ctx, j.env, delay)
}
