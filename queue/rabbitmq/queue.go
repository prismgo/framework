package rabbitmq

import (
	"context"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/internal/helper"
	"github.com/prismgo/framework/queue/payload"
)

// RabbitMQQueue 是 RabbitMQ driver 对 contracts/queue.Queue 的 transport 实现。
//
// 需求背景：父包 queue 不再保留 RabbitMQ adapter 或兼容薄壳；RabbitMQ 的
// publish/pop/topology/consumer intent 组合在 driver 子包内完成，父包只通过 Connector
// 取得统一 Queue contract。
type RabbitMQQueue struct {
	inner      *Connection
	codec      encodingcontract.Codec
	blockFor   time.Duration
	popSession bool
}

// NewRabbitMQQueue 创建 RabbitMQ Queue contract transport。
//
// 参数 name 是 queue.connections 配置名；options 是 RabbitMQ transport 配置；
// codec 用于把 runtime payload.Envelope 与 AMQP driver Envelope 之间转换。
func NewRabbitMQQueue(name string, options Options, codec encodingcontract.Codec, blockFor time.Duration) (*RabbitMQQueue, error) {
	options.Codec = payload.QueueCodec(codec)
	inner, err := NewRabbitMQConnection(name, options)
	if err != nil {
		return nil, err
	}
	return &RabbitMQQueue{inner: inner, codec: payload.QueueCodec(codec), blockFor: blockFor}, nil
}

func (q *RabbitMQQueue) NewPopSession() queuecontract.Queue {
	if q == nil {
		return q
	}
	return &RabbitMQQueue{
		inner:      q.inner,
		codec:      q.codec,
		blockFor:   q.blockFor,
		popSession: true,
	}
}

func (q *RabbitMQQueue) Push(ctx context.Context, queue string, body queuecontract.Payload) error {
	return q.publish(ctx, queue, body, 0)
}

func (q *RabbitMQQueue) Later(ctx context.Context, queue string, body queuecontract.Payload, delay time.Duration) error {
	return q.publish(ctx, queue, body, delay)
}

func (q *RabbitMQQueue) Bulk(ctx context.Context, queue string, bodies []queuecontract.Payload) (queuecontract.BulkResult, error) {
	if len(bodies) == 0 {
		return queuecontract.BulkResult{}, nil
	}
	items := make([]rabbitMQBulkPublishItem, 0, len(bodies))
	for _, body := range bodies {
		env, err := q.decodeEnvelope(body)
		if err != nil {
			return queuecontract.BulkResult{}, err
		}
		items = append(items, rabbitMQBulkPublishItem{Queue: queue, Envelope: env})
	}
	return q.inner.PushBulk(ctx, queue, items)
}

func (q *RabbitMQQueue) Pop(ctx context.Context, queues []string, wait ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	queueNames := normalizePopQueues(queues)
	blockFor := time.Duration(0)
	if normalizePopWaitMode(wait) == queuecontract.PopWaitAvailable {
		blockFor = q.blockFor
	}
	env, err := q.inner.Pop(ctx, queueNames, PopOptions{BlockFor: blockFor})
	if err != nil {
		return nil, err
	}
	body, err := q.codec.Marshal(env)
	if err != nil {
		return nil, err
	}
	return &RabbitMQReservedJob{queue: q, env: env, body: queuecontract.Payload(body)}, nil
}

func normalizePopWaitMode(wait []queuecontract.PopWaitMode) queuecontract.PopWaitMode {
	return helper.NormalizePopWaitMode(wait)
}

func normalizePopQueues(queues []string) []string {
	return helper.NormalizeQueues(queues, "default")
}

func (q *RabbitMQQueue) Size(ctx context.Context, queue string) (int64, error) {
	return q.inner.Size(ctx, queue)
}

func (q *RabbitMQQueue) Clear(ctx context.Context, queue string) error {
	return q.inner.Clear(ctx, queue)
}

func (q *RabbitMQQueue) Close() error {
	if q == nil || q.inner == nil {
		return nil
	}
	if q.popSession {
		return nil
	}
	return q.inner.Close()
}

func (q *RabbitMQQueue) AcquireConsumerIntent(queues []string) (func() error, error) {
	return q.inner.AcquireConsumerIntent(queues)
}

func (q *RabbitMQQueue) publish(ctx context.Context, queue string, body queuecontract.Payload, delay time.Duration) error {
	env, err := q.decodeEnvelope(body)
	if err != nil {
		return err
	}
	return q.inner.Push(ctx, queue, env, delay)
}

func (q *RabbitMQQueue) decodeEnvelope(body queuecontract.Payload) (*payload.Envelope, error) {
	var env payload.Envelope
	if err := q.codec.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	return &env, nil
}
