package queue

import (
	"context"
	"sync"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/internal/helper"
	"github.com/prismgo/framework/queue/payload"
)

// SyncConnection 是进程内 Queue transport，主要用于测试和 sync 之外的内存队列场景。
type SyncConnection struct {
	mu                 sync.Mutex
	codec              encodingcontract.Codec
	queues             map[string][]queuecontract.Payload
	immediateProcessor func(context.Context, string, queuecontract.Payload) error
}

// NewSyncConnection 创建进程内连接。
func NewSyncConnection(codecs ...encodingcontract.Codec) *SyncConnection {
	codec := payload.QueueCodec(nil)
	if len(codecs) > 0 && codecs[0] != nil {
		codec = payload.QueueCodec(codecs[0])
	}
	return &SyncConnection{
		codec:  codec,
		queues: make(map[string][]queuecontract.Payload),
	}
}

func (c *SyncConnection) Push(ctx context.Context, queue string, body queuecontract.Payload) error {
	return c.push(ctx, queue, body)
}

func (c *SyncConnection) push(ctx context.Context, queue string, body queuecontract.Payload) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	current := append(queuecontract.Payload(nil), body...)
	c.queues[queue] = append(c.queues[queue], current)
	processor := c.immediateProcessor
	if processor != nil {
		items := c.queues[queue]
		c.queues[queue] = append(items[:len(items)-1:len(items)-1], items[len(items):]...)
	}
	c.mu.Unlock()
	if processor != nil {
		return processor(ctx, queue, current)
	}
	return nil
}

// Later 在 sync transport 中等同于 Push：delay 参数被忽略。
//
// 设计原因：SyncConnection 是进程内即时 transport，不支持延迟投递语义。
// 调用方（如 batch dispatchSingle）传入 delay 不会报错，但消息会立即入队。
// 需要真实延迟的场景应使用 Redis 或 RabbitMQ transport。
func (c *SyncConnection) Later(ctx context.Context, queue string, body queuecontract.Payload, _ time.Duration) error {
	return c.push(ctx, queue, body)
}

func (c *SyncConnection) Bulk(ctx context.Context, queue string, bodies []queuecontract.Payload) (queuecontract.BulkResult, error) {
	accepted := 0
	for _, body := range bodies {
		if err := c.Push(ctx, queue, body); err != nil {
			return queuecontract.BulkResult{Accepted: accepted}, err
		}
		accepted++
	}
	return queuecontract.BulkResult{Accepted: accepted}, nil
}

func (c *SyncConnection) Pop(_ context.Context, queues []string, _ ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	queueNames := normalizeSyncPopQueues(queues)
	c.mu.Lock()
	defer c.mu.Unlock()
	var queue string
	var items []queuecontract.Payload
	for _, queueName := range queueNames {
		if current := c.queues[queueName]; len(current) > 0 {
			queue = queueName
			items = current
			break
		}
	}
	if len(items) == 0 {
		return nil, ErrEmpty
	}
	body := append(queuecontract.Payload(nil), items[0]...)
	c.queues[queue] = items[1:]
	reserved, err := payload.NewReservationCodec(c.codec).Reserve(body, queue, time.Now())
	if err != nil {
		return nil, err
	}
	return &syncReservedJob{queue: c, env: cloneEnvelope(reserved.Envelope), body: reserved.Body}, nil
}

func normalizeSyncPopQueues(queues []string) []string {
	return helper.NormalizeQueues(queues, "default")
}

func (c *SyncConnection) Clear(_ context.Context, queue string) error {
	c.mu.Lock()
	delete(c.queues, queue)
	c.mu.Unlock()
	return nil
}

func (c *SyncConnection) Size(_ context.Context, queue string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(len(c.queues[queue])), nil
}

func (c *SyncConnection) Close() error { return nil }

// SetImmediateProcessor 注入 sync transport 的即时消费回调。
//
// 需求背景：Manager 不应按连接名特判 sync；sync 仍通过 Queue contract 完成 Push/Pop，
// 只是在 transport 内把刚写入的 payload 立即交给 runtime 处理。
func (c *SyncConnection) SetImmediateProcessor(processor func(context.Context, string, queuecontract.Payload) error) {
	c.mu.Lock()
	c.immediateProcessor = processor
	c.mu.Unlock()
}

type syncReservedJob struct {
	queue *SyncConnection
	env   *payload.Envelope
	body  queuecontract.Payload
}

func (j *syncReservedJob) ID() string {
	if j == nil || j.env == nil {
		return ""
	}
	return j.env.ID
}

func (j *syncReservedJob) Name() string {
	if j == nil || j.env == nil {
		return ""
	}
	return j.env.Name
}

func (j *syncReservedJob) Payload() queuecontract.Payload {
	if j == nil {
		return nil
	}
	return append(queuecontract.Payload(nil), j.body...)
}

func (j *syncReservedJob) Attempts() int {
	if j == nil || j.env == nil {
		return 0
	}
	return j.env.Attempts
}

func (j *syncReservedJob) Delete(context.Context) error { return nil }

func (j *syncReservedJob) Release(ctx context.Context, delay time.Duration) error {
	if j == nil || j.queue == nil || j.env == nil {
		return nil
	}
	j.env.ReservedAt = 0
	body, err := j.queue.codec.Marshal(j.env)
	if err != nil {
		return err
	}
	return j.queue.Later(ctx, j.env.Queue, queuecontract.Payload(body), delay)
}

func (j *syncReservedJob) envelope() *payload.Envelope {
	if j == nil {
		return nil
	}
	return cloneEnvelope(j.env)
}

func cloneEnvelope(env *payload.Envelope) *payload.Envelope {
	if env == nil {
		return nil
	}
	cloned := *env
	cloned.Payload = append([]byte(nil), env.Payload...)
	cloned.BackoffSec = append([]int(nil), env.BackoffSec...)
	cloned.Chain = append([]payload.PendingJob(nil), env.Chain...)
	cloned.Tags = append([]string(nil), env.Tags...)
	return &cloned
}
