package redis

import (
	"context"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
)

type RedisReservedJob struct {
	queue        *RedisQueue
	env          *payload.Envelope
	reservedBody string
	body         queuecontract.Payload
}

func (j *RedisReservedJob) ID() string {
	if j == nil || j.env == nil {
		return ""
	}
	return j.env.ID
}

func (j *RedisReservedJob) Name() string {
	if j == nil || j.env == nil {
		return ""
	}
	return j.env.Name
}

func (j *RedisReservedJob) Payload() queuecontract.Payload {
	if j == nil {
		return nil
	}
	return append(queuecontract.Payload(nil), j.body...)
}

func (j *RedisReservedJob) Attempts() int {
	if j == nil || j.env == nil {
		return 0
	}
	return j.env.Attempts
}

func (j *RedisReservedJob) Delete(ctx context.Context) error {
	if j == nil || j.queue == nil || j.env == nil || j.reservedBody == "" {
		return nil
	}
	return j.queue.client.ZRem(ctx, j.queue.reservedKey(j.env.Queue), j.reservedBody).Err()
}

func (j *RedisReservedJob) Release(ctx context.Context, delay time.Duration) error {
	return j.releaseWithEnvelope(ctx, cloneEnvelope(j.env), delay)
}

func (j *RedisReservedJob) releaseWithEnvelope(ctx context.Context, env *payload.Envelope, delay time.Duration) error {
	if j == nil || env == nil {
		return nil
	}
	if err := j.Delete(ctx); err != nil {
		return err
	}
	env.ReservedAt = 0
	body, err := j.queue.codec.Marshal(env)
	if err != nil {
		return err
	}
	return j.queue.Later(ctx, env.Queue, queuecontract.Payload(body), delay)
}

func (j *RedisReservedJob) envelope() *payload.Envelope {
	if j == nil {
		return nil
	}
	return cloneEnvelope(j.env)
}
