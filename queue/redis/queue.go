package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	queueerrors "github.com/prismgo/framework/queue/internal/errors"
	queueevents "github.com/prismgo/framework/queue/internal/events"
	"github.com/prismgo/framework/queue/internal/helper"
	"github.com/prismgo/framework/queue/internal/observability"
	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
	"github.com/redis/go-redis/v9"
)

const redisEventDriver = "redis"

var ErrEmpty = queueerrors.ErrEmpty

func normalizeQueuePage(page state.PageRequest) state.PageRequest { return state.NormalizePage(page) }

// RedisQueue 使用 Redis list + sorted set 实现 Queue transport。
type RedisQueue struct {
	client     *redis.Client
	name       string
	prefix     string
	retryAfter time.Duration
	blockFor   time.Duration
	failedTTL  time.Duration
	codec      encodingcontract.Codec
	popSession bool
}

// NewRedisQueue 创建 Redis 队列 transport。
//
// 返回值：连接解析成功时返回 RedisQueue 实例和 nil error；
// ResolveQueueClient 失败时发出 connection_failed 事件并返回错误，
// 避免构造函数发出误导性的 connected 事件。
func NewRedisQueue(options RedisOptions) (*RedisQueue, error) {
	name := redisConnectionName(options.Name)
	emitRedisInfrastructureEvent(context.Background(), queueevents.EventConnectionConnecting, name, "", nil)
	client, err := ResolveQueueClient(options)
	if err != nil {
		// 连接解析失败，发出连接失败事件并返回错误，不发出 connected 事件
		emitRedisInfrastructureEvent(context.Background(), queueevents.EventConnectionDisconnected, name, "", err)
		return nil, fmt.Errorf("queue: redis connection %q: %w", name, err)
	}
	conn := &RedisQueue{
		client:     client,
		name:       name,
		prefix:     cleanPrefix(options.Prefix, "queue"),
		retryAfter: defaultDuration(options.RetryAfter, 90*time.Second),
		blockFor:   options.BlockFor,
		failedTTL:  options.FailedTTL,
		codec:      redisCodec(options.Codec),
	}
	emitRedisInfrastructureEvent(context.Background(), queueevents.EventConnectionConnected, name, "", nil)
	return conn, nil
}

// NewRedisQueueFromClient 基于已解析的 Redis client 创建 Queue transport。
func NewRedisQueueFromClient(client *redis.Client, options RedisOptions) *RedisQueue {
	name := redisConnectionName(options.Name)
	if client != nil {
		emitRedisInfrastructureEvent(context.Background(), queueevents.EventConnectionConnecting, name, "", nil)
	}
	conn := &RedisQueue{
		client:     client,
		name:       name,
		prefix:     cleanPrefix(options.Prefix, "queue"),
		retryAfter: defaultDuration(options.RetryAfter, 90*time.Second),
		blockFor:   options.BlockFor,
		failedTTL:  options.FailedTTL,
		codec:      redisCodec(options.Codec),
	}
	if client != nil {
		emitRedisInfrastructureEvent(context.Background(), queueevents.EventConnectionConnected, name, "", nil)
	}
	return conn
}

func (c *RedisQueue) NewPopSession() queuecontract.Queue {
	if c == nil {
		return c
	}
	return &RedisQueue{
		client:     c.client,
		name:       c.name,
		prefix:     c.prefix,
		retryAfter: c.retryAfter,
		blockFor:   c.blockFor,
		failedTTL:  c.failedTTL,
		codec:      c.codec,
		popSession: true,
	}
}

func (c *RedisQueue) Push(ctx context.Context, queue string, body queuecontract.Payload) error {
	return c.pushBody(ctx, queue, body, 0)
}

func (c *RedisQueue) Later(ctx context.Context, queue string, body queuecontract.Payload, delay time.Duration) error {
	return c.pushBody(ctx, queue, body, delay)
}

func (c *RedisQueue) Bulk(ctx context.Context, queue string, bodies []queuecontract.Payload) (queuecontract.BulkResult, error) {
	if len(bodies) == 0 {
		return queuecontract.BulkResult{}, nil
	}
	values := make([]any, 0, len(bodies))
	for _, body := range bodies {
		values = append(values, string(body))
	}
	// 使用单个 LPUSH 命令推送多个 token，避免 notify list 快速增长
	tokens := make([]any, len(bodies))
	for i := range bodies {
		tokens[i] = "1"
	}
	_, err := c.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.RPush(ctx, c.readyKey(queue), values...)
		pipe.LPush(ctx, c.notifyKey(queue), tokens...)
		return nil
	})
	if err != nil {
		c.emitInfrastructureEvent(ctx, queueevents.EventPublishFailed, queue, err)
		return queuecontract.BulkResult{}, err
	}
	return queuecontract.BulkResult{Accepted: len(bodies)}, nil
}

func (c *RedisQueue) pushBody(ctx context.Context, queue string, body queuecontract.Payload, delay time.Duration) error {
	if delay > 0 {
		err := c.client.ZAdd(ctx, c.delayedKey(queue), redis.Z{
			Score:  float64(time.Now().Add(delay).UnixMilli()),
			Member: string(body),
		}).Err()
		if err != nil {
			c.emitInfrastructureEvent(ctx, queueevents.EventPublishFailed, queue, err)
		}
		return err
	}
	_, err := c.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.RPush(ctx, c.readyKey(queue), string(body))
		pipe.LPush(ctx, c.notifyKey(queue), "1")
		return nil
	})
	if err != nil {
		c.emitInfrastructureEvent(ctx, queueevents.EventPublishFailed, queue, err)
	}
	return err
}

func (c *RedisQueue) Pop(ctx context.Context, queues []string, wait ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	queueNames := normalizePopQueues(queues)
	reserveFor := retryAfter(0, c.retryAfter)
	if err := c.migrateDue(ctx, queueNames); err != nil {
		return nil, err
	}
	keys := c.readyKeys(queueNames)
	reserved, err := c.nonBlockingPop(ctx, keys, reserveFor)
	if err == nil || !errors.Is(err, ErrEmpty) {
		return reserved, err
	}
	if normalizePopWaitMode(wait) != queuecontract.PopWaitAvailable || defaultDuration(0, c.blockFor) <= 0 {
		return nil, ErrEmpty
	}
	if err := c.waitForNotify(ctx, keys, defaultDuration(0, c.blockFor)); err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrEmpty
		}
		return nil, err
	}
	return c.nonBlockingPop(ctx, keys, reserveFor)
}

func normalizePopWaitMode(wait []queuecontract.PopWaitMode) queuecontract.PopWaitMode {
	return helper.NormalizePopWaitMode(wait)
}

func normalizePopQueues(queues []string) []string {
	return helper.NormalizeQueues(queues, "default")
}

func (c *RedisQueue) readyKeys(queues []string) []string {
	keys := make([]string, 0, len(queues))
	for _, queue := range queues {
		keys = append(keys, c.readyKey(queue))
	}
	return keys
}

func (c *RedisQueue) Clear(ctx context.Context, queue string) error {
	return c.client.Del(ctx, c.readyKey(queue), c.delayedKey(queue), c.reservedKey(queue), c.notifyKey(queue)).Err()
}

func (c *RedisQueue) Size(ctx context.Context, queue string) (int64, error) {
	ready, err := c.client.LLen(ctx, c.readyKey(queue)).Result()
	if err != nil {
		return 0, err
	}
	delayed, err := c.client.ZCard(ctx, c.delayedKey(queue)).Result()
	if err != nil {
		return 0, err
	}
	return ready + delayed, nil
}

func (c *RedisQueue) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	if c.popSession {
		return nil
	}
	c.emitInfrastructureEvent(context.Background(), queueevents.EventConnectionDisconnected, "", nil)
	return nil
}

// Codec 返回当前 Redis transport 使用的 queue Payload Encoding。
//
// 该方法供测试和诊断确认编码边界；业务代码不应绕过 Queue contract 直接组装 payload。
func (c *RedisQueue) Codec() encodingcontract.Codec {
	if c == nil {
		return redisCodec(nil)
	}
	return c.codec
}

func (c *RedisQueue) Client() *redis.Client {
	if c == nil {
		return nil
	}
	return c.client
}

// AcquireConsumerIntent 为 Redis worker 生命周期发出每个队列的 started/stopped 事件。
//
// 逻辑说明：Redis driver 仍保持轮询 list/zset 的消费模型，这个 lease 只表达 worker
// 本地计划消费哪些队列，便于监听方观察生命周期；它不建立长连接 consumer、不改变队列优先级，
// 也不复用 RabbitMQ 的 reconnect recovery 引用计数语义。
//
// 参数说明：queues 是本次 worker 配置的队列列表；空值兼容为 default，并按出现顺序去重。
func (c *RedisQueue) AcquireConsumerIntent(queues []string) (func() error, error) {
	normalized := normalizeRedisConsumerQueues(queues)
	for _, queue := range normalized {
		c.emitInfrastructureEvent(context.Background(), queueevents.EventConsumerStarted, queue, nil)
	}
	var once sync.Once
	return func() error {
		once.Do(func() {
			for _, queue := range normalized {
				c.emitInfrastructureEvent(context.Background(), queueevents.EventConsumerStopped, queue, nil)
			}
		})
		return nil
	}, nil
}

// migrateDue 把 delayed/reserved 中已经到期的任务迁回 ready 队列。
//
// 设计原因：Redis driver 使用 sorted set 表示延迟和保留中的任务；worker 每次 pop 前都需要
// 先迁移到期项。单个 zset 的迁移由 Lua 保证“先 ZREM 成功者才 RPUSH”，避免并发 worker
// 把同一个 body 重复放回 ready list。
func (c *RedisQueue) migrateDue(ctx context.Context, queues []string) error {
	now := time.Now().UnixMilli()
	for _, queue := range queues {
		if err := c.migrateZSet(ctx, c.delayedKey(queue), c.readyKey(queue), c.notifyKey(queue), now); err != nil {
			return err
		}
		if err := c.migrateZSet(ctx, c.reservedKey(queue), c.readyKey(queue), c.notifyKey(queue), now); err != nil {
			return err
		}
	}
	return nil
}

func (c *RedisQueue) migrateZSet(ctx context.Context, source string, ready string, notify string, now int64) error {
	return c.client.Eval(ctx, redisMigrateDueScript, []string{source, ready, notify}, fmt.Sprint(now)).Err()
}

// waitForNotify 对 ready key 对应的 :notify key 执行 BLPOP。
//
// 参数说明：keys 是 ready list key 列表；timeout 是本轮最多阻塞时间。返回 nil 只表示收到
// notify token，调用方仍需重新 reserve，因为 token 可能是旧 token 或任务已被其他 worker 抢先消费。
func (c *RedisQueue) waitForNotify(ctx context.Context, keys []string, timeout time.Duration) error {
	notifyKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		queue := keyQueueName(c.prefix, key)
		notifyKeys = append(notifyKeys, c.notifyKey(queue))
	}
	result, err := c.client.BLPop(ctx, timeout, notifyKeys...).Result()
	if err != nil {
		return err
	}
	if len(result) < 2 {
		return redis.Nil
	}
	return nil
}

func (c *RedisQueue) nonBlockingPop(ctx context.Context, keys []string, reserveFor time.Duration) (queuecontract.ReservedJob, error) {
	for _, key := range keys {
		queue := keyQueueName(c.prefix, key)
		body, err := c.reserveReadyBody(ctx, key, c.reservedKey(queue), c.notifyKey(queue), reserveFor)
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return c.reserve(ctx, queue, body)
	}
	return nil, ErrEmpty
}

// reserveReadyBody 原子地从 ready list 取出 raw body 并放入 reserved。
func (c *RedisQueue) reserveReadyBody(ctx context.Context, readyKey string, reservedKey string, notifyKey string, reserveFor time.Duration) (string, error) {
	score := time.Now().Add(reserveFor).UnixMilli()
	return c.client.Eval(ctx, redisReserveReadyScript, []string{readyKey, reservedKey, notifyKey}, fmt.Sprint(score)).Text()
}

// reserve 将 reserved 中的 raw body 解码为 Envelope，并用更新后的 body 替换原记录。
//
// 错误边界：解码失败说明 body 已经不可执行，只能从 reserved 移除并发出 poison 事件；
// 替换失败则保留原 reserved body，等待 retry_after 后再次迁移消费。
func (c *RedisQueue) reserve(ctx context.Context, queue string, body string) (queuecontract.ReservedJob, error) {
	reserved, err := payload.NewReservationCodec(c.codec).Reserve(queuecontract.Payload(body), queue, time.Now())
	if err != nil {
		_ = c.client.ZRem(ctx, c.reservedKey(queue), body).Err()
		poisonErr := fmt.Errorf("%w: decode redis envelope with %s payload encoding on queue %q: %w", queueerrors.ErrPoisonEnvelope, c.codec.Name(), queue, err)
		c.emitPoisonEnvelopeEvent(ctx, queue, queueevents.PoisonEnvelopeActionDiscard, []byte(body), poisonErr)
		return nil, poisonErr
	}
	if replaced, err := c.client.Eval(ctx, redisReplaceReservedScript, []string{c.reservedKey(queue)}, body, string(reserved.Body)).Int(); err != nil {
		return nil, err
	} else if replaced != 1 {
		return nil, ErrEmpty
	}
	return &RedisReservedJob{
		queue:        c,
		env:          cloneEnvelope(reserved.Envelope),
		reservedBody: string(reserved.Body),
		body:         reserved.Body,
	}, nil
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

// emitRedisInfrastructureEvent 统一组装 Redis driver 的通用基础设施事件。
//
// 逻辑说明：Redis 路径复用 queue.UseEventSink 暴露连接、发布失败和 worker 生命周期，
// 事件只写入可排障的连接名、driver 类型、队列名、错误文本与时间戳，不携带 Redis 地址、
// 密码、raw payload 或 Go error 对象，避免把敏感运行时细节泄露给通用事件监听方。
//
// 参数说明：ctx 沿用调用链上下文；eventName 使用 queue 通用事件常量；connection 是
// queue.connections 配置 key；queue 是业务队列名；err 会被转换为纯文本写入事件。
func emitRedisInfrastructureEvent(ctx context.Context, eventName, connection, queue string, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	queueevents.Fire(ctx, observability.InfrastructureEvent(observability.InfrastructureFacts{
		EventName:  eventName,
		Connection: redisConnectionName(connection),
		Driver:     redisEventDriver,
		Queue:      queue,
		Err:        err,
	}))
}

// emitInfrastructureEvent 使用当前 Redis 连接名发送通用基础设施事件。
//
// 设计思路：RedisQueue 已经持有 manager 注入的配置名，方法级事件只需要补充队列名
// 和错误边界，避免各调用点重复处理默认连接名和 payload 组装细节。
func (c *RedisQueue) emitInfrastructureEvent(ctx context.Context, eventName, queue string, err error) {
	if c == nil {
		return
	}
	emitRedisInfrastructureEvent(ctx, eventName, c.name, queue, err)
}

// emitPoisonEnvelopeEvent 统一组装 Redis 坏消息事件。
//
// 需求背景：Redis Pop 已经从 ready list 取走 raw body 后才进入 Envelope Payload Encoding 解码边界，
// 解码失败时没有可信 Envelope 可写入 reserved 或 FailedStore，因此 driver 只能丢弃该 body，
// 发出 poison envelope 事件，并返回可由 errors.Is 识别的 queueerrors.ErrPoisonEnvelope。
//
// 参数说明：ctx 是 Pop 调用上下文；queue 是收到坏消息的业务队列；action 当前为 discard；
// body 是已从 ready list 移除的原始消息；err 是写入事件的错误文本来源。
func (c *RedisQueue) emitPoisonEnvelopeEvent(ctx context.Context, queue, action string, body []byte, err error) {
	if c == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queueevents.Fire(ctx, observability.PoisonEnvelope(observability.PoisonEnvelopeFacts{
		Connection: redisConnectionName(c.name),
		Driver:     redisEventDriver,
		Queue:      queue,
		Action:     action,
		Encoding:   c.codec.Name(),
		Body:       body,
		Err:        err,
	}))
}

func normalizeRedisConsumerQueues(queues []string) []string {
	return helper.NormalizeQueues(queues, "default")
}
