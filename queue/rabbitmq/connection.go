package rabbitmq

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
	queueevents "github.com/prismgo/framework/queue/internal/events"
	"github.com/prismgo/framework/queue/payload"
	amqp "github.com/rabbitmq/amqp091-go"
)

// 本文件保留 RabbitMQ driver 的公开连接生命周期与 Queue Connection 接口实现。
// 具体的 AMQP 适配、拓扑声明、consumer、重连和配置解析已按职责拆到 rabbitmq_*.go。

// NewRabbitMQConnection 创建一个会在初始化阶段立即建连的 RabbitMQ 连接。
//
// 设计原因：
// issue 01 已确认 RabbitMQ 初始建连失败必须立刻暴露，不能静默留到第一次 dispatch 或 worker Pop。
func NewRabbitMQConnection(name string, options Options) (*Connection, error) {
	resolved := resolveRabbitMQOptions(options)
	codec := payload.QueueCodec(resolved.Codec)
	resolved.Codec = codec
	address := resolved.connectionURL()
	emitRabbitMQInfrastructureEvent(context.Background(), queueevents.EventConnectionConnecting, name, "", resolved.Exchange, 0, nil)
	conn, err := resolved.dialer()(address, amqp.Config{
		Heartbeat: resolved.Heartbeat,
		Locale:    "en_US",
	})
	if err != nil {
		wrapped := fmt.Errorf("%w: connection %q (%s): %v", ErrRabbitMQDialFailed, name, redactedRabbitMQURL(address), err)
		emitRabbitMQInfrastructureEvent(context.Background(), queueevents.EventConnectionDisconnected, name, "", resolved.Exchange, 0, wrapped)
		return nil, wrapped
	}
	readyCh := make(chan struct{})
	close(readyCh)
	connection := &Connection{
		name:             name,
		options:          resolved,
		codec:            codec,
		address:          address,
		amqpConnection:   conn,
		declaredQueues:   make(map[string]struct{}),
		delayedQueues:    make(map[string]struct{}),
		ttlDelayQueues:   make(map[string]struct{}),
		restartQueues:    make(map[string]struct{}),
		consumers:        make(map[string]<-chan amqp.Delivery),
		consumerTags:     make(map[string]string),
		verifiedTopology: make(map[rabbitMQTopologyVerificationKey]struct{}),
		knownQueues:      make(map[string]struct{}),
		knownDelayed:     make(map[string]struct{}),
		knownTTLDelay:    make(map[string]rabbitMQTTLDLXDelayTopology),
		activeConsumers:  make(map[string]struct{}),
		consumerRefs:     make(map[string]int),
		topologyUsage:    make(map[rabbitMQTopologyCacheKey]rabbitMQTopologyUsageEntry),
		topologyLRU:      list.New(),
		readyCh:          readyCh,
		closedCh:         make(chan struct{}),
		ready:            true,
	}
	connection.emitInfrastructureEvent(context.Background(), queueevents.EventConnectionConnected, "", resolved.Exchange, 0, nil)
	connection.monitorConnection(conn)
	return connection, nil
}

func (c *Connection) codecOrDefault() encodingcontract.Codec {
	if c != nil {
		return payload.QueueCodec(c.codec)
	}
	return encodingpkg.Msgpack()
}

func (c *Connection) Pop(ctx context.Context, queues []string, opts PopOptions) (*payload.Envelope, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	queues = normalizeRabbitMQQueues(queues)
	if opts.BlockFor <= 0 {
		if c.isReconnecting() {
			return nil, ErrEmpty
		}
		if err := c.waitReady(ctx); err != nil {
			return nil, err
		}
		for _, queue := range queues {
			if err := c.ensureQueueConsumer(queue); err != nil {
				return nil, err
			}
		}
		return c.popReadyDelivery(ctx, queues, 0)
	}

	deadline := time.Now().Add(opts.BlockFor)
retry:
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, ErrEmpty
		}
		waitCtx, cancel := context.WithTimeout(ctx, remaining)
		err := c.waitReady(waitCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, ErrEmpty
			}
			return nil, err
		}
		for _, queue := range queues {
			if err := c.ensureQueueConsumer(queue); err != nil {
				if errors.Is(err, ErrConnectionClosed) && ctx.Err() == nil {
					continue retry
				}
				return nil, err
			}
		}
		env, err := c.popReadyDelivery(ctx, queues, remaining)
		if err == nil {
			return env, nil
		}
		if errors.Is(err, ErrEmpty) && c.isReconnecting() && ctx.Err() == nil {
			continue
		}
		return nil, err
	}
}

// popReadyDelivery 只负责在当前可用连接上取一次 delivery 并恢复 Envelope。
//
// 重连等待、BlockFor/context 边界由 Pop 外层控制；这里保持单一职责，避免取消息逻辑同时承担连接状态机。
func (c *Connection) popReadyDelivery(ctx context.Context, queues []string, blockFor time.Duration) (*payload.Envelope, error) {
	delivery, queue, err := c.receiveDelivery(ctx, queues, blockFor)
	if err != nil {
		return nil, err
	}
	reserved, err := payload.NewReservationCodec(c.codecOrDefault()).Reserve(queuecontract.Payload(delivery.Body), queue, time.Now())
	if err != nil {
		return nil, c.rejectPoisonDelivery(ctx, queue, delivery, err)
	}
	rememberEnvelopeDelivery(reserved.Envelope, &rabbitMQDeliveryState{delivery: delivery})
	return reserved.Envelope, nil
}

// rejectPoisonDelivery 处理无法恢复为 Envelope 的 RabbitMQ delivery。
//
// 需求背景：
// 坏消息发生在 adapter 解码边界，worker 尚未拿到可信 Envelope，因此不能进入 FailedStore 或 job_failed。
// 对 Prefetch=1 的 consumer 来说，未终结的坏消息会长期占住 unacked slot，阻塞后续正常消息。
//
// 设计思路：
// 只对当前 delivery 执行 Reject(false)，明确交给 RabbitMQ 的 DLX 或 broker 丢弃语义处理；
// 不 Ack、不 Nack/Reject(requeue=true)、不重新发布坏 body，也不在同一次 Pop 内继续 drain 下一条消息。
//
// 参数说明：
// ctx 是 Pop 调用上下文；queue 是当前 delivery 来源队列；delivery 是要终结的 AMQP 消息；
// decodeErr 是 Envelope 解码失败原因，返回错误必须保留该上下文并匹配 ErrPoisonEnvelope。
func (c *Connection) rejectPoisonDelivery(ctx context.Context, queue string, delivery amqp.Delivery, decodeErr error) error {
	poisonErr := fmt.Errorf("%w: decode rabbitmq envelope with %s payload encoding on queue %q: %w", ErrPoisonEnvelope, c.codecOrDefault().Name(), queue, decodeErr)
	if rejectErr := delivery.Reject(false); rejectErr != nil {
		wrapped := fmt.Errorf("%w: reject poison delivery on queue %q: %w", poisonErr, queue, rejectErr)
		c.emitPoisonEnvelopeEvent(ctx, queue, queueevents.PoisonEnvelopeActionRejectFailed, delivery.Body, wrapped)
		return wrapped
	}
	c.emitPoisonEnvelopeEvent(ctx, queue, queueevents.PoisonEnvelopeActionReject, delivery.Body, poisonErr)
	return poisonErr
}

// Delete 在 RabbitMQ 成功路径上执行最终 ack。
//
// 约束：
// 1. Delete(nil) 必须安全返回，兼容手工构造 Envelope 或重试路径。
// 2. 缺少 delivery 内部状态时也必须 no-op，不能 panic。
// 3. 真实 ack 失败不能吞掉。
func (c *Connection) Delete(_ context.Context, env *payload.Envelope) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	delivery := envelopeDelivery(env)
	if env == nil || delivery == nil {
		return nil
	}
	if err := delivery.Ack(); err != nil {
		return err
	}
	forgetEnvelopeDelivery(env)
	return nil
}

// Release 终结当前 RabbitMQ delivery 后重新发布 envelope。
//
// RabbitMQ 的 requeue 不能表达当前 queue.ReleaseAfter/backoff 的延迟语义，因此这里先 ack
// 当前 delivery，再走 Push 的延迟发布分支；缺少 delivery state 时直接重新发布，兼容手工恢复路径。
func (c *Connection) Release(ctx context.Context, env *payload.Envelope, delay time.Duration) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if env == nil {
		return fmt.Errorf("queue: envelope is nil")
	}
	if delay > 0 && c.normalizedDelayMode() == rabbitMQDelayModeNone {
		return fmt.Errorf("%w: rabbitmq delay_mode=none does not support delayed release", ErrUnsupportedOperation)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if envelopeDelivery(env) != nil {
		return c.releaseAckedDelivery(ctx, env, delay)
	}
	env.ReservedAt = 0
	return c.Push(ctx, env.Queue, env, delay)
}

// releaseAckedDelivery 集中处理 RabbitMQ 已 ack 后的 release 替换发布窗口。
//
// 需求背景：
// RabbitMQ 的 nack/requeue 不能表达 queue.ReleaseAfter/backoff 的延迟语义，因此 issue 11 已确认使用
// ack-then-republish 策略。ack 成功后，原 delivery 已经从 broker 视角结束，driver 不能再回退到
// nack/requeue；如果替换发布失败，必须把“原消息已结束但新消息未确认进入 broker”的风险显式暴露。
//
// 设计思路：
// 该函数是 post-ack republish 的唯一入口，负责 ack、清理 delivery handle、重置 ReservedAt、发布替换
// envelope，并把替换发布或 confirm 失败包装为 ErrRabbitMQReleaseRepublishFailed。发布失败事件也在这里
// 切换为 queue.release_republish_failed，避免同时发出普通 queue.publish_failed。
//
// 参数说明：
// ctx 为 release 调用上下文；env 必须带 RabbitMQ delivery state；delay 为替换消息的延迟投递时间。
func (c *Connection) releaseAckedDelivery(ctx context.Context, env *payload.Envelope, delay time.Duration) error {
	delivery := envelopeDelivery(env)
	if err := delivery.Ack(); err != nil {
		return err
	}
	forgetEnvelopeDelivery(env)
	env.ReservedAt = 0
	if err := c.push(ctx, env.Queue, env, delay, queueevents.EventReleaseRepublishFailed); err != nil {
		return fmt.Errorf("%w: %w", ErrRabbitMQReleaseRepublishFailed, err)
	}
	return nil
}

// Clear 清除指定 RabbitMQ 队列当前可直接消费的 ready 消息。
//
// 需求背景：
// contracts/queue.Queue 需要给运维命令提供统一清空入口，但 RabbitMQ 的 QueuePurge
// 只会移除队列中尚未投递给 consumer 的 ready 消息。
//
// 设计约束：
// 1. 不影响已经推送给 worker、等待 ack/nack 的 unacked 消息。
// 2. 不影响 delayed-message 插件内部尚未到期的延迟消息。
// 3. declare=false 时先做被动 topology 检查，资源缺失要返回清晰错误。
func (c *Connection) Clear(_ context.Context, queue string) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	queue = normalizeRabbitMQQueueName(queue, nil)
	if err := c.ensureQueueTopology(queue); err != nil {
		return err
	}
	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		return err
	}
	_, err = channel.QueuePurge(queue, c.options.NoWait)
	if err != nil {
		_ = c.resetTopologyChannel()
	}
	return err
}

// Size 返回 RabbitMQ 当前能稳定查询到的 ready message count。
//
// 设计原因：
// RabbitMQ 的 QueueInspect 只能可靠提供队列内 ready 消息数。这里不额外维护应用侧计数，
// 避免为了模拟 Redis 的 ready+delayed 统计而引入新的状态一致性问题。
//
// 结果边界：
// 1. 不包含已经投递给 consumer 但尚未 ack 的 unacked 消息。
// 2. 不包含 delayed-message 插件内部尚未到期、还没进入业务队列的消息。
// 3. declare=true 时会按需声明队列；declare=false 时仅检查既有 topology。
func (c *Connection) Size(_ context.Context, queue string) (int64, error) {
	if err := c.ensureOpen(); err != nil {
		return 0, err
	}
	queue = normalizeRabbitMQQueueName(queue, nil)
	if !c.options.Declare {
		return c.sizeExistingQueue(queue)
	}
	if err := c.ensureQueueTopology(queue); err != nil {
		return 0, err
	}
	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		return 0, err
	}
	inspected, err := channel.QueueInspect(queue)
	if err != nil {
		_ = c.resetTopologyChannel()
		return 0, err
	}
	return int64(inspected.Messages), nil
}

// sizeExistingQueue 处理 declare=false 下的 Size 查询。
//
// 需求背景：
// Size 的业务结果必须来自 RabbitMQ 当前的 QueueInspect ready message count，不能因为 topology
// 已验证过就返回本地缓存的旧数量。但默认 exchange 的存在性验证是固定成本，命中 verification cache 后不应重复探测。
//
// 设计思路：
// 1. 先确保默认 exchange 已被动验证；该步骤可命中连接级缓存。
// 2. 每次都执行 QueueInspect(queue) 获取最新 Messages。
// 3. 如果该 queue 之前尚未验证过，成功的 QueueInspect 同时写入 queue verification cache 并发出一次 topology_declared。
// 4. QueueInspect 失败不写缓存，后续调用仍可在资源恢复后重新验证。
func (c *Connection) sizeExistingQueue(queue string) (int64, error) {
	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	if err := c.ensureExistingExchangeTopologyLocked(queue); err != nil {
		return 0, err
	}
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		return 0, err
	}
	queueKey := c.queueVerificationKey(queue)
	wasVerified := c.isTopologyVerified(queueKey)
	inspected, err := channel.QueueInspect(queue)
	if err != nil {
		wrapped := fmt.Errorf("%w: queue %q: %v", ErrRabbitMQTopologyMissing, queue, err)
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, wrapped)
		return 0, wrapped
	}
	if !wasVerified {
		c.markTopologyVerified(queueKey)
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclared, queue, c.options.Exchange, 0, nil)
	}
	return int64(inspected.Messages), nil
}

// Close 主动关闭 RabbitMQ connection 及其派生的 channel 缓存。
//
// 设计思路：
// 先在 c.mu 内把连接状态切换为 closed，并快照 publish/topology/consumer channel；
// 再释放 c.mu 后逐个关闭真实 AMQP 资源。这样可以避免 Close 阻塞其它只需要观察 closed 状态的调用，
// 也避免在连接全局锁内等待 slot.mu 或 broker 网络调用。
func (c *Connection) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	// 这些对象都绑定在旧 connection 上，先从 Connection 状态中摘除，再在锁外执行真实 Close。
	conn := c.amqpConnection
	publishSlots := c.publishSlots
	topologyChannel := c.topologyChannel
	consumerChannel := c.consumerChannel
	stoppedConsumers := keysOfConsumerMap(c.consumers)
	c.amqpConnection = nil
	c.publishSlots = nil
	c.publishNext.Store(0)
	c.topologyChannel = nil
	c.consumerChannel = nil
	c.declaredQueues = nil
	c.delayedQueues = nil
	c.ttlDelayQueues = nil
	c.restartQueues = nil
	c.consumers = nil
	c.consumerTags = nil
	c.verifiedTopology = nil
	c.knownQueues = nil
	c.knownDelayed = nil
	c.knownTTLDelay = nil
	c.activeConsumers = nil
	c.consumerRefs = nil
	c.topologyUsage = nil
	if c.closedCh != nil {
		close(c.closedCh)
	}
	if !c.ready && c.readyCh != nil {
		c.ready = true
		close(c.readyCh)
	}
	c.mu.Unlock()
	c.clearRestartSignalCache()
	for _, queue := range stoppedConsumers {
		c.emitInfrastructureEvent(context.Background(), queueevents.EventConsumerStopped, queue, c.options.Exchange, 0, nil)
	}

	var first error
	if err := closeRabbitMQPublishSlots(publishSlots); err != nil && first == nil {
		first = err
	}
	if topologyChannel != nil {
		if err := topologyChannel.Close(); err != nil && first == nil {
			first = err
		}
	}
	if consumerChannel != nil {
		if err := consumerChannel.Close(); err != nil && first == nil {
			first = err
		}
	}
	if conn == nil || conn.IsClosed() {
		return first
	}
	if err := conn.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

// ensureOpen 只检查当前连接是否仍然可立即使用。
//
// Push/Pop 有各自的重连等待语义，其他管理类操作则需要明确暴露“当前不可用”，
// 因此这里不隐式等待后台重连完成。
func (c *Connection) ensureOpen() error {
	if c == nil {
		return ErrConnectionClosed
	}

	c.mu.RLock()
	closed := c.closed
	conn := c.amqpConnection
	c.mu.RUnlock()

	if closed || conn == nil || conn.IsClosed() {
		return ErrConnectionClosed
	}
	return nil
}
