package rabbitmq

import (
	"fmt"
	"strings"
	"time"

	queueevents "github.com/prismgo/framework/queue/internal/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

// 本文件集中处理 RabbitMQ exchange、queue、binding 与 delay topology。
// declared* 缓存只描述当前 AMQP connection；reconnect 恢复集合由 worker Consumer Intent lease 决定。

// ensurePublishTopology 选择本次发布的 exchange/routing key，并按 delay_mode 声明所需拓扑。
func (c *Connection) ensurePublishTopology(queue string, delay time.Duration) (string, string, error) {
	queue = normalizeRabbitMQQueueName(queue, nil)
	if delay <= 0 {
		if err := c.ensureQueueTopology(queue); err != nil {
			return "", "", err
		}
		return c.options.Exchange, queue, nil
	}

	switch c.normalizedDelayMode() {
	case rabbitMQDelayModePlugin:
		if err := c.ensurePluginDelayTopology(queue); err != nil {
			return "", "", err
		}
		return c.delayedExchangeName(), queue, nil
	case rabbitMQDelayModeTTLDLX:
		if err := c.ensureQueueTopology(queue); err != nil {
			return "", "", err
		}
		bucket := c.delayBucket(delay)
		delayQueue := c.ttlDelayQueueName(queue, bucket)
		if err := c.ensureTTLDLXDelayTopology(queue, delayQueue, bucket); err != nil {
			return "", "", err
		}
		return c.options.Exchange, delayQueue, nil
	case rabbitMQDelayModeNone:
		return "", "", fmt.Errorf("%w: rabbitmq delay_mode=none does not support delayed publish", ErrUnsupportedOperation)
	default:
		return "", "", fmt.Errorf("%w: rabbitmq delay_mode %q is not supported", ErrUnsupportedOperation, c.options.DelayMode)
	}
}

// ensurePluginDelayTopology 声明 RabbitMQ delayed message exchange 插件所需拓扑。
//
// 插件模式保持精确延迟：业务队列同时绑定到普通 exchange 和 delayed exchange，
// 延迟消息发布到 delayed exchange，由 broker 按 x-delay 到期后路由回业务队列。
func (c *Connection) ensurePluginDelayTopology(queue string) error {
	if err := c.ensureQueueTopology(queue); err != nil {
		return err
	}
	delayedExchange := c.delayedExchangeName()
	if !c.options.Declare {
		c.topologyMu.Lock()
		defer c.topologyMu.Unlock()
		c.pruneExpiredTopologyCache()
		key := c.delayExchangeVerificationKey(delayedExchange)
		if c.isTopologyVerified(key) {
			c.pruneTopologyCacheCapacity()
			return nil
		}
		channel, err := c.ensureTopologyChannel()
		if err != nil {
			c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, delayedExchange, 0, err)
			return err
		}
		if err := channel.ExchangeDeclarePassive(delayedExchange, "x-delayed-message", c.options.ExchangeDurable, c.options.AutoDelete, false, c.options.NoWait, amqp.Table{"x-delayed-type": c.options.ExchangeType}); err != nil {
			wrapped := fmt.Errorf("queue: rabbitmq delay topology: %w: exchange %q: %v", ErrRabbitMQTopologyMissing, delayedExchange, err)
			_ = c.resetTopologyChannel()
			c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, delayedExchange, 0, wrapped)
			return wrapped
		}
		c.markTopologyVerified(key)
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclared, queue, delayedExchange, 0, nil)
		return nil
	}

	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	c.pruneExpiredTopologyCache()
	if c.isPluginDelayQueueDeclared(queue) {
		c.pruneTopologyCacheCapacity()
		return nil
	}
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, delayedExchange, 0, err)
		return err
	}
	if err := channel.ExchangeDeclare(delayedExchange, "x-delayed-message", c.options.ExchangeDurable, c.options.AutoDelete, false, c.options.NoWait, amqp.Table{"x-delayed-type": c.options.ExchangeType}); err != nil {
		wrapped := fmt.Errorf("queue: rabbitmq delay topology: declare delayed exchange %q: %w", delayedExchange, err)
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, delayedExchange, 0, wrapped)
		return wrapped
	}
	if err := channel.QueueBind(queue, queue, delayedExchange, c.options.NoWait, nil); err != nil {
		wrapped := fmt.Errorf("queue: rabbitmq delay topology: bind queue %q to delayed exchange %q: %w", queue, delayedExchange, err)
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, delayedExchange, 0, wrapped)
		return wrapped
	}
	c.mu.Lock()
	if c.delayedQueues == nil {
		c.delayedQueues = make(map[string]struct{})
	}
	c.delayedQueues[queue] = struct{}{}
	c.markTopologyUsageLocked(rabbitMQPluginDelayCacheKey(queue), c.topologyCacheNow())
	c.mu.Unlock()
	c.pruneTopologyCacheCapacity()
	c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclared, queue, delayedExchange, 0, nil)
	return nil
}

// ensureTTLDLXDelayTopology 声明固定 bucket delay queue，并通过 DLX 投回业务 exchange。
//
// 这里的 delayQueue 是可复用的 bucket 队列，不随单条消息变化；bucket 决定 x-message-ttl。
func (c *Connection) ensureTTLDLXDelayTopology(queue, delayQueue string, bucket time.Duration) error {
	if !c.options.Declare {
		c.topologyMu.Lock()
		defer c.topologyMu.Unlock()
		c.pruneExpiredTopologyCache()
		key := c.delayQueueVerificationKey(delayQueue, queue, bucket)
		if c.isTopologyVerified(key) {
			c.pruneTopologyCacheCapacity()
			return nil
		}
		channel, err := c.ensureTopologyChannel()
		if err != nil {
			c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, delayQueue, c.options.Exchange, 0, err)
			return err
		}
		expectedArgs := amqp.Table{
			"x-message-ttl":             int32(bucket.Milliseconds()),
			"x-dead-letter-exchange":    c.options.Exchange,
			"x-dead-letter-routing-key": queue,
		}
		if _, err := channel.QueueDeclarePassive(delayQueue, c.options.QueueDurable, c.options.AutoDelete, c.options.Exclusive, c.options.NoWait, expectedArgs); err != nil {
			wrapped := fmt.Errorf("%w: queue %q: %v", ErrRabbitMQTopologyMissing, delayQueue, err)
			_ = c.resetTopologyChannel()
			c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, delayQueue, c.options.Exchange, 0, wrapped)
			return wrapped
		}
		c.markTopologyVerified(key)
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclared, delayQueue, c.options.Exchange, 0, nil)
		return nil
	}

	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	c.pruneExpiredTopologyCache()
	if c.isTTLDLXDelayQueueDeclared(delayQueue, queue) {
		c.pruneTopologyCacheCapacity()
		return nil
	}
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, delayQueue, c.options.Exchange, 0, err)
		return err
	}
	args := amqp.Table{
		"x-message-ttl":             int32(bucket.Milliseconds()),
		"x-dead-letter-exchange":    c.options.Exchange,
		"x-dead-letter-routing-key": queue,
	}
	if _, err := channel.QueueDeclare(delayQueue, c.options.QueueDurable, c.options.AutoDelete, c.options.Exclusive, c.options.NoWait, args); err != nil {
		wrapped := fmt.Errorf("queue: rabbitmq delay topology: declare ttl delay queue %q: %w", delayQueue, err)
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, delayQueue, c.options.Exchange, 0, wrapped)
		return wrapped
	}
	if err := channel.QueueBind(delayQueue, delayQueue, c.options.Exchange, c.options.NoWait, nil); err != nil {
		wrapped := fmt.Errorf("queue: rabbitmq delay topology: bind ttl delay queue %q: %w", delayQueue, err)
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, delayQueue, c.options.Exchange, 0, wrapped)
		return wrapped
	}
	c.mu.Lock()
	if c.ttlDelayQueues == nil {
		c.ttlDelayQueues = make(map[string]struct{})
	}
	c.ttlDelayQueues[delayQueue] = struct{}{}
	c.markTopologyUsageLocked(rabbitMQTTLDLXDelayCacheKey(delayQueue, queue), c.topologyCacheNow())
	c.mu.Unlock()
	c.pruneTopologyCacheCapacity()
	c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclared, delayQueue, c.options.Exchange, 0, nil)
	return nil
}

// ensureQueueTopology 按当前 issue 的最小要求声明 exchange/queue/binding。
//
// 这里仍然走“queue name 作为 routing key”的约定，
// 因为 RabbitMQ driver 的第一版拓扑边界已经在基线 issue 中固定为 direct exchange。
func (c *Connection) ensureQueueTopology(queue string) error {
	queue = normalizeRabbitMQQueueName(queue, nil)
	if !c.options.Declare {
		return c.ensureExistingQueueTopology(queue)
	}

	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	c.pruneExpiredTopologyCache()
	if c.isQueueDeclared(queue) {
		c.pruneTopologyCacheCapacity()
		return nil
	}
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, err)
		return err
	}
	if err := channel.ExchangeDeclare(c.options.Exchange, c.options.ExchangeType, c.options.ExchangeDurable, c.options.AutoDelete, false, c.options.NoWait, nil); err != nil {
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, err)
		return err
	}
	if _, err := channel.QueueDeclare(queue, c.options.QueueDurable, c.options.AutoDelete, c.options.Exclusive, c.options.NoWait, c.queueDeclareArgs()); err != nil {
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, err)
		return err
	}
	if err := channel.QueueBind(queue, queue, c.options.Exchange, c.options.NoWait, nil); err != nil {
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, err)
		return err
	}
	c.mu.Lock()
	if c.declaredQueues == nil {
		c.declaredQueues = make(map[string]struct{})
	}
	c.declaredQueues[queue] = struct{}{}
	c.markTopologyUsageLocked(rabbitMQDeclaredQueueCacheKey(queue), c.topologyCacheNow())
	c.mu.Unlock()
	c.pruneTopologyCacheCapacity()
	c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclared, queue, c.options.Exchange, 0, nil)
	return nil
}

// ensureExistingQueueTopology 在 declare=false 时只做被动检查，不创建任何资源。
//
// 需求背景：
// 生产环境可能由运维或 IaC 预先创建 RabbitMQ 拓扑。此时 driver 不能静默声明资源，
// 但也不能在目标 queue 不存在时继续发布并让消息被 broker 丢弃。
func (c *Connection) ensureExistingQueueTopology(queue string) error {
	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	c.pruneExpiredTopologyCache()
	exchangeKey := c.exchangeVerificationKey()
	queueKey := c.queueVerificationKey(queue)
	if c.isTopologyVerified(exchangeKey) && c.isTopologyVerified(queueKey) {
		c.pruneTopologyCacheCapacity()
		return nil
	}
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, err)
		return err
	}
	if !c.isTopologyVerified(exchangeKey) {
		if err := channel.ExchangeDeclarePassive(c.options.Exchange, c.options.ExchangeType, c.options.ExchangeDurable, c.options.AutoDelete, false, c.options.NoWait, nil); err != nil {
			wrapped := fmt.Errorf("%w: exchange %q: %v", ErrRabbitMQTopologyMissing, c.options.Exchange, err)
			_ = c.resetTopologyChannel()
			c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, wrapped)
			return wrapped
		}
		c.markTopologyVerified(exchangeKey)
	}
	if c.isTopologyVerified(queueKey) {
		c.pruneTopologyCacheCapacity()
		return nil
	}
	if _, err := channel.QueueDeclarePassive(queue, c.options.QueueDurable, c.options.AutoDelete, c.options.Exclusive, c.options.NoWait, c.queueDeclareArgs()); err != nil {
		wrapped := fmt.Errorf("%w: queue %q: %v", ErrRabbitMQTopologyMissing, queue, err)
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, wrapped)
		return wrapped
	}
	c.markTopologyVerified(queueKey)
	c.pruneTopologyCacheCapacity()
	c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclared, queue, c.options.Exchange, 0, nil)
	return nil
}

// ensureExistingExchangeTopologyLocked 确保 declare=false 路径下默认业务 exchange 已存在。
//
// 调用约束：
// 调用方必须已经持有 topologyMu。该约束保证首次未命中缓存时只有一个 goroutine 会向 broker
// 发送 ExchangeDeclarePassive，排队的 goroutine 在拿到锁后会二次检查缓存并直接返回。
//
// 参数说明：
// queue 仅用于失败事件 payload，便于定位是哪条业务队列操作触发了本次 exchange 验证。
func (c *Connection) ensureExistingExchangeTopologyLocked(queue string) error {
	c.pruneExpiredTopologyCache()
	key := c.exchangeVerificationKey()
	if c.isTopologyVerified(key) {
		c.pruneTopologyCacheCapacity()
		return nil
	}
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, err)
		return err
	}
	if err := channel.ExchangeDeclarePassive(c.options.Exchange, c.options.ExchangeType, c.options.ExchangeDurable, c.options.AutoDelete, false, c.options.NoWait, nil); err != nil {
		wrapped := fmt.Errorf("%w: exchange %q: %v", ErrRabbitMQTopologyMissing, c.options.Exchange, err)
		_ = c.resetTopologyChannel()
		c.emitInfrastructureEvent(nil, queueevents.EventTopologyDeclareFailed, queue, c.options.Exchange, 0, wrapped)
		return wrapped
	}
	c.markTopologyVerified(key)
	c.pruneTopologyCacheCapacity()
	return nil
}

// exchangeVerificationKey 生成默认业务 exchange 的被动验证缓存键。
//
// exchange 的存在性验证受 exchange 类型、durable、auto_delete 和 no_wait 等声明参数影响，
// 因此这些参数必须进入 key，避免不同配置共用同一条验证结果。
func (c *Connection) exchangeVerificationKey() rabbitMQTopologyVerificationKey {
	return rabbitMQTopologyVerificationKey{
		Kind:         rabbitMQVerifiedExchange,
		Name:         c.options.Exchange,
		ExchangeType: c.options.ExchangeType,
		Durable:      c.options.ExchangeDurable,
		AutoDelete:   c.options.AutoDelete,
		NoWait:       c.options.NoWait,
	}
}

// queueVerificationKey 生成业务 queue 的被动验证缓存键。
//
// queue 验证只表示该 queue name 在当前 AMQP connection 上已经通过 QueueInspect 确认存在，
// 不代表它已绑定到默认 exchange，也不代表发布 routing key 一定能送达该 queue。
func (c *Connection) queueVerificationKey(queue string) rabbitMQTopologyVerificationKey {
	return rabbitMQTopologyVerificationKey{
		Kind:  rabbitMQVerifiedQueue,
		Name:  queue,
		Queue: queue,
	}
}

// delayExchangeVerificationKey 生成 plugin delay exchange 的被动验证缓存键。
//
// RabbitMQ delayed-message 插件使用 x-delayed-message exchange，并通过 x-delayed-type 指定底层
// exchange 类型。这里把插件 exchange kind 和 x-delayed-type 都纳入 key，避免与默认业务 exchange 混淆。
func (c *Connection) delayExchangeVerificationKey(exchange string) rabbitMQTopologyVerificationKey {
	return rabbitMQTopologyVerificationKey{
		Kind:         rabbitMQVerifiedDelayExchange,
		Name:         exchange,
		ExchangeType: "x-delayed-message",
		Durable:      c.options.ExchangeDurable,
		AutoDelete:   c.options.AutoDelete,
		NoWait:       c.options.NoWait,
		DelayedType:  c.options.ExchangeType,
	}
}

// delayQueueVerificationKey 生成 ttl_dlx 固定 bucket delay queue 的被动验证缓存键。
//
// TTL+DLX 模式按固定 bucket 复用 delay queue。缓存命中只说明该 delay queue 存在，
// 不说明 DLX 参数或回投 routing 完整安全；routing/binding 安全由独立问题处理。
func (c *Connection) delayQueueVerificationKey(delayQueue, queue string, bucket time.Duration) rabbitMQTopologyVerificationKey {
	return rabbitMQTopologyVerificationKey{
		Kind:   rabbitMQVerifiedDelayQueue,
		Name:   delayQueue,
		Queue:  queue,
		Bucket: bucket,
	}
}

// isTopologyVerified 查询当前 AMQP connection 生命周期内是否已经成功验证过指定 topology。
//
// 该函数只读本地缓存，不访问 broker；失败验证不会写入缓存，因此 true 只可能来自成功的被动检查。
func (c *Connection) isTopologyVerified(key rabbitMQTopologyVerificationKey) bool {
	if !c.topologyCacheEnabled() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		_, ok := c.verifiedTopology[key]
		return ok
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.verifiedTopology[key]
	if ok {
		c.touchTopologyUsageLocked(rabbitMQVerifiedTopologyCacheKey(key), c.topologyCacheNow())
		c.pruneTopologyCacheCapacityLocked()
	}
	return ok
}

// markTopologyVerified 记录一次成功的 declare=false 被动存在性验证。
//
// 只有 broker 调用成功后才能写入缓存，避免资源暂时缺失时形成 negative cache。
// 缓存生命周期由 connection 状态切换控制：运行期断线、重连安装新 connection 和主动 Close 都会清空。
func (c *Connection) markTopologyVerified(key rabbitMQTopologyVerificationKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.verifiedTopology == nil {
		c.verifiedTopology = make(map[rabbitMQTopologyVerificationKey]struct{})
	}
	c.verifiedTopology[key] = struct{}{}
	c.markTopologyUsageLocked(rabbitMQVerifiedTopologyCacheKey(key), c.topologyCacheNow())
	c.pruneTopologyCacheCapacityLocked()
}

// queueDeclareArgs 生成 queue 声明时的 RabbitMQ 专属参数。
//
// 需求背景：
// 第一版只允许在 topology 层开启 priority queue 能力，对外不增加 Job 级优先级 API。
// 因此这里仅在配置了正数 QueueMaxPriority 时写入 x-max-priority，默认保持普通队列。
func (c *Connection) queueDeclareArgs() amqp.Table {
	if c.options.QueueMaxPriority <= 0 {
		return nil
	}
	return amqp.Table{"x-max-priority": int32(c.options.QueueMaxPriority)}
}

func (c *Connection) isQueueDeclared(queue string) bool {
	if !c.topologyCacheEnabled() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		_, ok := c.declaredQueues[queue]
		return ok
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.declaredQueues[queue]
	if ok {
		c.touchTopologyUsageLocked(rabbitMQDeclaredQueueCacheKey(queue), c.topologyCacheNow())
		c.pruneTopologyCacheCapacityLocked()
	}
	return ok
}

func (c *Connection) isPluginDelayQueueDeclared(queue string) bool {
	if !c.topologyCacheEnabled() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		_, ok := c.delayedQueues[queue]
		return ok
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.delayedQueues[queue]
	if ok {
		c.touchTopologyUsageLocked(rabbitMQPluginDelayCacheKey(queue), c.topologyCacheNow())
		c.pruneTopologyCacheCapacityLocked()
	}
	return ok
}

func (c *Connection) isTTLDLXDelayQueueDeclared(delayQueue, queue string) bool {
	if !c.topologyCacheEnabled() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		_, ok := c.ttlDelayQueues[delayQueue]
		return ok
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.ttlDelayQueues[delayQueue]
	if ok {
		c.touchTopologyUsageLocked(rabbitMQTTLDLXDelayCacheKey(delayQueue, queue), c.topologyCacheNow())
		c.pruneTopologyCacheCapacityLocked()
	}
	return ok
}

// normalizedDelayMode 统一 delay_mode 的大小写和空值默认，便于所有分支使用同一套常量。
func (c *Connection) normalizedDelayMode() string {
	mode := strings.TrimSpace(strings.ToLower(c.options.DelayMode))
	if mode == "" {
		return rabbitMQDelayModePlugin
	}
	return mode
}

// delayedExchangeName 固定使用业务 exchange 的派生名，避免改变原有普通发布拓扑。
func (c *Connection) delayedExchangeName() string {
	return c.options.Exchange + ".delayed"
}

// ttlDelayQueueName 把业务队列和 bucket 编进队列名，使同一业务队列可复用固定延迟队列。
func (c *Connection) ttlDelayQueueName(queue string, bucket time.Duration) string {
	return fmt.Sprintf("%s.%s.delay.%ds", c.options.Exchange, queue, int(bucket/time.Second))
}

// delayBucket 返回大于等于目标 delay 的最小 bucket；超过最大 bucket 时使用最大 bucket。
func (c *Connection) delayBucket(delay time.Duration) time.Duration {
	buckets := sanitizeRabbitMQDelayBuckets(c.options.DelayBuckets)
	var selected time.Duration
	maxBucket := buckets[0]
	for _, bucket := range buckets {
		if bucket > maxBucket {
			maxBucket = bucket
		}
		if bucket >= delay && (selected == 0 || bucket < selected) {
			selected = bucket
		}
	}
	if selected > 0 {
		return selected
	}
	return maxBucket
}
