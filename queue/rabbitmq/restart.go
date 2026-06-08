package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 本文件实现 RabbitMQ 版本的 queue:restart 存储。
// 它只负责跨进程可见的重启时间戳，不承载 failed job、batch 或业务任务队列语义。

// RequestRestart 将 queue:restart 信号写入 RabbitMQ 的持久化时间戳队列。
//
// 需求背景：
// RabbitMQ worker 可能分布在不同进程中，重启信号不能只保存在当前 Manager 内存里。
// 这里把信号保存为 UnixNano 时间戳消息，让独立进程创建的 manager/worker 都能读到同一份状态。
//
// 设计约束：
// 1. 只实现 RestartStore，不把 failed job 或 batch 状态塞进 RabbitMQ。
// 2. 使用默认 exchange 直接投递到 restart queue，由队列保存最新一条时间戳。
// 3. 发布仍走 publisher confirm；只有 broker 确认收到后，queue:restart 才算成功。
func (c *Connection) RequestRestart(ctx context.Context, at time.Time) error {
	if c == nil {
		return ErrConnectionClosed
	}
	if !c.options.RestartEnabled {
		return fmt.Errorf("%w: rabbitmq restart store is disabled", ErrUnsupportedOperation)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// queue:restart 也是发布路径，必须和普通 Push 共用同一类超时边界；
	// 该窗口覆盖等待重连、restart topology、publish slot、PublishWithContext 和 confirm。
	publishCtx := ctx
	cancel := func() {}
	if c.options.PublishTimeout > 0 {
		publishCtx, cancel = context.WithTimeout(ctx, c.options.PublishTimeout)
	}
	defer cancel()
	if at.IsZero() {
		at = time.Now()
	}
	restartQueue := normalizeRabbitMQQueueName(c.options.RestartQueue, nil)
	message := amqp.Publishing{
		Body:        []byte(strconv.FormatInt(at.UnixNano(), 10)),
		ContentType: "text/plain",
		Timestamp:   at,
	}
	if c.options.MessagePersistent {
		message.DeliveryMode = amqp.Persistent
	}

	for {
		// 运行期断线时允许在 PublishTimeout 内等待恢复；窗口耗尽后稳定归类为发布超时。
		if err := c.waitReady(publishCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, err)
			}
			return err
		}
		if err := c.ensureRestartTopology(restartQueue); err != nil {
			if errors.Is(err, ErrConnectionClosed) && publishCtx.Err() == nil {
				continue
			}
			// topology 声明阶段消耗完整个发布窗口时，也按 publish timeout 暴露给调用方。
			if errors.Is(publishCtx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, publishCtx.Err())
			}
			return err
		}
		err := c.publishWithConfirm(publishCtx, "", restartQueue, message)
		if err == nil {
			c.storeRestartSignalCache(at)
			return nil
		}
		if errors.Is(err, ErrConnectionClosed) && publishCtx.Err() == nil {
			continue
		}
		return err
	}
}

// RestartRequestedAt 读取 RabbitMQ 中当前可见的 queue:restart 时间戳。
//
// RabbitMQ 没有真正的 peek API。为满足“读取时不消费”的语义，这里使用
// basic.get(autoAck=false) 取到消息后立即 Nack(requeue=true)，让同一条信号继续留在
// 队列里供其他 manager/worker 观察。空队列、空 payload 或损坏 payload 都按“当前没有
// 有效重启信号”处理，避免坏数据误停 worker。
func (c *Connection) RestartRequestedAt(ctx context.Context) (time.Time, error) {
	if err := c.ensureOpen(); err != nil {
		return time.Time{}, err
	}
	if !c.options.RestartEnabled {
		return time.Time{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return c.cachedRestartRequestedAt(ctx)
}

// cachedRestartRequestedAt 在 connection 级 TTL 窗口内复用 restart 读取结果。
//
// 逻辑说明：
// 缓存未过期时直接返回最近一次读取到的零时间或 timestamp，不访问 broker。缓存过期时，当前
// goroutine 持有 restartCache.mu 完成 topology 检查、basic.get、Nack(requeue=true) 和缓存写入；
// 其他并发调用会等待该刷新完成并复用结果，从而把同一 connection 上的并发刷新合并为一次 broker 读取。
//
// 失败边界：
// topology 检查、basic.get 或 Nack 失败都不写缓存，调用方下一次检查可以立即重试。这样短暂 broker
// 故障不会被本地 negative cache 放大成固定窗口内的不可见 restart 信号。
func (c *Connection) cachedRestartRequestedAt(ctx context.Context) (time.Time, error) {
	c.restartCache.mu.Lock()
	defer c.restartCache.mu.Unlock()

	now := time.Now()
	if now.Before(c.restartCache.expiresAt) {
		return c.restartCache.at, nil
	}
	restartQueue := normalizeRabbitMQQueueName(c.options.RestartQueue, nil)
	if err := c.ensureRestartTopology(restartQueue); err != nil {
		return time.Time{}, err
	}
	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		return time.Time{}, err
	}
	delivery, ok, err := channel.Get(restartQueue, false)
	if err != nil {
		_ = c.resetTopologyChannel()
		return time.Time{}, err
	}
	if !ok {
		c.restartCache.at = time.Time{}
		c.restartCache.expiresAt = time.Now().Add(c.options.RestartPollInterval)
		return c.restartCache.at, nil
	}
	if delivery.Acknowledger != nil {
		if err := delivery.Nack(false, true); err != nil {
			_ = c.resetTopologyChannel()
			return time.Time{}, err
		}
	}
	at := parseRabbitMQRestartTimestamp(delivery.Body)
	c.restartCache.at = at
	c.restartCache.expiresAt = time.Now().Add(c.options.RestartPollInterval)
	return at, nil
}

// storeRestartSignalCache 在当前 connection 内实现 RequestRestart 的 read-your-own-write。
//
// RequestRestart 只有在 broker 发布成功后才调用该方法。这样同一 connection 即使刚缓存过零时间，
// 也能立刻读到自己刚写入的 restart timestamp；其他进程或 connection 仍以 RabbitMQ restart queue
// 为事实源，并最多等待 restart_poll_interval 后看见新信号。
func (c *Connection) storeRestartSignalCache(at time.Time) {
	c.restartCache.mu.Lock()
	defer c.restartCache.mu.Unlock()
	c.restartCache.at = at
	c.restartCache.expiresAt = time.Now().Add(c.options.RestartPollInterval)
}

// clearRestartSignalCache 失效当前 connection 级 restart 读取缓存。
//
// Close、运行期断线和安装新 AMQP connection 都会调用该方法。缓存不跨 AMQP connection 生命周期保留，
// 因为 restart queue 是跨进程事实源，新连接应重新向 broker 读取当前可见状态。
func (c *Connection) clearRestartSignalCache() {
	c.restartCache.mu.Lock()
	defer c.restartCache.mu.Unlock()
	c.restartCache.at = time.Time{}
	c.restartCache.expiresAt = time.Time{}
}

// ensureRestartTopology 声明或检查 restart 信号队列。
//
// 需求背景：
// restart queue 不是业务任务队列，不需要绑定业务 exchange，也不应该被 worker 消费。
// 它只作为跨进程可见的“最新时间戳”存储点使用。
//
// 设计约束：
// 1. declare=true 时由 driver 自动声明 durable queue。
// 2. declare=false 时只做 QueueInspect，被动验证运维或 IaC 已提前创建资源。
// 3. 队列参数固定为单消息 latest-value 语义，重复 restart 请求保留最新时间戳。
func (c *Connection) ensureRestartTopology(queue string) error {
	queue = normalizeRabbitMQQueueName(queue, nil)
	if !c.options.Declare {
		c.topologyMu.Lock()
		defer c.topologyMu.Unlock()
		c.pruneExpiredTopologyCache()
		if c.isRestartQueueTopologyCached(queue) {
			c.pruneTopologyCacheCapacity()
			return nil
		}
		channel, err := c.ensureTopologyChannel()
		if err != nil {
			return err
		}
		if _, err := channel.QueueInspect(queue); err != nil {
			_ = c.resetTopologyChannel()
			return fmt.Errorf("%w: restart queue %q: %v", ErrRabbitMQTopologyMissing, queue, err)
		}
		c.markRestartQueueTopology(queue)
		c.pruneTopologyCacheCapacity()
		return nil
	}

	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	c.pruneExpiredTopologyCache()
	if c.isRestartQueueTopologyCached(queue) {
		c.pruneTopologyCacheCapacity()
		return nil
	}
	channel, err := c.ensureTopologyChannel()
	if err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(queue, true, false, false, c.options.NoWait, rabbitMQRestartQueueArgs()); err != nil {
		_ = c.resetTopologyChannel()
		return err
	}
	c.markRestartQueueTopology(queue)
	c.pruneTopologyCacheCapacity()
	return nil
}

func (c *Connection) isRestartQueueTopologyCached(queue string) bool {
	if !c.topologyCacheEnabled() {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.restartQueues[queue]
	if ok {
		c.touchTopologyUsageLocked(rabbitMQRestartQueueCacheKey(queue), c.topologyCacheNow())
		c.pruneTopologyCacheCapacityLocked()
	}
	return ok
}

func (c *Connection) markRestartQueueTopology(queue string) {
	if !c.topologyCacheEnabled() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.restartQueues == nil {
		c.restartQueues = make(map[string]struct{})
	}
	c.restartQueues[queue] = struct{}{}
	c.markTopologyUsageLocked(rabbitMQRestartQueueCacheKey(queue), c.topologyCacheNow())
	c.pruneTopologyCacheCapacityLocked()
}

// rabbitMQRestartQueueArgs 返回 restart queue 的最新值存储参数。
//
// x-max-length=1 限制队列最多保存一条时间戳；x-overflow=drop-head 在新信号进入时丢弃旧信号，
// 与“最新 queue:restart 时间戳获胜”的语义一致。
func rabbitMQRestartQueueArgs() amqp.Table {
	return amqp.Table{
		"x-max-length": int32(1),
		"x-overflow":   "drop-head",
	}
}

// parseRabbitMQRestartTimestamp 解析 restart queue 中保存的时间戳 payload。
//
// 兼容边界：
// 当前写入路径使用纯文本 UnixNano；这里也兼容 JSON number 和 JSON string，方便后续运维工具
// 或历史测试数据以 JSON 形式写入。无法解析或非正数时返回零时间，表示没有有效重启信号。
func parseRabbitMQRestartTimestamp(body []byte) time.Time {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return time.Time{}
	}
	var nano int64
	if err := json.Unmarshal([]byte(trimmed), &nano); err == nil && nano > 0 {
		return time.Unix(0, nano)
	}
	var text string
	if err := json.Unmarshal([]byte(trimmed), &text); err == nil {
		trimmed = strings.TrimSpace(text)
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || parsed <= 0 {
		return time.Time{}
	}
	return time.Unix(0, parsed)
}

func ParseRestartTimestamp(body []byte) time.Time {
	return parseRabbitMQRestartTimestamp(body)
}
