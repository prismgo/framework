package rabbitmq

import (
	"container/list"
	"context"
	"time"

	queueevents "github.com/prismgo/framework/queue/internal/events"
	"github.com/prismgo/framework/routine"
	amqp "github.com/rabbitmq/amqp091-go"
)

// 本文件承载 RabbitMQ 运行期断线后的 best-effort 重连状态机。
// 初始建连仍在 NewRabbitMQConnection 中同步完成，只有已成功连接后的 NotifyClose 会进入这里。

// monitorConnection 订阅当前 AMQP connection 的 NotifyClose 信号。
//
// 设计边界：
// 1. 只监控 NewRabbitMQConnection 已经成功建立后的运行期断线。
// 2. 主动 Close 会先设置 c.closed，因此这里收到关闭通知后会被 handleConnectionClosed 忽略。
// 3. AMQP 库在连接异常关闭时会关闭 notify channel 或投递错误，两种情况都按运行期断线处理。
func (c *Connection) monitorConnection(conn AMQPConnection) {
	if c == nil || conn == nil {
		return
	}
	notify := conn.NotifyClose(make(chan *amqp.Error, 1))
	routine.Task(context.Background(), func(context.Context) error {
		closeErr, ok := <-notify
		if !ok && !conn.IsClosed() {
			return nil
		}
		// NotifyClose 在部分测试桩和 broker 正常关闭路径里可能返回 nil *amqp.Error。
		// 先转成真正的 nil error，避免后续事件序列化调用 typed-nil Error 方法触发 panic。
		var err error
		if closeErr != nil {
			err = closeErr
		}
		c.handleConnectionClosed(conn, err)
		return nil
	}).
		Component("queue").
		Name("rabbitmq.notify_close").
		Go()
}

// handleConnectionClosed 把连接切换到“暂不可用、正在恢复”的状态。
//
// 这里不会直接对调用方返回永久关闭错误，而是关闭旧 channel 缓存、清空当前连接的声明缓存，
// 并启动后台重连。已投递但尚未 ack 的消息不在 driver 内模拟 Redis reserved timeout，
// 由 RabbitMQ 在 consumer/connection 断开后按 broker redelivery 语义重新投递。
func (c *Connection) handleConnectionClosed(conn AMQPConnection, closeErr error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed || c.reconnecting || (conn != nil && c.amqpConnection != conn) {
		c.mu.Unlock()
		return
	}
	// 旧发布 slot、拓扑 channel 和消费 channel 都绑定在已断开的 AMQP connection 上。
	// 这里先快照出来并从 Connection 状态中摘除，后续在锁外关闭，避免网络关闭动作阻塞状态切换。
	publishSlots := c.publishSlots
	topologyChannel := c.topologyChannel
	consumerChannel := c.consumerChannel
	stoppedConsumers := keysOfConsumerMap(c.consumers)
	c.amqpConnection = nil
	// 发布池必须整体清空；旧 slot 中可能残留未完成的 confirm 流，不能跨 connection 复用。
	c.publishSlots = nil
	c.publishNext.Store(0)
	c.topologyChannel = nil
	c.consumerChannel = nil
	c.declaredQueues = make(map[string]struct{})
	c.delayedQueues = make(map[string]struct{})
	c.ttlDelayQueues = make(map[string]struct{})
	c.restartQueues = make(map[string]struct{})
	c.consumers = make(map[string]<-chan amqp.Delivery)
	c.consumerTags = make(map[string]string)
	c.verifiedTopology = make(map[rabbitMQTopologyVerificationKey]struct{})
	c.topologyUsage = make(map[rabbitMQTopologyCacheKey]rabbitMQTopologyUsageEntry)
	c.topologyLRU = list.New()
	c.reconnecting = true
	c.ready = false
	c.readyCh = make(chan struct{})
	// 逻辑说明：reconnectLooping 标记和 goroutine 启动都在同一个锁范围内完成，
	// 保证只有一个重连循环被启动。routine.Task().Go() 本身只是提交任务，不会阻塞锁。
	startLoop := !c.reconnectLooping
	if startLoop {
		c.reconnectLooping = true
	}
	c.mu.Unlock()
	c.clearRestartSignalCache()
	c.emitInfrastructureEvent(context.Background(), queueevents.EventConnectionDisconnected, "", c.options.Exchange, 0, closeErr)
	for _, queue := range stoppedConsumers {
		c.emitInfrastructureEvent(context.Background(), queueevents.EventConsumerStopped, queue, c.options.Exchange, 0, nil)
	}

	_ = closeRabbitMQPublishSlots(publishSlots)
	_ = closeRabbitMQChannel(topologyChannel)
	_ = closeRabbitMQChannel(consumerChannel)
	if conn != nil && !conn.IsClosed() {
		_ = conn.Close()
	}
	if startLoop {
		routine.Task(context.Background(), func(context.Context) error {
			c.reconnectLoop()
			return nil
		}).
			Component("queue").
			Name("rabbitmq.reconnect").
			Go()
	}
}

// reconnectLoop 按有界退避重建 AMQP connection，并在成功后恢复拓扑和 active consumer。
//
// 初始 dial 失败仍由 NewRabbitMQConnection 立即返回；只有运行期 NotifyClose 进入这里。
// 循环本身是 best-effort：broker 长时间不可用时调用方会受 publish_timeout、BlockFor 或 context 约束退出。
func (c *Connection) reconnectLoop() {
	delay := c.options.ReconnectMinDelay
	if delay <= 0 {
		delay = defaultRabbitMQReconnectMin
	}
	attempt := 0
	for {
		if c.isClosed() {
			return
		}
		attempt++
		c.emitInfrastructureEvent(context.Background(), queueevents.EventConnectionReconnecting, "", c.options.Exchange, attempt, nil)
		conn, err := c.options.dialer()(c.address, amqp.Config{
			Heartbeat: c.options.Heartbeat,
			Locale:    "en_US",
		})
		if err == nil {
			if c.installReconnectedConnection(conn) {
				if restoreErr := c.restoreRabbitMQState(); restoreErr == nil {
					c.markReady(conn)
					c.emitInfrastructureEvent(context.Background(), queueevents.EventConnectionReconnected, "", c.options.Exchange, attempt, nil)
					return
				} else {
					c.emitInfrastructureEvent(context.Background(), queueevents.EventConnectionReconnectFailed, "", c.options.Exchange, attempt, restoreErr)
				}
			}
			if conn != nil && !conn.IsClosed() {
				_ = conn.Close()
			}
		} else {
			c.emitInfrastructureEvent(context.Background(), queueevents.EventConnectionReconnectFailed, "", c.options.Exchange, attempt, err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-c.closedNotify():
			timer.Stop()
			return
		}
		delay *= 2
		if delay > c.options.ReconnectMaxDelay {
			delay = c.options.ReconnectMaxDelay
		}
		if delay <= 0 {
			delay = defaultRabbitMQReconnectMax
		}
	}
}

// installReconnectedConnection 安装新 connection，并重置所有绑定到旧连接的 channel 缓存。
//
// declared* 缓存只表示“当前 AMQP connection 已经声明”，因此每次重连都必须清空；
// known* 拓扑意图会继续保留，用于 restoreRabbitMQState 重新声明。
// publishSlots 也必须清空并重置 round-robin 计数器，因为 publisher confirm 流只对创建它的 channel 有效，
// 旧 slot 不能迁移到新 connection。
func (c *Connection) installReconnectedConnection(conn AMQPConnection) bool {
	c.clearRestartSignalCache()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.amqpConnection = conn
	// 新 connection 使用新的发布 channel 池，避免旧 confirm channel 与新 broker 会话混用。
	c.publishSlots = nil
	c.publishNext.Store(0)
	c.topologyChannel = nil
	c.consumerChannel = nil
	c.declaredQueues = make(map[string]struct{})
	c.delayedQueues = make(map[string]struct{})
	c.ttlDelayQueues = make(map[string]struct{})
	c.restartQueues = make(map[string]struct{})
	c.consumers = make(map[string]<-chan amqp.Delivery)
	c.consumerTags = make(map[string]string)
	c.verifiedTopology = make(map[rabbitMQTopologyVerificationKey]struct{})
	c.topologyUsage = make(map[rabbitMQTopologyCacheKey]rabbitMQTopologyUsageEntry)
	c.topologyLRU = list.New()
	return true
}

// restoreRabbitMQState 按 live Consumer Intent 恢复业务 topology 与 active consumer。
//
// 需求背景：
// publish、Size、Clear 和历史 delayed publish 都可能碰过大量动态队列；这些使用痕迹不能成为
// 跨 connection 生命周期的永久恢复清单，否则 reconnect 成本会随历史噪音增长。
//
// 设计思路：
// live Consumer Intent 由 worker lease 持有。恢复阶段只为仍有 live 引用的业务队列重建
// queue topology 和 AMQP consumer；delay topology 在下一次 delayed Push/Release 时按需声明或验证。
func (c *Connection) restoreRabbitMQState() error {
	consumers := c.snapshotReconnectConsumers()
	for _, queue := range consumers {
		if err := c.ensureQueueConsumer(queue); err != nil {
			return err
		}
	}
	return nil
}

// snapshotReconnectConsumers 在不长时间持锁的情况下复制重连恢复所需的 live consumer。
//
// 后续声明和 Consume 调用可能访问 broker，不能在 c.mu 持有期间执行，否则公开方法会被阻塞。
func (c *Connection) snapshotReconnectConsumers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return keysOfStringSet(c.activeConsumers)
}

// markReady 发布恢复完成信号，并为新 connection 重新安装 NotifyClose 监听。
//
// readyCh 采用 close 广播语义，可以一次唤醒所有正在等待恢复的 Push/Pop 调用。
func (c *Connection) markReady(conn AMQPConnection) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		if conn != nil && !conn.IsClosed() {
			_ = conn.Close()
		}
		return
	}
	c.reconnecting = false
	c.reconnectLooping = false
	if !c.ready {
		c.ready = true
		close(c.readyCh)
	}
	c.mu.Unlock()
	c.monitorConnection(conn)
}

// waitReady 等待连接进入可用状态，或在调用方边界到达时返回。
//
// Push 传入带 publish_timeout 的 context，因此等待重连、恢复拓扑、发布和 confirm 共用同一时间窗口；
// Pop 则由 BlockFor/context 生成等待边界，窗口耗尽后外层转换为 ErrEmpty。
func (c *Connection) waitReady(ctx context.Context) error {
	if c == nil {
		return ErrConnectionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		c.mu.RLock()
		closed := c.closed
		ready := c.ready
		readyCh := c.readyCh
		conn := c.amqpConnection
		reconnecting := c.reconnecting
		c.mu.RUnlock()
		if closed {
			return ErrConnectionClosed
		}
		if ready && conn != nil && !conn.IsClosed() {
			return nil
		}
		if !reconnecting && conn != nil && conn.IsClosed() {
			c.handleConnectionClosed(conn, nil)
			continue
		}
		if ready && conn == nil {
			return ErrConnectionClosed
		}
		select {
		case <-readyCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Connection) isClosed() bool {
	if c == nil {
		return true
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *Connection) isReconnecting() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reconnecting
}

func (c *Connection) closedNotify() <-chan struct{} {
	if c == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closedCh
}

func closeRabbitMQChannel(channel AMQPChannel) error {
	if channel == nil {
		return nil
	}
	return channel.Close()
}

func keysOfStringSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func keysOfConsumerMap(values map[string]<-chan amqp.Delivery) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
