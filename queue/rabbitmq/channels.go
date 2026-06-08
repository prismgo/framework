package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// nextPublishSlot 按全局 round-robin 选择发布 slot。
//
// 设计思路：
//  1. 先用 atomic 计数器获取原始序号，用于 round-robin 分布。
//  2. index 计算和 slot 读取统一在同一个锁范围内完成，避免在 RUnlock 和后续 Lock
//     之间另一个 goroutine 触发 publishSlots 重新分配导致原始 index 越界。
//  3. slot 按需懒加载；已有 slot 走读锁快速返回，缺失时再进入写锁创建。
//
// 返回值：
// - 成功时返回本次发布应该使用的 slot。
// - 连接为空、已关闭或底层 AMQP connection 已关闭时返回 ErrConnectionClosed，让 Push 外层进入重连等待或失败路径。
func (c *Connection) nextPublishSlot() (*rabbitMQPublishSlot, error) {
	if c == nil {
		return nil, ErrConnectionClosed
	}
	// 逻辑说明：atomic.Add 在锁外执行，仅用于 round-robin 分布；
	// 实际 index 计算在锁内基于当前 slots 长度取模，防止 slots 重分配后越界。
	raw := c.publishNext.Add(1) - 1

	c.mu.RLock()
	if c.closed || c.amqpConnection == nil || c.amqpConnection.IsClosed() {
		c.mu.RUnlock()
		return nil, ErrConnectionClosed
	}
	slots := c.publishSlots
	// slots 为空时无法在 RLock 内命中，直接进入写锁路径创建
	if len(slots) == 0 {
		c.mu.RUnlock()
		// fall through to write lock below
	} else {
		// 在锁内计算 index，保证与当前 slots 长度一致
		index := int(raw) % len(slots)
		if index < 0 {
			index = -index
		}
		var slot *rabbitMQPublishSlot
		if index < len(slots) {
			slot = slots[index]
		}
		c.mu.RUnlock()
		if slot != nil {
			return slot, nil
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.amqpConnection == nil || c.amqpConnection.IsClosed() {
		return nil, ErrConnectionClosed
	}
	size := normalizeRabbitMQPublishChannels(c.options.PublishChannels)
	if len(c.publishSlots) != size {
		c.publishSlots = make([]*rabbitMQPublishSlot, size)
	}
	// 在写锁内重新计算 index，使用当前 slots 长度
	index := int(raw) % len(c.publishSlots)
	if index < 0 {
		index = -index
	}
	if c.publishSlots[index] == nil {
		c.publishSlots[index] = &rabbitMQPublishSlot{}
	}
	return c.publishSlots[index], nil
}

// ensurePublishSlotChannel 按需创建绑定到单个发布 slot 的 AMQP channel、confirm 流和 return 流。
//
// 参数说明：
// - slot：nextPublishSlot 返回的发布槽位；调用方需要先持有 slot.mu，保证同一 slot 的 channel 创建与使用串行。
//
// 设计思路：
// 发布 channel 绑定在 slot 上而不是每次发布临时创建，减少 broker channel 创建成本。
// NotifyReturn 保持注册以覆盖 Confirm=true 的 mandatory return 检测；Confirm=false 是 best-effort
// 快速发布模式，发布成功后不会读取 return 流。
// Confirm 开启时必须先调用 channel.Confirm，再注册 NotifyPublish；如果中途失败，立即关闭半初始化 channel，
// 避免把不可用 channel 留在池里。
func (c *Connection) ensurePublishSlotChannel(slot *rabbitMQPublishSlot) (AMQPChannel, <-chan amqp.Confirmation, <-chan amqp.Return, error) {
	return c.ensurePublishSlotChannelForBulk(slot, 1)
}

// ensurePublishSlotChannelForBulk 返回可承载当前批次 confirm/return 的发布 channel。
//
// 需求背景：RabbitMQ publisher confirm 的 NotifyPublish channel 如果容量小于一次连续发布的消息数，
// AMQP reader 可能在调用方开始等待 confirm 前被写满并阻塞。bulk 发布必须让 confirm/return buffer
// 至少覆盖本批数量，或重建当前 slot 的 channel。
func (c *Connection) ensurePublishSlotChannelForBulk(slot *rabbitMQPublishSlot, batchSize int) (AMQPChannel, <-chan amqp.Confirmation, <-chan amqp.Return, error) {
	if slot == nil {
		return nil, nil, nil, ErrConnectionClosed
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if slot.channel != nil && (!c.options.Confirm || (cap(slot.confirms) >= batchSize && cap(slot.returns) >= batchSize)) {
		return slot.channel, slot.confirms, slot.returns, nil
	}
	if slot.channel != nil {
		_ = resetPublishSlotLocked(slot)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if slot.channel != nil && (!c.options.Confirm || (cap(slot.confirms) >= batchSize && cap(slot.returns) >= batchSize)) {
		return slot.channel, slot.confirms, slot.returns, nil
	}
	if slot.channel != nil {
		_ = resetPublishSlotLocked(slot)
	}
	if c.closed || c.amqpConnection == nil || c.amqpConnection.IsClosed() {
		return nil, nil, nil, ErrConnectionClosed
	}
	channel, err := c.amqpConnection.Channel()
	if err != nil {
		return nil, nil, nil, err
	}
	// return 流与 AMQP channel 生命周期绑定；重连或 slot reset 后必须随新 channel 重新注册。
	slot.returns = channel.NotifyReturn(make(chan amqp.Return, batchSize))
	if c.options.Confirm {
		if err := channel.Confirm(false); err != nil {
			_ = channel.Close()
			return nil, nil, nil, err
		}
		slot.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, batchSize))
	}
	slot.channel = channel
	return slot.channel, slot.confirms, slot.returns, nil
}

// resetPublishSlotLocked 清空一个失败或即将关闭的发布 slot。
//
// 调用约束：
// 调用方必须已经持有 slot.mu。失败只影响当前 slot，不会清空整个发布池，也不会主动触发连接重连；
// 后续发布再次命中该 slot 时会重新创建 channel 和 confirm 流。
func resetPublishSlotLocked(slot *rabbitMQPublishSlot) error {
	if slot == nil {
		return nil
	}
	channel := slot.channel
	slot.channel = nil
	slot.confirms = nil
	slot.returns = nil
	return closeRabbitMQChannel(channel)
}

// resetTopologyChannel 丢弃当前缓存的 topology channel。
//
// 设计原因：
// RabbitMQ 会把部分 declare/passive/inspect/purge 失败作为 channel-level exception，例如 passive
// 检查缺失资源或声明参数不一致时会关闭当前 AMQP channel。丢弃缓存 channel 可以避免后续 topology
// 操作继续复用已被 broker 关闭的 channel。
func (c *Connection) resetTopologyChannel() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	channel := c.topologyChannel
	c.topologyChannel = nil
	c.mu.Unlock()
	return closeRabbitMQChannel(channel)
}

// closeRabbitMQPublishSlots 关闭一组发布 slot，并返回第一个关闭错误。
//
// 需求背景：
// 主动 Close、运行期断线和重连安装新 connection 时都需要丢弃旧 connection 上的发布 channel。
// 这里逐个获取 slot.mu，保证不会与正在进行的 PublishWithContext/confirm 等待并发关闭同一 channel。
func closeRabbitMQPublishSlots(slots []*rabbitMQPublishSlot) error {
	var first error
	for _, slot := range slots {
		if slot == nil {
			continue
		}
		slot.mu.Lock()
		if err := resetPublishSlotLocked(slot); err != nil && first == nil {
			first = err
		}
		slot.mu.Unlock()
	}
	return first
}

// ensureConsumerChannel 返回 worker 消费用 channel，并在首次创建时配置 QoS。
func (c *Connection) ensureConsumerChannel() (AMQPChannel, error) {
	c.mu.RLock()
	channel := c.consumerChannel
	c.mu.RUnlock()
	if channel != nil {
		return channel, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumerChannel != nil {
		return c.consumerChannel, nil
	}
	if c.closed || c.amqpConnection == nil || c.amqpConnection.IsClosed() {
		return nil, ErrConnectionClosed
	}
	channel, err := c.amqpConnection.Channel()
	if err != nil {
		return nil, err
	}
	if err := channel.Qos(c.options.Prefetch, 0, false); err != nil {
		_ = channel.Close()
		return nil, err
	}
	c.consumerChannel = channel
	return c.consumerChannel, nil
}

// ensureTopologyChannel 返回声明、检查和 purge 操作使用的专用 AMQP channel。
func (c *Connection) ensureTopologyChannel() (AMQPChannel, error) {
	c.mu.RLock()
	channel := c.topologyChannel
	c.mu.RUnlock()
	if channel != nil {
		return channel, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.topologyChannel != nil {
		return c.topologyChannel, nil
	}
	if c.closed || c.amqpConnection == nil || c.amqpConnection.IsClosed() {
		return nil, ErrConnectionClosed
	}
	channel, err := c.amqpConnection.Channel()
	if err != nil {
		return nil, err
	}
	c.topologyChannel = channel
	return c.topologyChannel, nil
}
