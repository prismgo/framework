package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	queueevents "github.com/prismgo/framework/queue/internal/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

// 本文件负责 RabbitMQ worker consumer 的创建、缓存和 delivery 接收。
// Pop 的外层负责等待重连和超时边界，这里只处理活跃 consumer channel 上的取消息逻辑。

// ensureQueueConsumer 为指定队列创建长生命周期 push consumer。
//
// 需求背景：
// AMQP consumer 是当前 connection 上的真实 broker 资源；Consumer Intent 则由 worker lease
// 持有并决定 reconnect 是否恢复。这里只负责在 Pop 路径按需创建真实 consumer，不再把
// publish-only 或普通 Pop 历史偷偷提升为长期恢复意图。
//
// 设计思路：
// 为每个 queue 生成连接内唯一 consumer tag 并保存，最后一个 worker lease 释放时才能执行
// basic.cancel。consumers 只保存当前 connection 上的 delivery channel，断线时会被清空。
func (c *Connection) ensureQueueConsumer(queue string) error {
	queue = normalizeRabbitMQQueueName(queue, nil)
	if err := c.ensureQueueTopology(queue); err != nil {
		return err
	}

	c.mu.RLock()
	_, exists := c.consumers[queue]
	c.mu.RUnlock()
	if exists {
		return nil
	}

	c.consumerInitMu.Lock()
	defer c.consumerInitMu.Unlock()
	c.mu.RLock()
	_, exists = c.consumers[queue]
	c.mu.RUnlock()
	if exists {
		return nil
	}

	// worker 需要的是长生命周期 push consumer，
	// 不是每次 Pop 都新建一个 basic.get 风格的拉取请求。
	channel, err := c.ensureConsumerChannel()
	if err != nil {
		return err
	}
	tag := c.nextConsumerTag(queue)
	deliveries, err := channel.Consume(queue, tag, false, false, false, false, nil)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.consumers == nil {
		c.consumers = make(map[string]<-chan amqp.Delivery)
	}
	if c.consumerTags == nil {
		c.consumerTags = make(map[string]string)
	}
	c.consumers[queue] = deliveries
	c.consumerTags[queue] = tag
	c.mu.Unlock()
	c.emitInfrastructureEvent(nil, queueevents.EventConsumerStarted, queue, c.options.Exchange, 0, nil)
	return nil
}

// AcquireConsumerIntent 记录本 worker 计划消费的 RabbitMQ queue，并返回幂等 release 函数。
//
// 需求背景：
// RabbitMQ reconnect 只应该恢复仍有 worker 存活的消费意图，不能把 publish、Size、Clear 或历史
// delay topology 当作永久注册表。Worker 在 Work 生命周期开始时调用该可选接口，退出时释放。
//
// 设计思路：
// 获取 lease 只更新本地引用计数和 activeConsumers，不访问 broker、不声明 topology、不创建 AMQP
// consumer。真实 consumer 仍由 Pop 的 ensureQueueConsumer 创建；release 时最后一个引用负责
// basic.cancel 当前 AMQP consumer。
//
// 参数说明：
// queues 是本次 worker 的目标队列列表；空值按 RabbitMQ 默认队列 default 处理，重复队列会去重。
func (c *Connection) AcquireConsumerIntent(queues []string) (func() error, error) {
	if c == nil {
		return nil, ErrConnectionClosed
	}
	normalized := uniqueRabbitMQQueues(queues)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrConnectionClosed
	}
	if c.activeConsumers == nil {
		c.activeConsumers = make(map[string]struct{})
	}
	if c.consumerRefs == nil {
		c.consumerRefs = make(map[string]int)
	}
	for _, queue := range normalized {
		c.consumerRefs[queue]++
		c.activeConsumers[queue] = struct{}{}
	}
	c.mu.Unlock()

	var released bool
	return func() error {
		if released {
			return nil
		}
		released = true
		return c.releaseConsumerIntents(normalized)
	}, nil
}

// uniqueRabbitMQQueues 规范化并去重 worker 传入的 RabbitMQ 队列列表。
//
// 需求背景：
// Consumer Intent lease 是按 queue 做引用计数的，同一个 worker 如果传入重复队列，
// 只能占用一次引用；否则 release 时会出现引用计数和真实 worker 生命周期不一致的问题。
//
// 设计思路：
// 先复用 normalizeRabbitMQQueues 处理空队列、空白名称和默认队列语义，再按首次出现顺序去重。
// 保留顺序是为了不改变 worker 多队列消费时“前序队列尽力优先”的行为。
//
// 参数说明：
// queues 是调用方传入的原始队列名称列表。
//
// 返回值：
// 返回规范化、去重后的队列名称列表；当调用方未指定队列时包含默认队列。
func uniqueRabbitMQQueues(queues []string) []string {
	return normalizeRabbitMQQueues(queues)
}

// releaseConsumerIntents 释放一次 worker lease 持有的所有队列 Consumer Intent。
//
// 需求背景：
// 一个 worker 可以同时监听多个队列，其中某个队列 basic.cancel 失败时，不能阻断其它队列释放。
// 否则多队列 worker 退出会让未失败队列继续残留在 reconnect recovery set 中。
//
// 设计思路：
// 逐个队列调用 releaseConsumerIntent，并收集所有释放错误。这里不做提前返回，
// 由 errors.Join 把多个 cancel 失败合并给 Worker.Work 的 defer 路径。
//
// 参数说明：
// queues 是 AcquireConsumerIntent 已经规范化和去重后的队列列表。
//
// 返回值：
// 所有队列释放成功时返回 nil；一个或多个队列释放失败时返回合并后的错误。
func (c *Connection) releaseConsumerIntents(queues []string) error {
	var errs []error
	for _, queue := range queues {
		if err := c.releaseConsumerIntent(queue); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// releaseConsumerIntent 释放单个队列的一次 Consumer Intent 引用。
//
// 需求背景：
// Consumer Intent 代表仍需在 reconnect 后恢复的 live worker 消费意图。最后一个引用释放时，
// 当前 AMQP connection 上的 broker consumer 也必须通过 basic.cancel 主动停止。
//
// 设计思路：
// 先在锁内用 consumerStopSnapshot 判断当前释放是否需要 cancel。多 worker 共享同一队列时，
// 非最后一个引用只递减计数；断线、重连中、已关闭或尚未创建真实 consumer 时，只清理本地 intent。
// 如果需要 cancel，则在锁外调用 AMQP channel，避免把网络调用放进共享状态锁。
//
// 参数说明：
// queue 是要释放引用的队列名称，函数内部会再次规范化以保护直接调用路径。
//
// 返回值：
// cancel 成功或无需 cancel 时返回 nil；cancel 失败时返回包装后的错误，并触发
// queue.consumer_stop_failed 事件且保留该队列 intent。
func (c *Connection) releaseConsumerIntent(queue string) error {
	queue = normalizeRabbitMQQueueName(queue, nil)
	channel, tag, shouldCancel, shouldCleanup, hadConsumer := c.consumerStopSnapshot(queue)
	if !shouldCancel && !shouldCleanup {
		return nil
	}
	if !shouldCancel {
		c.finishConsumerIntentRelease(queue, "", nil, hadConsumer)
		return nil
	}
	if err := channel.Cancel(tag, c.options.NoWait); err != nil {
		wrapped := fmt.Errorf("queue: rabbitmq stop consumer %q: %w", queue, err)
		c.emitInfrastructureEvent(nil, queueevents.EventConsumerStopFailed, queue, c.options.Exchange, 0, wrapped)
		return wrapped
	}
	c.finishConsumerIntentRelease(queue, tag, nil, true)
	return nil
}

// consumerStopSnapshot 在锁内计算释放单个队列 Consumer Intent 所需的停止决策。
//
// 需求背景：
// release 路径既要维护本地引用计数，又要避免在断线或重连状态下误报 cancel 失败。
// 真实 basic.cancel 不能在锁内执行，因此需要先提取一个稳定快照。
//
// 设计思路：
// 当引用数大于 1 时只递减引用并返回无需清理；当释放最后一个引用时，根据当前 connection、
// consumer channel 和 consumer tag 状态判断是否需要向 broker 发送 basic.cancel。
// 断线、重连中、关闭或尚未创建 delivery channel 时，旧 broker consumer 已经不可继续投递，
// 所以返回“只清理本地 intent”的决策。
//
// 参数说明：
// queue 是已经规范化的队列名称。
//
// 返回值：
// 第一个返回值是需要执行 cancel 的 AMQP channel；第二个返回值是 consumer tag；
// shouldCancel 表示是否必须调用 basic.cancel；shouldCleanup 表示是否需要清理本地 intent；
// hadConsumer 表示本地是否曾经存在真实 delivery channel 和 consumer tag。
func (c *Connection) consumerStopSnapshot(queue string) (AMQPChannel, string, bool, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	refs := c.consumerRefs[queue]
	if refs <= 0 {
		return nil, "", false, false, false
	}
	if refs > 1 {
		c.consumerRefs[queue] = refs - 1
		return nil, "", false, false, false
	}
	deliveries := c.consumers[queue]
	tag := c.consumerTags[queue]
	hadConsumer := deliveries != nil && tag != ""
	disconnected := c.closed || c.reconnecting || c.amqpConnection == nil || c.amqpConnection.IsClosed() || c.consumerChannel == nil
	if disconnected || !hadConsumer {
		return nil, tag, false, true, hadConsumer
	}
	return c.consumerChannel, tag, true, true, hadConsumer
}

// finishConsumerIntentRelease 在 cancel 成功或无需 cancel 后完成本地 Consumer Intent 清理。
//
// 需求背景：
// 本地 activeConsumers 和 consumerRefs 是 reconnect recovery set 的来源，必须只在确认释放成功
// 或确认当前连接不需要 cancel 时删除；cancel 失败时不能调用本函数，否则会丢失 live intent。
//
// 设计思路：
// 在锁内删除最后一个引用对应的 active intent、delivery channel 和 consumer tag。
// tag 为空表示没有真实 AMQP consumer，需要按本地清理路径删除；tag 不为空时只清理匹配的
// consumer tag，避免清理掉并发重建后的新 consumer。
//
// 参数说明：
// queue 是要清理的队列名称；tag 是本次确认停止的 consumer tag；stopped 表示是否已经停止过
// 真实 AMQP consumer，并决定是否发送 queue.consumer_stopped 事件。
func (c *Connection) finishConsumerIntentRelease(queue, tag string, _ error, stopped bool) {
	c.mu.Lock()
	refs := c.consumerRefs[queue]
	if refs <= 1 {
		delete(c.consumerRefs, queue)
		delete(c.activeConsumers, queue)
	} else {
		c.consumerRefs[queue] = refs - 1
	}
	if tag == "" || c.consumerTags[queue] == tag || refs <= 1 {
		delete(c.consumers, queue)
		delete(c.consumerTags, queue)
	}
	c.mu.Unlock()
	if stopped {
		c.emitInfrastructureEvent(nil, queueevents.EventConsumerStopped, queue, c.options.Exchange, 0, nil)
	}
}

// nextConsumerTag 为当前 RabbitMQ connection 生成可追踪的 AMQP consumer tag。
//
// 需求背景：
// basic.cancel 必须使用创建 consumer 时的 tag。旧实现依赖空 tag 让 broker 自动生成，
// 本地无法可靠保存和取消指定 consumer。
//
// 设计思路：
// 使用 connection 名称、队列名称和单调递增序号组成 tag，保证同一进程内当前 connection 的
// consumer tag 非空且可区分。tag 不携带 URL、密码、payload 或 headers，避免事件和错误泄露敏感信息。
//
// 参数说明：
// queue 是正在创建 AMQP consumer 的队列名称。
//
// 返回值：
// 返回传给 AMQP Consume 的 consumer tag。
func (c *Connection) nextConsumerTag(queue string) string {
	seq := c.consumerTagNext.Add(1)
	return fmt.Sprintf("prismgo.%s.%s.%d", c.name, queue, seq)
}

// receiveDelivery 实现 RabbitMQ 的 block_for 语义。
//
// 需求背景：
// RabbitMQ 使用 push consumer 投递消息，但 worker 的 Pop 仍需要表现为支持 block_for 的拉取接口。
// 这里把多个 queue 的 delivery channel 统一成一次 Pop 调用的返回结果。
//
// 设计约束：
// 1. BlockFor=0 时必须走非阻塞检查，没有消息直接返回 ErrEmpty。
// 2. BlockFor>0 时最多等待指定时长。
// 3. context 取消优先于等待超时。
//
// 参数说明：
// ctx 控制本次 Pop 等待生命周期；queues 是按优先级排列的队列列表；blockFor 是最长等待时间。
//
// 返回值：
// 返回收到的 AMQP delivery、来源队列名称以及错误；没有消息时返回 ErrEmpty。
func (c *Connection) receiveDelivery(ctx context.Context, queues []string, blockFor time.Duration) (amqp.Delivery, string, error) {
	if blockFor <= 0 {
		for _, queue := range queues {
			deliveries := c.consumerFor(queue)
			if deliveries == nil {
				continue
			}
			select {
			case delivery, ok := <-deliveries:
				if !ok {
					c.dropConsumer(queue)
					continue
				}
				return delivery, queue, nil
			default:
			}
		}
		return amqp.Delivery{}, "", ErrEmpty
	}

	timer := time.NewTimer(blockFor)
	defer timer.Stop()
	for {
		// BlockFor>0 不能直接进入随机 select；先做一次有序非阻塞检查，
		// 让已经 ready 的前序队列优先返回，同时保留后续等待 push delivery 的能力。
		if delivery, queue, ok := c.readyDelivery(queues); ok {
			return delivery, queue, nil
		}
		// 在所有活跃 consumer channel 上做统一 select，
		// 保持“按队列顺序尽力优先”的语义，同时避免退化成轮询 basic.get。
		cases, caseQueues, hasContextCase := c.selectCases(ctx, queues, timer.C)
		if len(caseQueues) == 0 {
			return amqp.Delivery{}, "", ErrEmpty
		}
		chosen, value, ok := reflect.Select(cases)
		switch {
		case chosen < len(caseQueues):
			if !ok {
				c.dropConsumer(caseQueues[chosen])
				continue
			}
			// 安全类型断言：reflect.Select 返回的 value 类型取决于 channel 元素类型，
			// 如果 channel 类型不匹配（例如测试桩或内部重构），硬断言会导致 panic。
			delivery, isDelivery := value.Interface().(amqp.Delivery)
			if !isDelivery {
				c.dropConsumer(caseQueues[chosen])
				continue
			}
			return delivery, caseQueues[chosen], nil
		case hasContextCase && chosen == len(caseQueues):
			return amqp.Delivery{}, "", ctx.Err()
		default:
			return amqp.Delivery{}, "", ErrEmpty
		}
	}
}

// readyDelivery 按调用方队列顺序做一次非阻塞检查，用于维护多队列的尽力优先语义。
//
// 需求背景：
// reflect.Select 会在多个 ready channel 中随机选择。如果直接 select，低优先级队列可能抢先返回，
// 与 worker 多队列“按配置顺序尽力优先”的语义不一致。
//
// 设计思路：
// 在进入阻塞等待前先按 queues 顺序尝试读取一次。关闭的 delivery channel 会触发 dropConsumer，
// 后续 Pop 会在需要时重新创建 consumer。
//
// 参数说明：
// queues 是本次 Pop 的队列优先级顺序。
//
// 返回值：
// 找到 ready delivery 时返回 delivery、队列名称和 true；否则返回零值和 false。
func (c *Connection) readyDelivery(queues []string) (amqp.Delivery, string, bool) {
	for _, queue := range queues {
		deliveries := c.consumerFor(queue)
		if deliveries == nil {
			continue
		}
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				c.dropConsumer(queue)
				continue
			}
			return delivery, queue, true
		default:
		}
	}
	return amqp.Delivery{}, "", false
}

// selectCases 把当前活跃 consumer、context 和超时计时器拼成 reflect.Select 所需的 case 列表。
//
// 需求背景：
// worker 支持动态数量的队列，Go 的普通 select 无法直接处理运行时决定数量的 channel。
//
// 设计思路：
// 只把当前仍有 delivery channel 的队列加入 select case，并保留 caseQueues 与 cases 的索引对应关系。
// context case 和 timer case 追加在队列 case 后面，调用方可通过索引区分返回来源。
//
// 参数说明：
// ctx 提供取消信号；queues 是候选队列列表；timer 是 block_for 超时信号。
//
// 返回值：
// cases 是 reflect.Select 的输入；caseQueues 与前 len(caseQueues) 个 cases 一一对应；
// hasContextCase 表示 cases 中是否包含 context 取消分支。
func (c *Connection) selectCases(ctx context.Context, queues []string, timer <-chan time.Time) ([]reflect.SelectCase, []string, bool) {
	cases := make([]reflect.SelectCase, 0, len(queues)+2)
	caseQueues := make([]string, 0, len(queues))
	for _, queue := range queues {
		deliveries := c.consumerFor(queue)
		if deliveries == nil {
			continue
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(deliveries)})
		caseQueues = append(caseQueues, queue)
	}
	hasContextCase := false
	if done := ctx.Done(); done != nil {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)})
		hasContextCase = true
	}
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timer)})
	return cases, caseQueues, hasContextCase
}

// consumerFor 读取指定队列在当前 connection 上缓存的 delivery channel。
//
// 需求背景：
// Pop 路径需要频繁检查多个队列的活跃 consumer。直接暴露 consumers map 会让调用方绕过锁，
// 并增加断线清理、重连重建时的数据竞争风险。
//
// 设计思路：
// 用读锁读取当前 channel，并对 nil connection 和 nil map 做防御。返回的 channel 只用于接收，
// 调用方不能通过它修改本地 consumer 状态。
//
// 参数说明：
// queue 是要查询的队列名称。
//
// 返回值：
// 返回只读 delivery channel；队列没有活跃 consumer 或 connection 为 nil 时返回 nil。
func (c *Connection) consumerFor(queue string) <-chan amqp.Delivery {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.consumers == nil {
		return nil
	}
	return c.consumers[queue]
}

// dropConsumer 删除当前 connection 上已经失效的队列 delivery channel。
//
// 需求背景：
// AMQP delivery channel 关闭表示当前 broker consumer 已经无法继续投递。本地必须丢弃该 channel，
// 否则后续 Pop 会不断读取关闭 channel，而不是重新创建 consumer。
//
// 设计思路：
// 这里只清理当前 connection 的 consumers 和 consumerTags，不删除 activeConsumers 和 consumerRefs。
// Consumer Intent 的生命周期仍归 worker lease 管理，避免一次 channel 关闭误删仍在运行的 worker intent。
//
// 参数说明：
// queue 是 delivery channel 已关闭的队列名称。
func (c *Connection) dropConsumer(queue string) {
	c.mu.Lock()
	_, existed := c.consumers[queue]
	delete(c.consumers, queue)
	delete(c.consumerTags, queue)
	c.mu.Unlock()
	if existed {
		c.emitInfrastructureEvent(nil, queueevents.EventConsumerStopped, queue, c.options.Exchange, 0, nil)
	}
}
