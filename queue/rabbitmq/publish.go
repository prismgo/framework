package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
	queueevents "github.com/prismgo/framework/queue/internal/events"
	"github.com/prismgo/framework/queue/payload"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (c *Connection) Push(ctx context.Context, queue string, env *payload.Envelope, delay time.Duration) error {
	return c.push(ctx, queue, env, delay, queueevents.EventPublishFailed)
}

type rabbitMQBulkPublishItem struct {
	Queue    string
	Envelope *payload.Envelope
}

// PushBulk 批量发布同一 ready queue 的 payload。
//
// 需求背景：Laravel Batch::add 会调用 queue connection 的 bulk。RabbitMQ 单条 publisher
// confirm RTT 会限制吞吐，所以同一 queue、无 delay 的 ready payload 在 transport 层复用一次
// topology、一个 publish slot 和一组足够大的 confirm/return buffer 连续发布。
func (c *Connection) PushBulk(ctx context.Context, queue string, items []rabbitMQBulkPublishItem) (queuecontract.BulkResult, error) {
	if len(items) == 0 {
		return queuecontract.BulkResult{}, nil
	}
	if c == nil {
		return queuecontract.BulkResult{}, ErrConnectionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	pushCtx := ctx
	cancel := func() {}
	if c.options.PublishTimeout > 0 {
		pushCtx, cancel = context.WithTimeout(ctx, c.options.PublishTimeout)
	}
	defer cancel()
	queue = normalizeRabbitMQQueueName(queue, items[0].Envelope)

	for {
		if err := c.waitReady(pushCtx); err != nil {
			wrapped := err
			if errors.Is(err, context.DeadlineExceeded) {
				wrapped = fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, err)
			}
			c.emitInfrastructureEvent(ctx, queueevents.EventPublishFailed, queue, c.options.Exchange, 0, wrapped)
			return queuecontract.BulkResult{}, wrapped
		}
		exchange, routingKey, err := c.ensurePublishTopology(queue, 0)
		if err != nil {
			if errors.Is(err, ErrConnectionClosed) && pushCtx.Err() == nil {
				continue
			}
			if exchange == "" {
				exchange = c.options.Exchange
			}
			c.emitInfrastructureEvent(ctx, queueevents.EventPublishFailed, queue, exchange, 0, err)
			return queuecontract.BulkResult{}, err
		}
		result, err := c.publishBulkWithConfirm(pushCtx, queue, exchange, routingKey, items)
		if err != nil {
			if errors.Is(err, ErrConnectionClosed) && pushCtx.Err() == nil {
				continue
			}
			c.emitInfrastructureEvent(ctx, queueevents.EventPublishFailed, queue, exchange, 0, err)
			return result, err
		}
		return result, nil
	}
}

// push 是 RabbitMQ 发布路径的内部实现。
//
// 需求背景：
// 普通 Dispatch/Push 失败应继续发出 queue.publish_failed；Release 在原 delivery 已经 ack 后的
// 替换发布失败，则需要发出 queue.release_republish_failed，避免同一个可能丢任务窗口产生两类告警。
//
// 设计思路：
// 复用同一套 Payload Encoding、delay topology、PublishWithContext 和 publisher confirm 逻辑，只通过
// failureEvent 参数切换失败事件名称。这样普通发布和 release 替换发布的成功语义保持一致，失败可观测
// 语义由调用方明确传入。
//
// 参数说明：
// ctx 为调用链上下文；queue 为目标业务队列；env 为要发布的任务信封；delay 为延迟投递时间；
// failureEvent 为本次发布失败时要发出的基础设施事件名。
func (c *Connection) push(ctx context.Context, queue string, env *payload.Envelope, delay time.Duration, failureEvent string) error {
	if c == nil {
		return ErrConnectionClosed
	}
	if env == nil {
		err := fmt.Errorf("queue: envelope is nil")
		c.emitInfrastructureEvent(ctx, failureEvent, queue, c.options.Exchange, 0, err)
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	pushCtx := ctx
	cancel := func() {}
	if c != nil && c.options.PublishTimeout > 0 {
		pushCtx, cancel = context.WithTimeout(ctx, c.options.PublishTimeout)
	}
	defer cancel()
	queue = normalizeRabbitMQQueueName(queue, env)
	codec := c.codecOrDefault()
	body, err := codec.Marshal(env)
	if err != nil {
		c.emitInfrastructureEvent(ctx, failureEvent, queue, c.options.Exchange, 0, err)
		return err
	}
	message := amqp.Publishing{
		Body:        body,
		ContentType: rabbitMQContentType(codec.Name()),
		Headers: amqp.Table{
			rabbitMQHeaderQueue:   queue,
			rabbitMQHeaderJobID:   env.ID,
			rabbitMQHeaderJobName: env.Name,
		},
		Timestamp: time.Now(),
	}
	if delay > 0 && c.normalizedDelayMode() == rabbitMQDelayModePlugin {
		message.Headers[rabbitMQHeaderDelay] = int32(delay.Milliseconds())
	}
	if c.options.MessagePersistent {
		message.DeliveryMode = amqp.Persistent
	}

	for {
		if err := c.waitReady(pushCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				wrapped := fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, err)
				c.emitInfrastructureEvent(ctx, failureEvent, queue, c.options.Exchange, 0, wrapped)
				return wrapped
			}
			c.emitInfrastructureEvent(ctx, failureEvent, queue, c.options.Exchange, 0, err)
			return err
		}
		targetExchange, routingKey, err := c.ensurePublishTopology(queue, delay)
		if err != nil {
			if errors.Is(err, ErrConnectionClosed) && pushCtx.Err() == nil {
				continue
			}
			if targetExchange == "" {
				targetExchange = c.options.Exchange
			}
			c.emitInfrastructureEvent(ctx, failureEvent, queue, targetExchange, 0, err)
			return err
		}
		err = c.publishWithConfirm(pushCtx, targetExchange, routingKey, message)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrConnectionClosed) && pushCtx.Err() == nil {
			continue
		}
		c.emitInfrastructureEvent(ctx, failureEvent, queue, targetExchange, 0, err)
		return err
	}
}

// publishWithConfirm 统一 RabbitMQ 发布与 publisher confirm 等待流程。
//
// 需求背景：
// 普通任务投递、delay 投递、release 替换投递和 restart 信号投递在 Confirm=true 时必须避免
// “broker confirm 成功但消息未路由”的假成功，因此发布统一使用 mandatory=true 并观察 NotifyReturn。
// Confirm=false 是性能/兼容模式，只要求 PublishWithContext 成功，不再等待 confirm 或 basic.return。
//
// 并发边界：
// RabbitMQ 的 confirm/return 流都与 publish channel 成对使用，多 goroutine 共享时必须串行等待。
// 尤其 amqp.Return 没有 delivery tag，必须依赖 slot.mu 保证同一 slot 内只有一次发布处于 in-flight 状态。
func (c *Connection) publishWithConfirm(ctx context.Context, exchange, routingKey string, message amqp.Publishing) error {
	slot, err := c.nextPublishSlot()
	if err != nil {
		return err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	// slot 级锁覆盖发布和 confirm 等待全过程，确保当前 goroutine 读取到的确认结果只属于本次发布。
	channel, confirms, returns, err := c.ensurePublishSlotChannel(slot)
	if err != nil {
		return err
	}
	if err := channel.PublishWithContext(ctx, exchange, routingKey, true, false, message); err != nil {
		// 发布失败通常表示该 channel 已不可继续信任，只重置当前 slot，其他发布 slot 保持可用。
		_ = resetPublishSlotLocked(slot)
		// PublishWithContext 本身可能先于 confirm 等待命中发布窗口；对外仍保持稳定的 RabbitMQ 发布超时错误。
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, err)
		}
		return err
	}
	if !c.options.Confirm {
		// Confirm=false 的公开契约是 best-effort 快速发布：PublishWithContext 成功即返回。
		// 这里故意不读取 returns，否则会把该模式重新变成等待 PublishTimeout 的慢路径。
		return nil
	}
	waitCtx, cancel := c.publishWaitContext(ctx)
	defer cancel()
	// 等待循环优先轮询 return，避免 confirm ack 已到达时忽略同一发布已经到达的 unrouted return。
	for {
		if err := c.pollPublishReturn(slot, returns, exchange, routingKey); err != nil {
			return err
		}
		select {
		case returned, ok := <-returns:
			if !ok {
				_ = resetPublishSlotLocked(slot)
				return ErrRabbitMQPublishConfirmClosed
			}
			_ = resetPublishSlotLocked(slot)
			return rabbitMQPublishUnroutedError(returned, exchange, routingKey)
		case confirmation, ok := <-confirms:
			if !ok {
				// confirm 流关闭说明 broker 或 channel 已断开，清空 slot 后让下一次发布重新建 channel。
				_ = resetPublishSlotLocked(slot)
				return ErrRabbitMQPublishConfirmClosed
			}
			if !confirmation.Ack {
				// broker 明确 nack 当前消息时丢弃该 slot，避免后续复用可能处于异常状态的发布 channel。
				_ = resetPublishSlotLocked(slot)
				return ErrRabbitMQPublishNacked
			}
			return c.pollPublishReturn(slot, returns, exchange, routingKey)
		case <-waitCtx.Done():
			// 超时后无法判断 broker 后续是否还会返回 ack/nack，关闭该 slot 可以避免残留确认污染下一次发布。
			_ = resetPublishSlotLocked(slot)
			return fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, waitCtx.Err())
		}
	}
}

// publishBulkWithConfirm 在同一个 publish slot 内连续发布并等待本批 confirm。
//
// 参数 queue 是业务队列名；exchange/routingKey 是本批 ready payload 共享的 AMQP 目标；
// items 是已解码 envelope 列表。调用方保证这些 item 属于同一 ready queue，delay 不进入该路径。
func (c *Connection) publishBulkWithConfirm(ctx context.Context, queue string, exchange string, routingKey string, items []rabbitMQBulkPublishItem) (queuecontract.BulkResult, error) {
	if c.options.Confirm {
		return c.publishBulkWithPublisherConfirm(ctx, queue, exchange, routingKey, items)
	}
	slot, err := c.nextPublishSlot()
	if err != nil {
		return queuecontract.BulkResult{}, err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	channel, _, _, err := c.ensurePublishSlotChannelForBulk(slot, len(items))
	if err != nil {
		return queuecontract.BulkResult{}, err
	}
	published := 0
	for _, item := range items {
		message, err := c.bulkPublishing(queue, item.Envelope)
		if err != nil {
			return queuecontract.BulkResult{Accepted: published}, err
		}
		if err := channel.PublishWithContext(ctx, exchange, routingKey, true, false, message); err != nil {
			_ = resetPublishSlotLocked(slot)
			if errors.Is(err, context.DeadlineExceeded) {
				return queuecontract.BulkResult{Accepted: published}, fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, err)
			}
			return queuecontract.BulkResult{Accepted: published}, err
		}
		published++
	}
	return queuecontract.BulkResult{Accepted: published}, nil
}

func (c *Connection) publishBulkWithPublisherConfirm(ctx context.Context, queue string, exchange string, routingKey string, items []rabbitMQBulkPublishItem) (queuecontract.BulkResult, error) {
	messages := make([]amqp.Publishing, 0, len(items))
	for _, item := range items {
		message, err := c.bulkPublishing(queue, item.Envelope)
		if err != nil {
			// Confirm=true 只有 broker ack 后才算 accepted；预构建失败时不发布本批任何消息。
			return queuecontract.BulkResult{}, err
		}
		messages = append(messages, message)
	}
	slot, err := c.nextPublishSlot()
	if err != nil {
		return queuecontract.BulkResult{}, err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	channel, confirms, returns, err := c.ensurePublishSlotChannelForBulk(slot, len(messages))
	if err != nil {
		return queuecontract.BulkResult{}, err
	}
	for _, message := range messages {
		if err := channel.PublishWithContext(ctx, exchange, routingKey, true, false, message); err != nil {
			_ = resetPublishSlotLocked(slot)
			// 已 publish 但未 confirm 的消息不能计入 accepted，避免调用方把不确定状态当成功。
			if errors.Is(err, context.DeadlineExceeded) {
				return queuecontract.BulkResult{}, fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, err)
			}
			return queuecontract.BulkResult{}, err
		}
	}
	return c.waitBulkConfirms(ctx, slot, confirms, returns, exchange, routingKey, len(messages))
}

func (c *Connection) bulkPublishing(queue string, env *payload.Envelope) (amqp.Publishing, error) {
	if env == nil {
		return amqp.Publishing{}, fmt.Errorf("queue: envelope is nil")
	}
	codec := c.codecOrDefault()
	body, err := codec.Marshal(env)
	if err != nil {
		return amqp.Publishing{}, err
	}
	message := amqp.Publishing{
		Body:        body,
		ContentType: rabbitMQContentType(codec.Name()),
		Headers: amqp.Table{
			rabbitMQHeaderQueue:   queue,
			rabbitMQHeaderJobID:   env.ID,
			rabbitMQHeaderJobName: env.Name,
		},
		Timestamp: time.Now(),
	}
	if c.options.MessagePersistent {
		message.DeliveryMode = amqp.Persistent
	}
	return message, nil
}

func (c *Connection) waitBulkConfirms(ctx context.Context, slot *rabbitMQPublishSlot, confirms <-chan amqp.Confirmation, returns <-chan amqp.Return, exchange string, routingKey string, published int) (queuecontract.BulkResult, error) {
	waitCtx, cancel := c.publishWaitContext(ctx)
	defer cancel()
	accepted := 0
	for i := 0; i < published; i++ {
		if err := c.pollPublishReturn(slot, returns, exchange, routingKey); err != nil {
			return queuecontract.BulkResult{Accepted: accepted}, err
		}
		select {
		case returned, ok := <-returns:
			if !ok {
				_ = resetPublishSlotLocked(slot)
				return queuecontract.BulkResult{Accepted: accepted}, ErrRabbitMQPublishConfirmClosed
			}
			_ = resetPublishSlotLocked(slot)
			return queuecontract.BulkResult{Accepted: accepted}, rabbitMQPublishUnroutedError(returned, exchange, routingKey)
		case confirmation, ok := <-confirms:
			if !ok {
				_ = resetPublishSlotLocked(slot)
				return queuecontract.BulkResult{Accepted: accepted}, ErrRabbitMQPublishConfirmClosed
			}
			if !confirmation.Ack {
				_ = resetPublishSlotLocked(slot)
				return queuecontract.BulkResult{Accepted: accepted}, ErrRabbitMQPublishNacked
			}
			accepted++
		case <-waitCtx.Done():
			_ = resetPublishSlotLocked(slot)
			return queuecontract.BulkResult{Accepted: accepted}, fmt.Errorf("%w: %w", ErrRabbitMQPublishTimeout, waitCtx.Err())
		}
	}
	if err := c.pollPublishReturn(slot, returns, exchange, routingKey); err != nil {
		return queuecontract.BulkResult{Accepted: accepted}, err
	}
	return queuecontract.BulkResult{Accepted: accepted}, nil
}

// publishWaitContext 返回发布边界使用的等待上下文。
//
// 设计思路：外层 Push/RequestRestart 通常已经带 PublishTimeout；若调用方直接传入无 deadline 的上下文，
// 这里补齐同一个超时窗口，避免 confirm 或 return 等待无限阻塞。
func (c *Connection) publishWaitContext(ctx context.Context) (context.Context, func()) {
	if _, ok := ctx.Deadline(); !ok && c.options.PublishTimeout > 0 {
		return context.WithTimeout(ctx, c.options.PublishTimeout)
	}
	return ctx, func() {}
}

// pollPublishReturn 非阻塞检查当前 slot 是否已经收到 basic.return。
//
// 需求背景：RabbitMQ 对 mandatory unroutable 消息通常先发送 basic.return，再发送 confirm ack；
// 但 Go 调度可能让两个事件在同一次 select 前都已可读，所以 confirm ack 前后都要显式检查 return。
//
// 并发边界：调用方必须持有 slot.mu，保证 resetPublishSlotLocked 与同一 slot 上的
// 发布和 confirm 等待串行执行，其他 goroutine 不会在 slot 重置期间使用已重置的 channel。
func (c *Connection) pollPublishReturn(slot *rabbitMQPublishSlot, returns <-chan amqp.Return, exchange, routingKey string) error {
	if returns == nil {
		return nil
	}
	select {
	case returned, ok := <-returns:
		if !ok {
			_ = resetPublishSlotLocked(slot)
			return ErrRabbitMQPublishConfirmClosed
		}
		_ = resetPublishSlotLocked(slot)
		return rabbitMQPublishUnroutedError(returned, exchange, routingKey)
	default:
		return nil
	}
}

// rabbitMQPublishUnroutedError 把 broker basic.return 转换成稳定 sentinel 错误。
//
// 安全边界：错误只包含 exchange、routing key 和 broker reply 元数据，不包含 message body、headers 或 payload。
func rabbitMQPublishUnroutedError(returned amqp.Return, exchange, routingKey string) error {
	if returned.Exchange != "" {
		exchange = returned.Exchange
	}
	if returned.RoutingKey != "" {
		routingKey = returned.RoutingKey
	}
	return fmt.Errorf("%w: exchange %q routing_key %q reply_code %d reply_text %q",
		ErrRabbitMQPublishUnrouted,
		exchange,
		routingKey,
		returned.ReplyCode,
		returned.ReplyText,
	)
}

func rabbitMQContentType(name string) string {
	if name == encodingpkg.NameJSON {
		return rabbitMQContentTypeJSON
	}
	return rabbitMQContentTypeMsgpack
}
