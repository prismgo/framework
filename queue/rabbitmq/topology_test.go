package rabbitmq

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitMQTopologyTestConnection struct {
	closed       bool
	channels     []AMQPChannel
	channelCalls int
	channelErr   error
	notify       chan *amqp.Error
}

func (c *rabbitMQTopologyTestConnection) Channel() (AMQPChannel, error) {
	if c.channelErr != nil {
		return nil, c.channelErr
	}
	if len(c.channels) == 0 {
		return nil, errors.New("no channel")
	}
	if c.channelCalls >= len(c.channels) {
		c.channelCalls++
		return c.channels[len(c.channels)-1], nil
	}
	channel := c.channels[c.channelCalls]
	c.channelCalls++
	return channel, nil
}

func (c *rabbitMQTopologyTestConnection) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	if c.notify != nil {
		return c.notify
	}
	return receiver
}

func (c *rabbitMQTopologyTestConnection) Close() error {
	c.closed = true
	return nil
}

func (c *rabbitMQTopologyTestConnection) IsClosed() bool {
	return c.closed
}

type rabbitMQTopologyTestChannel struct {
	closeErr            error
	exchangeErr         error
	exchangePassiveErr  error
	queueDeclareErr     error
	queueDeclarePassErr error
	queueInspectErr     error
	queuePurgeErr       error
	bindErr             error
	confirmErr          error
	publishErr          error
	publishErrAt        int
	qosErr              error
	consumeErr          error
	cancelErr           error
	getErr              error
	getDelivery         *amqp.Delivery
	getOK               bool

	closed                  bool
	exchangeDeclares        []string
	exchangePassiveDeclares []string
	queueDeclares           []string
	queueDeclareArgs        []amqp.Table
	queuePassiveDeclares    []string
	queuePassiveArgs        []amqp.Table
	queueInspects           []string
	queuePurges             []string
	queueBinds              []string
	queueBindArgs           []amqp.Table
	consumeCalls            []string
	qosPrefetch             []int
	cancelTags              []string
	confirmed               bool
	returns                 chan amqp.Return
	confirms                chan amqp.Confirmation
	notifyPublishCaps       []int
	confirmAck              bool
	confirmAcks             []bool
	confirmsSent            int
	suppressConfirms        bool
	suppressConfirmsAfter   int
	closeConfirmsOnPublish  bool
	returnOnPublish         *amqp.Return
	returnOnPublishAt       int
	returnOnPublishDelay    time.Duration
	messages                map[string]int
	ready                   map[string][]amqp.Delivery
	consumers               map[string]chan amqp.Delivery
	published               []amqp.Publishing
	publishedKeys           []string
	nextTag                 uint64
	ackCount                int
	nackCount               int
	rejectCount             int
	ackErr                  error
	nackErr                 error
	rejectErr               error
}

func (c *rabbitMQTopologyTestChannel) Close() error {
	c.closed = true
	return c.closeErr
}

func (c *rabbitMQTopologyTestChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	c.exchangeDeclares = append(c.exchangeDeclares, name+":"+kind)
	return c.exchangeErr
}

func (c *rabbitMQTopologyTestChannel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	c.exchangePassiveDeclares = append(c.exchangePassiveDeclares, name+":"+kind)
	return c.exchangePassiveErr
}

func (c *rabbitMQTopologyTestChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.queueDeclares = append(c.queueDeclares, name)
	c.queueDeclareArgs = append(c.queueDeclareArgs, args)
	if c.queueDeclareErr != nil {
		return amqp.Queue{}, c.queueDeclareErr
	}
	if c.messages == nil {
		c.messages = make(map[string]int)
	}
	if c.ready != nil {
		c.messages[name] = len(c.ready[name])
	}
	return amqp.Queue{Name: name, Messages: c.messages[name]}, nil
}

func (c *rabbitMQTopologyTestChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.queuePassiveDeclares = append(c.queuePassiveDeclares, name)
	c.queuePassiveArgs = append(c.queuePassiveArgs, args)
	if c.queueDeclarePassErr != nil {
		return amqp.Queue{}, c.queueDeclarePassErr
	}
	if c.messages == nil {
		c.messages = make(map[string]int)
	}
	if c.ready != nil {
		c.messages[name] = len(c.ready[name])
	}
	return amqp.Queue{Name: name, Messages: c.messages[name]}, nil
}

func (c *rabbitMQTopologyTestChannel) QueueInspect(name string) (amqp.Queue, error) {
	c.queueInspects = append(c.queueInspects, name)
	if c.queueInspectErr != nil {
		return amqp.Queue{}, c.queueInspectErr
	}
	if c.messages == nil {
		c.messages = make(map[string]int)
	}
	if c.ready != nil {
		c.messages[name] = len(c.ready[name])
	}
	return amqp.Queue{Name: name, Messages: c.messages[name]}, nil
}

func (c *rabbitMQTopologyTestChannel) QueuePurge(name string, noWait bool) (int, error) {
	c.queuePurges = append(c.queuePurges, name)
	if c.queuePurgeErr != nil {
		return 0, c.queuePurgeErr
	}
	count := c.messages[name]
	if c.ready != nil {
		count = len(c.ready[name])
		c.ready[name] = nil
	}
	c.messages[name] = 0
	return count, nil
}

func (c *rabbitMQTopologyTestChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	c.queueBinds = append(c.queueBinds, name+":"+key+":"+exchange)
	c.queueBindArgs = append(c.queueBindArgs, args)
	return c.bindErr
}

func (c *rabbitMQTopologyTestChannel) Confirm(noWait bool) error {
	if c.confirmErr != nil {
		return c.confirmErr
	}
	c.confirmed = true
	return nil
}

func (c *rabbitMQTopologyTestChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	c.confirms = confirm
	c.notifyPublishCaps = append(c.notifyPublishCaps, cap(confirm))
	return confirm
}

func (c *rabbitMQTopologyTestChannel) NotifyReturn(receiver chan amqp.Return) chan amqp.Return {
	c.returns = receiver
	return receiver
}

func (c *rabbitMQTopologyTestChannel) PublishWithContext(_ context.Context, _ string, key string, _ bool, _ bool, msg amqp.Publishing) error {
	if c.publishErr != nil && (c.publishErrAt == 0 || len(c.published)+1 == c.publishErrAt) {
		return c.publishErr
	}
	copied := msg
	copied.Body = append([]byte(nil), msg.Body...)
	c.published = append(c.published, copied)
	c.publishedKeys = append(c.publishedKeys, key)
	c.nextTag++
	delivery := amqp.Delivery{
		Body:         append([]byte(nil), msg.Body...),
		DeliveryTag:  c.nextTag,
		Acknowledger: (*rabbitMQTopologyTestAcknowledger)(c),
	}
	if c.consumers != nil {
		if consumer := c.consumers[key]; consumer != nil {
			consumer <- delivery
		}
	}
	if c.ready == nil {
		c.ready = make(map[string][]amqp.Delivery)
	}
	if c.consumers == nil || c.consumers[key] == nil {
		c.ready[key] = append(c.ready[key], delivery)
	}
	if c.returnOnPublish != nil && c.returns != nil && (c.returnOnPublishAt == 0 || len(c.published) == c.returnOnPublishAt) {
		returned := *c.returnOnPublish
		if c.returnOnPublishDelay > 0 {
			go func() {
				time.Sleep(c.returnOnPublishDelay)
				c.returns <- returned
			}()
		} else {
			c.returns <- returned
		}
	}
	if c.closeConfirmsOnPublish && c.confirms != nil {
		close(c.confirms)
		c.confirms = nil
		return nil
	}
	if c.confirms != nil && !c.suppressConfirms && (c.suppressConfirmsAfter == 0 || c.confirmsSent < c.suppressConfirmsAfter) {
		ack := true
		if len(c.confirmAcks) > 0 && c.confirmsSent < len(c.confirmAcks) {
			ack = c.confirmAcks[c.confirmsSent]
		} else if c.confirmAck {
			ack = false
		}
		c.confirmsSent++
		c.confirms <- amqp.Confirmation{Ack: ack}
	}
	return nil
}

func (c *rabbitMQTopologyTestChannel) Get(string, bool) (amqp.Delivery, bool, error) {
	if c.getErr != nil {
		return amqp.Delivery{}, false, c.getErr
	}
	if c.getDelivery != nil {
		return *c.getDelivery, c.getOK, nil
	}
	return amqp.Delivery{}, false, nil
}

func (c *rabbitMQTopologyTestChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	c.qosPrefetch = append(c.qosPrefetch, prefetchCount)
	return c.qosErr
}

func (c *rabbitMQTopologyTestChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	c.consumeCalls = append(c.consumeCalls, queue)
	if c.consumeErr != nil {
		return nil, c.consumeErr
	}
	if c.consumers == nil {
		c.consumers = make(map[string]chan amqp.Delivery)
	}
	deliveries := c.consumers[queue]
	if deliveries == nil {
		deliveries = make(chan amqp.Delivery, 16)
		c.consumers[queue] = deliveries
		for _, delivery := range c.ready[queue] {
			deliveries <- delivery
		}
		delete(c.ready, queue)
	}
	return deliveries, nil
}

func (c *rabbitMQTopologyTestChannel) Cancel(consumer string, noWait bool) error {
	c.cancelTags = append(c.cancelTags, consumer)
	return c.cancelErr
}

type rabbitMQTopologyTestAcknowledger rabbitMQTopologyTestChannel

func (a *rabbitMQTopologyTestAcknowledger) Ack(uint64, bool) error {
	channel := (*rabbitMQTopologyTestChannel)(a)
	channel.ackCount++
	return channel.ackErr
}

func (a *rabbitMQTopologyTestAcknowledger) Nack(uint64, bool, bool) error {
	channel := (*rabbitMQTopologyTestChannel)(a)
	channel.nackCount++
	return channel.nackErr
}

func (a *rabbitMQTopologyTestAcknowledger) Reject(uint64, bool) error {
	channel := (*rabbitMQTopologyTestChannel)(a)
	channel.rejectCount++
	return channel.rejectErr
}

func newRabbitMQTopologyTestConnection(channel *rabbitMQTopologyTestChannel, options Options) *Connection {
	resolved := resolveRabbitMQOptions(options)
	readyCh := make(chan struct{})
	close(readyCh)
	return &Connection{
		name:             "test",
		options:          resolved,
		address:          resolved.connectionURL(),
		amqpConnection:   &rabbitMQTopologyTestConnection{channels: []AMQPChannel{channel}},
		declaredQueues:   make(map[string]struct{}),
		delayedQueues:    make(map[string]struct{}),
		ttlDelayQueues:   make(map[string]struct{}),
		restartQueues:    make(map[string]struct{}),
		consumers:        make(map[string]<-chan amqp.Delivery),
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
}

func TestPushPopDeleteAndReleaseUseAMQPChannels(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})

	env := &payload.Envelope{
		ID:          "job-1",
		Name:        "ExampleJob",
		Queue:       "jobs",
		Payload:     payload.Payload(`{"ok":true}`),
		CreatedAt:   1,
		AvailableAt: 1,
	}
	if err := conn.Push(context.Background(), "jobs", env, 0); err != nil {
		t.Fatalf("push: %v", err)
	}
	if len(channel.published) != 1 || channel.publishedKeys[0] != "jobs" {
		t.Fatalf("published = keys %v messages %d, want one jobs message", channel.publishedKeys, len(channel.published))
	}

	popped, err := conn.Pop(context.Background(), []string{"jobs"}, PopOptions{})
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if popped.ID != env.ID || popped.Attempts != 1 || envelopeDelivery(popped) == nil {
		t.Fatalf("popped = %#v, want job with attempt and delivery state", popped)
	}
	if err := conn.Delete(context.Background(), popped); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if channel.ackCount != 1 {
		t.Fatalf("ack count after delete = %d, want 1", channel.ackCount)
	}

	if err := conn.Push(context.Background(), "jobs", env, 0); err != nil {
		t.Fatalf("second push: %v", err)
	}
	popped, err = conn.Pop(context.Background(), []string{"jobs"}, PopOptions{})
	if err != nil {
		t.Fatalf("second pop: %v", err)
	}
	if err := conn.Release(context.Background(), popped, 0); err != nil {
		t.Fatalf("release: %v", err)
	}
	if channel.ackCount != 2 {
		t.Fatalf("ack count after release = %d, want 2", channel.ackCount)
	}
	if len(channel.published) != 3 {
		t.Fatalf("publish count after release = %d, want 3", len(channel.published))
	}

	none := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), DelayMode: rabbitMQDelayModeNone})
	if err := none.Release(context.Background(), &payload.Envelope{Queue: "jobs"}, time.Second); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("release with delay_mode none err = %v, want ErrUnsupportedOperation", err)
	}
}

func TestPruneTopologyCacheCapacityLockedReturnsWhenAllEntriesProtected(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{TopologyCacheMaxEntries: 1})
	queueA := rabbitMQDeclaredQueueCacheKey("jobs-a")
	queueB := rabbitMQDeclaredQueueCacheKey("jobs-b")
	conn.consumerRefs["jobs-a"] = 1
	conn.consumerRefs["jobs-b"] = 1
	conn.markTopologyUsageLocked(queueA, time.Unix(1, 0))
	conn.markTopologyUsageLocked(queueB, time.Unix(2, 0))

	done := make(chan struct{})
	go func() {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		conn.pruneTopologyCacheCapacityLocked()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("pruneTopologyCacheCapacityLocked blocked when all entries were protected")
	}
	if len(conn.topologyUsage) != 2 {
		t.Fatalf("topology usage size = %d, want protected entries kept", len(conn.topologyUsage))
	}
}

func TestCleanupDeliveryRegistryKeepsUnackedExpiredEntries(t *testing.T) {
	env := &payload.Envelope{ID: "long-running", Queue: "jobs"}
	state := &rabbitMQDeliveryState{}
	rabbitMQDeliveryRegistry = sync.Map{}
	t.Cleanup(func() { rabbitMQDeliveryRegistry = sync.Map{} })
	rabbitMQDeliveryRegistry.Store(env, &rabbitMQDeliveryEntry{
		state:    state,
		storedAt: time.Now().Add(-deliveryRegistryTTL - time.Minute),
	})

	cleanupDeliveryRegistry()

	if envelopeDelivery(env) == nil {
		t.Fatal("cleanupDeliveryRegistry removed unacked delivery entry")
	}
}

func TestRabbitMQQueueBulkUsesSingleBulkPublishSlotWithBufferedConfirms(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	queueConn := &RabbitMQQueue{inner: conn, codec: payload.QueueCodec(nil)}
	bodies := []queuecontract.Payload{}
	for _, id := range []string{"job-1", "job-2", "job-3"} {
		body, err := queueConn.codec.Marshal(&payload.Envelope{
			ID:          id,
			Name:        "ExampleJob",
			Queue:       "jobs",
			Payload:     payload.Payload(`{"ok":true}`),
			CreatedAt:   1,
			AvailableAt: 1,
		})
		if err != nil {
			t.Fatalf("marshal %s: %v", id, err)
		}
		bodies = append(bodies, queuecontract.Payload(body))
	}

	done := make(chan error, 1)
	go func() {
		result, err := queueConn.Bulk(context.Background(), "jobs", bodies)
		if err == nil && result.Accepted != len(bodies) {
			err = fmt.Errorf("accepted = %d, want %d", result.Accepted, len(bodies))
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bulk: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bulk blocked; confirm/return buffer should cover the whole batch")
	}
	if len(channel.published) != 3 {
		t.Fatalf("published = %d, want 3", len(channel.published))
	}
	if len(channel.queueDeclares) != 1 {
		t.Fatalf("queue declares = %v, want one topology declaration for the batch", channel.queueDeclares)
	}
	if channel.confirmsSent != 3 {
		t.Fatalf("confirms sent = %d, want one confirm per published message", channel.confirmsSent)
	}
	if len(channel.notifyPublishCaps) == 0 || channel.notifyPublishCaps[len(channel.notifyPublishCaps)-1] < len(bodies) {
		t.Fatalf("notify publish caps = %v, want capacity at least batch size %d", channel.notifyPublishCaps, len(bodies))
	}
}

func TestRabbitMQQueueBulkEmptyReturnsWithoutPublish(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	queueConn := &RabbitMQQueue{inner: conn, codec: payload.QueueCodec(nil)}

	if result, err := queueConn.Bulk(context.Background(), "jobs", nil); err != nil || result.Accepted != 0 {
		t.Fatalf("empty bulk: %v", err)
	}
	if len(channel.published) != 0 || len(channel.notifyPublishCaps) != 0 {
		t.Fatalf("empty bulk published=%d notify=%v, want no AMQP work", len(channel.published), channel.notifyPublishCaps)
	}
}

func TestRabbitMQBulkConfirmFailuresResetPublishSlot(t *testing.T) {
	cases := []struct {
		name    string
		channel *rabbitMQTopologyTestChannel
		want    error
	}{
		{name: "nack", channel: &rabbitMQTopologyTestChannel{confirmAck: true}, want: ErrRabbitMQPublishNacked},
		{name: "unrouted", channel: &rabbitMQTopologyTestChannel{returnOnPublish: &amqp.Return{Exchange: "ex", RoutingKey: "jobs", ReplyCode: 312, ReplyText: "NO_ROUTE"}}, want: ErrRabbitMQPublishUnrouted},
		{name: "closed", channel: &rabbitMQTopologyTestChannel{closeConfirmsOnPublish: true}, want: ErrRabbitMQPublishConfirmClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := newRabbitMQTopologyTestConnection(tc.channel, Options{Declare: Bool(true), Confirm: Bool(true)})
			result, err := conn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
				{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
				{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-2", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("bulk err = %v, want %v", err, tc.want)
			}
			if result.Accepted != 0 {
				t.Fatalf("accepted = %d, want 0 before first confirm failure", result.Accepted)
			}
			if len(conn.publishSlots) == 0 || conn.publishSlots[0].channel != nil {
				t.Fatal("failed bulk publish should reset current publish slot")
			}
		})
	}
}

func TestRabbitMQBulkConfirmFailureReturnsAcceptedCount(t *testing.T) {
	cases := []struct {
		name    string
		channel *rabbitMQTopologyTestChannel
		want    error
		timeout time.Duration
	}{
		{name: "second-nack", channel: &rabbitMQTopologyTestChannel{confirmAcks: []bool{true, false}}, want: ErrRabbitMQPublishNacked, timeout: time.Millisecond},
		{name: "second-unrouted", channel: &rabbitMQTopologyTestChannel{returnOnPublish: &amqp.Return{Exchange: "ex", RoutingKey: "jobs", ReplyCode: 312, ReplyText: "NO_ROUTE"}, returnOnPublishAt: 2, returnOnPublishDelay: time.Millisecond, suppressConfirmsAfter: 1}, want: ErrRabbitMQPublishUnrouted, timeout: 50 * time.Millisecond},
		{name: "second-timeout", channel: &rabbitMQTopologyTestChannel{suppressConfirmsAfter: 1}, want: ErrRabbitMQPublishTimeout, timeout: time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := newRabbitMQTopologyTestConnection(tc.channel, Options{Declare: Bool(true), Confirm: Bool(true), PublishTimeout: tc.timeout})
			result, err := conn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
				{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
				{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-2", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("bulk err = %v, want %v", err, tc.want)
			}
			if result.Accepted != 1 {
				t.Fatalf("accepted = %d, want first confirmed payload only", result.Accepted)
			}
			if len(conn.publishSlots) == 0 || conn.publishSlots[0].channel != nil {
				t.Fatal("partial failed bulk publish should reset current publish slot")
			}
		})
	}
}

func TestRabbitMQBulkConfirmTimeoutResetsPublishSlot(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{suppressConfirms: true}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true), PublishTimeout: time.Millisecond})

	result, err := conn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-2", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
	})
	if !errors.Is(err, ErrRabbitMQPublishTimeout) {
		t.Fatalf("bulk timeout err = %v, want ErrRabbitMQPublishTimeout", err)
	}
	if result.Accepted != 0 {
		t.Fatalf("accepted = %d, want 0 before timeout confirm", result.Accepted)
	}
	if len(conn.publishSlots) == 0 || conn.publishSlots[0].channel != nil {
		t.Fatal("timed out bulk publish should reset current publish slot")
	}
}

func TestRabbitMQBulkConfirmFalseReturnsAfterPublish(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(false)})

	result, err := conn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-2", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
	})
	if err != nil {
		t.Fatalf("confirm false bulk: %v", err)
	}
	if result.Accepted != 2 {
		t.Fatalf("accepted = %d, want 2", result.Accepted)
	}
	if len(channel.published) != 2 || len(channel.notifyPublishCaps) != 0 {
		t.Fatalf("published=%d notify=%v, want two publishes and no confirm listener", len(channel.published), channel.notifyPublishCaps)
	}
}

func TestRabbitMQBulkRejectsInvalidPayloadBeforePublish(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	queueConn := &RabbitMQQueue{inner: conn, codec: payload.QueueCodec(nil)}

	if result, err := queueConn.Bulk(context.Background(), "jobs", []queuecontract.Payload{queuecontract.Payload(`not-msgpack`)}); err == nil || result.Accepted != 0 {
		t.Fatal("invalid encoded envelope should fail")
	}
	if len(channel.published) != 0 {
		t.Fatalf("invalid payload published %d messages, want 0", len(channel.published))
	}
}

func TestRabbitMQBulkConfirmPrebuildFailureDoesNotPublishEarlierMessages(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})

	result, err := conn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
		{Queue: "jobs", Envelope: nil},
	})
	if err == nil || result.Accepted != 0 {
		t.Fatalf("prebuild error result=(%#v, %v), want accepted=0 with error", result, err)
	}
	if len(channel.published) != 0 {
		t.Fatalf("published = %d, want 0 when confirm bulk prebuild fails", len(channel.published))
	}
}

func TestRabbitMQBulkConfirmPublishFailureDoesNotAcceptUnconfirmedMessages(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{publishErr: errors.New("second publish failed"), publishErrAt: 2}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})

	result, err := conn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-2", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
	})
	if err == nil || !strings.Contains(err.Error(), "second publish failed") {
		t.Fatalf("publish error = %v, want second publish failed", err)
	}
	if result.Accepted != 0 {
		t.Fatalf("accepted = %d, want 0 because first message was not confirmed", result.Accepted)
	}
	if len(channel.published) != 1 {
		t.Fatalf("published = %d, want first message attempted before second publish failure", len(channel.published))
	}
	if len(conn.publishSlots) == 0 || conn.publishSlots[0].channel != nil {
		t.Fatal("publish error should reset current publish slot")
	}
}

func TestRabbitMQPushBulkFailureBranches(t *testing.T) {
	var nilConn *Connection
	if result, err := nilConn.PushBulk(context.Background(), "jobs", nil); err != nil || result.Accepted != 0 {
		t.Fatalf("nil connection empty bulk should be no-op: %v", err)
	}
	if result, err := nilConn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{{Envelope: &payload.Envelope{}}}); !errors.Is(err, ErrConnectionClosed) || result.Accepted != 0 {
		t.Fatalf("nil connection err = %v, want ErrConnectionClosed", err)
	}

	nilEnvelopeChannel := &rabbitMQTopologyTestChannel{}
	nilEnvelopeConn := newRabbitMQTopologyTestConnection(nilEnvelopeChannel, Options{Declare: Bool(true), Confirm: Bool(true)})
	if result, err := nilEnvelopeConn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{{Queue: "jobs"}}); err == nil || result.Accepted != 0 {
		t.Fatal("nil envelope should fail")
	}
	if len(nilEnvelopeChannel.published) != 0 {
		t.Fatalf("nil envelope published %d messages, want 0", len(nilEnvelopeChannel.published))
	}

	publishErrChannel := &rabbitMQTopologyTestChannel{publishErr: errors.New("publish failed")}
	publishErrConn := newRabbitMQTopologyTestConnection(publishErrChannel, Options{Declare: Bool(true), Confirm: Bool(true)})
	result, err := publishErrConn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
	})
	if err == nil || !strings.Contains(err.Error(), "publish failed") {
		t.Fatalf("publish error = %v, want publish failed", err)
	}
	if result.Accepted != 0 {
		t.Fatalf("accepted = %d, want 0 for publish error", result.Accepted)
	}
	if len(publishErrConn.publishSlots) == 0 || publishErrConn.publishSlots[0].channel != nil {
		t.Fatal("publish error should reset current publish slot")
	}

	channelErrConn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), Confirm: Bool(true)})
	channelErrConn.amqpConnection.(*rabbitMQTopologyTestConnection).channelErr = errors.New("channel failed")
	if result, err := channelErrConn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
	}); err == nil || !strings.Contains(err.Error(), "channel failed") || result.Accepted != 0 {
		t.Fatalf("channel error = %v, want channel failed", err)
	}
}

func TestRabbitMQBulkRebuildsSmallConfirmBuffer(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	env := &payload.Envelope{ID: "job-1", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}
	if err := conn.Push(context.Background(), "jobs", env, 0); err != nil {
		t.Fatalf("single push: %v", err)
	}
	if result, err := conn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-2", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
		{Queue: "jobs", Envelope: &payload.Envelope{ID: "job-3", Name: "ExampleJob", Queue: "jobs", Payload: payload.Payload(`{}`)}},
	}); err != nil || result.Accepted != 2 {
		t.Fatalf("bulk after single push: %v", err)
	}
	if len(channel.notifyPublishCaps) < 2 || channel.notifyPublishCaps[0] != 1 || channel.notifyPublishCaps[len(channel.notifyPublishCaps)-1] < 2 {
		t.Fatalf("notify caps = %v, want single cap then rebuilt bulk cap", channel.notifyPublishCaps)
	}
}

func TestReconnectRestoresOnlyLiveConsumerIntent(t *testing.T) {
	first := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(first, Options{Declare: Bool(true), DelayMode: rabbitMQDelayModePlugin})

	if _, _, err := conn.ensurePublishTopology("publish-only", 0); err != nil {
		t.Fatalf("ensure publish-only topology: %v", err)
	}
	if _, _, err := conn.ensurePublishTopology("delayed-history", time.Second); err != nil {
		t.Fatalf("ensure delayed topology: %v", err)
	}
	release, err := conn.AcquireConsumerIntent([]string{"live"})
	if err != nil {
		t.Fatalf("acquire live intent: %v", err)
	}
	defer func() { _ = release() }()

	reconnected := &rabbitMQTopologyTestChannel{}
	if !conn.installReconnectedConnection(&rabbitMQTopologyTestConnection{channels: []AMQPChannel{reconnected}}) {
		t.Fatal("install reconnected connection failed")
	}
	if err := conn.restoreRabbitMQState(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if !hasRabbitMQTopologyTestString(reconnected.queueDeclares, "live") {
		t.Fatalf("queue declares after restore = %v, want live", reconnected.queueDeclares)
	}
	if hasRabbitMQTopologyTestString(reconnected.queueDeclares, "publish-only") || hasRabbitMQTopologyTestString(reconnected.queueDeclares, "delayed-history") {
		t.Fatalf("restore declared historical queues: %v", reconnected.queueDeclares)
	}
	for _, exchange := range reconnected.exchangeDeclares {
		if exchange == defaultRabbitMQExchange+".delayed:x-delayed-message" {
			t.Fatalf("restore declared historical delayed exchange: %v", reconnected.exchangeDeclares)
		}
	}
	if len(reconnected.consumeCalls) != 1 || reconnected.consumeCalls[0] != "live" {
		t.Fatalf("consume calls after restore = %v, want only live", reconnected.consumeCalls)
	}
}

func TestConsumerIntentCancelFailureStaysInReconnectSet(t *testing.T) {
	cancelErr := errors.New("broker cancel failed")
	first := &rabbitMQTopologyTestChannel{cancelErr: cancelErr}
	conn := newRabbitMQTopologyTestConnection(first, Options{Declare: Bool(true)})
	release, err := conn.AcquireConsumerIntent([]string{"live"})
	if err != nil {
		t.Fatalf("acquire live intent: %v", err)
	}
	if _, err := conn.Pop(context.Background(), []string{"live"}, PopOptions{}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("pop empty err = %v, want ErrEmpty", err)
	}
	if err := release(); !errors.Is(err, cancelErr) {
		t.Fatalf("release err = %v, want cancel failure", err)
	}

	reconnected := &rabbitMQTopologyTestChannel{}
	if !conn.installReconnectedConnection(&rabbitMQTopologyTestConnection{channels: []AMQPChannel{reconnected}}) {
		t.Fatal("install reconnected connection failed")
	}
	if err := conn.restoreRabbitMQState(); err != nil {
		t.Fatalf("restore after cancel failure: %v", err)
	}
	if len(reconnected.consumeCalls) != 1 || reconnected.consumeCalls[0] != "live" {
		t.Fatalf("restore consume calls = %v, want live retained after cancel failure", reconnected.consumeCalls)
	}
}

func TestDynamicConsumerIntentChurnDoesNotRestoreHistoricalQueues(t *testing.T) {
	first := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(first, Options{Declare: Bool(true)})
	for _, queue := range []string{"tenant-a", "tenant-b", "tenant-c"} {
		release, err := conn.AcquireConsumerIntent([]string{queue})
		if err != nil {
			t.Fatalf("acquire %s intent: %v", queue, err)
		}
		if _, err := conn.Pop(context.Background(), []string{queue}, PopOptions{}); !errors.Is(err, ErrEmpty) {
			t.Fatalf("pop empty %s err = %v, want ErrEmpty", queue, err)
		}
		if err := release(); err != nil {
			t.Fatalf("release %s intent: %v", queue, err)
		}
	}

	reconnected := &rabbitMQTopologyTestChannel{}
	if !conn.installReconnectedConnection(&rabbitMQTopologyTestConnection{channels: []AMQPChannel{reconnected}}) {
		t.Fatal("install reconnected connection failed")
	}
	if err := conn.restoreRabbitMQState(); err != nil {
		t.Fatalf("restore after churn: %v", err)
	}
	if len(reconnected.queueDeclares) != 0 || len(reconnected.consumeCalls) != 0 {
		t.Fatalf("restore after churn declared queues %v and consumers %v, want none", reconnected.queueDeclares, reconnected.consumeCalls)
	}
}

func TestTopologyDeclareFailureBranches(t *testing.T) {
	t.Run("topology channel error", func(t *testing.T) {
		conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
		conn.amqpConnection = &rabbitMQTopologyTestConnection{channelErr: errors.New("channel failed")}
		if err := conn.ensureQueueTopology("jobs"); err == nil {
			t.Fatal("expected topology channel error")
		}
	})

	t.Run("queue topology declare errors", func(t *testing.T) {
		for name, channel := range map[string]*rabbitMQTopologyTestChannel{
			"exchange": {exchangeErr: errors.New("exchange failed")},
			"queue":    {queueDeclareErr: errors.New("queue failed")},
			"bind":     {bindErr: errors.New("bind failed")},
		} {
			conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true)})
			if err := conn.ensureQueueTopology("jobs"); err == nil {
				t.Fatalf("%s: expected declare error", name)
			}
		}
	})

	t.Run("plugin delay declare errors", func(t *testing.T) {
		for name, channel := range map[string]*rabbitMQTopologyTestChannel{
			"exchange": {exchangeErr: errors.New("delay exchange failed")},
			"bind":     {bindErr: errors.New("delay bind failed")},
		} {
			conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true)})
			conn.declaredQueues["jobs"] = struct{}{}
			if err := conn.ensurePluginDelayTopology("jobs"); err == nil {
				t.Fatalf("%s: expected plugin delay error", name)
			}
		}
	})

	t.Run("ttl dlx declare errors", func(t *testing.T) {
		for name, channel := range map[string]*rabbitMQTopologyTestChannel{
			"queue": {queueDeclareErr: errors.New("ttl queue failed")},
			"bind":  {bindErr: errors.New("ttl bind failed")},
		} {
			conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), DelayMode: rabbitMQDelayModeTTLDLX})
			if err := conn.ensureTTLDLXDelayTopology("jobs", "jobs.delay", time.Second); err == nil {
				t.Fatalf("%s: expected ttl delay error", name)
			}
		}
	})
}

func TestTopologyOperationFailureDropsCachedChannel(t *testing.T) {
	firstErr := errors.New("declare failed")
	first := &rabbitMQTopologyTestChannel{queueDeclareErr: firstErr}
	second := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(first, Options{Declare: Bool(true)})
	stub := &rabbitMQTopologyTestConnection{channels: []AMQPChannel{first, second}}
	conn.amqpConnection = stub

	if err := conn.ensureQueueTopology("jobs"); !errors.Is(err, firstErr) {
		t.Fatalf("first topology err = %v, want declare failed", err)
	}
	if !first.closed {
		t.Fatal("failed topology channel should be closed")
	}
	if err := conn.ensureQueueTopology("jobs"); err != nil {
		t.Fatalf("second topology after channel reset err = %v", err)
	}
	if stub.channelCalls != 2 {
		t.Fatalf("channel calls = %d, want fresh topology channel for retry", stub.channelCalls)
	}
	if len(second.queueDeclares) != 1 || second.queueDeclares[0] != "jobs" {
		t.Fatalf("second channel queue declares = %#v, want jobs", second.queueDeclares)
	}
}

func TestDeclareFalseTopologyFailureBranches(t *testing.T) {
	t.Run("exchange passive error", func(t *testing.T) {
		channel := &rabbitMQTopologyTestChannel{exchangePassiveErr: errors.New("exchange missing")}
		conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
		if err := conn.ensureExistingQueueTopology("jobs"); !errors.Is(err, ErrRabbitMQTopologyMissing) {
			t.Fatalf("ensure existing queue err = %v, want topology missing", err)
		}
	})

	t.Run("queue passive error", func(t *testing.T) {
		channel := &rabbitMQTopologyTestChannel{queueDeclarePassErr: errors.New("queue missing")}
		conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
		if err := conn.ensureExistingQueueTopology("jobs"); !errors.Is(err, ErrRabbitMQTopologyMissing) {
			t.Fatalf("ensure existing queue err = %v, want topology missing", err)
		}
	})

	t.Run("plugin delay passive error", func(t *testing.T) {
		channel := &rabbitMQTopologyTestChannel{exchangePassiveErr: errors.New("delay exchange missing")}
		conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
		conn.markTopologyVerified(conn.exchangeVerificationKey())
		conn.markTopologyVerified(conn.queueVerificationKey("jobs"))
		if err := conn.ensurePluginDelayTopology("jobs"); !errors.Is(err, ErrRabbitMQTopologyMissing) {
			t.Fatalf("ensure plugin delay err = %v, want topology missing", err)
		}
	})

	t.Run("ttl delay passive error", func(t *testing.T) {
		channel := &rabbitMQTopologyTestChannel{queueDeclarePassErr: errors.New("delay queue missing")}
		conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
		if err := conn.ensureTTLDLXDelayTopology("jobs", "jobs.delay", time.Second); !errors.Is(err, ErrRabbitMQTopologyMissing) {
			t.Fatalf("ensure ttl delay err = %v, want topology missing", err)
		}
	})
}

func TestExistingExchangeTopologyLockedBranches(t *testing.T) {
	t.Run("cached exchange skips broker", func(t *testing.T) {
		channel := &rabbitMQTopologyTestChannel{}
		conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
		conn.markTopologyVerified(conn.exchangeVerificationKey())
		conn.topologyMu.Lock()
		err := conn.ensureExistingExchangeTopologyLocked("jobs")
		conn.topologyMu.Unlock()
		if err != nil {
			t.Fatalf("cached exchange err = %v", err)
		}
		if len(channel.exchangePassiveDeclares) != 0 {
			t.Fatalf("passive declares = %v, want cache hit", channel.exchangePassiveDeclares)
		}
	})

	t.Run("channel error", func(t *testing.T) {
		conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(false)})
		conn.amqpConnection = &rabbitMQTopologyTestConnection{channelErr: errors.New("channel failed")}
		conn.topologyMu.Lock()
		err := conn.ensureExistingExchangeTopologyLocked("jobs")
		conn.topologyMu.Unlock()
		if err == nil {
			t.Fatal("expected channel error")
		}
	})

	t.Run("passive error", func(t *testing.T) {
		channel := &rabbitMQTopologyTestChannel{exchangePassiveErr: errors.New("exchange missing")}
		conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
		conn.topologyMu.Lock()
		err := conn.ensureExistingExchangeTopologyLocked("jobs")
		conn.topologyMu.Unlock()
		if !errors.Is(err, ErrRabbitMQTopologyMissing) {
			t.Fatalf("passive err = %v, want topology missing", err)
		}
	})

	t.Run("success marks cache", func(t *testing.T) {
		channel := &rabbitMQTopologyTestChannel{}
		conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
		conn.topologyMu.Lock()
		err := conn.ensureExistingExchangeTopologyLocked("jobs")
		conn.topologyMu.Unlock()
		if err != nil {
			t.Fatalf("success err = %v", err)
		}
		if !conn.isTopologyVerified(conn.exchangeVerificationKey()) {
			t.Fatal("exchange verification cache was not marked")
		}
	})
}

func TestConsumerIntentLeaseEdgeBranches(t *testing.T) {
	var nilConn *Connection
	if _, err := nilConn.AcquireConsumerIntent([]string{"jobs"}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil acquire err = %v, want ErrConnectionClosed", err)
	}

	conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	conn.closed = true
	if _, err := conn.AcquireConsumerIntent([]string{"jobs"}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed acquire err = %v, want ErrConnectionClosed", err)
	}

	conn = newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	release, err := conn.AcquireConsumerIntent([]string{"jobs", "jobs"})
	if err != nil {
		t.Fatalf("acquire duplicate intent: %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release without consumer: %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("second release without consumer: %v", err)
	}
	if len(conn.activeConsumers) != 0 || len(conn.consumerRefs) != 0 {
		t.Fatalf("intent maps after release = active %v refs %v, want empty", conn.activeConsumers, conn.consumerRefs)
	}
}

func TestOptionsDialerOverridesDefaultDialer(t *testing.T) {
	dialer := func(string, amqp.Config) (AMQPConnection, error) {
		return nil, errors.New("test dialer")
	}
	if _, err := resolveRabbitMQOptions(Options{Dialer: dialer}).dialer()("", amqp.Config{}); err == nil {
		t.Fatal("expected options dialer error")
	}
	if resolveRabbitMQOptions(Options{}).dialer() == nil {
		t.Fatal("nil options dialer should use default dialer")
	}

	var nilConn *Connection
	if !nilConn.isClosed() {
		t.Fatal("nil connection should be closed")
	}
	if nilConn.isReconnecting() {
		t.Fatal("nil connection should not be reconnecting")
	}
	select {
	case <-nilConn.closedNotify():
	default:
		t.Fatal("nil closedNotify should return a closed channel")
	}

	conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	conn.closed = true
	replacement := &rabbitMQTopologyTestConnection{}
	conn.markReady(replacement)
	if !replacement.closed {
		t.Fatal("markReady on closed connection should close replacement")
	}

	conn = newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{queueDeclareErr: errors.New("restore failed")}, Options{Declare: Bool(true)})
	if _, err := conn.AcquireConsumerIntent([]string{"jobs"}); err != nil {
		t.Fatalf("acquire intent: %v", err)
	}
	if err := conn.restoreRabbitMQState(); err == nil {
		t.Fatal("expected restore error")
	}
}

func TestReceiveDeliveryAndAckerBranches(t *testing.T) {
	conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	if _, _, ok := conn.readyDelivery([]string{"missing"}); ok {
		t.Fatal("missing consumer should not produce ready delivery")
	}

	closed := make(chan amqp.Delivery)
	close(closed)
	conn.consumers["closed"] = closed
	if _, _, ok := conn.readyDelivery([]string{"closed"}); ok {
		t.Fatal("closed consumer should not produce ready delivery")
	}
	if _, ok := conn.consumers["closed"]; ok {
		t.Fatal("closed consumer should be dropped")
	}

	ready := make(chan amqp.Delivery, 1)
	ready <- amqp.Delivery{Body: []byte(`{}`)}
	conn.consumers["ready"] = ready
	if _, queue, ok := conn.readyDelivery([]string{"ready"}); !ok || queue != "ready" {
		t.Fatalf("ready delivery queue = %q ok=%v, want ready", queue, ok)
	}

	if _, _, err := conn.receiveDelivery(context.Background(), []string{"none"}, time.Millisecond); !errors.Is(err, ErrEmpty) {
		t.Fatalf("receive without consumers err = %v, want ErrEmpty", err)
	}
	waiting := make(chan amqp.Delivery)
	conn.consumers["waiting"] = waiting
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := conn.receiveDelivery(ctx, []string{"waiting"}, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("receive canceled err = %v, want context.Canceled", err)
	}
	if _, _, err := conn.receiveDelivery(context.Background(), []string{"waiting"}, time.Millisecond); !errors.Is(err, ErrEmpty) {
		t.Fatalf("receive timeout err = %v, want ErrEmpty", err)
	}

	var nilEnv *payload.Envelope
	if envelopeDelivery(nilEnv) != nil {
		t.Fatal("nil envelope delivery should be nil")
	}
	var nilState *rabbitMQDeliveryState
	if err := nilState.Ack(); err != nil {
		t.Fatalf("nil delivery state ack err = %v", err)
	}
	state := &rabbitMQDeliveryState{delivery: amqp.Delivery{Acknowledger: (*rabbitMQTopologyTestAcknowledger)(&rabbitMQTopologyTestChannel{})}}
	if err := state.Ack(); err != nil {
		t.Fatalf("ack err = %v", err)
	}
	if err := state.Ack(); err != nil {
		t.Fatalf("second ack err = %v", err)
	}
}

func TestSizeAndPublishWaitContextBranches(t *testing.T) {
	var nilConn *Connection
	if _, err := nilConn.Size(context.Background(), "jobs"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil size err = %v, want ErrConnectionClosed", err)
	}

	conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	conn.closed = true
	if _, err := conn.Size(context.Background(), "jobs"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed size err = %v, want ErrConnectionClosed", err)
	}

	conn = newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(false)})
	conn.amqpConnection = &rabbitMQTopologyTestConnection{channelErr: errors.New("channel failed")}
	if _, err := conn.Size(context.Background(), "jobs"); err == nil {
		t.Fatal("expected size channel error")
	}

	channel := &rabbitMQTopologyTestChannel{queueInspectErr: errors.New("queue missing")}
	conn = newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
	conn.markTopologyVerified(conn.exchangeVerificationKey())
	if _, err := conn.Size(context.Background(), "jobs"); !errors.Is(err, ErrRabbitMQTopologyMissing) {
		t.Fatalf("size inspect err = %v, want topology missing", err)
	}

	channel = &rabbitMQTopologyTestChannel{messages: map[string]int{"jobs": 4}}
	conn = newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})
	size, err := conn.Size(context.Background(), "jobs")
	if err != nil || size != 4 {
		t.Fatalf("size existing = %d err=%v, want 4 nil", size, err)
	}

	conn = newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{PublishTimeout: time.Second})
	waitCtx, cancel := conn.publishWaitContext(context.Background())
	defer cancel()
	if _, ok := waitCtx.Deadline(); !ok {
		t.Fatal("publishWaitContext should add deadline")
	}
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), time.Second)
	defer deadlineCancel()
	sameCtx, sameCancel := conn.publishWaitContext(deadlineCtx)
	defer sameCancel()
	if sameCtx != deadlineCtx {
		t.Fatal("publishWaitContext should keep existing deadline context")
	}
}

func TestSmallRabbitMQHelperBranches(t *testing.T) {
	var nilConn *Connection
	nilConn.emitInfrastructureEvent(context.Background(), "ignored", "", "", 0, nil)
	if nilConn.consumerFor("jobs") != nil {
		t.Fatal("nil connection consumerFor should return nil")
	}

	conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	conn.consumers = nil
	if conn.consumerFor("jobs") != nil {
		t.Fatal("nil consumers map should return nil")
	}
	if got := conn.normalizedDelayMode(); got != rabbitMQDelayModePlugin {
		t.Fatalf("empty delay mode = %q, want plugin", got)
	}
	conn.options.DelayMode = " TTL_DLX "
	if got := conn.normalizedDelayMode(); got != rabbitMQDelayModeTTLDLX {
		t.Fatalf("normalized delay mode = %q, want ttl_dlx", got)
	}
	conn.verifiedTopology = nil
	conn.markTopologyVerified(conn.exchangeVerificationKey())
	if !conn.isTopologyVerified(conn.exchangeVerificationKey()) {
		t.Fatal("markTopologyVerified should initialize nil map")
	}
	if got := normalizeRabbitMQQueueName("", &payload.Envelope{Queue: "mail"}); got != "mail" {
		t.Fatalf("queue from envelope = %q, want mail", got)
	}
	if got := normalizeRabbitMQQueueName(" ", nil); got != "default" {
		t.Fatalf("blank queue = %q, want default", got)
	}
	if got := firstNonEmpty("", ""); got != "" {
		t.Fatalf("firstNonEmpty blanks = %q, want empty", got)
	}

	if err := conn.pollPublishReturn(&rabbitMQPublishSlot{}, nil, "ex", "rk"); err != nil {
		t.Fatalf("nil return channel err = %v", err)
	}
	returns := make(chan amqp.Return, 1)
	returns <- amqp.Return{Exchange: "ex", RoutingKey: "rk", ReplyCode: 312, ReplyText: "NO_ROUTE"}
	if err := conn.pollPublishReturn(&rabbitMQPublishSlot{}, returns, "fallback", "fallback"); !errors.Is(err, ErrRabbitMQPublishUnrouted) {
		t.Fatalf("publish return err = %v, want unrouted", err)
	}
	closedReturns := make(chan amqp.Return)
	close(closedReturns)
	if err := conn.pollPublishReturn(&rabbitMQPublishSlot{}, closedReturns, "ex", "rk"); !errors.Is(err, ErrRabbitMQPublishConfirmClosed) {
		t.Fatalf("closed return err = %v, want confirm closed", err)
	}
}

func hasRabbitMQTopologyTestString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestDeclareModeTopologySizeClearAndPluginDelay(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{messages: map[string]int{"jobs": 3}}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), QueueMaxPriority: 7})

	exchange, routingKey, err := conn.ensurePublishTopology("jobs", 0)
	if err != nil {
		t.Fatalf("ensure publish topology: %v", err)
	}
	if exchange != defaultRabbitMQExchange || routingKey != "jobs" {
		t.Fatalf("publish target = %q/%q", exchange, routingKey)
	}
	if len(channel.exchangeDeclares) != 1 || len(channel.queueDeclares) != 1 || len(channel.queueBinds) != 1 {
		t.Fatalf("topology calls = exchanges %v queues %v binds %v", channel.exchangeDeclares, channel.queueDeclares, channel.queueBinds)
	}
	if got := channel.queueDeclareArgs[0]["x-max-priority"]; got != int32(7) {
		t.Fatalf("x-max-priority = %#v, want int32(7)", got)
	}

	size, err := conn.Size(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if size != 3 {
		t.Fatalf("size = %d, want 3", size)
	}
	if err := conn.Clear(context.Background(), "jobs"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if channel.messages["jobs"] != 0 {
		t.Fatalf("messages after clear = %d, want 0", channel.messages["jobs"])
	}

	exchange, routingKey, err = conn.ensurePublishTopology("jobs", 2*time.Second)
	if err != nil {
		t.Fatalf("ensure plugin delay topology: %v", err)
	}
	if exchange != defaultRabbitMQExchange+".delayed" || routingKey != "jobs" {
		t.Fatalf("plugin delay target = %q/%q", exchange, routingKey)
	}
	if len(channel.exchangeDeclares) != 2 || len(channel.queueBinds) != 2 {
		t.Fatalf("plugin delay calls = exchanges %v binds %v", channel.exchangeDeclares, channel.queueBinds)
	}
}

func TestTTLDLXDelayTopologyAndUnsupportedDelayModes(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{
		Declare:      Bool(true),
		DelayMode:    rabbitMQDelayModeTTLDLX,
		DelayBuckets: []time.Duration{5 * time.Second, 10 * time.Second},
	})

	exchange, routingKey, err := conn.ensurePublishTopology("jobs", 6*time.Second)
	if err != nil {
		t.Fatalf("ensure ttl dlx topology: %v", err)
	}
	if exchange != defaultRabbitMQExchange || routingKey != defaultRabbitMQExchange+".jobs.delay.10s" {
		t.Fatalf("ttl dlx target = %q/%q", exchange, routingKey)
	}
	if len(channel.queueDeclares) != 2 {
		t.Fatalf("queue declares = %v, want business queue and ttl delay queue", channel.queueDeclares)
	}
	args := channel.queueDeclareArgs[1]
	if args["x-message-ttl"] != int32(10000) || args["x-dead-letter-routing-key"] != "jobs" {
		t.Fatalf("ttl dlx args = %#v", args)
	}

	none := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), DelayMode: rabbitMQDelayModeNone})
	if _, _, err := none.ensurePublishTopology("jobs", time.Second); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("delay_mode none err = %v, want ErrUnsupportedOperation", err)
	}
	unknown := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), DelayMode: "other"})
	if _, _, err := unknown.ensurePublishTopology("jobs", time.Second); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("unknown delay mode err = %v, want ErrUnsupportedOperation", err)
	}
}

func TestDeclareFalsePassiveTopologyCachesAndSizeRefreshes(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{messages: map[string]int{
		"jobs":       4,
		"jobs.delay": 0,
	}}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false)})

	if err := conn.ensureExistingQueueTopology("jobs"); err != nil {
		t.Fatalf("ensure existing queue: %v", err)
	}
	if err := conn.ensureExistingQueueTopology("jobs"); err != nil {
		t.Fatalf("ensure existing queue from cache: %v", err)
	}
	if len(channel.exchangePassiveDeclares) != 1 {
		t.Fatalf("exchange passive declares = %v, want one cached verification", channel.exchangePassiveDeclares)
	}
	if len(channel.queuePassiveDeclares) != 1 {
		t.Fatalf("queue passive declares = %v, want one cached queue verification", channel.queuePassiveDeclares)
	}
	if len(channel.queueBinds) != 0 {
		t.Fatalf("declare=false should not bind queue, binds = %v", channel.queueBinds)
	}

	size, err := conn.Size(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("size existing queue: %v", err)
	}
	if size != 4 {
		t.Fatalf("size = %d, want 4", size)
	}
	channel.messages["jobs"] = 9
	size, err = conn.Size(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("size existing queue second read: %v", err)
	}
	if size != 9 {
		t.Fatalf("size after broker change = %d, want 9", size)
	}

	if err := conn.ensurePluginDelayTopology("jobs"); err != nil {
		t.Fatalf("ensure existing plugin delay: %v", err)
	}
	if err := conn.ensureTTLDLXDelayTopology("jobs", "jobs.delay", 5*time.Second); err != nil {
		t.Fatalf("ensure existing ttl delay: %v", err)
	}
	if len(channel.exchangePassiveDeclares) != 2 || len(channel.queuePassiveDeclares) < 2 {
		t.Fatalf("passive topology calls = exchanges %v queuePassive %v", channel.exchangePassiveDeclares, channel.queuePassiveDeclares)
	}
	if len(channel.queueBinds) != 0 {
		t.Fatalf("declare=false delay verification should not bind queue, binds = %v", channel.queueBinds)
	}
}

func TestRabbitMQTopologyCacheDefaultDoesNotPrune(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true)})

	if err := conn.ensureQueueTopology("jobs"); err != nil {
		t.Fatalf("first ensure queue topology: %v", err)
	}
	if err := conn.ensureQueueTopology("jobs"); err != nil {
		t.Fatalf("second ensure queue topology: %v", err)
	}
	if len(channel.exchangeDeclares) != 1 || len(channel.queueDeclares) != 1 || len(channel.queueBinds) != 1 {
		t.Fatalf("topology calls with default cache settings = exchanges %v queues %v binds %v", channel.exchangeDeclares, channel.queueDeclares, channel.queueBinds)
	}
}

func TestRabbitMQTopologyCacheTTLExpiresPublishOnlyQueue(t *testing.T) {
	now := time.Unix(100, 0)
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), TopologyCacheTTL: time.Minute})
	conn.setTopologyCacheNowForTest(func() time.Time { return now })

	if err := conn.ensureQueueTopology("jobs"); err != nil {
		t.Fatalf("first ensure queue topology: %v", err)
	}
	now = now.Add(30 * time.Second)
	if err := conn.ensureQueueTopology("jobs"); err != nil {
		t.Fatalf("cached ensure queue topology: %v", err)
	}
	now = now.Add(time.Minute + time.Second)
	if err := conn.ensureQueueTopology("jobs"); err != nil {
		t.Fatalf("expired ensure queue topology: %v", err)
	}
	if len(channel.queueDeclares) != 2 || len(channel.queueBinds) != 2 {
		t.Fatalf("queue topology calls after TTL expiry = declares %v binds %v", channel.queueDeclares, channel.queueBinds)
	}
}

func TestRabbitMQTopologyCacheCapacityEvictsLeastRecentlyUsedQueue(t *testing.T) {
	now := time.Unix(200, 0)
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), TopologyCacheMaxEntries: 2})
	conn.setTopologyCacheNowForTest(func() time.Time { return now })

	for _, queue := range []string{"one", "two"} {
		if err := conn.ensureQueueTopology(queue); err != nil {
			t.Fatalf("ensure queue %s: %v", queue, err)
		}
		now = now.Add(time.Second)
	}
	if err := conn.ensureQueueTopology("one"); err != nil {
		t.Fatalf("touch one: %v", err)
	}
	now = now.Add(time.Second)
	if err := conn.ensureQueueTopology("three"); err != nil {
		t.Fatalf("ensure three: %v", err)
	}
	if err := conn.ensureQueueTopology("two"); err != nil {
		t.Fatalf("ensure evicted two: %v", err)
	}
	if len(channel.queueDeclares) != 4 {
		t.Fatalf("queue declares = %v, want two to be re-declared after LRU eviction", channel.queueDeclares)
	}
}

func TestRabbitMQTopologyCacheKeepsLiveConsumerIntentEntries(t *testing.T) {
	now := time.Unix(300, 0)
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), TopologyCacheTTL: time.Minute, TopologyCacheMaxEntries: 1})
	conn.setTopologyCacheNowForTest(func() time.Time { return now })
	conn.activeConsumers["jobs"] = struct{}{}
	conn.consumerRefs["jobs"] = 1

	if err := conn.ensureQueueTopology("jobs"); err != nil {
		t.Fatalf("ensure protected queue: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if err := conn.ensureQueueTopology("dynamic"); err != nil {
		t.Fatalf("ensure dynamic queue: %v", err)
	}
	if _, ok := conn.declaredQueues["jobs"]; !ok {
		t.Fatal("live Consumer Intent queue should remain in declared topology cache")
	}
	delete(conn.activeConsumers, "jobs")
	delete(conn.consumerRefs, "jobs")
	now = now.Add(time.Second)
	if err := conn.ensureQueueTopology("dynamic-2"); err != nil {
		t.Fatalf("ensure second dynamic queue: %v", err)
	}
	if _, ok := conn.declaredQueues["jobs"]; ok {
		t.Fatal("expired queue should be evicted after Consumer Intent is released")
	}
}

func TestRabbitMQDeclareFalseTopologyCacheEvictionReverifies(t *testing.T) {
	now := time.Unix(400, 0)
	channel := &rabbitMQTopologyTestChannel{messages: map[string]int{"jobs": 7}}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(false), TopologyCacheTTL: time.Minute})
	conn.setTopologyCacheNowForTest(func() time.Time { return now })

	size, err := conn.Size(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("first size: %v", err)
	}
	if size != 7 {
		t.Fatalf("first size = %d, want 7", size)
	}
	now = now.Add(time.Minute + time.Second)
	size, err = conn.Size(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("size after TTL: %v", err)
	}
	if size != 7 {
		t.Fatalf("size after TTL = %d, want 7", size)
	}
	if len(channel.exchangePassiveDeclares) != 2 {
		t.Fatalf("exchange passive declares = %v, want reverify after TTL eviction", channel.exchangePassiveDeclares)
	}
	if len(channel.queueInspects) != 2 {
		t.Fatalf("queue inspects = %v, want one QueueInspect per Size call", channel.queueInspects)
	}

	now = now.Add(time.Minute + time.Second)
	channel.queueInspectErr = errors.New("queue missing")
	if _, err := conn.Size(context.Background(), "jobs"); !errors.Is(err, ErrRabbitMQTopologyMissing) {
		t.Fatalf("size after evicted missing queue err = %v, want ErrRabbitMQTopologyMissing", err)
	}
}
