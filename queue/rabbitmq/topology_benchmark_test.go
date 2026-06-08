package rabbitmq

import (
	"container/list"
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitMQTopologyBenchmarkChannel struct {
	exchangeDeclares atomic.Int64
	queueDeclares    atomic.Int64
	queueBinds       atomic.Int64
}

func (c *rabbitMQTopologyBenchmarkChannel) Close() error { return nil }

func (c *rabbitMQTopologyBenchmarkChannel) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error {
	c.exchangeDeclares.Add(1)
	return nil
}

func (c *rabbitMQTopologyBenchmarkChannel) ExchangeDeclarePassive(string, string, bool, bool, bool, bool, amqp.Table) error {
	return nil
}

func (c *rabbitMQTopologyBenchmarkChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.queueDeclares.Add(1)
	return amqp.Queue{Name: name}, nil
}

func (c *rabbitMQTopologyBenchmarkChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, nil
}

func (c *rabbitMQTopologyBenchmarkChannel) QueueInspect(name string) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, nil
}

func (c *rabbitMQTopologyBenchmarkChannel) QueuePurge(string, bool) (int, error) { return 0, nil }

func (c *rabbitMQTopologyBenchmarkChannel) QueueBind(string, string, string, bool, amqp.Table) error {
	c.queueBinds.Add(1)
	return nil
}

func (c *rabbitMQTopologyBenchmarkChannel) Confirm(bool) error { return nil }

func (c *rabbitMQTopologyBenchmarkChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	return confirm
}

func (c *rabbitMQTopologyBenchmarkChannel) NotifyReturn(receiver chan amqp.Return) chan amqp.Return {
	return receiver
}

func (c *rabbitMQTopologyBenchmarkChannel) PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error {
	return nil
}

func (c *rabbitMQTopologyBenchmarkChannel) Get(string, bool) (amqp.Delivery, bool, error) {
	return amqp.Delivery{}, false, nil
}

func (c *rabbitMQTopologyBenchmarkChannel) Qos(int, int, bool) error { return nil }

func (c *rabbitMQTopologyBenchmarkChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return make(chan amqp.Delivery), nil
}

func (c *rabbitMQTopologyBenchmarkChannel) Cancel(string, bool) error { return nil }

func newRabbitMQTopologyBenchmarkConnection(channel AMQPChannel, options Options) *Connection {
	resolved := resolveRabbitMQOptions(options)
	readyCh := make(chan struct{})
	close(readyCh)
	return &Connection{
		name:             "benchmark",
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

// BenchmarkRabbitMQTopologyCacheHitCost 衡量固定队列稳态成本：
// 当前 AMQP connection 已经声明过 topology，循环内只支付 topologyMu 与本地 cache lookup/touch 成本。
func BenchmarkRabbitMQTopologyCacheHitCost(b *testing.B) {
	channel := &rabbitMQTopologyBenchmarkChannel{}
	conn := newRabbitMQTopologyBenchmarkConnection(channel, Options{Declare: Bool(true)})
	if err := conn.ensureQueueTopology("jobs"); err != nil {
		b.Fatalf("prime topology cache: %v", err)
	}
	b.ReportMetric(float64(channel.queueDeclares.Load()), "initial_queue_declares")

	var firstErrMu sync.Mutex
	var firstErr error
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := conn.ensureQueueTopology("jobs"); err != nil {
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	firstErrMu.Lock()
	err := firstErr
	firstErrMu.Unlock()
	if err != nil {
		b.Fatalf("ensure queue topology: %v", err)
	}
	if got := channel.queueDeclares.Load(); got != 1 {
		b.Fatalf("queue declares = %d, want only the initial declare", got)
	}
}

// BenchmarkRabbitMQTopologyCacheDynamicChurnCost 衡量动态队列 churn 成本：
// 每次操作使用新的 queue name，并开启 topology cache 容量上限，触发同一 connection 内的 LRU 懒淘汰。
func BenchmarkRabbitMQTopologyCacheDynamicChurnCost(b *testing.B) {
	const maxEntries = 256

	now := atomic.Int64{}
	now.Store(time.Unix(1_000, 0).UnixNano())
	channel := &rabbitMQTopologyBenchmarkChannel{}
	conn := newRabbitMQTopologyBenchmarkConnection(channel, Options{
		Declare:                 Bool(true),
		TopologyCacheTTL:        time.Hour,
		TopologyCacheMaxEntries: maxEntries,
	})
	conn.setTopologyCacheNowForTest(func() time.Time {
		return time.Unix(0, now.Load())
	})

	var seq atomic.Uint64
	var firstErrMu sync.Mutex
	var firstErr error
	b.ReportMetric(float64(maxEntries), "topology_cache_max_entries")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := seq.Add(1)
			now.Add(int64(time.Microsecond))
			queue := "dynamic-" + strconv.FormatUint(id, 10)
			if err := conn.ensureQueueTopology(queue); err != nil {
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	firstErrMu.Lock()
	err := firstErr
	firstErrMu.Unlock()
	if err != nil {
		b.Fatalf("ensure dynamic queue topology: %v", err)
	}
	if got := channel.queueDeclares.Load(); got != int64(b.N) {
		b.Fatalf("queue declares = %d, want %d", got, b.N)
	}
}
