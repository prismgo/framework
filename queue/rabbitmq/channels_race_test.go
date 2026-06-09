package rabbitmq

import (
	"sync"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestNextPublishSlot_ConcurrentSlotCreation 验证多个 goroutine 并发获取 slot 时
// 不会因为 publishSlots 重新分配导致越界或返回 nil。
//
// 需求背景：原实现在 RLock 外部做 atomic.Add 计算 index，然后在 RLock 内读取 slots。
// 在 RUnlock 到后续 Lock 之间，另一个 goroutine 可能触发 slots 重新分配，
// 导致原始 index 失效或越界。
func TestNextPublishSlotConcurrentSlotCreation(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery), messages: map[string]int{}}
	conn := newRabbitMQTopologyTestConnection(channel, Options{
		Declare:         Bool(true),
		Confirm:         Bool(false),
		PublishChannels: 4,
	})

	const goroutines = 32
	const iterations = 100
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				slot, err := conn.nextPublishSlot()
				if err != nil {
					errCount.Add(1)
					continue
				}
				if slot == nil {
					t.Errorf("nextPublishSlot returned nil slot without error")
					return
				}
			}
		}()
	}
	wg.Wait()

	if errCount.Load() > 0 {
		t.Fatalf("got %d errors from nextPublishSlot", errCount.Load())
	}

	// 验证所有 slot 都已创建
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if len(conn.publishSlots) != 4 {
		t.Fatalf("expected 4 publish slots, got %d", len(conn.publishSlots))
	}
	for i, slot := range conn.publishSlots {
		if slot == nil {
			t.Errorf("publish slot %d is nil", i)
		}
	}
}

// TestNextPublishSlot_RoundRobinDistribution 验证 round-robin 分布均匀性。
//
// 逻辑说明：多 goroutine 并发获取 slot 后，每个 slot 应该被大致均匀地选中。
func TestNextPublishSlotRoundRobinDistribution(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery), messages: map[string]int{}}
	conn := newRabbitMQTopologyTestConnection(channel, Options{
		Declare:         Bool(true),
		Confirm:         Bool(false),
		PublishChannels: 4,
	})

	// 先预热，创建所有 slot
	for i := 0; i < 4; i++ {
		if _, err := conn.nextPublishSlot(); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	// 记录每个 slot 被选中的次数
	var counts [4]atomic.Int64
	const goroutines = 16
	const iterations = 1000
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				slot, err := conn.nextPublishSlot()
				if err != nil {
					continue
				}
				// 找到 slot 在数组中的位置
				conn.mu.RLock()
				for idx, s := range conn.publishSlots {
					if s == slot {
						counts[idx].Add(1)
						break
					}
				}
				conn.mu.RUnlock()
			}
		}()
	}
	wg.Wait()

	total := goroutines * iterations
	expected := total / 4
	for i := range counts {
		count := int(counts[i].Load())
		// 允许 50% 偏差（并发 round-robin 不保证精确均匀）
		if count < expected/2 || count > expected*2 {
			t.Errorf("slot %d: count=%d, expected ~%d (total=%d)", i, count, expected, total)
		}
	}
}

// TestNextPublishSlot_ClosedConnection 验证关闭连接后获取 slot 返回错误。
func TestNextPublishSlotClosedConnection(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery), messages: map[string]int{}}
	conn := newRabbitMQTopologyTestConnection(channel, Options{
		Declare:         Bool(true),
		Confirm:         Bool(false),
		PublishChannels: 1,
	})
	// 关闭连接
	conn.mu.Lock()
	conn.closed = true
	conn.mu.Unlock()

	slot, err := conn.nextPublishSlot()
	if err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
	if slot != nil {
		t.Fatal("expected nil slot for closed connection")
	}
}
