package rabbitmq

import (
	"sync"
	"testing"
	"time"

	"github.com/prismgo/framework/queue/payload"
)

// TestCleanupDeliveryRegistry_NilState 验证 state 为 nil 时不会 panic。
func TestCleanupDeliveryRegistry_NilState(t *testing.T) {
	// 重置全局状态
	rabbitMQDeliveryRegistry = sync.Map{}

	// 存储一个 state 为 nil 的条目
	env := &payload.Envelope{ID: "test-nil-state"}
	rabbitMQDeliveryRegistry.Store(env, &rabbitMQDeliveryEntry{
		state:    nil,
		storedAt: time.Now().Add(-2 * deliveryRegistryTTL), // 超过 TTL
	})

	// cleanup 不应该 panic
	cleanupDeliveryRegistry()

	// 验证条目被清理（超过 TTL）
	if _, ok := rabbitMQDeliveryRegistry.Load(env); ok {
		t.Error("expected entry with nil state to be cleaned up")
	}
}

// TestCleanupDeliveryRegistry_LockedState 验证 state.mu 被锁住时 cleanup 不会死锁。
func TestCleanupDeliveryRegistry_LockedState(t *testing.T) {
	// 重置全局状态
	rabbitMQDeliveryRegistry = sync.Map{}

	// 创建一个 state 并锁定
	state := &rabbitMQDeliveryState{
		acked: false,
	}
	state.mu.Lock() // 锁定，模拟长时间持锁

	// 存储条目
	env := &payload.Envelope{ID: "test-locked-state"}
	rabbitMQDeliveryRegistry.Store(env, &rabbitMQDeliveryEntry{
		state:    state,
		storedAt: time.Now(), // 未超过 TTL
	})

	// 在 goroutine 中运行 cleanup，设置超时
	done := make(chan struct{})
	go func() {
		cleanupDeliveryRegistry()
		close(done)
	}()

	// 等待 cleanup 完成或超时
	select {
	case <-done:
		// cleanup 完成，但由于锁被持有，它应该阻塞
		// 实际上 cleanup 会尝试获取锁，所以会阻塞直到我们释放
	case <-time.After(100 * time.Millisecond):
		// 超时，说明 cleanup 被锁阻塞
		// 这是预期行为，但我们需要释放锁让 cleanup 完成
	}

	// 释放锁
	state.mu.Unlock()

	// 等待 cleanup 完成
	<-done

	// 验证条目未被清理（未 ack 且未超过 TTL）
	if _, ok := rabbitMQDeliveryRegistry.Load(env); !ok {
		t.Error("expected entry to remain (not acked, not expired)")
	}
}

// TestCleanupDeliveryRegistry_ConcurrentAccess 验证并发访问不会死锁。
func TestCleanupDeliveryRegistry_ConcurrentAccess(t *testing.T) {
	// 重置全局状态
	rabbitMQDeliveryRegistry = sync.Map{}

	// 创建多个条目
	const numEntries = 100
	entries := make([]*payload.Envelope, numEntries)
	states := make([]*rabbitMQDeliveryState, numEntries)

	for i := 0; i < numEntries; i++ {
		entries[i] = &payload.Envelope{ID: string(rune('A' + i%26)) + string(rune('0'+i/26))}
		states[i] = &rabbitMQDeliveryState{acked: false}
		rabbitMQDeliveryRegistry.Store(entries[i], &rabbitMQDeliveryEntry{
			state:    states[i],
			storedAt: time.Now(),
		})
	}

	// 并发执行 cleanup 和 Ack/Delete 操作
	var wg sync.WaitGroup
	wg.Add(3)

	// Goroutine 1: 执行 cleanup
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			cleanupDeliveryRegistry()
			time.Sleep(time.Millisecond)
		}
	}()

	// Goroutine 2: 执行 Ack 操作
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if i < numEntries {
				states[i].mu.Lock()
				states[i].acked = true
				states[i].mu.Unlock()
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Goroutine 3: 执行 Delete 操作
	go func() {
		defer wg.Done()
		for i := 50; i < numEntries; i++ {
			rabbitMQDeliveryRegistry.Delete(entries[i])
			time.Sleep(time.Millisecond)
		}
	}()

	// 等待所有 goroutine 完成，设置超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 成功完成
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock detected: concurrent operations did not complete in 5s")
	}
}

// TestCleanupDeliveryRegistry_UnackedExpiredEntries 验证未 ack 的过期条目应该被保留。
// 这是设计意图：未 ack 的活跃 delivery 仍要支持后续 Delete/Release 找回底层 delivery handle。
func TestCleanupDeliveryRegistry_UnackedExpiredEntries(t *testing.T) {
	// 重置全局状态
	rabbitMQDeliveryRegistry = sync.Map{}

	// 创建超过 TTL 但未 ack 的条目
	envExpired := &payload.Envelope{ID: "test-expired"}
	rabbitMQDeliveryRegistry.Store(envExpired, &rabbitMQDeliveryEntry{
		state:    &rabbitMQDeliveryState{acked: false},
		storedAt: time.Now().Add(-deliveryRegistryTTL - time.Minute), // 超过 TTL
	})

	// 执行 cleanup
	cleanupDeliveryRegistry()

	// 验证未 ack 的过期条目应该被保留（设计意图）
	if _, ok := rabbitMQDeliveryRegistry.Load(envExpired); !ok {
		t.Error("expected unacked expired entry to remain (design intent)")
	}
}
