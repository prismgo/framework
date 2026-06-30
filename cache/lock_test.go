package cache

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestDistributedLockBetweenBlockedAttemptsSleepForRace 检测 BetweenBlockedAttemptsSleepFor 的数据竞争
func TestDistributedLockBetweenBlockedAttemptsSleepForRace(t *testing.T) {
	provider := newMemoryLockProvider()
	lock := newLock(provider, "test-key", time.Second, 50*time.Millisecond)

	var wg sync.WaitGroup
	iterations := 100

	// 并发调用 BetweenBlockedAttemptsSleepFor
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(d time.Duration) {
			defer wg.Done()
			lock.BetweenBlockedAttemptsSleepFor(d)
		}(time.Duration(i+1) * time.Millisecond)
	}

	// 并发调用 Block 读取 retrySleep
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			_, _ = lock.Block(ctx, 5*time.Millisecond, func(ctx context.Context) error {
				return nil
			})
		}()
	}

	wg.Wait()
}

// TestDistributedLockBetweenBlockedAttemptsSleepForUpdatesValue 验证设置生效
func TestDistributedLockBetweenBlockedAttemptsSleepForUpdatesValue(t *testing.T) {
	provider := newMemoryLockProvider()
	lock := newLock(provider, "test-key", time.Second, 50*time.Millisecond)

	newSleep := 100 * time.Millisecond
	result := lock.BetweenBlockedAttemptsSleepFor(newSleep)

	if result != lock {
		t.Error("BetweenBlockedAttemptsSleepFor should return the lock itself")
	}

	lock.mu.Lock()
	actualSleep := lock.retrySleep
	lock.mu.Unlock()

	if actualSleep != newSleep {
		t.Errorf("retrySleep = %v, want %v", actualSleep, newSleep)
	}
}

// TestDistributedLockBetweenBlockedAttemptsSleepForIgnoresZero 验证零值被忽略
func TestDistributedLockBetweenBlockedAttemptsSleepForIgnoresZero(t *testing.T) {
	provider := newMemoryLockProvider()
	originalSleep := 50 * time.Millisecond
	lock := newLock(provider, "test-key", time.Second, originalSleep)

	lock.BetweenBlockedAttemptsSleepFor(0)

	lock.mu.Lock()
	actualSleep := lock.retrySleep
	lock.mu.Unlock()

	if actualSleep != originalSleep {
		t.Errorf("retrySleep = %v, want %v (should not change)", actualSleep, originalSleep)
	}
}
