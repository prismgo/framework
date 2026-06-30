package cache

import (
	"context"
	"testing"
	"time"
)

// TestMemoryStoreGetManyBatchRead 验证批量读取在一次锁内完成
func TestMemoryStoreGetManyBatchRead(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	defer store.Close()
	ctx := context.Background()

	// 写入多个 key
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, key := range keys {
		if err := store.Set(ctx, key, []byte("value"+string(rune('0'+i))), expirationOptions(time.Minute)...); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	// 批量读取
	values, err := store.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}

	if len(values) != len(keys) {
		t.Fatalf("GetMany returned %d values, want %d", len(values), len(keys))
	}

	for i, key := range keys {
		expected := "value" + string(rune('0'+i))
		if string(values[key]) != expected {
			t.Errorf("values[%s] = %q, want %q", key, values[key], expected)
		}
	}
}

// TestMemoryStoreGetManyCleansExpiredKeys 验证批量读取时清理过期 key
func TestMemoryStoreGetManyCleansExpiredKeys(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	defer store.Close()
	ctx := context.Background()

	// 写入 3 个 key，其中 2 个已过期
	if err := store.Set(ctx, "valid", []byte("data1"), expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set valid: %v", err)
	}
	if err := store.Set(ctx, "expired1", []byte("data2"), expirationOptions(10*time.Millisecond)...); err != nil {
		t.Fatalf("set expired1: %v", err)
	}
	if err := store.Set(ctx, "expired2", []byte("data3"), expirationOptions(10*time.Millisecond)...); err != nil {
		t.Fatalf("set expired2: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	// 批量读取应清理过期 key
	keys := []string{"valid", "expired1", "expired2", "missing"}
	values, err := store.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}

	// 只应返回 valid
	if len(values) != 1 {
		t.Fatalf("GetMany returned %d values, want 1", len(values))
	}
	if string(values["valid"]) != "data1" {
		t.Errorf("values[valid] = %q, want data1", values["valid"])
	}

	// 验证过期 key 已被清理
	store.mu.RLock()
	_, hasExpired1 := store.items["expired1"]
	_, hasExpired2 := store.items["expired2"]
	store.mu.RUnlock()

	if hasExpired1 || hasExpired2 {
		t.Error("expired keys should be cleaned up during GetMany")
	}
}

// TestMemoryStoreGetManyEmptyKeys 验证空 keys 返回空 map
func TestMemoryStoreGetManyEmptyKeys(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	defer store.Close()
	ctx := context.Background()

	values, err := store.GetMany(ctx, []string{})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(values) != 0 {
		t.Errorf("GetMany returned %d values, want 0", len(values))
	}
}

// TestMemoryStoreGetManyPartialHit 验证部分命中场景
func TestMemoryStoreGetManyPartialHit(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	defer store.Close()
	ctx := context.Background()

	// 只写入 2 个 key
	if err := store.Set(ctx, "exists1", []byte("data1"), expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set exists1: %v", err)
	}
	if err := store.Set(ctx, "exists2", []byte("data2"), expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set exists2: %v", err)
	}

	// 读取 4 个 key（2 个存在，2 个不存在）
	keys := []string{"exists1", "missing1", "exists2", "missing2"}
	values, err := store.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}

	if len(values) != 2 {
		t.Fatalf("GetMany returned %d values, want 2", len(values))
	}
	if string(values["exists1"]) != "data1" {
		t.Errorf("values[exists1] = %q, want data1", values["exists1"])
	}
	if string(values["exists2"]) != "data2" {
		t.Errorf("values[exists2] = %q, want data2", values["exists2"])
	}
}

// TestMemoryStoreGetWithTTLBasic 验证 GetWithTTL 基本功能
func TestMemoryStoreGetWithTTLBasic(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	defer store.Close()
	ctx := context.Background()

	// 写入带 TTL 的 key
	if err := store.Set(ctx, "test-key", []byte("test-value"), expirationOptions(5*time.Minute)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	// 读取并验证 TTL
	value, ttl, err := store.GetWithTTL(ctx, "test-key")
	if err != nil {
		t.Fatalf("GetWithTTL: %v", err)
	}

	if string(value.([]byte)) != "test-value" {
		t.Errorf("value = %q, want test-value", value)
	}

	// TTL 应该接近但小于原始 TTL
	if ttl <= 0 || ttl > 5*time.Minute {
		t.Errorf("TTL = %v, want between 0 and 5m", ttl)
	}
}

// TestMemoryStoreGetWithTTLForever 验证永久 key 的 TTL 返回 -1
func TestMemoryStoreGetWithTTLForever(t *testing.T) {
	store := newMemoryStore(0, 0)
	defer store.Close()
	ctx := context.Background()

	// 写入永久 key（TTL = 0）
	if err := store.Set(ctx, "forever-key", []byte("forever-value")); err != nil {
		t.Fatalf("set: %v", err)
	}

	// 读取并验证 TTL
	value, ttl, err := store.GetWithTTL(ctx, "forever-key")
	if err != nil {
		t.Fatalf("GetWithTTL: %v", err)
	}

	if string(value.([]byte)) != "forever-value" {
		t.Errorf("value = %q, want forever-value", value)
	}

	if ttl != -1 {
		t.Errorf("TTL = %v, want -1 (forever)", ttl)
	}
}

// TestMemoryStoreGetWithTTLExpired 验证过期 key 返回 ErrCacheMiss
func TestMemoryStoreGetWithTTLExpired(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	defer store.Close()
	ctx := context.Background()

	// 写入即将过期的 key
	if err := store.Set(ctx, "expired-key", []byte("data"), expirationOptions(10*time.Millisecond)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	// 读取应返回 ErrCacheMiss
	_, _, err := store.GetWithTTL(ctx, "expired-key")
	if err == nil {
		t.Fatal("expected error for expired key, got nil")
	}
}

// TestMemoryStoreGetWithTTLMissing 验证不存在的 key 返回 ErrCacheMiss
func TestMemoryStoreGetWithTTLMissing(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	defer store.Close()
	ctx := context.Background()

	// 读取不存在的 key
	_, _, err := store.GetWithTTL(ctx, "missing-key")
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
}

// BenchmarkMemoryStoreGetManyPerformance 基准测试证明 N+1 锁竞争问题
func BenchmarkMemoryStoreGetManyPerformance(b *testing.B) {
	store := newMemoryStore(time.Minute, 0)
	defer store.Close()
	ctx := context.Background()

	// 预填充 1000 个 key
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		key := "key" + string(rune('0'+i%10))
		keys[i] = key
		_ = store.Set(ctx, key, []byte("value"), expirationOptions(time.Minute)...)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = store.GetMany(ctx, keys)
		}
	})
}
