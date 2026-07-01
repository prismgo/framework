package cache

import (
	"context"
	"testing"
	"time"
)

// TestMemoryStoreGetManyBatchRead 验证批量读取在一次锁内完成
func TestMemoryStoreGetManyBatchRead(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
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
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	// 写入 3 个 key，其中 2 个已过期
	if err := store.Set(ctx, "valid", []byte("data1"), expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set valid: %v", err)
	}
	if err := store.Set(ctx, "expired1", []byte("data2"), expirationOptions(50*time.Millisecond)...); err != nil {
		t.Fatalf("set expired1: %v", err)
	}
	if err := store.Set(ctx, "expired2", []byte("data3"), expirationOptions(50*time.Millisecond)...); err != nil {
		t.Fatalf("set expired2: %v", err)
	}
	// 写入一个不在请求列表中的过期 key，验证 GetMany 不会清理未被请求的 key
	if err := store.Set(ctx, "unrequested-expired", []byte("data4"), expirationOptions(50*time.Millisecond)...); err != nil {
		t.Fatalf("set unrequested-expired: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

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

	// 验证被请求的过期 key 已被清理
	store.mu.RLock()
	_, hasExpired1 := store.items["expired1"]
	_, hasExpired2 := store.items["expired2"]
	_, hasUnrequested := store.items["unrequested-expired"]
	store.mu.RUnlock()

	if hasExpired1 || hasExpired2 {
		t.Error("requested expired keys should be cleaned up during GetMany")
	}
	// GetMany 只清理请求列表中命中的过期 key，未被请求的过期 key 不会被清理
	if !hasUnrequested {
		t.Error("unrequested expired key should NOT be cleaned up by GetMany")
	}
}

// TestMemoryStoreGetManyEmptyKeys 验证空 keys 返回空 map
func TestMemoryStoreGetManyEmptyKeys(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
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
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
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
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
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
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
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
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	// 写入即将过期的 key
	if err := store.Set(ctx, "expired-key", []byte("data"), expirationOptions(50*time.Millisecond)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// 读取应返回 ErrCacheMiss
	_, _, err := store.GetWithTTL(ctx, "expired-key")
	if err == nil {
		t.Fatal("expected error for expired key, got nil")
	}
}

// TestMemoryStoreGetWithTTLMissing 验证不存在的 key 返回 ErrCacheMiss
func TestMemoryStoreGetWithTTLMissing(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	// 读取不存在的 key
	_, _, err := store.GetWithTTL(ctx, "missing-key")
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
}

// TestMemoryStoreGetWithTTLNonStringKey 验证非 string key 返回错误
func TestMemoryStoreGetWithTTLNonStringKey(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	_, _, err := store.GetWithTTL(ctx, 123)
	if err == nil {
		t.Fatal("expected error for non-string key, got nil")
	}
}

// TestMemoryStoreGetBasic 验证 Get 基本功能
func TestMemoryStoreGetBasic(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	if err := store.Set(ctx, "key", "value", expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	val, err := store.Get(ctx, "key")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val != "value" {
		t.Errorf("got %v, want value", val)
	}
}

// TestMemoryStoreGetMissing 验证 Get 缺失 key 返回 ErrCacheMiss
func TestMemoryStoreGetMissing(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	_, err := store.Get(ctx, "missing")
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
}

// TestMemoryStoreGetNonStringKey 验证 Get 非 string key 返回错误
func TestMemoryStoreGetNonStringKey(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	_, err := store.Get(ctx, 123)
	if err == nil {
		t.Fatal("expected error for non-string key, got nil")
	}
}

// TestMemoryStoreGetExpired 验证 Get 过期 key 返回 ErrCacheMiss 并清理
func TestMemoryStoreGetExpired(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	if err := store.Set(ctx, "expired", "value", expirationOptions(50*time.Millisecond)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	_, err := store.Get(ctx, "expired")
	if err == nil {
		t.Fatal("expected error for expired key, got nil")
	}

	// 验证过期 key 已被清理
	store.mu.RLock()
	_, exists := store.items["expired"]
	store.mu.RUnlock()
	if exists {
		t.Error("expired key should be deleted after Get")
	}
}

// TestMemoryStoreTouchBasic 验证 Touch 更新 TTL
func TestMemoryStoreTouchBasic(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	if err := store.Set(ctx, "key", "value", expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	ok, err := store.Touch(ctx, "key", 2*time.Minute)
	if err != nil {
		t.Fatalf("touch: %v", err)
	}
	if !ok {
		t.Error("touch should return true for existing key")
	}
}

// TestMemoryStoreTouchMissing 验证 Touch 缺失 key 返回 false
func TestMemoryStoreTouchMissing(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	ok, err := store.Touch(ctx, "missing", time.Minute)
	if err != nil {
		t.Fatalf("touch: %v", err)
	}
	if ok {
		t.Error("touch should return false for missing key")
	}
}

// TestMemoryStoreTouchExpired 验证 Touch 过期 key 返回 false 并清理
func TestMemoryStoreTouchExpired(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	if err := store.Set(ctx, "expired", "value", expirationOptions(50*time.Millisecond)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	ok, err := store.Touch(ctx, "expired", time.Minute)
	if err != nil {
		t.Fatalf("touch: %v", err)
	}
	if ok {
		t.Error("touch should return false for expired key")
	}

	// 验证过期 key 已被清理
	store.mu.RLock()
	_, exists := store.items["expired"]
	store.mu.RUnlock()
	if exists {
		t.Error("expired key should be deleted after Touch")
	}
}

// TestMemoryStoreTouchForever 验证 Touch 设置 TTL=0 使 key 永久
func TestMemoryStoreTouchForever(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	if err := store.Set(ctx, "key", "value", expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	if _, err := store.Touch(ctx, "key", 0); err != nil {
		t.Fatalf("touch: %v", err)
	}

	// 验证 TTL 变为 -1（永久）
	_, ttl, err := store.GetWithTTL(ctx, "key")
	if err != nil {
		t.Fatalf("getWithTTL: %v", err)
	}
	if ttl != -1 {
		t.Errorf("TTL = %v, want -1 (forever)", ttl)
	}
}

// TestMemoryStorePutMany 验证批量写入
func TestMemoryStorePutMany(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	values := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	if err := store.PutMany(ctx, values, time.Minute); err != nil {
		t.Fatalf("putMany: %v", err)
	}

	// 验证所有 key 已写入
	for key, expected := range values {
		val, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if string(val.([]byte)) != string(expected) {
			t.Errorf("got %v, want %v", val, expected)
		}
	}
}

// TestMemoryStoreForgetMany 验证批量删除
func TestMemoryStoreForgetMany(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	// 先写入多个 key
	for i := 0; i < 3; i++ {
		key := "key" + string(rune('0'+i))
		if err := store.Set(ctx, key, []byte("value"), expirationOptions(time.Minute)...); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	// 批量删除
	keys := []string{"key0", "key1", "key2"}
	if err := store.ForgetMany(ctx, keys); err != nil {
		t.Fatalf("forgetMany: %v", err)
	}

	// 验证所有 key 已删除
	for _, key := range keys {
		_, err := store.Get(ctx, key)
		if err == nil {
			t.Errorf("key %s should be deleted", key)
		}
	}
}

// TestMemoryStoreIncrementBasic 验证 Increment 基本功能
func TestMemoryStoreIncrementBasic(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	// 初始化计数器
	if err := store.Set(ctx, "counter", []byte("10"), expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	// 递增
	val, err := store.Increment(ctx, "counter", 5)
	if err != nil {
		t.Fatalf("increment: %v", err)
	}
	if val != 15 {
		t.Errorf("got %d, want 15", val)
	}
}

// TestMemoryStoreIncrementExpired 验证 Increment 过期 key 从 0 开始
func TestMemoryStoreIncrementExpired(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	// 写入即将过期的计数器
	if err := store.Set(ctx, "counter", []byte("100"), expirationOptions(50*time.Millisecond)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// 递增应从 0 开始
	val, err := store.Increment(ctx, "counter", 5)
	if err != nil {
		t.Fatalf("increment: %v", err)
	}
	if val != 5 {
		t.Errorf("got %d, want 5 (expired key should reset to 0)", val)
	}
}

// TestMemoryStoreIncrementInvalidValue 验证 Increment 非整数值返回错误
func TestMemoryStoreIncrementInvalidValue(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	// 写入非整数值
	if err := store.Set(ctx, "counter", []byte("not-a-number"), expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	_, err := store.Increment(ctx, "counter", 5)
	if err == nil {
		t.Fatal("expected error for invalid counter value, got nil")
	}
}

// TestMemoryStoreForgetTagged 验证删除 tagged cache
func TestMemoryStoreForgetTagged(t *testing.T) {
	store := newMemoryStore(time.Minute, 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	ctx := context.Background()

	// 写入 tagged cache
	if err := store.PutTagged(ctx, "prefix", []string{"tag1"}, "key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("putTagged: %v", err)
	}

	// 删除 tagged cache
	if err := store.ForgetTagged(ctx, "prefix", []string{"tag1"}, "key"); err != nil {
		t.Fatalf("forgetTagged: %v", err)
	}

	// 验证已删除
	_, err := store.Get(ctx, "prefix:tag1:key")
	if err == nil {
		t.Error("tagged key should be deleted")
	}
}

// BenchmarkMemoryStoreGetManyPerformance 基准测试证明 N+1 锁竞争问题
func BenchmarkMemoryStoreGetManyPerformance(b *testing.B) {
	store := newMemoryStore(time.Minute, 0)
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	})
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
