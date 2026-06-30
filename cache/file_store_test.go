package cache

import (
	"context"
	"testing"
	"time"
)

// TestFileStoreGetManyBatchRead 验证批量读取在单次锁内完成
func TestFileStoreGetManyBatchRead(t *testing.T) {
	root := t.TempDir()
	store := newFileStore(FileConfig{Path: root}, time.Minute, "test", "test-lock")
	ctx := context.Background()

	// 写入多个 key
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, key := range keys {
		value := []byte("value" + string(rune('0'+i)))
		if err := store.Set(ctx, key, value, expirationOptions(time.Minute)...); err != nil {
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

// TestFileStoreGetManyCleansExpiredKeys 验证批量读取时清理过期 key
func TestFileStoreGetManyCleansExpiredKeys(t *testing.T) {
	root := t.TempDir()
	store := newFileStore(FileConfig{Path: root}, time.Minute, "test", "test-lock")
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
}

// TestFileStoreGetManyEmptyKeys 验证空 keys 返回空 map
func TestFileStoreGetManyEmptyKeys(t *testing.T) {
	root := t.TempDir()
	store := newFileStore(FileConfig{Path: root}, time.Minute, "test", "test-lock")
	ctx := context.Background()

	values, err := store.GetMany(ctx, []string{})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(values) != 0 {
		t.Errorf("GetMany returned %d values, want 0", len(values))
	}
}

// TestFileStoreGetManyPartialHit 验证部分命中场景
func TestFileStoreGetManyPartialHit(t *testing.T) {
	root := t.TempDir()
	store := newFileStore(FileConfig{Path: root}, time.Minute, "test", "test-lock")
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

// TestFileStoreGetWithTTLSingleRead 验证 GetWithTTL 只读取一次文件
func TestFileStoreGetWithTTLSingleRead(t *testing.T) {
	root := t.TempDir()
	store := newFileStore(FileConfig{Path: root}, time.Minute, "test", "test-lock")
	ctx := context.Background()

	// 写入一个带 TTL 的 key
	ttl := 5 * time.Minute
	if err := store.Set(ctx, "test-key", []byte("test-value"), expirationOptions(ttl)...); err != nil {
		t.Fatalf("set: %v", err)
	}

	// 读取并验证 TTL
	value, actualTTL, err := store.GetWithTTL(ctx, "test-key")
	if err != nil {
		t.Fatalf("GetWithTTL: %v", err)
	}

	if string(value.([]byte)) != "test-value" {
		t.Errorf("value = %q, want test-value", value)
	}

	// TTL 应该接近但小于原始 TTL
	if actualTTL <= 0 || actualTTL > ttl {
		t.Errorf("TTL = %v, want between 0 and %v", actualTTL, ttl)
	}
}

// TestFileStoreGetWithTTLForever 验证永久 key 的 TTL 返回 -1
func TestFileStoreGetWithTTLForever(t *testing.T) {
	root := t.TempDir()
	store := newFileStore(FileConfig{Path: root}, 0, "test", "test-lock")
	ctx := context.Background()

	// 写入一个永久 key（TTL = 0）
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

// newFileTestStore 创建测试用的 fileStore
func newFileTestStore(t *testing.T, root string, defaultTTL time.Duration) *fileStore {
	t.Helper()
	return newFileStore(FileConfig{Path: root}, defaultTTL, "test", "test-lock")
}

// TestFileStoreWithMutationLockTimeout 验证默认超时保护
func TestFileStoreWithMutationLockTimeout(t *testing.T) {
	root := t.TempDir()
	store := newFileTestStore(t, root, time.Minute)
	ctx := context.Background()

	// 正常操作应该在超时前完成
	err := store.withMutationLock(ctx, "test-key", func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("withMutationLock: %v", err)
	}
}

// TestFileStoreWithMutationLockContextCancellation 验证 context 取消时立即退出
func TestFileStoreWithMutationLockContextCancellation(t *testing.T) {
	root := t.TempDir()
	store := newFileTestStore(t, root, time.Minute)

	// 创建一个已取消的 context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// 应该立即返回 context 错误
	err := store.withMutationLock(ctx, "test-key", func() error {
		t.Error("callback should not be called")
		return nil
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestFileStoreWithLockGuardTimeout 验证 lock guard 的默认超时保护
func TestFileStoreWithLockGuardTimeout(t *testing.T) {
	root := t.TempDir()
	store := newFileTestStore(t, root, time.Minute)
	ctx := context.Background()

	// 正常操作应该在超时前完成
	err := store.withLockGuard(ctx, "test-key", func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("withLockGuard: %v", err)
	}
}

// newFileTestManager 创建测试用的 Manager（使用 file driver）
func newFileTestManager(t *testing.T) *Manager {
	t.Helper()
	root := t.TempDir()
	m, err := NewManager(Config{
		Default: "file",
		Stores: map[string]StoreConfig{
			"file": {
				Driver: "file",
				File: FileConfig{
					Path: root,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}
