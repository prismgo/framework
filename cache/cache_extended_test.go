package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
)

// TestMemoryHasForeverAddCountersAndPull 覆盖 Laravel 风格新增基础接口。
func TestMemoryHasForeverAddCountersAndPull(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Prefix:  "test",
		Stores: map[string]StoreConfig{
			"memory": {
				Driver:          "memory",
				DefaultTTL:      20 * time.Millisecond,
				CleanupInterval: time.Millisecond,
			},
		},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)

	has, err := Has(ctx, "site.name")
	if err != nil {
		t.Fatalf("has missing failed: %v", err)
	}
	if has {
		t.Fatal("missing key should not exist")
	}

	if err := Forever(ctx, "site.name", "My App"); err != nil {
		t.Fatalf("forever failed: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	has, err = Has(ctx, "site.name")
	if err != nil {
		t.Fatalf("has forever failed: %v", err)
	}
	if !has {
		t.Fatal("forever key should ignore store default ttl")
	}

	ok, err := Add(ctx, "send:sms:13800138000", 1, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("add first failed: %v", err)
	}
	if !ok {
		t.Fatal("first add should write")
	}
	ok, err = Add(ctx, "send:sms:13800138000", 2, time.Second)
	if err != nil {
		t.Fatalf("add second failed: %v", err)
	}
	if ok {
		t.Fatal("second add should not overwrite existing key")
	}
	addedValue, err := Get[int](ctx, "send:sms:13800138000")
	if err != nil {
		t.Fatalf("get added value failed: %v", err)
	}
	if addedValue != 1 {
		t.Fatalf("added value = %d, want 1", addedValue)
	}
	time.Sleep(40 * time.Millisecond)
	ok, err = Add(ctx, "send:sms:13800138000", 3, time.Second)
	if err != nil {
		t.Fatalf("add after expiration failed: %v", err)
	}
	if !ok {
		t.Fatal("add should write after key expires")
	}

	count, err := Increment(ctx, "page.views")
	if err != nil {
		t.Fatalf("increment default failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("increment default = %d, want 1", count)
	}
	count, err = Increment(ctx, "page.views", 5)
	if err != nil {
		t.Fatalf("increment delta failed: %v", err)
	}
	if count != 6 {
		t.Fatalf("increment delta = %d, want 6", count)
	}
	count, err = Decrement(ctx, "page.views")
	if err != nil {
		t.Fatalf("decrement default failed: %v", err)
	}
	if count != 5 {
		t.Fatalf("decrement default = %d, want 5", count)
	}
	count, err = Decrement(ctx, "page.views", 2)
	if err != nil {
		t.Fatalf("decrement delta failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("decrement delta = %d, want 3", count)
	}

	if err := Put(ctx, "bad.counter", "not-number", time.Minute); err != nil {
		t.Fatalf("put bad counter failed: %v", err)
	}
	if _, err := Increment(ctx, "bad.counter"); !errors.Is(err, ErrInvalidCounter) {
		t.Fatalf("bad counter err = %v, want ErrInvalidCounter", err)
	}

	if err := Put(ctx, "once_token", "token-value", time.Minute); err != nil {
		t.Fatalf("put token failed: %v", err)
	}
	token, err := Pull[string](ctx, "once_token")
	if err != nil {
		t.Fatalf("pull token failed: %v", err)
	}
	if token != "token-value" {
		t.Fatalf("pull token = %q, want token-value", token)
	}
	has, err = Has(ctx, "once_token")
	if err != nil {
		t.Fatalf("has after pull failed: %v", err)
	}
	if has {
		t.Fatal("pull should delete key")
	}
	fallback, err := Pull[string](ctx, "once_token", Value("fallback"))
	if err != nil {
		t.Fatalf("pull fallback failed: %v", err)
	}
	if fallback != "fallback" {
		t.Fatalf("pull fallback = %q, want fallback", fallback)
	}
}

func TestMemoryStoreCloseIsIdempotent(t *testing.T) {
	store := newMemoryStore(time.Minute, time.Millisecond)

	if err := store.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestRedisAddCountersAndPull 使用 miniredis 验证 Redis store 的新增原子操作。
func TestRedisAddCountersAndPull(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis failed: %v", err)
	}
	t.Cleanup(srv.Close)
	useRedisLifecycleManager(t, srv.Addr())

	m, err := NewManager(Config{
		Default: "redis",
		Prefix:  "test",
		Stores: map[string]StoreConfig{
			"redis": {
				Driver: "redis",
				Redis:  RedisConfig{Connection: "default"},
			},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)

	ctx := context.Background()
	ok, err := Add(ctx, "dedupe", "first", time.Minute)
	if err != nil {
		t.Fatalf("redis add first failed: %v", err)
	}
	if !ok {
		t.Fatal("redis first add should write")
	}
	ok, err = Add(ctx, "dedupe", "second", time.Minute)
	if err != nil {
		t.Fatalf("redis add second failed: %v", err)
	}
	if ok {
		t.Fatal("redis second add should not overwrite existing key")
	}
	value, err := Get[string](ctx, "dedupe")
	if err != nil {
		t.Fatalf("redis get added failed: %v", err)
	}
	if value != "first" {
		t.Fatalf("redis added value = %q, want first", value)
	}

	count, err := Increment(ctx, "views", 5)
	if err != nil {
		t.Fatalf("redis increment failed: %v", err)
	}
	if count != 5 {
		t.Fatalf("redis increment = %d, want 5", count)
	}
	count, err = Decrement(ctx, "views", 2)
	if err != nil {
		t.Fatalf("redis decrement failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("redis decrement = %d, want 3", count)
	}

	if err := Put(ctx, "once", map[string]string{"token": "abc"}, time.Minute); err != nil {
		t.Fatalf("redis put pull value failed: %v", err)
	}
	pulled, err := Pull[map[string]string](ctx, "once")
	if err != nil {
		t.Fatalf("redis pull failed: %v", err)
	}
	if pulled["token"] != "abc" {
		t.Fatalf("redis pulled = %#v", pulled)
	}
	if _, err := Pull[map[string]string](ctx, "once"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("redis second pull err = %v, want ErrCacheMiss", err)
	}

	if err := Put(ctx, "flush-me", "gone", time.Minute); err != nil {
		t.Fatalf("redis put flush value: %v", err)
	}
	if err := srv.Set("outside", "keep"); err != nil {
		t.Fatalf("redis set outside value: %v", err)
	}
	if err := Flush(ctx); err != nil {
		t.Fatalf("redis flush: %v", err)
	}
	if _, err := Get[string](ctx, "flush-me"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("redis flushed key err = %v, want ErrCacheMiss", err)
	}
	if outside, err := srv.Get("outside"); err != nil || outside != "keep" {
		t.Fatalf("redis outside value = %q err=%v, want keep", outside, err)
	}
}

func TestRedisTouchPersistReportsExistingPermanentKey(t *testing.T) {
	srv := miniredis.RunT(t)
	useRedisLifecycleManager(t, srv.Addr())

	m, err := NewManager(Config{
		Default: "redis",
		Prefix:  "touch",
		Stores: map[string]StoreConfig{
			"redis": {Driver: "redis", Redis: RedisConfig{Connection: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	repo := m.defaultRepository()
	if err := repo.Forever(ctx, "permanent", "value"); err != nil {
		t.Fatalf("redis forever: %v", err)
	}
	ok, err := repo.Touch(ctx, "permanent", 0)
	if err != nil || !ok {
		t.Fatalf("touch permanent key ok=%v err=%v, want true nil", ok, err)
	}
	ok, err = repo.Touch(ctx, "missing", 0)
	if err != nil || ok {
		t.Fatalf("touch missing key ok=%v err=%v, want false nil", ok, err)
	}
}

func TestRedisTaggedIndexPrunesExpiredMembers(t *testing.T) {
	srv := miniredis.RunT(t)
	useRedisLifecycleManager(t, srv.Addr())

	m, err := NewManager(Config{
		Default: "redis",
		Prefix:  "tag-prune",
		Stores: map[string]StoreConfig{
			"redis": {Driver: "redis", Redis: RedisConfig{Connection: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	repo := m.defaultRepository()
	tagged := repo.Tags("reports")
	if err := tagged.Put(ctx, "old", "old-value", 10*time.Millisecond); err != nil {
		t.Fatalf("put old tagged key: %v", err)
	}
	oldDataKey := taggedDataKey(repo.prefix, []string{"reports"}, "old")
	newDataKey := taggedDataKey(repo.prefix, []string{"reports"}, "new")
	indexKey := tagIndexKey(repo.prefix, "reports")
	time.Sleep(20 * time.Millisecond)

	if err := tagged.Put(ctx, "new", "new-value", time.Minute); err != nil {
		t.Fatalf("put new tagged key: %v", err)
	}
	redisStore := repo.store.(*redisStore)
	members, err := redisStore.client.ZRange(ctx, indexKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read redis tag index zset: %v", err)
	}
	if containsString(members, oldDataKey) {
		t.Fatalf("tag index members = %#v, should not contain expired key %q", members, oldDataKey)
	}
	if !containsString(members, newDataKey) {
		t.Fatalf("tag index members = %#v, want new key %q", members, newDataKey)
	}

	if err := tagged.Flush(ctx); err != nil {
		t.Fatalf("flush tagged key: %v", err)
	}
	if _, err := tagged.Get(ctx, "new"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("new tagged key after flush err=%v, want miss", err)
	}
	if exists, err := redisStore.client.Exists(ctx, indexKey).Result(); err != nil || exists != 0 {
		t.Fatalf("tag index exists=%d err=%v, want deleted", exists, err)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
