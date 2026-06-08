package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	prismredis "github.com/prismgo/framework/redis"
)

// newTestManager 创建只包含 memory store 的隔离测试 Manager。
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager(Config{
		Default:  "memory",
		Encoding: "json",
		Prefix:   "test",
		Stores: map[string]StoreConfig{
			"memory": {
				Driver:          "memory",
				CleanupInterval: time.Millisecond,
			},
		},
		Lock: LockConfig{RetrySleep: time.Millisecond},
		Flexible: FlexibleConfig{
			RefreshTimeout: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func useCacheTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func bindCacheManagerForTest(t *testing.T, manager *Manager) *container.Container {
	t.Helper()
	redisManager := container.Value[*prismredis.Manager]("redis")
	registry := useCacheTestContainer(t)
	if redisManager != nil {
		if err := registry.Instance("redis", redisManager, prismredis.ManagerCloseOption()); err != nil {
			t.Fatalf("rebind redis manager: %v", err)
		}
	}
	if manager != nil {
		if err := registry.Instance(serviceKey, manager); err != nil {
			t.Fatalf("bind cache manager: %v", err)
		}
	}
	return registry
}

func registerCacheFactoryForTest(t *testing.T, registry *container.Container, factory func() (*Manager, error)) {
	t.Helper()
	if registry == nil {
		registry = useCacheTestContainer(t)
	}
	if factory == nil {
		registry.Forget(serviceKey)
		return
	}
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return factory()
	}); err != nil {
		t.Fatalf("register cache factory: %v", err)
	}
}

func bindConfigForCacheTest(t *testing.T, cfg *configpkg.Config) *container.Container {
	t.Helper()
	registry := useCacheTestContainer(t)
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	return registry
}

// TestMemoryPutGetDefaultRememberForget 覆盖 memory store 的基础读写、默认值和删除能力。
func TestMemoryPutGetDefaultRememberForget(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	bindCacheManagerForTest(t, m)

	if err := Put(ctx, "name", "alice", time.Second); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	got, err := Get[string](ctx, "name")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got != "alice" {
		t.Fatalf("get = %q, want alice", got)
	}

	missing, err := Get[int](ctx, "missing", Value(7))
	if err != nil {
		t.Fatalf("get default failed: %v", err)
	}
	if missing != 7 {
		t.Fatalf("default = %d, want 7", missing)
	}

	var loaded int32
	lazy, err := Get[int](ctx, "missing.lazy", Lazy(func(context.Context) (int, error) {
		atomic.AddInt32(&loaded, 1)
		return 9, nil
	}))
	if err != nil {
		t.Fatalf("lazy default failed: %v", err)
	}
	if lazy != 9 || atomic.LoadInt32(&loaded) != 1 {
		t.Fatalf("lazy = %d loaded=%d, want 9/1", lazy, loaded)
	}
	if _, err := Get[int](ctx, "missing.lazy"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("lazy default should not write cache, got err %v", err)
	}

	remembered, err := Remember(ctx, "answer", time.Second, func(context.Context) (int, error) {
		atomic.AddInt32(&loaded, 1)
		return 42, nil
	})
	if err != nil {
		t.Fatalf("remember failed: %v", err)
	}
	if remembered != 42 {
		t.Fatalf("remember = %d, want 42", remembered)
	}
	remembered, err = Remember(ctx, "answer", time.Second, func(context.Context) (int, error) {
		return 100, nil
	})
	if err != nil {
		t.Fatalf("remember cached failed: %v", err)
	}
	if remembered != 42 {
		t.Fatalf("remember cached = %d, want 42", remembered)
	}

	if err := Forget(ctx, "answer"); err != nil {
		t.Fatalf("forget failed: %v", err)
	}
	if _, err := Get[int](ctx, "answer"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("forgotten key err = %v, want cache miss", err)
	}
}

// TestTouchExtendsTTLWithoutRewritingValue 验证 Touch 只延长 TTL，不改变原缓存值。
func TestTouchExtendsTTLWithoutRewritingValue(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	bindCacheManagerForTest(t, m)

	if err := Put(ctx, "hot", "value", 30*time.Millisecond); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	ok, err := Touch(ctx, "hot", 80*time.Millisecond)
	if err != nil {
		t.Fatalf("touch failed: %v", err)
	}
	if !ok {
		t.Fatal("touch should report existing key")
	}
	time.Sleep(35 * time.Millisecond)
	got, err := Get[string](ctx, "hot")
	if err != nil {
		t.Fatalf("get after touch failed: %v", err)
	}
	if got != "value" {
		t.Fatalf("get after touch = %q, want value", got)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := Get[string](ctx, "hot"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("expired key err = %v, want cache miss", err)
	}
}

// TestFlexibleReturnsStaleThenRefreshesAfterDeferredRun 验证 stale 期先返回旧值再延后刷新。
func TestFlexibleReturnsStaleThenRefreshesAfterDeferredRun(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	var calls int32
	loader := func(context.Context) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		return string(rune('a' + n - 1)), nil
	}

	got, err := flexibleTyped(ctx, repo, "flex", FlexibleWindow{Fresh: 20 * time.Millisecond, Stale: time.Second}, loader)
	if err != nil {
		t.Fatalf("initial flexible failed: %v", err)
	}
	if got != "a" {
		t.Fatalf("initial flexible = %q, want a", got)
	}
	time.Sleep(30 * time.Millisecond)

	deferredCtx, runDeferred := WithDeferred(ctx)
	got, err = flexibleTyped(deferredCtx, repo, "flex", FlexibleWindow{Fresh: 20 * time.Millisecond, Stale: time.Second}, loader)
	if err != nil {
		t.Fatalf("stale flexible failed: %v", err)
	}
	if got != "a" {
		t.Fatalf("stale flexible = %q, want old value a", got)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("loader should not run before deferred queue, calls=%d", calls)
	}

	runDeferred()
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&calls) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got, err = flexibleTyped(ctx, repo, "flex", FlexibleWindow{Fresh: time.Second, Stale: 2 * time.Second}, loader)
	if err != nil {
		t.Fatalf("fresh after refresh failed: %v", err)
	}
	if got != "b" {
		t.Fatalf("fresh after refresh = %q, want b", got)
	}
}

func TestRememberAndFlexibleErrorBranches(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	if _, err := rememberTyped(ctx, repo, "remember-error", time.Minute, func(context.Context) (string, error) {
		return "", errors.New("loader failed")
	}); err == nil {
		t.Fatal("expected remember loader error")
	}
	if _, err := refreshFlexibleTyped(ctx, repo, "flex-loader-error", FlexibleWindow{Fresh: time.Second, Stale: time.Second}, func(context.Context) (string, error) {
		return "", errors.New("loader failed")
	}); err == nil {
		t.Fatal("expected flexible loader error")
	}
	if _, err := refreshFlexibleTyped(ctx, repo, "flex-encode-error", FlexibleWindow{Fresh: time.Second, Stale: time.Second}, func(context.Context) (func(), error) {
		return func() {}, nil
	}); err == nil {
		t.Fatal("expected flexible encode error")
	}
}

// TestLockGetReleaseAndBlock 覆盖 memory 锁的显式释放和阻塞等待用法。
func TestLockGetReleaseAndBlock(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	lock := repo.Lock("foo", time.Second)
	ok, err := lock.Get(ctx)
	if err != nil {
		t.Fatalf("lock get failed: %v", err)
	}
	if !ok {
		t.Fatal("expected first lock acquisition")
	}

	other := repo.Lock("foo", time.Second)
	ok, err = other.Get(ctx)
	if err != nil {
		t.Fatalf("second lock get failed: %v", err)
	}
	if ok {
		t.Fatal("second lock should not acquire while first is held")
	}

	if err := lock.Release(ctx); err != nil {
		t.Fatalf("release failed: %v", err)
	}
	ok, err = other.Get(ctx)
	if err != nil {
		t.Fatalf("lock after release failed: %v", err)
	}
	if !ok {
		t.Fatal("expected lock after explicit release")
	}
	if err := other.Release(ctx); err != nil {
		t.Fatalf("release other failed: %v", err)
	}

	restored := repo.Lock("restore", time.Second)
	ok, err = restored.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("restore lock get failed: ok=%v err=%v", ok, err)
	}
	owner := restored.Owner()
	if owner == "" {
		t.Fatal("restore lock owner should not be empty")
	}
	if err := repo.RestoreLock("restore", owner).Release(ctx); err != nil {
		t.Fatalf("restored lock release failed: %v", err)
	}
	ok, err = repo.Lock("restore", time.Second).Get(ctx)
	if err != nil || !ok {
		t.Fatalf("restore lock reacquire failed: ok=%v err=%v", ok, err)
	}

	ok, err = repo.Lock("bar", time.Second).Block(ctx, 50*time.Millisecond, func(context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("block failed: %v", err)
	}
	if !ok {
		t.Fatal("block should acquire")
	}
}

func TestLockFailureAndForceReleaseBranches(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	lock := repo.Lock("force-memory", time.Second)
	ok, err := lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("memory force lock get: ok=%v err=%v", ok, err)
	}
	blocked := repo.Lock("force-memory", time.Second)
	if ok, err = blocked.Get(ctx); err != nil || ok {
		t.Fatalf("memory blocked lock get: ok=%v err=%v", ok, err)
	}
	if err := lock.ForceRelease(ctx); err != nil {
		t.Fatalf("memory force release: %v", err)
	}
	if ok, err = blocked.Get(ctx); err != nil || !ok {
		t.Fatalf("memory lock after force release: ok=%v err=%v", ok, err)
	}

	if err := repo.Lock("not-held", time.Second).Release(ctx); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("release not held err = %v, want ErrLockNotHeld", err)
	}
	if ok, err := repo.Lock("callback-error", time.Second).Get(ctx, func(context.Context) error {
		return errors.New("callback failed")
	}); err == nil || !ok {
		t.Fatalf("callback lock err=%v ok=%v, want callback error after lock", err, ok)
	}

	held := repo.Lock("cancel-block", time.Second)
	if ok, err = held.Get(ctx); err != nil || !ok {
		t.Fatalf("cancel block held lock: ok=%v err=%v", ok, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	ok, err = repo.Lock("cancel-block", time.Second).
		BetweenBlockedAttemptsSleepFor(time.Millisecond).
		Block(canceled, time.Second, nil)
	if err == nil || ok {
		t.Fatalf("cancel block result ok=%v err=%v, want context error", ok, err)
	}
}

// TestRedisStorePutTouchAndLockRelease 使用 miniredis 验证 Redis store 的读写、Touch 和锁释放。
func TestRedisStorePutTouchAndLockRelease(t *testing.T) {
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
		Lock: LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new redis manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)

	ctx := context.Background()
	if err := Put(ctx, "redis-key", map[string]string{"name": "alice"}, time.Second); err != nil {
		t.Fatalf("redis put failed: %v", err)
	}
	got, err := Get[map[string]string](ctx, "redis-key")
	if err != nil {
		t.Fatalf("redis get failed: %v", err)
	}
	if got["name"] != "alice" {
		t.Fatalf("redis value = %#v", got)
	}
	ok, err := Touch(ctx, "redis-key", 2*time.Second)
	if err != nil {
		t.Fatalf("redis touch failed: %v", err)
	}
	if !ok {
		t.Fatal("redis touch should report existing key")
	}

	lock := Lock("redis-lock", time.Second)
	ok, err = lock.Get(ctx)
	if err != nil {
		t.Fatalf("redis lock get failed: %v", err)
	}
	if !ok {
		t.Fatal("expected redis lock")
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("redis lock release failed: %v", err)
	}
	ok, err = Lock("redis-lock", time.Second).Get(ctx)
	if err != nil {
		t.Fatalf("redis lock reacquire failed: %v", err)
	}
	if !ok {
		t.Fatal("expected redis lock reacquire after release")
	}
}
