package cache

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	libstore "github.com/eko/gocache/lib/v4/store"
	"github.com/gin-gonic/gin"
	configpkg "github.com/prismgo/framework/config"
)

type stubStore struct {
	value   any
	clear   bool
	missing bool
}

func (s *stubStore) Get(ctx context.Context, key any) (any, error) {
	_ = ctx
	_ = key
	if s.missing {
		return nil, libstore.NotFoundWithCause(ErrCacheMiss)
	}
	return s.value, nil
}

func (s *stubStore) GetWithTTL(ctx context.Context, key any) (any, time.Duration, error) {
	value, err := s.Get(ctx, key)
	return value, -1, err
}

func (s *stubStore) Set(ctx context.Context, key any, value any, options ...libstore.Option) error {
	_ = ctx
	_ = key
	_ = options
	s.value = value
	return nil
}

func (s *stubStore) Delete(ctx context.Context, key any) error {
	_ = ctx
	_ = key
	s.missing = true
	return nil
}

func (s *stubStore) Invalidate(ctx context.Context, options ...libstore.InvalidateOption) error {
	_ = ctx
	_ = options
	return nil
}

func (s *stubStore) Clear(ctx context.Context) error {
	_ = ctx
	s.clear = true
	return nil
}

func (s *stubStore) GetType() string {
	return "stub"
}

func TestFacadeLazyFactoryAndStoreHelpers(t *testing.T) {
	registry := useCacheTestContainer(t)

	calls := 0
	registerCacheFactoryForTest(t, registry, func() (*Manager, error) {
		calls++
		return NewManager(Config{
			Default: "memory",
			Prefix:  "facade",
			Stores: map[string]StoreConfig{
				"memory": {Driver: "memory", CleanupInterval: time.Millisecond},
			},
		})
	})

	if Resolve() == nil {
		t.Fatal("resolve lazy manager returned nil")
	}
	if !registry.Resolved(serviceKey) || calls != 1 {
		t.Fatalf("expected Resolve to register manager once, calls=%d", calls)
	}
	if DefaultName() != "memory" {
		t.Fatalf("expected facade DefaultName memory, got %s", DefaultName())
	}
	ctx := context.Background()
	if err := Put(ctx, "lazy", "loaded", time.Minute); err != nil {
		t.Fatalf("put via lazy facade: %v", err)
	}
	got, err := Get[string](ctx, "lazy")
	if err != nil {
		t.Fatalf("get via lazy facade: %v", err)
	}
	if got != "loaded" || calls != 1 {
		t.Fatalf("expected lazy value/call count, got value=%q calls=%d", got, calls)
	}
	if Store("").Name() != Default().Name() {
		t.Fatal("expected blank store name to resolve default repository")
	}
	if Name() != Default().Name() {
		t.Fatal("expected facade Name to return default repository name")
	}
	if err := Close(); err != nil {
		t.Fatalf("facade Close failed: %v", err)
	}
}

func TestFacadePanicsWhenFactoryFails(t *testing.T) {
	registry := useCacheTestContainer(t)

	registerCacheFactoryForTest(t, registry, func() (*Manager, error) {
		return nil, errors.New("factory failed")
	})

	assertPanics(t, func() { _ = Resolve() })
}

func TestApplicationManagerAndServiceProvider(t *testing.T) {
	registry := useCacheTestContainer(t)

	configpkg.Add("cache", func() map[string]any {
		fileRoot := filepath.Join(t.TempDir(), "cache")
		return map[string]any{
			"default": "memory",
			"prefix":  "app",
			"lock": map[string]any{
				"prefix":         "cache_locks",
				"retry_sleep_ms": 2,
			},
			"flexible": map[string]any{"refresh_timeout": 1},
			"stores": map[string]any{
				"memory": map[string]any{
					"driver":           "memory",
					"prefix":           "users",
					"default_ttl":      "60",
					"cleanup_interval": float64(1),
				},
				"file": map[string]any{
					"driver":    "file",
					"prefix":    "files",
					"path":      filepath.Join(fileRoot, "data"),
					"lock_path": filepath.Join(fileRoot, "locks"),
				},
				"ignored": "not-a-map",
			},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	closer, manager, err := NewManagerFromConfig()
	if err != nil {
		t.Fatalf("new application cache manager: %v", err)
	}
	if closer == nil || manager == nil {
		t.Fatalf("expected closer and manager, got closer nil=%v manager=%v", closer == nil, manager)
	}
	if manager.DefaultName() != "memory" {
		t.Fatalf("default store = %s, want memory", manager.DefaultName())
	}
	if manager.specs["memory"].Options["prefix"] != "users" {
		t.Fatalf("expected raw store options to include prefix, got %#v", manager.specs["memory"].Options)
	}
	if err := manager.storeRepository("file").Put(context.Background(), "boot", "ok", time.Minute); err != nil {
		t.Fatalf("application file store put: %v", err)
	}
	if err := closer(); err != nil {
		t.Fatalf("close application cache manager: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}
	resolved := Resolve()
	if resolved == nil || resolved.DefaultName() != "memory" {
		t.Fatalf("unexpected registered cache manager: %#v", resolved)
	}
}

func TestApplicationManagerRejectsEmptyStores(t *testing.T) {
	registry := useCacheTestContainer(t)
	if err := registry.Instance("config.default", configpkg.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	_, _, err := NewManagerFromConfig()
	if err == nil {
		t.Fatal("expected empty cache.stores error")
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}
}

func TestDeferredRunsQueuedTasks(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	done := make(chan struct{}, 1)
	router.Use(func(c *gin.Context) {
		ctx, run := WithDeferred(c.Request.Context())
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		run()
	})
	router.GET("/deferred", func(c *gin.Context) {
		if q := deferredFromContext(c.Request.Context()); q == nil {
			t.Fatal("expected deferred queue in request context")
		} else {
			q.Push(func() { done <- struct{}{} })
		}
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/deferred", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deferred task was not executed")
	}
}

func TestRepositoryAnyHelpersAndErrorRepository(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	if err := repo.Put(ctx, "any", map[string]any{"name": "alice"}, time.Minute); err != nil {
		t.Fatalf("repo put: %v", err)
	}
	value, err := repo.Get(ctx, "any")
	if err != nil {
		t.Fatalf("repo get: %v", err)
	}
	if row, ok := value.(map[string]any); !ok || row["name"] != "alice" {
		t.Fatalf("unexpected repo get value: %#v", value)
	}

	fallbacks := []any{
		"literal",
		func() any { return "zero-arg" },
		func() (any, error) { return "zero-arg-error", nil },
		func(context.Context) any { return "ctx" },
		func(context.Context) (any, error) { return "ctx-error", nil },
	}
	for _, fallback := range fallbacks {
		got, err := repo.Get(ctx, "missing-any", fallback)
		if err != nil {
			t.Fatalf("repo get fallback %T: %v", fallback, err)
		}
		if got == nil {
			t.Fatalf("repo get fallback %T returned nil", fallback)
		}
	}

	remembered, err := repo.Remember(ctx, "remember-any", time.Minute, func(context.Context) (any, error) {
		return map[string]any{"ok": true}, nil
	})
	if err != nil {
		t.Fatalf("repo remember: %v", err)
	}
	if remembered == nil {
		t.Fatal("expected remembered value")
	}
	flexible, err := repo.Flexible(ctx, "flex-any", FlexibleWindow{Fresh: time.Minute, Stale: time.Minute}, func(context.Context) (any, error) {
		return "fresh", nil
	})
	if err != nil {
		t.Fatalf("repo flexible: %v", err)
	}
	if flexible != "fresh" {
		t.Fatalf("repo flexible = %v, want fresh", flexible)
	}
	if err := repo.Put(ctx, "pull-any", "once", time.Minute); err != nil {
		t.Fatalf("repo pull put: %v", err)
	}
	pulled, err := repo.Pull(ctx, "pull-any")
	if err != nil || pulled != "once" {
		t.Fatalf("repo pull = %v err=%v", pulled, err)
	}
	fallback, err := repo.Pull(ctx, "pull-any", "fallback")
	if err != nil || fallback != "fallback" {
		t.Fatalf("repo pull fallback = %v err=%v", fallback, err)
	}

	errRepo := m.storeRepository("missing-store")
	if err := errRepo.Put(ctx, "x", "y", time.Minute); err == nil {
		t.Fatal("expected error repository put failure")
	}
	if _, err := errRepo.Get(ctx, "x"); err == nil {
		t.Fatal("expected error repository get failure")
	}
	if _, err := errRepo.Pull(ctx, "x"); err == nil {
		t.Fatal("expected error repository pull failure")
	}
	if ok, err := errRepo.Lock("x", time.Second).Get(ctx); err == nil || ok {
		t.Fatalf("expected error lock failure, ok=%v err=%v", ok, err)
	}
	if err := errRepo.Lock("x", time.Second).ForceRelease(ctx); err == nil {
		t.Fatal("expected error lock force release failure")
	}
	if err := errRepo.Flush(ctx); err == nil {
		t.Fatal("expected error repository flush failure")
	}
}

func TestRepositoryUnsupportedStoreBranches(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	store := &stubStore{value: float64(12)}
	repo := newRepository("stub", "", store, nil, nil, nil, nil, nil, nil, nil, nil, "", m, true)

	value, err := repo.Get(ctx, "number")
	if err != nil || value.(float64) != 12 {
		t.Fatalf("stub repo get = %#v err=%v", value, err)
	}
	if _, err := repo.Add(ctx, "x", "y", time.Minute); err == nil {
		t.Fatal("expected unsupported add error")
	}
	if _, err := repo.Increment(ctx, "x"); err == nil {
		t.Fatal("expected unsupported increment error")
	}
	if _, err := repo.Pull(ctx, "x"); err == nil {
		t.Fatal("expected unsupported pull error")
	}
	if _, err := repo.Touch(ctx, "x", time.Minute); err == nil {
		t.Fatal("expected unsupported touch error")
	}
	if ok, err := repo.Lock("x", time.Second).Get(ctx); err == nil || ok {
		t.Fatalf("expected unsupported lock error, ok=%v err=%v", ok, err)
	}
	if err := repo.Flush(ctx); err != nil || !store.clear {
		t.Fatalf("stub flush clear=%v err=%v", store.clear, err)
	}
}

func TestFacadeLockFunnelAndFlexibleHelpers(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	bindCacheManagerForTest(t, m)

	flexible, err := Flexible(ctx, "facade-flex", FlexibleWindow{Fresh: time.Second, Stale: time.Second}, func(context.Context) (string, error) {
		return "fresh", nil
	})
	if err != nil || flexible != "fresh" {
		t.Fatalf("facade flexible = %q err=%v", flexible, err)
	}

	lock := Lock("facade-restore", time.Second).BetweenBlockedAttemptsSleepFor(time.Millisecond)
	ok, err := lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("facade lock get: ok=%v err=%v", ok, err)
	}
	if err := RestoreLock("facade-restore", lock.Owner()).Release(ctx); err != nil {
		t.Fatalf("facade restore release: %v", err)
	}

	held := Lock("funnel:facade", time.Second)
	if ok, err = held.Get(ctx); err != nil || !ok {
		t.Fatalf("facade funnel held lock: ok=%v err=%v", ok, err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = held.Release(context.Background())
	}()
	ran := false
	ok, err = Funnel("facade").
		ExpireAfter(time.Second).
		BlockFor(200*time.Millisecond).
		SleepFor(time.Millisecond).
		Then(ctx, func(context.Context) error {
			ran = true
			return nil
		})
	if err != nil || !ok || !ran {
		t.Fatalf("facade funnel block: ok=%v ran=%v err=%v", ok, ran, err)
	}
}

func TestMemoryStoreDirectBranches(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore(0, time.Millisecond)
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Set(ctx, "ttl", []byte(`"value"`)); err != nil {
		t.Fatalf("memory set: %v", err)
	}
	value, ttl, err := store.GetWithTTL(ctx, "ttl")
	if err != nil {
		t.Fatalf("memory get ttl: %v", err)
	}
	if string(value.([]byte)) != `"value"` || ttl != -1 {
		t.Fatalf("unexpected memory value/ttl: %v %s", value, ttl)
	}
	if err := store.Invalidate(ctx); err != nil {
		t.Fatalf("memory invalidate: %v", err)
	}
	if got := store.GetType(); got != memoryType {
		t.Fatalf("memory type = %s, want %s", got, memoryType)
	}
	if err := store.Clear(ctx); err != nil {
		t.Fatalf("memory clear: %v", err)
	}
	if _, err := store.Get(ctx, "ttl"); !isMiss(err) {
		t.Fatalf("expected miss after clear, got %v", err)
	}

	noJanitor := newMemoryStore(0, 0)
	if err := noJanitor.Close(); err != nil {
		t.Fatalf("memory close without janitor: %v", err)
	}
	if err := noJanitor.Set(ctx, 123, "bad"); err == nil {
		t.Fatal("expected non-string memory key set error")
	}
	if _, err := noJanitor.Get(ctx, 123); err == nil {
		t.Fatal("expected non-string memory key get error")
	}
	if err := noJanitor.Delete(ctx, 123); err == nil {
		t.Fatal("expected non-string memory key delete error")
	}
}

func TestRedisHelpersAndCounterErrors(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	useRedisLifecycleManager(t, srv.Addr())

	m, err := NewManager(Config{
		Default: "redis",
		Stores: map[string]StoreConfig{
			"redis": {Driver: "redis", Redis: RedisConfig{Connection: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager: %v", err)
	}
	repo := m.defaultRepository()
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	if err := repo.Put(ctx, "bad-counter", "not-number", time.Minute); err != nil {
		t.Fatalf("redis put bad counter: %v", err)
	}
	if _, err := repo.Increment(ctx, "bad-counter"); !errors.Is(err, ErrInvalidCounter) {
		t.Fatalf("redis invalid counter err = %v, want ErrInvalidCounter", err)
	}
	lock := repo.Lock("redis-extra", time.Second)
	ok, err := lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("redis lock get: ok=%v err=%v", ok, err)
	}
	owner := lock.Owner()
	if owner == "" {
		t.Fatal("expected redis lock owner")
	}
	if err := repo.RestoreLock("redis-extra", owner).Release(ctx); err != nil {
		t.Fatalf("redis restored lock release: %v", err)
	}
	ok, err = lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("redis lock reacquire: ok=%v err=%v", ok, err)
	}
	if err := lock.ForceRelease(ctx); err != nil {
		t.Fatalf("redis lock force release: %v", err)
	}

	if got := normalizeCounterError(nil); got != nil {
		t.Fatalf("nil counter error normalized to %v", got)
	}
}
