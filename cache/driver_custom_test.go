package cache

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type customLifecycleStore struct {
	*memoryStore
	closed atomic.Bool
}

func (s *customLifecycleStore) Close() error {
	s.closed.Store(true)
	return nil
}

type customPrefixFlusher struct {
	store  *memoryStore
	prefix string
	called atomic.Bool
}

func (f *customPrefixFlusher) Flush(ctx context.Context, prefix string) error {
	f.prefix = prefix
	f.called.Store(true)
	return f.store.Clear(ctx)
}

func TestCustomDriverExtendBuildsRepositoryWithOptionsAndCapabilities(t *testing.T) {
	ctx := context.Background()
	driverName := "custom-memory-driver"
	var calls atomic.Int32
	var captured StoreFactoryContext
	var builtStore *customLifecycleStore
	var flusher *customPrefixFlusher

	Extend(driverName, func(factoryCtx StoreFactoryContext) (StoreDriver, error) {
		calls.Add(1)
		captured = factoryCtx
		builtStore = &customLifecycleStore{memoryStore: newMemoryStore(0, 0)}
		flusher = &customPrefixFlusher{store: builtStore.memoryStore}
		driver := NewStoreDriver(builtStore)
		driver.Flush = flusher
		return driver, nil
	})

	m, err := NewManager(Config{
		Default:  "primary",
		Encoding: "json",
		Prefix:   "global",
		Stores: map[string]StoreConfig{
			"primary": {
				Driver:  "CUSTOM-MEMORY-DRIVER",
				Prefix:  ":tenant:",
				Options: map[string]any{"endpoint": "in-memory"},
			},
		},
		Lock: LockConfig{Prefix: "locks", RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new manager with custom driver: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	repo := m.defaultRepository()
	if calls.Load() != 1 {
		t.Fatalf("custom driver factory calls = %d, want 1", calls.Load())
	}
	if repo != m.storeRepository("primary") || calls.Load() != 1 {
		t.Fatalf("custom repository should be cached, calls=%d", calls.Load())
	}
	if captured.Name != "primary" || captured.Driver != driverName {
		t.Fatalf("unexpected custom context name/driver: %#v", captured)
	}
	if captured.GlobalPrefix != "global" || captured.StorePrefix != "tenant" {
		t.Fatalf("unexpected custom context prefixes: %#v", captured)
	}
	if captured.Prefix != "global:tenant" || captured.LockPrefix != "global:tenant:locks" {
		t.Fatalf("unexpected custom context full prefixes: %#v", captured)
	}
	if captured.Config.Options["endpoint"] != "in-memory" {
		t.Fatalf("custom options not passed: %#v", captured.Config.Options)
	}
	captured.Config.Options["endpoint"] = "mutated"
	if m.specs["primary"].Options["endpoint"] != "in-memory" {
		t.Fatalf("manager store config should not share factory context options: %#v", m.specs["primary"].Options)
	}

	if err := repo.Put(ctx, "profile", map[string]string{"name": "alice"}, time.Minute); err != nil {
		t.Fatalf("custom put: %v", err)
	}
	value, err := repo.Get(ctx, "profile")
	if err != nil {
		t.Fatalf("custom get: %v", err)
	}
	got, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("custom get type = %T, want map[string]any", value)
	}
	if got["name"] != "alice" {
		t.Fatalf("custom get value = %#v", got)
	}
	if !customStoreHasKey(builtStore.memoryStore, "global:tenant:profile") {
		t.Fatal("custom store should receive repository-prefixed key")
	}
	ok, err = repo.Touch(ctx, "profile", time.Minute)
	if err != nil || !ok {
		t.Fatalf("custom touch ok=%v err=%v", ok, err)
	}
	ok, err = repo.Add(ctx, "once", "first", time.Minute)
	if err != nil || !ok {
		t.Fatalf("custom add first ok=%v err=%v", ok, err)
	}
	ok, err = repo.Add(ctx, "once", "second", time.Minute)
	if err != nil || ok {
		t.Fatalf("custom add duplicate ok=%v err=%v", ok, err)
	}
	count, err := repo.Increment(ctx, "counter", 2)
	if err != nil || count != 2 {
		t.Fatalf("custom increment count=%d err=%v", count, err)
	}
	lock := repo.Lock("job", time.Second)
	ok, err = lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("custom lock ok=%v err=%v", ok, err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("custom lock release: %v", err)
	}
	if err := repo.Flush(ctx); err != nil {
		t.Fatalf("custom flush: %v", err)
	}
	if !flusher.called.Load() || flusher.prefix != "global:tenant" {
		t.Fatalf("custom prefix flusher called=%v prefix=%q", flusher.called.Load(), flusher.prefix)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("custom manager close: %v", err)
	}
	if !builtStore.closed.Load() {
		t.Fatal("custom close capability should be called")
	}
}

func TestCustomDriverUnknownNilAndIgnoredRegistrations(t *testing.T) {
	Extend("", func(StoreFactoryContext) (StoreDriver, error) {
		t.Fatal("empty custom driver registration should be ignored")
		return StoreDriver{}, nil
	})
	Extend("custom-ignored-nil", nil)

	unknown, err := NewManager(Config{
		Default: "custom",
		Stores: map[string]StoreConfig{
			"custom": {Driver: "custom-ignored-nil"},
		},
	})
	if err != nil {
		t.Fatalf("new manager with ignored custom driver: %v", err)
	}
	if err := unknown.defaultRepository().Put(context.Background(), "key", "value", time.Minute); err == nil || !strings.Contains(err.Error(), "unknown driver") {
		t.Fatalf("ignored custom driver err = %v, want unknown driver", err)
	}

	Extend("custom-nil-store", func(StoreFactoryContext) (StoreDriver, error) {
		return StoreDriver{}, nil
	})
	nilStore, err := NewManager(Config{
		Default: "custom",
		Stores: map[string]StoreConfig{
			"custom": {Driver: "custom-nil-store"},
		},
	})
	if err != nil {
		t.Fatalf("new manager with nil custom store: %v", err)
	}
	if err := nilStore.defaultRepository().Put(context.Background(), "key", "value", time.Minute); err == nil || !strings.Contains(err.Error(), "custom driver store is nil") {
		t.Fatalf("nil custom store err = %v, want nil store error", err)
	}
}

func TestCustomDriverExtendReplacesExistingFactory(t *testing.T) {
	driverName := "custom-replace-store"
	first := newMemoryStore(0, 0)
	second := newMemoryStore(0, 0)
	var used string
	Extend(driverName, func(StoreFactoryContext) (StoreDriver, error) {
		used = "first"
		return NewStoreDriver(first), nil
	})
	Extend(driverName, func(StoreFactoryContext) (StoreDriver, error) {
		used = "second"
		return NewStoreDriver(second), nil
	})

	m, err := NewManager(Config{
		Default: "custom",
		Stores: map[string]StoreConfig{
			"custom": {Driver: driverName},
		},
	})
	if err != nil {
		t.Fatalf("new manager with replacement custom driver: %v", err)
	}
	if err := m.defaultRepository().Put(context.Background(), "key", "value", time.Minute); err != nil {
		t.Fatalf("write through replacement custom driver: %v", err)
	}
	if used != "second" || !customStoreHasKey(second, "key") || customStoreHasKey(first, "key") {
		t.Fatalf("replacement custom driver used=%q first=%v second=%v", used, customStoreHasKey(first, "key"), customStoreHasKey(second, "key"))
	}
}

func customStoreHasKey(store *memoryStore, key string) bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	_, ok := store.items[key]
	return ok
}
