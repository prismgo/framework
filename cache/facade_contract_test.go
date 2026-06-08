package cache

import (
	"errors"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	cachecontract "github.com/prismgo/framework/contracts/cache"
)

func useIsolatedFacadeRegistry(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func TestResolveRequiresCurrentRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	assertPanics(t, func() { _ = Resolve() })
}

func TestFacadeReturnsCacheContracts(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	registerCacheFactoryForTest(t, registry, func() (*Manager, error) {
		return NewManager(Config{
			Default: "memory",
			Stores: map[string]StoreConfig{
				"memory": {Driver: "memory", CleanupInterval: time.Millisecond},
			},
		})
	})

	var factory cachecontract.Factory = Resolve()
	if factory == nil {
		t.Fatal("Resolve() = nil, want cache contract factory")
	}
	var defaultRepo cachecontract.Repository = Default()
	if defaultRepo == nil {
		t.Fatal("Default() = nil, want cache contract repository")
	}
	var namedRepo cachecontract.Repository = Store("memory")
	if namedRepo == nil {
		t.Fatal("Store(memory) = nil, want cache contract repository")
	}
}

func TestDefaultResolvesRegisteredFactory(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	calls := 0
	registerCacheFactoryForTest(t, registry, func() (*Manager, error) {
		calls++
		return NewManager(Config{
			Default: "factory",
			Stores: map[string]StoreConfig{
				"factory": {Driver: "memory", CleanupInterval: time.Millisecond},
			},
		})
	})

	if registry.Resolved(serviceKey) {
		t.Fatal("cache manager should not resolve before facade access")
	}
	if got := Default().Name(); got != "factory" {
		t.Fatalf("Default repository = %q, want factory", got)
	}
	if calls != 1 {
		t.Fatalf("factory calls = %d, want 1", calls)
	}
}

func TestResolvePanicsWhenFactoryFails(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	factoryErr := errors.New("cache factory failed")
	registerCacheFactoryForTest(t, registry, func() (*Manager, error) {
		return nil, factoryErr
	})

	assertPanics(t, func() { _ = Resolve() })
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}
