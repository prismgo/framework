package queue

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func useIsolatedFacadeRegistry(t *testing.T) {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
}

func TestResolveRequiresCurrentRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	assertPanics(t, func() { _ = Resolve() })
}

func TestExtendRequiresCurrentManager(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })
	assertPanics(t, func() {
		Extend("facade-package-extend-no-registry", connectorResolver(&capturingConnector{queue: &contractOnlyQueue{}}))
	})
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

func TestResolveReturnsRegisteredQueueManager(t *testing.T) {
	useIsolatedFacadeRegistry(t)

	factoryManager := newSyncManager()
	factoryManager.runtime.defaultQueue = "factory"
	calls := 0
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		calls++
		return factoryManager, nil
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	if got := Resolve(); got != factoryManager {
		t.Fatalf("Resolve manager = %#v, want factory manager", got)
	}
	if calls != 1 {
		t.Fatalf("factory calls = %d, want 1", calls)
	}
}
