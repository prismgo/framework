package session

import (
	"path/filepath"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestResolveRequiresCurrentRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Resolve without current registry did not panic")
		}
	}()

	_ = Resolve()
}

func TestResolveReturnsRegisteredFactoryManager(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Files = filepath.Join(t.TempDir(), "factory-sessions")
	manager, err := NewManager(cfg, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	registry := useSessionTestContainer(t)
	calls := 0
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		calls++
		return manager, nil
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	if got := Resolve(); got != manager {
		t.Fatalf("Resolve manager = %#v, want factory manager", got)
	}
	if calls != 1 {
		t.Fatalf("factory calls = %d, want 1", calls)
	}
}
