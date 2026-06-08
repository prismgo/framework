package config

import (
	"errors"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func useIsolatedFacadeRegistry(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func TestDefaultReturnsNilWhenFactoryFails(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	factoryErr := errors.New("config factory failed")

	if err := registry.Singleton("config.default", func(containercontract.Resolver) (any, error) {
		return nil, factoryErr
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Resolve should panic when config factory fails")
		}
	}()
	if cfg := Resolve(); cfg != nil {
		t.Fatalf("Resolve returned %#v before panic, want nil", cfg)
	}
}
