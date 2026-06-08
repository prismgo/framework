package event

import (
	"errors"
	"testing"

	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestResolveReturnsNilWhenDispatcherFactoryFails(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	factoryErr := errors.New("event factory failed")

	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return nil, factoryErr
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	if dispatcher := Resolve(); dispatcher != nil {
		t.Fatalf("Resolve returned %#v, want nil", dispatcher)
	}
}
