package event

import (
	"errors"
	"fmt"
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

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Resolve should panic when dispatcher factory fails")
		}
		if got := fmt.Sprint(recovered); got != "event factory failed" {
			t.Fatalf("panic = %q, want factory error", got)
		}
	}()

	_ = Resolve()
}
