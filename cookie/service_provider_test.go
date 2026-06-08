package cookie

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestServiceProviderRegistersLazyCookieQueueFactory(t *testing.T) {
	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound("cookie.queue") {
		t.Fatal("provider Register should bind cookie queue factory")
	}
	if registry.Resolved("cookie.queue") {
		t.Fatal("provider Register should not construct cookie queue")
	}
}

func TestServiceProviderPreservesExplicitCookieQueue(t *testing.T) {
	registry := container.NewContainer()
	explicit := NewQueue()
	if err := registry.Instance("cookie.queue", explicit); err != nil {
		t.Fatalf("seed cookie queue: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("cookie.queue")
	if err != nil {
		t.Fatalf("resolve cookie queue: %v", err)
	}
	got, ok := raw.(*Queue)
	if !ok {
		t.Fatalf("resolve cookie queue type = %T, want *Queue", raw)
	}
	if got != explicit {
		t.Fatal("service provider should preserve explicit cookie queue")
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
