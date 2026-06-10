package logger

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestServiceProviderRegistersLazyLoggerFactory(t *testing.T) {
	registry := container.NewContainer()
	provider := ServiceProvider{}
	if err := provider.Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound("logger.manager") {
		t.Fatal("provider Register should bind logger manager factory")
	}
	if registry.Resolved("logger.manager") {
		t.Fatal("provider Register should not construct logger manager")
	}
}

func TestServiceProviderPreservesExplicitLoggerManager(t *testing.T) {
	registry := container.NewContainer()
	explicit, err := NewManager(Config{
		Default:  "null",
		Channels: map[string]ChannelOptions{"null": {Driver: "null"}},
	})
	if err != nil {
		t.Fatalf("new explicit manager: %v", err)
	}
	if err := registry.Instance("logger.manager", explicit); err != nil {
		t.Fatalf("seed logger manager: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("logger.manager")
	if err != nil {
		t.Fatalf("resolve logger manager: %v", err)
	}
	got, ok := raw.(*Manager)
	if !ok {
		t.Fatalf("resolve logger manager type = %T, want *Manager", raw)
	}
	if got != explicit {
		t.Fatal("service provider should preserve explicit logger manager")
	}
}

func TestServiceProviderIdentityAndBootAreStable(t *testing.T) {
	// Provider identity is used by application lifecycle bookkeeping and should stay stable.
	provider := ServiceProvider{}
	if got := provider.Name(); got != "logger" {
		t.Fatalf("provider name = %q, want logger", got)
	}

	// Boot intentionally has no side effects; channel construction remains lazy after Register.
	registry := container.NewContainer()
	if err := provider.Boot(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
	if registry.Bound("logger.manager") {
		t.Fatal("Boot should not register logger.manager")
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
