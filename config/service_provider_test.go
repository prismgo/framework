package config

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestServiceProviderRegistersConfigFactory(t *testing.T) {
	registry := container.NewContainer()
	provider := ServiceProvider{}

	if err := provider.Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("config.default")
	if err != nil {
		t.Fatalf("resolve config from provider factory: %v", err)
	}
	cfg, ok := raw.(*Config)
	if !ok {
		t.Fatalf("resolve config type = %T, want *Config", raw)
	}
	if cfg == nil {
		t.Fatal("expected config from service provider")
	}
}

func TestServiceProviderPreservesExplicitConfig(t *testing.T) {
	registry := container.NewContainer()
	explicit := New()
	if err := registry.Instance("config.default", explicit); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("config.default")
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}
	got, ok := raw.(*Config)
	if !ok {
		t.Fatalf("resolve config type = %T, want *Config", raw)
	}
	if got != explicit {
		t.Fatal("service provider should preserve explicit config")
	}
}

func TestServiceProviderNameAndBootAreStableNoops(t *testing.T) {
	// provider identity 与 Boot 空实现是框架生命周期契约的一部分，需要保持稳定。
	registry := container.NewContainer()
	provider := ServiceProvider{}

	if got := provider.Name(); got != "config" {
		t.Fatalf("Name() = %q, want config", got)
	}
	if err := provider.Boot(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Boot should be a no-op, got %v", err)
	}
	if registry.Bound("config.default") {
		t.Fatal("Boot should not bind config.default")
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
