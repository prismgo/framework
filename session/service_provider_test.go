package session

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestServiceProviderRegistersLazySessionManagerFactory(t *testing.T) {
	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound("session.manager") {
		t.Fatal("provider Register should bind session manager factory")
	}
	if registry.Resolved("session.manager") {
		t.Fatal("provider Register should not construct session manager")
	}
}

func TestServiceProviderPreservesExplicitSessionManager(t *testing.T) {
	registry := container.NewContainer()
	explicit, err := NewManager(Config{Driver: "file", Files: t.TempDir()}, nil)
	if err != nil {
		t.Fatalf("new explicit session manager: %v", err)
	}
	if err := registry.Instance("session.manager", explicit); err != nil {
		t.Fatalf("seed session manager: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("session.manager")
	if err != nil {
		t.Fatalf("resolve session manager: %v", err)
	}
	got, _ := raw.(*Manager)
	if got != explicit {
		t.Fatal("service provider should preserve explicit session manager")
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
