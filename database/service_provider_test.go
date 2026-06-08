package database

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"gorm.io/gorm"
)

func TestServiceProviderRegistersLazyDatabaseFactory(t *testing.T) {
	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound("database.default") {
		t.Fatal("provider Register should bind database factory")
	}
	if registry.Resolved("database.default") {
		t.Fatal("provider Register should not open database connection")
	}
}

func TestServiceProviderPreservesCustomDatabaseFactory(t *testing.T) {
	registry := container.NewContainer()
	custom := &gorm.DB{}
	if err := registry.Singleton("database.default", func(containercontract.Resolver) (any, error) {
		return custom, nil
	}); err != nil {
		t.Fatalf("seed database factory: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("database.default")
	if err != nil {
		t.Fatalf("resolve database: %v", err)
	}
	got, _ := raw.(*gorm.DB)
	if got != custom {
		t.Fatal("service provider should preserve custom database factory")
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
