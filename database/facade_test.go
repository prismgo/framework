package database

import (
	"errors"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"gorm.io/gorm"
)

func TestFacadeUseAndCurrent(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	if err := registry.Instance("database.default", db); err != nil {
		t.Fatalf("bind db: %v", err)
	}

	if got := container.Value[*gorm.DB]("database.default"); got != db {
		t.Fatal("expected Current to return the registered database connection")
	}
	if got := container.Value[*gorm.DB]("database.default"); got != db {
		t.Fatal("expected Default to return the registered database connection")
	}
}

func TestFacadeSetDefaultDelegatesToUse(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	if err := registry.Instance("database.default", db); err != nil {
		t.Fatalf("bind db: %v", err)
	}

	if got := container.Value[*gorm.DB]("database.default"); got != db {
		t.Fatal("expected SetDefault to update current database connection")
	}
}

func TestFacadeCurrentReturnsNilBeforeUse(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if got := container.Value[*gorm.DB]("database.default"); got != nil {
		t.Fatalf("expected nil current db before Use, got %#v", got)
	}
	if got := container.Value[*gorm.DB]("database.default"); got != nil {
		t.Fatalf("expected nil default db before Use, got %#v", got)
	}
}

func TestFacadeDefaultDoesNotCreateFallbackDBAfterFactoryFailure(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	boom := errors.New("factory failed")
	if err := registry.Singleton("database.default", func(containercontract.Resolver) (any, error) {
		return nil, boom
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}
	if _, err := container.Make[*gorm.DB]("database.default"); !errors.Is(err, boom) {
		t.Fatalf("Make error = %v, want factory failure", err)
	}
	if got := container.Value[*gorm.DB]("database.default"); got != nil {
		t.Fatalf("Value after factory failure = %#v, want nil", got)
	}
}

func TestFacadeRegisterFactoryInRequiresRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })
	if _, err := container.Make[*gorm.DB]("database.default"); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatalf("Make nil error = %v, want ErrNoCurrentContainer", err)
	}
}

func TestFacadeResolveRequiresCurrentRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	if _, err := container.Make[*gorm.DB]("database.default"); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatalf("Make without current registry error = %v, want ErrNoCurrentRegistry", err)
	}
	if got := container.Value[*gorm.DB]("database.default"); got != nil {
		t.Fatalf("Value without current registry = %#v, want nil", got)
	}
}
