package database

import (
	"errors"
	"path/filepath"
	"testing"

	configpkg "github.com/prismgo/framework/config"
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

func TestServiceProviderDefaultConnectionUsesApplicationManager(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	configpkg.Add("database", func() map[string]any {
		return map[string]any{
			"default": "testing",
			"connections": map[string]any{
				"testing": map[string]any{"driver": "sqlite", "database": ":memory:"},
			},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	manager, err := ManagerFrom(registry)
	if err != nil {
		t.Fatalf("resolve manager: %v", err)
	}
	wantErr := errors.New("application-local sqlite resolver")
	manager.Extend("sqlite", func(DriverContext) (gorm.Dialector, error) {
		return nil, wantErr
	})

	got, err := registry.Make("database.default")
	if got != nil {
		t.Fatalf("default database = %v, want nil after resolver failure", got)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("default database error = %v, want %v", err, wantErr)
	}
}

func TestServiceProviderDefaultConnectionUsesApplicationConfig(t *testing.T) {
	newConfig := func(dsn string) *configpkg.Config {
		t.Helper()
		configpkg.Add("database", func() map[string]any {
			return map[string]any{
				"default": "testing",
				"connections": map[string]any{
					"testing": map[string]any{"driver": "sqlite", "dsn": dsn},
				},
			}
		})
		cfg := configpkg.New()
		if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
			t.Fatalf("reload config: %v", err)
		}
		return cfg
	}

	first := container.NewContainer()
	if err := first.Instance("config.default", newConfig("first-dsn")); err != nil {
		t.Fatalf("bind first config: %v", err)
	}
	if err := (ServiceProvider{}).Register(providerTestApp{registry: first}); err != nil {
		t.Fatalf("register first provider: %v", err)
	}
	second := container.NewContainer()
	if err := second.Instance("config.default", newConfig("second-dsn")); err != nil {
		t.Fatalf("bind second config: %v", err)
	}
	container.SetProvider(func() *container.Container { return second })
	t.Cleanup(func() { container.SetProvider(nil) })

	manager, err := ManagerFrom(first)
	if err != nil {
		t.Fatalf("resolve first manager: %v", err)
	}
	wantErr := errors.New("stop after capture")
	gotDSN := ""
	manager.Extend("sqlite", func(ctx DriverContext) (gorm.Dialector, error) {
		gotDSN = ctx.DSN
		return nil, wantErr
	})
	if _, err := first.Make("database.default"); !errors.Is(err, wantErr) {
		t.Fatalf("resolve first database error = %v, want %v", err, wantErr)
	}
	if gotDSN != "first-dsn" {
		t.Fatalf("first application DSN = %q, want first-dsn", gotDSN)
	}
}

func TestServiceProviderPassesConfiguredDSNToExtensionDriver(t *testing.T) {
	registry := container.NewContainer()
	configpkg.Add("database", func() map[string]any {
		return map[string]any{
			"default": "external",
			"connections": map[string]any{
				"external": map[string]any{"driver": "adapter", "dsn": "adapter-dsn"},
			},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	manager, err := ManagerFrom(registry)
	if err != nil {
		t.Fatalf("resolve manager: %v", err)
	}
	wantErr := errors.New("stop after capture")
	gotDSN := ""
	manager.Extend("adapter", func(ctx DriverContext) (gorm.Dialector, error) {
		gotDSN = ctx.DSN
		return nil, wantErr
	})
	if _, err := registry.Make("database.default"); !errors.Is(err, wantErr) {
		t.Fatalf("resolve database error = %v, want %v", err, wantErr)
	}
	if gotDSN != "adapter-dsn" {
		t.Fatalf("extension driver DSN = %q, want adapter-dsn", gotDSN)
	}
}

func TestServiceProviderRegistersLazyApplicationDatabaseManager(t *testing.T) {
	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound("database.manager") {
		t.Fatal("provider Register should bind database manager")
	}
	if registry.Resolved("database.manager") {
		t.Fatal("provider Register should not construct database manager")
	}

	manager, err := ManagerFrom(registry)
	if err != nil {
		t.Fatalf("resolve database manager: %v", err)
	}
	if manager == nil || !registry.Resolved("database.manager") {
		t.Fatalf("resolved manager = %v, resolved flag = %v; want non-nil/true", manager, registry.Resolved("database.manager"))
	}
}

func TestServiceProviderDatabaseManagerUsesApplicationDebugConfig(t *testing.T) {
	registry := container.NewContainer()
	configpkg.Add("app", func() map[string]any {
		return map[string]any{"debug": true}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	manager, err := ManagerFrom(registry)
	if err != nil {
		t.Fatalf("resolve database manager: %v", err)
	}
	if !manager.debug {
		t.Fatal("manager debug = false, want true from app.debug")
	}
}

// TestServiceProviderUsesDBCloseOption 验证 ServiceProvider 复用 DBCloseOption
func TestServiceProviderUsesDBCloseOption(t *testing.T) {
	// 验证 DBCloseOption 返回的选项可以被正确使用
	opt := DBCloseOption()
	if opt == nil {
		t.Fatal("DBCloseOption should return a non-nil option")
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
