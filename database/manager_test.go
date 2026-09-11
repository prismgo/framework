package database

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prismgo/framework/container"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func bindDatabaseManagerForTest(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	manager := newTestDatabaseManager()
	if err := registry.Instance("database.manager", manager); err != nil {
		t.Fatalf("bind database manager: %v", err)
	}
	return registry
}

func newTestDatabaseManager() *Manager {
	manager := NewManager()
	manager.Extend("sqlite", func(ctx DriverContext) (gorm.Dialector, error) {
		return sqlite.Open(ctx.DSN), nil
	})
	manager.Extend("sqlite3", func(ctx DriverContext) (gorm.Dialector, error) {
		return sqlite.Open(ctx.DSN), nil
	})
	return manager
}

func TestManagerExtendUsesLatestDialectorFactory(t *testing.T) {
	manager := NewManager()
	firstErr := errors.New("first resolver")
	latestErr := errors.New("latest resolver")
	manager.Extend("custom", func(DriverContext) (gorm.Dialector, error) {
		return nil, firstErr
	})
	manager.Extend(" CUSTOM ", func(ctx DriverContext) (gorm.Dialector, error) {
		if ctx.Driver != "custom" || ctx.DSN != "driver-dsn" || ctx.TablePrefix != "tenant_" {
			t.Fatalf("driver context = %#v, want normalized driver, DSN, and table prefix", ctx)
		}
		return nil, latestErr
	})

	db, err := manager.Open("custom", "driver-dsn", MySQLConfig{
		Schema: MySQLSchemaConfig{TablePrefix: "tenant_"},
	})
	if db != nil {
		t.Fatalf("Open db = %v, want nil after resolver failure", db)
	}
	if !errors.Is(err, latestErr) {
		t.Fatalf("Open error = %v, want latest resolver error %v", err, latestErr)
	}
}

func TestManagerFromKeepsDialectorFactoriesApplicationLocal(t *testing.T) {
	firstContainer := container.NewContainer()
	secondContainer := container.NewContainer()
	firstManager := NewManager()
	secondManager := NewManager()
	if err := firstContainer.Instance("database.manager", firstManager); err != nil {
		t.Fatalf("bind first manager: %v", err)
	}
	if err := secondContainer.Instance("database.manager", secondManager); err != nil {
		t.Fatalf("bind second manager: %v", err)
	}
	firstErr := errors.New("first application")
	secondErr := errors.New("second application")
	firstManager.Extend("custom", func(DriverContext) (gorm.Dialector, error) { return nil, firstErr })
	secondManager.Extend("custom", func(DriverContext) (gorm.Dialector, error) { return nil, secondErr })

	gotFirst, err := ManagerFrom(firstContainer)
	if err != nil {
		t.Fatalf("resolve first manager: %v", err)
	}
	gotSecond, err := ManagerFrom(secondContainer)
	if err != nil {
		t.Fatalf("resolve second manager: %v", err)
	}
	if gotFirst != firstManager || gotSecond != secondManager {
		t.Fatalf("resolved managers = (%p, %p), want (%p, %p)", gotFirst, gotSecond, firstManager, secondManager)
	}
	if _, err := gotFirst.Open("custom", "", MySQLConfig{}); !errors.Is(err, firstErr) {
		t.Fatalf("first Open error = %v, want %v", err, firstErr)
	}
	if _, err := gotSecond.Open("custom", "", MySQLConfig{}); !errors.Is(err, secondErr) {
		t.Fatalf("second Open error = %v, want %v", err, secondErr)
	}
}

func TestManagerOpenAppliesSharedGORMConfiguration(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	manager := NewManager()
	manager.Extend("custom", func(ctx DriverContext) (gorm.Dialector, error) {
		if ctx.Options["mode"] != "strict" {
			t.Fatalf("driver options = %#v, want mode=strict", ctx.Options)
		}
		ctx.Options["mode"] = "changed"
		return mysql.New(mysql.Config{Conn: sqlDB, SkipInitializeWithVersion: true}), nil
	})
	cfg := MySQLConfig{
		Driver: MySQLDriverConfig{Options: map[string]string{"mode": "strict"}},
		Schema: MySQLSchemaConfig{TablePrefix: "tenant_"},
	}

	db, err := manager.Open("custom", "driver-dsn", cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if !db.DisableAutomaticPing {
		t.Fatal("Open should disable automatic ping")
	}
	if got := db.NamingStrategy.TableName("users"); got != "tenant_users" {
		t.Fatalf("table name = %q, want %q", got, "tenant_users")
	}
	if got := cfg.Driver.Options["mode"]; got != "strict" {
		t.Fatalf("caller options mutated to %q, want %q", got, "strict")
	}
}

func TestOpenDelegatesToCurrentApplicationManager(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	manager := NewManager()
	wantErr := errors.New("current application resolver")
	manager.Extend("sqlite", func(DriverContext) (gorm.Dialector, error) {
		return nil, wantErr
	})
	if err := registry.Instance("database.manager", manager); err != nil {
		t.Fatalf("bind manager: %v", err)
	}

	db, err := Open("sqlite", ":memory:", MySQLConfig{})
	if db != nil {
		t.Fatalf("Open db = %v, want nil after resolver failure", db)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Open error = %v, want %v", err, wantErr)
	}
}

func TestNewManagerRequiresSQLiteExtension(t *testing.T) {
	manager := NewManager()
	db, err := manager.Open("sqlite", ":memory:", MySQLConfig{})
	if db != nil {
		t.Fatalf("Open sqlite db = %v, want nil without extension", db)
	}
	want := `database: driver "sqlite" is not registered`
	if err == nil || err.Error() != want {
		t.Fatalf("Open sqlite error = %v, want %q", err, want)
	}
}
