package migration

import (
	"path/filepath"
	"strings"
	"testing"

	"gorm.io/gorm"

	"github.com/prismgo/framework/config"
	dbfacade "github.com/prismgo/framework/database"
)

func TestOpenDatabaseSessionBranches(t *testing.T) {
	registry := useMigrationTestContainer(t)
	if err := registry.Instance("database.default", &gorm.DB{}); err != nil {
		t.Fatalf("bind database: %v", err)
	}
	session, err := openDatabaseSession("")
	if err != nil {
		t.Fatalf("open default session failed: %v", err)
	}
	if session.DB == nil {
		t.Fatal("expected default db session")
	}
	session.Close()

	cfg := config.New()
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	_ = cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env"))
	_, err = openDatabaseSession("missing")
	if err == nil {
		t.Fatal("expected error when opening missing connection without config")
	}
}

func TestCommandEnvironmentFallbacks(t *testing.T) {
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "")
	if got := commandEnvironment(); got != "production" {
		t.Fatalf("commandEnvironment() = %q, want production", got)
	}
	t.Setenv("APP_ENV", "LOCAL")
	if got := commandEnvironment(); got != "local" {
		t.Fatalf("commandEnvironment() = %q, want local", got)
	}
}

func TestApplyMigrationRegistryAndDescribeBranches(t *testing.T) {
	db := &gorm.DB{}

	name := "202604280501_exec"
	called := false
	dbfacade.RegisterMigrationAs(name,
		func(*gorm.DB) error {
			called = true
			return nil
		},
		func(*gorm.DB) error { return nil },
	)
	spec := migrationSpec{Name: name, FilePath: "/tmp/202604280501_exec.go"}
	if err := applyMigrationWithTx(db, spec, true); err != nil {
		t.Fatalf("apply up failed: %v", err)
	}
	if !called {
		t.Fatal("expected migration up handler to run")
	}
	if err := applyMigrationWithTx(db, spec, false); err != nil {
		t.Fatalf("apply down failed: %v", err)
	}
	if err := applyMigrationUp(db, spec, true); err != nil {
		t.Fatalf("apply pretend should no-op: %v", err)
	}

	desc := describeMigrationOperation(spec, true)
	if !strings.Contains(desc, name) || !strings.Contains(desc, "/tmp/202604280501_exec.go") {
		t.Fatalf("unexpected describe result: %q", desc)
	}
	desc = describeMigrationOperation(migrationSpec{Name: name}, false)
	if !strings.Contains(desc, "<missing>") || !strings.Contains(desc, "down") {
		t.Fatalf("unexpected missing path describe result: %q", desc)
	}
}
