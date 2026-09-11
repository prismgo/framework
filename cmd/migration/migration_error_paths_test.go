package migration

import (
	"os"
	"path/filepath"
	"testing"

	"gorm.io/gorm"

	dbregistry "github.com/prismgo/framework/database"
)

func TestDependencyHelpersFallbacks(t *testing.T) {
	deps := firstMigrationDependencies()
	if got := deps.paths(); got != nil {
		t.Fatalf("expected nil paths, got %#v", got)
	}
	if got := deps.seedPaths(); got != nil {
		t.Fatalf("expected nil seed paths, got %#v", got)
	}
}

func TestResolveSourcePathErrors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "x.txt")
	_ = os.WriteFile(file, []byte("x"), 0o644)

	if _, err := resolveMigrationPaths([]string{"not-found-dir"}, true); err == nil {
		t.Fatal("expected invalid migration dir error")
	}
	if _, err := resolveMigrationPaths([]string{file}, true); err == nil {
		t.Fatal("expected non-directory migration path error")
	}
	if _, err := resolveSeedPaths([]string{"not-found-seeders"}, true); err == nil {
		t.Fatal("expected invalid seeder dir error")
	}
}

func TestCollectMigrationsErrorCases(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "bad_name.go"), []byte("package migrations"), 0o644)
	migrations, err := collectMigrations([]string{dir}, true)
	if err != nil {
		t.Fatalf("collect migrations with ignored files failed: %v", err)
	}
	if len(migrations) != 0 {
		t.Fatalf("expected ignored files to produce no migration, got %d", len(migrations))
	}
}

func TestApplyMigrationErrorBranches(t *testing.T) {
	db := &gorm.DB{}

	if err := applyMigrationWithTx(db, migrationSpec{Name: "missing"}, true); err == nil {
		t.Fatal("expected missing migration registration error")
	}

	upOnly := "202604280601_up_only"
	dbregistry.RegisterMigrationAs(upOnly, func(*gorm.DB) error { return nil }, nil)
	if err := applyMigrationWithTx(db, migrationSpec{Name: upOnly}, false); err == nil {
		t.Fatal("expected missing down handler error")
	}

	downOnly := "202604280602_down_only"
	dbregistry.RegisterMigrationAs(downOnly, nil, func(*gorm.DB) error { return nil })
	if err := applyMigrationWithTx(db, migrationSpec{Name: downOnly}, true); err == nil {
		t.Fatal("expected missing up handler error")
	}
}
