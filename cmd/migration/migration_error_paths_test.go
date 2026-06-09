package migration

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
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
	db, err := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	if err := applyMigration(db, migrationSpec{Name: "missing"}, false, true); err == nil {
		t.Fatal("expected missing migration registration error")
	}

	upOnly := "202604280601_up_only"
	dbregistry.RegisterMigrationAs(upOnly, func(*gorm.DB) error { return nil }, nil)
	if err := applyMigrationDown(db, migrationSpec{Name: upOnly}, false); err == nil {
		t.Fatal("expected missing down handler error")
	}

	downOnly := "202604280602_down_only"
	dbregistry.RegisterMigrationAs(downOnly, nil, func(*gorm.DB) error { return nil })
	if err := applyMigrationUp(db, migrationSpec{Name: downOnly}, false); err == nil {
		t.Fatal("expected missing up handler error")
	}
}

func TestMigrateAndSeedErrorBranches(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("config.default", config.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	db, err := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	dir := t.TempDir()
	name := "202604280603_create_demo_error_branch"
	_ = os.WriteFile(filepath.Join(dir, name+".go"), []byte("package migrations"), 0o644)

	dbregistry.RegisterMigrationAs(name, func(tx *gorm.DB) error {
		return tx.Exec("CREATE TABLE demo_error_branch (id integer primary key)").Error
	}, func(tx *gorm.DB) error {
		return tx.Exec("DROP TABLE demo_error_branch").Error
	})

	deps := MigrationDependencies{
		MigrationPaths: func() []string { return []string{dir} },
		SeedPaths:      func() []string { return []string{filepath.Join(dir, "missing-seeders")} },
	}

	migrate := NewMigrateCommand(deps)
	migrate.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	migrateCtx := console.NewCommandContext(context.Background(), migrate, *migrate.Definition(), fakeInput{bools: map[string]bool{"seed": true}}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate"})
	if err := migrate.Handle(migrateCtx); err == nil {
		t.Fatal("expected migrate seed path error")
	}

	dbSeed := NewDBSeedCommand(MigrationDependencies{
		SeedPaths: func() []string { return []string{dir} },
	})
	dbSeed.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	seedCtx := console.NewCommandContext(context.Background(), dbSeed, *dbSeed.Definition(), fakeInput{options: map[string]string{"class": "MissingSeederForErrorBranch"}}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "db:seed"})
	if err := dbSeed.Handle(seedCtx); err == nil {
		t.Fatal("expected db:seed missing class error")
	}
}
