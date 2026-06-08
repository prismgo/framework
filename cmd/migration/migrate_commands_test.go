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
	dbregistry "github.com/prismgo/framework/database"
)

func TestMigrateInstallAndStatusCommands(t *testing.T) {
	db, dir := openMigrationTestDB(t)
	createMigrationFileAndRegister(t, dir, "202604280401_create_demo_status")

	deps := testMigrationDeps(dir, dir)
	install := NewMigrateInstallCommand(deps)
	install.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	status := NewMigrateStatusCommand(deps)
	status.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }

	installCtx := console.NewCommandContext(context.Background(), install, *install.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate:install"})
	if err := install.Handle(installCtx); err != nil {
		t.Fatalf("migrate:install failed: %v", err)
	}

	statusCtx := console.NewCommandContext(context.Background(), status, *status.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate:status"})
	if err := status.Handle(statusCtx); err != nil {
		t.Fatalf("migrate:status failed: %v", err)
	}
}

func TestMigrateAndRollbackCommands(t *testing.T) {
	db, dir := openMigrationTestDB(t)
	createMigrationFileAndRegister(t, dir, "202604280402_create_demo_rollback")
	dbregistry.RegisterSeederAs(defaultSeederClass, func(*gorm.DB) error { return nil })

	deps := testMigrationDeps(dir, dir)
	migrate := NewMigrateCommand(deps)
	migrate.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	rollback := NewMigrateRollbackCommand(deps)
	rollback.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }

	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	migrateCtx := console.NewCommandContext(context.Background(), migrate, *migrate.Definition(), fakeInput{bools: map[string]bool{"seed": true}}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate"})
	if err := migrate.Handle(migrateCtx); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}

	store := newMigrationStore(db)
	applied, err := store.appliedMap()
	if err != nil {
		t.Fatalf("applied map failed: %v", err)
	}
	if len(applied) == 0 {
		t.Fatal("expected migrations applied")
	}

	rollbackCtx := console.NewCommandContext(context.Background(), rollback, *rollback.Definition(), fakeInput{options: map[string]string{"step": "1"}}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate:rollback"})
	if err := rollback.Handle(rollbackCtx); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
}

func TestMigrateFreshResetRefreshAndSeed(t *testing.T) {
	db, dir := openMigrationTestDB(t)
	createMigrationFileAndRegister(t, dir, "202604280403_create_demo_fresh")

	seeded := false
	dbregistry.RegisterSeederAs(defaultSeederClass, func(*gorm.DB) error {
		seeded = true
		return nil
	})
	deps := testMigrationDeps(dir, dir)

	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	fresh := NewMigrateFreshCommand(deps)
	fresh.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	freshCtx := console.NewCommandContext(context.Background(), fresh, *fresh.Definition(), fakeInput{bools: map[string]bool{"seed": true}}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate:fresh"})
	if err := fresh.Handle(freshCtx); err != nil {
		t.Fatalf("fresh failed: %v", err)
	}
	if !seeded {
		t.Fatal("expected seeder to be called in migrate:fresh")
	}

	refresh := NewMigrateRefreshCommand(deps)
	refresh.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	refreshCtx := console.NewCommandContext(context.Background(), refresh, *refresh.Definition(), fakeInput{options: map[string]string{"step": "1"}, bools: map[string]bool{"pretend": true}}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate:refresh"})
	if err := refresh.Handle(refreshCtx); err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	refreshCtxRun := console.NewCommandContext(context.Background(), refresh, *refresh.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate:refresh"})
	if err := refresh.Handle(refreshCtxRun); err != nil {
		t.Fatalf("refresh run failed: %v", err)
	}

	reset := NewMigrateResetCommand(deps)
	reset.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	resetCtx := console.NewCommandContext(context.Background(), reset, *reset.Definition(), fakeInput{bools: map[string]bool{"pretend": true}}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate:reset"})
	if err := reset.Handle(resetCtx); err != nil {
		t.Fatalf("reset failed: %v", err)
	}
	resetCtxRun := console.NewCommandContext(context.Background(), reset, *reset.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate:reset"})
	if err := reset.Handle(resetCtxRun); err != nil {
		t.Fatalf("reset run failed: %v", err)
	}

	dbSeed := NewDBSeedCommand(deps)
	dbSeed.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	seedCtx := console.NewCommandContext(context.Background(), dbSeed, *dbSeed.Definition(), fakeInput{options: map[string]string{"class": defaultSeederClass}}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "db:seed"})
	if err := dbSeed.Handle(seedCtx); err != nil {
		t.Fatalf("db:seed failed: %v", err)
	}
}

func TestForceProtectionInProduction(t *testing.T) {
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "production")

	cmd := NewMigrateCommand(MigrationDependencies{})
	cmd.openDB = func(string) (dbSession, error) { return dbSession{DB: &gorm.DB{}}, nil }
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate"})
	if err := cmd.Handle(ctx); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected production force guard error, got %v", err)
	}
}

func openMigrationTestDB(t *testing.T) (*gorm.DB, string) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	return db, t.TempDir()
}

func createMigrationFileAndRegister(t *testing.T, dir, name string) {
	t.Helper()
	if err := osWriteFile(filepath.Join(dir, name+".go"), "package migrations"); err != nil {
		t.Fatalf("write migration file failed: %v", err)
	}
	dbregistry.RegisterMigrationAs(name, func(tx *gorm.DB) error {
		return tx.Exec("CREATE TABLE IF NOT EXISTS demo (id integer primary key, name text)").Error
	}, func(tx *gorm.DB) error {
		return tx.Exec("DROP TABLE IF EXISTS demo").Error
	})
}

func testMigrationDeps(migrationPath, seedPath string) MigrationDependencies {
	return MigrationDependencies{
		MigrationPaths: func() []string { return []string{migrationPath} },
		SeedPaths:      func() []string { return []string{seedPath} },
	}
}

func osWriteFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
