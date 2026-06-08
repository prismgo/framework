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

func testCmdCtx(cmd console.Command, input fakeInput, use string) console.CommandContext {
	return console.NewCommandContext(
		context.Background(),
		cmd,
		*cmd.Definition(),
		input,
		console.NewIO(strings.NewReader(""), io.Discard, io.Discard),
		nil,
		&cobra.Command{Use: use},
	)
}

func testSQLiteDB(t *testing.T, name string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	return db
}

func addMigrationFile(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name+".go"), []byte("package migrations"), 0o644); err != nil {
		t.Fatalf("write migration file failed: %v", err)
	}
}

func TestRollbackAndResetEmptyRepositoryBranches(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	db := testSQLiteDB(t, strings.ReplaceAll(t.Name(), "/", "_"))
	dir := t.TempDir()
	addMigrationFile(t, dir, "202604280901_empty_branch")

	store := newMigrationStore(db)
	if err := store.ensureTable(); err != nil {
		t.Fatalf("ensure migration table failed: %v", err)
	}

	deps := MigrationDependencies{MigrationPaths: func() []string { return []string{dir} }}

	rollback := NewMigrateRollbackCommand(deps)
	rollback.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	if err := rollback.Handle(testCmdCtx(rollback, fakeInput{}, "migrate:rollback")); err != nil {
		t.Fatalf("rollback empty repository failed: %v", err)
	}

	reset := NewMigrateResetCommand(deps)
	reset.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	if err := reset.Handle(testCmdCtx(reset, fakeInput{}, "migrate:reset")); err != nil {
		t.Fatalf("reset empty repository failed: %v", err)
	}
}

func TestStatusIncludesMissingAppliedMigrationBranch(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)

	db := testSQLiteDB(t, strings.ReplaceAll(t.Name(), "/", "_"))
	dir := t.TempDir()
	addMigrationFile(t, dir, "202604280902_existing")

	store := newMigrationStore(db)
	if err := store.ensureTable(); err != nil {
		t.Fatalf("ensure migration table failed: %v", err)
	}
	if err := store.markApplied("202604280902_existing", 1); err != nil {
		t.Fatalf("mark existing migration failed: %v", err)
	}
	if err := store.markApplied("202604280903_missing", 2); err != nil {
		t.Fatalf("mark missing migration failed: %v", err)
	}

	status := NewMigrateStatusCommand(MigrationDependencies{MigrationPaths: func() []string { return []string{dir} }})
	status.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	if err := status.Handle(testCmdCtx(status, fakeInput{}, "migrate:status")); err != nil {
		t.Fatalf("status with missing migration branch failed: %v", err)
	}
}

func TestMigratePretendBranchDoesNotApply(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	db := testSQLiteDB(t, strings.ReplaceAll(t.Name(), "/", "_"))
	dir := t.TempDir()
	name := "202604280904_pretend"
	addMigrationFile(t, dir, name)

	called := false
	dbregistry.RegisterMigrationAs(name, func(*gorm.DB) error {
		called = true
		return nil
	}, func(*gorm.DB) error { return nil })

	migrate := NewMigrateCommand(MigrationDependencies{MigrationPaths: func() []string { return []string{dir} }})
	migrate.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	if err := migrate.Handle(testCmdCtx(migrate, fakeInput{bools: map[string]bool{"pretend": true}}, "migrate")); err != nil {
		t.Fatalf("migrate pretend failed: %v", err)
	}
	if called {
		t.Fatal("pretend mode should not execute migration handler")
	}
	if store := newMigrationStore(db); store.hasTable() {
		records, err := store.listAll()
		if err != nil {
			t.Fatalf("list migration records failed: %v", err)
		}
		if len(records) != 0 {
			t.Fatalf("pretend mode should not persist migration records, got %d", len(records))
		}
	}
}

func TestRefreshStepAndSeedBranches(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	db := testSQLiteDB(t, strings.ReplaceAll(t.Name(), "/", "_"))
	dir := t.TempDir()

	m1 := "202604280905_refresh_a"
	m2 := "202604280906_refresh_b"
	addMigrationFile(t, dir, m1)
	addMigrationFile(t, dir, m2)
	dbregistry.RegisterMigrationAs(m1,
		func(tx *gorm.DB) error { return tx.Exec("CREATE TABLE refresh_a (id integer primary key)").Error },
		func(tx *gorm.DB) error { return tx.Exec("DROP TABLE refresh_a").Error },
	)
	dbregistry.RegisterMigrationAs(m2,
		func(tx *gorm.DB) error { return tx.Exec("CREATE TABLE refresh_b (id integer primary key)").Error },
		func(tx *gorm.DB) error { return tx.Exec("DROP TABLE refresh_b").Error },
	)

	deps := MigrationDependencies{
		MigrationPaths: func() []string { return []string{dir} },
		SeedPaths:      func() []string { return []string{dir} },
	}
	migrate := NewMigrateCommand(deps)
	migrate.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	if err := migrate.Handle(testCmdCtx(migrate, fakeInput{}, "migrate")); err != nil {
		t.Fatalf("prepare migrate failed: %v", err)
	}

	seeded := false
	dbregistry.RegisterSeederAs(defaultSeederClass, func(*gorm.DB) error {
		seeded = true
		return nil
	})

	refresh := NewMigrateRefreshCommand(deps)
	refresh.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	input := fakeInput{options: map[string]string{"step": "1"}, bools: map[string]bool{"seed": true}}
	if err := refresh.Handle(testCmdCtx(refresh, input, "migrate:refresh")); err != nil {
		t.Fatalf("refresh step+seed failed: %v", err)
	}
	if !seeded {
		t.Fatal("expected seeder to run during refresh --seed")
	}
}

func TestFreshAndSeedForceGuards(t *testing.T) {
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "production")

	fresh := NewMigrateFreshCommand()
	if err := fresh.Handle(testCmdCtx(fresh, fakeInput{}, "migrate:fresh")); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected migrate:fresh force guard error, got %v", err)
	}

	seed := NewDBSeedCommand()
	if err := seed.Handle(testCmdCtx(seed, fakeInput{}, "db:seed")); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected db:seed force guard error, got %v", err)
	}
}
