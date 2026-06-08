package migration

import (
	"context"
	"errors"
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

type renamedDialector struct {
	gorm.Dialector
	name string
}

func (d renamedDialector) Name() string { return d.name }

func resetMigrationRegistriesForTest() {}

func newMigrationCmdContext(t *testing.T, cmd console.Command, input fakeInput, use string) console.CommandContext {
	t.Helper()
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

func TestRegistryHelpersAndSeederValidation(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)

	dbregistry.RegisterSeederAs("BSeeder", func(*gorm.DB) error { return nil })
	dbregistry.RegisterSeederAs("ASeeder", func(*gorm.DB) error { return nil })

	names := dbregistry.SeederClassNames()
	if !containsString(names, "ASeeder") || !containsString(names, "BSeeder") {
		t.Fatalf("unexpected seeder class names: %#v", names)
	}
	if err := dbregistry.EnsureSeederRegistered("ASeeder"); err != nil {
		t.Fatalf("ensure registered ASeeder failed: %v", err)
	}
	if err := dbregistry.EnsureSeederRegistered("MissingSeeder"); err == nil || !strings.Contains(err.Error(), "ASeeder") {
		t.Fatalf("expected missing seeder error with available names, got %v", err)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestResolveSourcePathsRelativeDefaultAndEmptyBranches(t *testing.T) {
	workdir := t.TempDir()
	migrationDir := filepath.Join(workdir, "database", "migrations")
	seederDir := filepath.Join(workdir, "database", "seeders")
	if err := os.MkdirAll(migrationDir, 0o755); err != nil {
		t.Fatalf("mkdir migration dir failed: %v", err)
	}
	if err := os.MkdirAll(seederDir, 0o755); err != nil {
		t.Fatalf("mkdir seeder dir failed: %v", err)
	}

	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd failed: %v", err)
	}
	if err := os.Chdir(workdir); err != nil {
		t.Fatalf("chdir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	paths, err := resolveSourcePaths([]string{"database/migrations", "", "database/migrations"}, false, "database/migrations", "migration")
	if err != nil {
		t.Fatalf("resolve relative migration paths failed: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("deduplicated path count = %d, want 1", len(paths))
	}

	defaultPaths, err := resolveSourcePaths(nil, false, "database/migrations", "migration")
	if err != nil {
		t.Fatalf("resolve default paths failed: %v", err)
	}
	if len(defaultPaths) != 1 {
		t.Fatalf("default path count = %d, want 1", len(defaultPaths))
	}

	if _, err := resolveSourcePaths([]string{"   "}, true, "database/migrations", "migration"); err == nil {
		t.Fatal("expected no migration path available error")
	}
}

func TestCommandOpenDBErrorBranches(t *testing.T) {
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	openErr := errors.New("open failed")

	install := NewMigrateInstallCommand()
	install.openDB = func(string) (dbSession, error) { return dbSession{}, openErr }
	if err := install.Handle(newMigrationCmdContext(t, install, fakeInput{}, "migrate:install")); !errors.Is(err, openErr) {
		t.Fatalf("migrate:install expected open error, got %v", err)
	}

	status := NewMigrateStatusCommand(MigrationDependencies{MigrationPaths: func() []string { return []string{t.TempDir()} }})
	status.openDB = func(string) (dbSession, error) { return dbSession{}, openErr }
	if err := status.Handle(newMigrationCmdContext(t, status, fakeInput{}, "migrate:status")); !errors.Is(err, openErr) {
		t.Fatalf("migrate:status expected open error, got %v", err)
	}

	migrate := NewMigrateCommand()
	migrate.openDB = func(string) (dbSession, error) { return dbSession{}, openErr }
	if err := migrate.Handle(newMigrationCmdContext(t, migrate, fakeInput{}, "migrate")); !errors.Is(err, openErr) {
		t.Fatalf("migrate expected open error, got %v", err)
	}

	rollback := NewMigrateRollbackCommand()
	rollback.openDB = func(string) (dbSession, error) { return dbSession{}, openErr }
	if err := rollback.Handle(newMigrationCmdContext(t, rollback, fakeInput{}, "migrate:rollback")); !errors.Is(err, openErr) {
		t.Fatalf("migrate:rollback expected open error, got %v", err)
	}

	reset := NewMigrateResetCommand()
	reset.openDB = func(string) (dbSession, error) { return dbSession{}, openErr }
	if err := reset.Handle(newMigrationCmdContext(t, reset, fakeInput{}, "migrate:reset")); !errors.Is(err, openErr) {
		t.Fatalf("migrate:reset expected open error, got %v", err)
	}

	refresh := NewMigrateRefreshCommand()
	refresh.openDB = func(string) (dbSession, error) { return dbSession{}, openErr }
	if err := refresh.Handle(newMigrationCmdContext(t, refresh, fakeInput{}, "migrate:refresh")); !errors.Is(err, openErr) {
		t.Fatalf("migrate:refresh expected open error, got %v", err)
	}

	fresh := NewMigrateFreshCommand()
	fresh.openDB = func(string) (dbSession, error) { return dbSession{}, openErr }
	if err := fresh.Handle(newMigrationCmdContext(t, fresh, fakeInput{}, "migrate:fresh")); !errors.Is(err, openErr) {
		t.Fatalf("migrate:fresh expected open error, got %v", err)
	}

	seed := NewDBSeedCommand()
	seed.openDB = func(string) (dbSession, error) { return dbSession{}, openErr }
	if err := seed.Handle(newMigrationCmdContext(t, seed, fakeInput{}, "db:seed")); !errors.Is(err, openErr) {
		t.Fatalf("db:seed expected open error, got %v", err)
	}
}

func TestStatusRollbackAndResetNoTableBranches(t *testing.T) {
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	db, err := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "202604280701_empty.go")
	if err := os.WriteFile(file, []byte("package migrations"), 0o644); err != nil {
		t.Fatalf("write migration file failed: %v", err)
	}

	deps := MigrationDependencies{
		MigrationPaths: func() []string { return []string{dir} },
	}

	status := NewMigrateStatusCommand(deps)
	status.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	if err := status.Handle(newMigrationCmdContext(t, status, fakeInput{}, "migrate:status")); err != nil {
		t.Fatalf("migrate:status no-table branch failed: %v", err)
	}

	rollback := NewMigrateRollbackCommand(deps)
	rollback.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	if err := rollback.Handle(newMigrationCmdContext(t, rollback, fakeInput{}, "migrate:rollback")); err != nil {
		t.Fatalf("migrate:rollback no-table branch failed: %v", err)
	}

	reset := NewMigrateResetCommand(deps)
	reset.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }
	if err := reset.Handle(newMigrationCmdContext(t, reset, fakeInput{}, "migrate:reset")); err != nil {
		t.Fatalf("migrate:reset no-table branch failed: %v", err)
	}
}

func TestMigrateStepModeAndNoPendingBranch(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)

	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "local")

	db, err := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	dir := t.TempDir()
	m1 := "202604280801_create_step_a"
	m2 := "202604280802_create_step_b"
	_ = os.WriteFile(filepath.Join(dir, m1+".go"), []byte("package migrations"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, m2+".go"), []byte("package migrations"), 0o644)
	dbregistry.RegisterMigrationAs(m1, func(tx *gorm.DB) error { return tx.Exec("CREATE TABLE step_a (id integer primary key)").Error }, func(tx *gorm.DB) error { return tx.Exec("DROP TABLE step_a").Error })
	dbregistry.RegisterMigrationAs(m2, func(tx *gorm.DB) error { return tx.Exec("CREATE TABLE step_b (id integer primary key)").Error }, func(tx *gorm.DB) error { return tx.Exec("DROP TABLE step_b").Error })

	migrate := NewMigrateCommand(MigrationDependencies{MigrationPaths: func() []string { return []string{dir} }})
	migrate.openDB = func(string) (dbSession, error) { return dbSession{DB: db}, nil }

	stepCtx := newMigrationCmdContext(t, migrate, fakeInput{bools: map[string]bool{"step": true}}, "migrate")
	if err := migrate.Handle(stepCtx); err != nil {
		t.Fatalf("migrate step mode failed: %v", err)
	}

	store := newMigrationStore(db)
	applied, err := store.appliedMap()
	if err != nil {
		t.Fatalf("applied map failed: %v", err)
	}
	if applied[m1].Batch == applied[m2].Batch {
		t.Fatalf("step mode should assign different batches, got %d and %d", applied[m1].Batch, applied[m2].Batch)
	}

	noPendingCtx := newMigrationCmdContext(t, migrate, fakeInput{}, "migrate")
	if err := migrate.Handle(noPendingCtx); err != nil {
		t.Fatalf("migrate no-pending branch failed: %v", err)
	}
}

func TestDropAllViewsAndTypesAlternativeDialectBranches(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	mysqlLike := *db
	mysqlLike.Dialector = renamedDialector{Dialector: db.Dialector, name: "mysql"}
	if err := dropAllViews(&mysqlLike); err == nil {
		t.Fatal("expected mysql-like branch query error on sqlite")
	}

	postgresLike := *db
	postgresLike.Dialector = renamedDialector{Dialector: db.Dialector, name: "postgres"}
	if err := dropAllTypes(&postgresLike); err == nil {
		t.Fatal("expected postgres-like branch query error on sqlite")
	}
}

func TestParsePositiveIntBranches(t *testing.T) {
	if got := parsePositiveInt("-12"); got != 0 {
		t.Fatalf("parsePositiveInt(-12) = %d, want 0", got)
	}
	if got := parsePositiveInt("42"); got != 42 {
		t.Fatalf("parsePositiveInt(42) = %d, want 42", got)
	}
	if got := parsePositiveInt("x"); got != 0 {
		t.Fatalf("parsePositiveInt(x) = %d, want 0", got)
	}
}

func TestRunSeederClassNilRegisteredBranch(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)

	dbregistry.RegisterSeederAs("NilSeeder", nil)
	if err := runSeederClass(&gorm.DB{}, "NilSeeder"); err != nil {
		t.Fatalf("nil seeder branch should reuse registry validation result: %v", err)
	}
}

func TestRequireForceAllowsProductionWhenForced(t *testing.T) {
	if err := requireForceInProduction(true, "db:seed"); err != nil {
		t.Fatalf("force=true should bypass production guard: %v", err)
	}
}
