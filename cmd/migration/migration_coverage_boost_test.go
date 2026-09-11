package migration

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/spf13/cobra"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/console"
	dbregistry "github.com/prismgo/framework/database"
)

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

type migrationNamedDialector struct {
	gorm.Dialector
	name string
}

func (d migrationNamedDialector) Name() string { return d.name }

func TestDropAllTypesPreservesPostgresFallback(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: sqlDB, SkipInitializeWithVersion: true}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v, want nil", err)
	}
	db.Dialector = migrationNamedDialector{Dialector: db.Dialector, name: "postgres"}
	if err := dropAllViews(db); err != nil {
		t.Fatalf("dropAllViews() error = %v, want nil for unsupported legacy dialect", err)
	}
	mock.ExpectQuery("SELECT typname FROM pg_type").WillReturnRows(sqlmock.NewRows([]string{"typname"}).AddRow("status"))
	mock.ExpectExec("DROP TYPE IF EXISTS \\\"status\\\" CASCADE").WillReturnResult(sqlmock.NewResult(0, 1))

	if err := dropAllTypes(db); err != nil {
		t.Fatalf("dropAllTypes() error = %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("dropAllTypes() SQL mismatch: %v", err)
	}
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
