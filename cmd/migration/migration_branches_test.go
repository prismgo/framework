package migration

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
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
	named, err := openDatabaseSession("missing")
	if err != nil {
		t.Fatalf("open named session failed: %v", err)
	}
	named.Close()
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
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

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
	if err := applyMigrationUp(db, spec, false); err != nil {
		t.Fatalf("apply up failed: %v", err)
	}
	if !called {
		t.Fatal("expected migration up handler to run")
	}
	if err := applyMigrationDown(db, spec, false); err != nil {
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

func TestRollbackCandidatesAndDropBranches(t *testing.T) {
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	store := newMigrationStore(db)
	if err := store.ensureTable(); err != nil {
		t.Fatalf("ensure table failed: %v", err)
	}
	_ = store.markApplied("m1", 1)
	_ = store.markApplied("m2", 2)
	_ = store.markApplied("m3", 2)

	if list, err := store.rollbackCandidates(1, 0); err != nil || len(list) != 1 {
		t.Fatalf("rollback step candidates err=%v len=%d", err, len(list))
	}
	if list, err := store.rollbackCandidates(0, 2); err != nil || len(list) != 2 {
		t.Fatalf("rollback batch candidates err=%v len=%d", err, len(list))
	}
	if list, err := store.rollbackCandidates(0, 0); err != nil || len(list) != 2 {
		t.Fatalf("rollback latest batch candidates err=%v len=%d", err, len(list))
	}

	if err := db.Exec("CREATE TABLE demo_view_src (id integer primary key)").Error; err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if err := db.Exec("CREATE VIEW demo_view AS SELECT id FROM demo_view_src").Error; err != nil {
		t.Fatalf("create view failed: %v", err)
	}
	if err := dropAllViews(db); err != nil {
		t.Fatalf("drop views failed: %v", err)
	}
	if err := dropAllTypes(db); err != nil {
		t.Fatalf("drop types should be no-op on sqlite: %v", err)
	}
}
