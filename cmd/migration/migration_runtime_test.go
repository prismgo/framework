package migration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	dbregistry "github.com/prismgo/framework/database"
)

func TestCollectMigrationsFromGoFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "202604280101_create_users.go"), []byte("package migrations"), 0o644); err != nil {
		t.Fatalf("write migration file failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# ignore"), 0o644); err != nil {
		t.Fatalf("write non-migration file failed: %v", err)
	}

	migrations, err := collectMigrations([]string{dir}, true)
	if err != nil {
		t.Fatalf("collect migrations failed: %v", err)
	}
	if len(migrations) != 1 {
		t.Fatalf("migrations len = %d, want 1", len(migrations))
	}
	if migrations[0].Name != "202604280101_create_users" {
		t.Fatalf("migration name = %q, want 202604280101_create_users", migrations[0].Name)
	}
}

func TestCollectMigrationsRejectsDuplicateNamesAcrossPaths(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	name := "202604280201_conflict.go"
	_ = os.WriteFile(filepath.Join(dir1, name), []byte("package migrations"), 0o644)
	_ = os.WriteFile(filepath.Join(dir2, name), []byte("package migrations"), 0o644)

	_, err := collectMigrations([]string{dir1, dir2}, true)
	if err == nil || !strings.Contains(err.Error(), "duplicated migration") {
		t.Fatalf("expected duplicated migration error, got %v", err)
	}
}

func TestStoreLifecycleAndApplyMigrationHandlers(t *testing.T) {
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	store := newMigrationStore(db)
	if err := store.ensureTable(); err != nil {
		t.Fatalf("ensure migration table failed: %v", err)
	}
	if err := store.markApplied("202604280301_demo", 1); err != nil {
		t.Fatalf("mark applied failed: %v", err)
	}
	applied, err := store.appliedMap()
	if err != nil || applied["202604280301_demo"].Batch != 1 {
		t.Fatalf("applied map unexpected: %v %#v", err, applied)
	}
	if err := store.deleteApplied("202604280301_demo"); err != nil {
		t.Fatalf("delete applied failed: %v", err)
	}

	migrationName := "202604280302_registry_apply"
	dbregistry.RegisterMigrationAs(migrationName,
		func(tx *gorm.DB) error {
			return tx.Exec("CREATE TABLE runtime_apply_demo (id integer primary key)").Error
		},
		func(tx *gorm.DB) error { return tx.Exec("DROP TABLE runtime_apply_demo").Error },
	)
	spec := migrationSpec{Name: migrationName}
	if err := applyMigrationUp(db, spec, false); err != nil {
		t.Fatalf("apply up failed: %v", err)
	}
	if !db.Migrator().HasTable("runtime_apply_demo") {
		t.Fatal("expected runtime_apply_demo table to exist")
	}
	if err := applyMigrationDown(db, spec, false); err != nil {
		t.Fatalf("apply down failed: %v", err)
	}
}

func TestApplyMigrationAndTrackCommitsSchemaAndRecord(t *testing.T) {
	db := openRuntimeMigrationTestDB(t)
	store := newMigrationStore(db)
	if err := store.ensureTable(); err != nil {
		t.Fatalf("ensure migration table failed: %v", err)
	}

	migrationName := "202604280303_registry_apply_and_track"
	dbregistry.RegisterMigrationAs(migrationName,
		func(tx *gorm.DB) error {
			return tx.Exec("CREATE TABLE tracked_apply_demo (id integer primary key)").Error
		},
		func(tx *gorm.DB) error {
			return tx.Exec("DROP TABLE tracked_apply_demo").Error
		},
	)

	if err := applyMigrationAndTrack(db, migrationSpec{Name: migrationName}, 2); err != nil {
		t.Fatalf("apply migration and track failed: %v", err)
	}
	if !db.Migrator().HasTable("tracked_apply_demo") {
		t.Fatal("expected tracked_apply_demo table to exist")
	}
	applied, err := store.appliedMap()
	if err != nil {
		t.Fatalf("read applied map failed: %v", err)
	}
	if applied[migrationName].Batch != 2 {
		t.Fatalf("applied batch = %d, want 2", applied[migrationName].Batch)
	}
}

func TestRollbackMigrationAndTrackCommitsSchemaAndRecord(t *testing.T) {
	db := openRuntimeMigrationTestDB(t)
	store := newMigrationStore(db)
	if err := store.ensureTable(); err != nil {
		t.Fatalf("ensure migration table failed: %v", err)
	}

	migrationName := "202604280304_registry_rollback_and_track"
	dbregistry.RegisterMigrationAs(migrationName,
		func(tx *gorm.DB) error {
			return tx.Exec("CREATE TABLE tracked_rollback_demo (id integer primary key)").Error
		},
		func(tx *gorm.DB) error {
			return tx.Exec("DROP TABLE tracked_rollback_demo").Error
		},
	)
	if err := applyMigrationAndTrack(db, migrationSpec{Name: migrationName}, 3); err != nil {
		t.Fatalf("seed apply migration and track failed: %v", err)
	}
	if err := rollbackMigrationAndTrack(db, migrationSpec{Name: migrationName}); err != nil {
		t.Fatalf("rollback migration and track failed: %v", err)
	}
	if db.Migrator().HasTable("tracked_rollback_demo") {
		t.Fatal("expected tracked_rollback_demo table to be dropped")
	}
	applied, err := store.appliedMap()
	if err != nil {
		t.Fatalf("read applied map failed: %v", err)
	}
	if _, exists := applied[migrationName]; exists {
		t.Fatalf("expected migration record %s to be removed", migrationName)
	}
}

func openRuntimeMigrationTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	return db
}
