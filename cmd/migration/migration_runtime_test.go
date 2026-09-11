package migration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCollectMigrationsFromGoFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "202604280101_create_users.go"), []byte("package migrations"), 0o644); err != nil {
		t.Fatalf("write migration file error = %v, want nil", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# ignore"), 0o644); err != nil {
		t.Fatalf("write non-migration file error = %v, want nil", err)
	}

	migrations, err := collectMigrations([]string{dir}, true)
	if err != nil {
		t.Fatalf("collect migrations error = %v, want nil", err)
	}
	if len(migrations) != 1 {
		t.Fatalf("migrations length = %d, want 1", len(migrations))
	}
	if migrations[0].Name != "202604280101_create_users" {
		t.Fatalf("migration name = %q, want %q", migrations[0].Name, "202604280101_create_users")
	}
}

func TestCollectMigrationsRejectsDuplicateNamesAcrossPaths(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	name := "202604280201_conflict.go"
	if err := os.WriteFile(filepath.Join(dir1, name), []byte("package migrations"), 0o644); err != nil {
		t.Fatalf("write first migration file error = %v, want nil", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, name), []byte("package migrations"), 0o644); err != nil {
		t.Fatalf("write second migration file error = %v, want nil", err)
	}

	_, err := collectMigrations([]string{dir1, dir2}, true)
	if err == nil || !strings.Contains(err.Error(), "duplicated migration") {
		t.Fatalf("collect migrations error = %v, want duplicated migration error", err)
	}
}
