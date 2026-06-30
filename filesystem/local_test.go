package filesystem

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestLocalDriverMoveIsAtomic verifies that Move uses atomic os.Rename.
func TestLocalDriverMoveIsAtomic(t *testing.T) {
	root := t.TempDir()
	driver, err := newLocalDriver(DiskConfig{
		Root:       root,
		Visibility: VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("create local driver failed: %v", err)
	}
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close driver failed: %v", err)
		}
	}()

	ctx := context.Background()
	srcPath := filepath.Join(root, "source.txt")
	dstPath := filepath.Join(root, "dest.txt")

	// Write source file
	if err := os.WriteFile(srcPath, []byte("test content"), 0o644); err != nil {
		t.Fatalf("write source file failed: %v", err)
	}

	// Move should be atomic (use os.Rename)
	if err := driver.Move(ctx, "source.txt", "dest.txt"); err != nil {
		t.Fatalf("Move failed: %v", err)
	}

	// Source should not exist
	if _, err := os.Stat(srcPath); !os.IsNotExist(err) {
		t.Error("source file should not exist after move")
	}

	// Destination should exist with correct content
	data, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read dest file failed: %v", err)
	}
	if string(data) != "test content" {
		t.Errorf("dest content mismatch: got %q, want %q", string(data), "test content")
	}
}

// TestLocalDriverMoveCrossDirectory verifies move works across directories.
func TestLocalDriverMoveCrossDirectory(t *testing.T) {
	root := t.TempDir()
	driver, err := newLocalDriver(DiskConfig{
		Root:       root,
		Visibility: VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("create local driver failed: %v", err)
	}
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close driver failed: %v", err)
		}
	}()

	ctx := context.Background()
	subDir := filepath.Join(root, "subdir")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("create subdir failed: %v", err)
	}

	srcPath := filepath.Join(root, "file.txt")
	dstPath := filepath.Join(subDir, "moved.txt")

	if err := os.WriteFile(srcPath, []byte("content"), 0o644); err != nil {
		t.Fatalf("write source failed: %v", err)
	}

	if err := driver.Move(ctx, "file.txt", "subdir/moved.txt"); err != nil {
		t.Fatalf("cross-directory move failed: %v", err)
	}

	if _, err := os.Stat(srcPath); !os.IsNotExist(err) {
		t.Error("source should not exist")
	}
	if _, err := os.Stat(dstPath); err != nil {
		t.Error("destination should exist")
	}
}

// TestLocalDriverMoveNonExistentSource verifies error handling for missing source.
func TestLocalDriverMoveNonExistentSource(t *testing.T) {
	root := t.TempDir()
	driver, err := newLocalDriver(DiskConfig{
		Root:       root,
		Visibility: VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("create local driver failed: %v", err)
	}
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close driver failed: %v", err)
		}
	}()

	ctx := context.Background()
	err = driver.Move(ctx, "nonexistent.txt", "dest.txt")
	if err == nil {
		t.Error("expected error when moving non-existent source")
	}
}
