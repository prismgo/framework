package path

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFromUsesWorkingDirectoryMarkerBeforeExecutableFallback(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "pkg", "tool")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.work"), []byte("go 1.26.2\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	if got := From(nested, t.TempDir()); got != root {
		t.Fatalf("From(nested, exe) = %q, want %q", got, root)
	}
}

func TestFromUsesExecutableDirectoryWhenItIsOnlySignal(t *testing.T) {
	root := t.TempDir()
	workingDirectory := t.TempDir()

	if got := From(workingDirectory, root); got != root {
		t.Fatalf("From(cwd, executable dir) = %q, want %q", got, root)
	}
}

func TestFromUsesStorageOrPublicLayoutWithoutConfig(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "storage"), 0o755); err != nil {
		t.Fatalf("mkdir storage: %v", err)
	}

	if got := From(t.TempDir(), root); got != root {
		t.Fatalf("From(cwd, storage layout) = %q, want %q", got, root)
	}
}

func TestJoinPreservesAbsoluteChildPath(t *testing.T) {
	root := t.TempDir()
	absolute := filepath.Join(t.TempDir(), "logs", "app.log")

	if got := Join(root, absolute); got != absolute {
		t.Fatalf("Join(root, abs) = %q, want %q", got, absolute)
	}
}
