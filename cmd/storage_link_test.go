package cmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
)

func TestStorageLinkCommandCreatesDefaultLink(t *testing.T) {
	root := setupStorageLinkApp(t, nil)

	cmd := NewStorageLinkCommand()
	if err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{})); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	assertSymlinkTarget(t, filepath.Join(root, "public", "storage"), filepath.Join(root, "storage", "app", "public"))
}

func TestStorageLinkCommandCreatesConfiguredLinks(t *testing.T) {
	root := setupStorageLinkApp(t, map[string]any{
		"public/storage": "storage/app/public",
		"":               "storage/app/ignored",
		"public/blank":   " ",
		"public/object":  map[string]any{"target": "storage/app/object"},
	})

	cmd := NewStorageLinkCommand()
	if err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{})); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	assertSymlinkTarget(t, filepath.Join(root, "public", "storage"), filepath.Join(root, "storage", "app", "public"))
	assertPathMissing(t, filepath.Join(root, "public", "blank"))
	assertPathMissing(t, filepath.Join(root, "public", "object"))
}

func TestStorageLinkCommandCreatesRelativeLink(t *testing.T) {
	root := setupStorageLinkApp(t, nil)

	cmd := NewStorageLinkCommand()
	if err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{bools: map[string]bool{"relative": true}})); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	assertSymlinkTarget(t, filepath.Join(root, "public", "storage"), filepath.Join("..", "storage", "app", "public"))
}

func TestStorageLinkCommandErrorsWhenLinkExistsWithoutForce(t *testing.T) {
	root := setupStorageLinkApp(t, nil)
	link := filepath.Join(root, "public", "storage")
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatalf("mkdir public: %v", err)
	}
	if err := os.WriteFile(link, []byte("existing"), 0o644); err != nil {
		t.Fatalf("write existing link path: %v", err)
	}

	cmd := NewStorageLinkCommand()
	err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{}))
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("Handle() error = %v, want already exists", err)
	}
	if data, readErr := os.ReadFile(link); readErr != nil || string(data) != "existing" {
		t.Fatalf("existing path should remain untouched, data = %q, err = %v", string(data), readErr)
	}
}

func TestStorageLinkCommandForceReplacesExistingSymlink(t *testing.T) {
	root := setupStorageLinkApp(t, nil)
	link := filepath.Join(root, "public", "storage")
	oldTarget := filepath.Join(root, "storage", "app", "old")
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatalf("mkdir public: %v", err)
	}
	if err := os.Symlink(oldTarget, link); err != nil {
		t.Fatalf("create existing symlink: %v", err)
	}

	cmd := NewStorageLinkCommand()
	if err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{bools: map[string]bool{"force": true}})); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	assertSymlinkTarget(t, link, filepath.Join(root, "storage", "app", "public"))
}

func TestStorageLinkCommandForceLeavesNormalPathsUntouched(t *testing.T) {
	for _, tt := range []struct {
		name   string
		create func(t *testing.T, path string)
		assert func(t *testing.T, path string)
	}{
		{
			name: "file",
			create: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("existing"), 0o644); err != nil {
					t.Fatalf("write existing file: %v", err)
				}
			},
			assert: func(t *testing.T, path string) {
				t.Helper()
				data, err := os.ReadFile(path)
				if err != nil || string(data) != "existing" {
					t.Fatalf("file should remain untouched, data = %q, err = %v", string(data), err)
				}
			},
		},
		{
			name: "directory",
			create: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Join(path, "nested"), 0o755); err != nil {
					t.Fatalf("mkdir existing directory: %v", err)
				}
			},
			assert: func(t *testing.T, path string) {
				t.Helper()
				if _, err := os.Stat(filepath.Join(path, "nested")); err != nil {
					t.Fatalf("directory should remain untouched: %v", err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := setupStorageLinkApp(t, nil)
			link := filepath.Join(root, "public", "storage")
			if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
				t.Fatalf("mkdir public: %v", err)
			}
			tt.create(t, link)

			cmd := NewStorageLinkCommand()
			err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{bools: map[string]bool{"force": true}}))
			if err == nil || !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("Handle() error = %v, want already exists", err)
			}
			tt.assert(t, link)
		})
	}
}

func TestStorageUnlinkCommandRemovesDefaultLink(t *testing.T) {
	root := setupStorageLinkApp(t, nil)
	link := filepath.Join(root, "public", "storage")
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatalf("mkdir public: %v", err)
	}
	if err := os.Symlink(filepath.Join(root, "storage", "app", "public"), link); err != nil {
		t.Fatalf("create default symlink: %v", err)
	}

	cmd := NewStorageUnlinkCommand()
	if err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{})); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	assertPathMissing(t, link)
}

func TestStorageUnlinkCommandRemovesConfiguredLinksAndIgnoresMissing(t *testing.T) {
	root := setupStorageLinkApp(t, map[string]any{
		"public/storage": "storage/app/public",
		"public/missing": "storage/app/missing",
	})
	link := filepath.Join(root, "public", "storage")
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatalf("mkdir public: %v", err)
	}
	if err := os.Symlink(filepath.Join(root, "storage", "app", "public"), link); err != nil {
		t.Fatalf("create configured symlink: %v", err)
	}

	cmd := NewStorageUnlinkCommand()
	if err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{})); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	assertPathMissing(t, link)
	assertPathMissing(t, filepath.Join(root, "public", "missing"))
}

func TestStorageUnlinkCommandLeavesNormalPathsUntouched(t *testing.T) {
	for _, tt := range []struct {
		name   string
		create func(t *testing.T, path string)
		assert func(t *testing.T, path string)
	}{
		{
			name: "file",
			create: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("existing"), 0o644); err != nil {
					t.Fatalf("write existing file: %v", err)
				}
			},
			assert: func(t *testing.T, path string) {
				t.Helper()
				data, err := os.ReadFile(path)
				if err != nil || string(data) != "existing" {
					t.Fatalf("file should remain untouched, data = %q, err = %v", string(data), err)
				}
			},
		},
		{
			name: "directory",
			create: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Join(path, "nested"), 0o755); err != nil {
					t.Fatalf("mkdir existing directory: %v", err)
				}
			},
			assert: func(t *testing.T, path string) {
				t.Helper()
				if _, err := os.Stat(filepath.Join(path, "nested")); err != nil {
					t.Fatalf("directory should remain untouched: %v", err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := setupStorageLinkApp(t, nil)
			link := filepath.Join(root, "public", "storage")
			if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
				t.Fatalf("mkdir public: %v", err)
			}
			tt.create(t, link)

			cmd := NewStorageUnlinkCommand()
			if err := cmd.Handle(storageLinkCommandContext(cmd, fakeInput{})); err != nil {
				t.Fatalf("Handle() error = %v", err)
			}
			tt.assert(t, link)
		})
	}
}

func setupStorageLinkApp(t *testing.T, links map[string]any) string {
	t.Helper()

	root := t.TempDir()
	c := container.NewContainer()
	container.SetProvider(func() *container.Container { return c })
	t.Cleanup(func() { container.SetProvider(nil) })

	configpkg.Add("filesystem", func() map[string]any {
		if links == nil {
			return map[string]any{}
		}
		return map[string]any{"links": links}
	})

	envPath := filepath.Join(root, ".env")
	if err := os.WriteFile(envPath, nil, 0o644); err != nil {
		t.Fatalf("write env file: %v", err)
	}
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(envPath); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	pathBindings := map[string]string{
		"path.base":    root,
		"path.public":  filepath.Join(root, "public"),
		"path.storage": filepath.Join(root, "storage"),
	}
	for key, value := range pathBindings {
		if err := c.Instance(key, value); err != nil {
			t.Fatalf("bind %s: %v", key, err)
		}
	}
	if err := c.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	return root
}

func storageLinkCommandContext(cmd console.Command, input fakeInput) console.CommandContext {
	// Direct contexts keep these command tests independent from Cobra flag parsing.
	return console.NewCommandContext(
		context.Background(),
		cmd,
		*cmd.Definition(),
		input,
		console.NewIO(strings.NewReader(""), io.Discard, io.Discard),
		nil,
		&cobra.Command{Use: cmd.Definition().Name},
	)
}

func assertSymlinkTarget(t *testing.T, link string, want string) {
	t.Helper()

	got, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("read symlink %q: %v", link, err)
	}
	if got != want {
		t.Fatalf("symlink %q target = %q, want %q", link, got, want)
	}
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("path %q should not exist, stat err = %v", path, err)
	}
}
