package filesystem

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
)

func TestServiceProviderBuildsManagerFromApplicationConfigWithoutCurrentFacade(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })
	root := t.TempDir()
	configpkg.Add("filesystem", func() map[string]any {
		return map[string]any{
			"default": "local",
			"disks": map[string]any{
				"local": map[string]any{"driver": "local", "root": root},
			},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	registry := container.NewContainer()
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := (ServiceProvider{}).Register(filesystemProviderApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	manager, err := ManagerFrom(registry)
	if err != nil {
		t.Fatalf("resolve manager without current facade: %v", err)
	}
	if manager.DefaultName() != "local" {
		t.Fatalf("default disk = %q, want local", manager.DefaultName())
	}
}

func TestManagerFromRejectsInvalidResolvers(t *testing.T) {
	if _, err := ManagerFrom(nil); err == nil || !strings.Contains(err.Error(), "resolver is nil") {
		t.Fatalf("ManagerFrom(nil) error = %v, want resolver is nil", err)
	}
	if _, err := ManagerFrom(container.NewContainer()); err == nil || !strings.Contains(err.Error(), "resolve manager") {
		t.Fatalf("ManagerFrom(empty) error = %v, want resolve manager error", err)
	}

	registry := container.NewContainer()
	if err := registry.Instance(serviceKey, "not-a-manager"); err != nil {
		t.Fatalf("bind invalid manager: %v", err)
	}
	if _, err := ManagerFrom(registry); err == nil || !strings.Contains(err.Error(), "want *filesystem.Manager") {
		t.Fatalf("ManagerFrom(invalid) error = %v, want manager type error", err)
	}
}

func TestManagerCloseOptionClosesBoundManager(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "local",
		Disks: map[string]DiskConfig{
			"local": {Driver: "local", Root: t.TempDir()},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v, want nil", err)
	}
	registry := container.NewContainer()
	if err := registry.Instance(serviceKey, manager, ManagerCloseOption()); err != nil {
		t.Fatalf("bind manager: %v", err)
	}
	if err := registry.Close(t.Context()); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := manager.Default().Put(t.Context(), "closed.txt", "value"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Put() after Close error = %v, want ErrManagerClosed", err)
	}
}
