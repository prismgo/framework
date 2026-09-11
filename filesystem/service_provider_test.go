package filesystem

import (
	"path/filepath"
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
