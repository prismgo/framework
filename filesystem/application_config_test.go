package filesystem

import (
	"context"
	"path/filepath"
	"testing"

	configpkg "github.com/prismgo/framework/config"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestApplicationManagerBuildConfigWithCustomDriver(t *testing.T) {
	registry := useFilesystemTestContainer(t)

	var captured DriverFactoryContext
	Extend("config-custom-driver", func(factoryCtx DriverFactoryContext) (Driver, error) {
		captured = factoryCtx
		return &fakeDriver{
			files:      make(map[string]fakeFile),
			pathPrefix: t.TempDir(),
			publicBase: "http://example.test/config-custom",
			tempBase:   "http://example.test/config-temp",
			visibility: VisibilityPrivate,
		}, nil
	})

	root := t.TempDir()
	configpkg.Add("app", func() map[string]any {
		return map[string]any{
			"key": "filesystem-signing-key",
			"url": "http://app.test",
		}
	})
	configpkg.Add("filesystem", func() map[string]any {
		return map[string]any{
			"default": "custom",
			"cloud":   "custom",
			"temporary_url": map[string]any{
				"signing_key": "",
			},
			"disks": map[string]any{
				"custom": map[string]any{
					"driver":     "config-custom-driver",
					"visibility": "private",
					"serve":      "true",
					"endpoint":   "in-memory",
				},
				"public": map[string]any{
					"driver":     "local",
					"root":       filepath.Join(root, "public"),
					"url":        "",
					"visibility": "public",
					"serve":      true,
				},
				"ignored": "not-a-map",
			},
		}
	})

	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	if err := (ServiceProvider{}).Register(filesystemProviderApp{registry: registry}); err != nil {
		t.Fatalf("register filesystem provider: %v", err)
	}
	manager := Resolve()
	if manager == nil {
		t.Fatal("resolve application filesystem manager returned nil")
	}
	if manager.DefaultName() != "custom" || manager.CloudName() != "custom" {
		t.Fatalf("unexpected disk aliases: default=%s cloud=%s", manager.DefaultName(), manager.CloudName())
	}
	if manager.tempURL.SigningKey != "filesystem-signing-key" {
		t.Fatalf("temporary signing key = %q, want app key fallback", manager.tempURL.SigningKey)
	}

	if err := manager.Default().Put(context.Background(), "docs/readme.txt", "config custom"); err != nil {
		t.Fatalf("custom application disk put: %v", err)
	}
	if captured.Name != "custom" || captured.Driver != "config-custom-driver" {
		t.Fatalf("unexpected custom factory context: %#v", captured)
	}
	if !captured.Config.Serve || captured.Config.Options["endpoint"] != "in-memory" {
		t.Fatalf("custom factory config missing options: %#v", captured.Config)
	}
	captured.Config.Options["endpoint"] = "mutated"
	if manager.specs["custom"].Options["endpoint"] != "in-memory" {
		t.Fatalf("manager disk options should not be mutated: %#v", manager.specs["custom"].Options)
	}

	publicURL, err := manager.Disk("public").URL("demo.txt")
	if err != nil {
		t.Fatalf("public application disk url: %v", err)
	}
	if publicURL != "http://app.test/storage/demo.txt" {
		t.Fatalf("public application disk url = %q, want app fallback url", publicURL)
	}
}

func TestBuildConfigResolvesLocalRootsFromApplicationStorage(t *testing.T) {
	base := t.TempDir()
	c := useFilesystemTestContainer(t)
	if err := c.Instance("path.storage", filepath.Join(base, "storage")); err != nil {
		t.Fatalf("bind storage path: %v", err)
	}
	absoluteRoot := filepath.Join(t.TempDir(), "absolute-public")

	configpkg.Add("app", func() map[string]any {
		return map[string]any{
			"key": "filesystem-signing-key",
			"url": "http://app.test",
		}
	})
	configpkg.Add("filesystem", func() map[string]any {
		return map[string]any{
			"default": "local",
			"disks": map[string]any{
				"local": map[string]any{
					"driver": "local",
					"root":   "storage/app/private",
				},
				"implicit-local": map[string]any{
					"root": "storage/app/implicit",
				},
				"public": map[string]any{
					"driver": "local",
					"root":   absoluteRoot,
					"url":    "http://cdn.test/storage",
				},
				"oss": map[string]any{
					"driver": "oss",
					"root":   "oss-relative-root",
				},
			},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if err := c.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	built, err := buildConfig()
	if err != nil {
		t.Fatalf("buildConfig error = %v", err)
	}
	if got, want := built.Disks["local"].Root, filepath.Join(base, "storage", "app", "private"); got != want {
		t.Fatalf("local root = %q, want %q", got, want)
	}
	if got, want := built.Disks["implicit-local"].Root, filepath.Join(base, "storage", "app", "implicit"); got != want {
		t.Fatalf("implicit local root = %q, want %q", got, want)
	}
	if got := built.Disks["public"].Root; got != absoluteRoot {
		t.Fatalf("absolute public root = %q, want %q", got, absoluteRoot)
	}
	if got := built.Disks["oss"].Root; got != "oss-relative-root" {
		t.Fatalf("oss root = %q, want original relative root", got)
	}
}

type filesystemProviderApp struct{ registry containercontract.Container }

func (a filesystemProviderApp) Container() containercontract.Container { return a.registry }
