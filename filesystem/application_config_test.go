package filesystem

import (
	"context"
	"path/filepath"
	"strings"
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

func TestNewManagerFromConfigAndProviderLifecycleBranches(t *testing.T) {
	// 测试意图：覆盖从运行时配置构造 Manager 的生产入口，以及 ServiceProvider 的无副作用生命周期分支。
	registry := useFilesystemTestContainer(t)
	root := t.TempDir()

	configpkg.Add("app", func() map[string]any {
		return map[string]any{
			"key": "new-manager-config-key",
			"url": "http://app.test",
		}
	})
	configpkg.Add("filesystem", func() map[string]any {
		return map[string]any{
			"default": "local",
			"cloud":   "local",
			"disks": map[string]any{
				"local": map[string]any{
					"driver":     "local",
					"root":       filepath.Join(root, "local"),
					"visibility": "private",
					"serve":      true,
				},
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

	closeManager, manager, err := NewManagerFromConfig()
	if err != nil {
		t.Fatalf("NewManagerFromConfig error = %v", err)
	}
	if manager.DefaultName() != "local" || manager.CloudName() != "local" {
		t.Fatalf("unexpected manager aliases: default=%s cloud=%s", manager.DefaultName(), manager.CloudName())
	}
	if err := manager.Default().Put(context.Background(), "docs/readme.txt", "ok"); err != nil {
		t.Fatalf("manager from config put: %v", err)
	}
	if closeManager == nil {
		t.Fatal("NewManagerFromConfig returned nil close function")
	}
	if err := closeManager(); err != nil {
		t.Fatalf("close manager from config: %v", err)
	}

	provider := ServiceProvider{}
	if provider.Name() != "filesystem" {
		t.Fatalf("provider name = %q, want filesystem", provider.Name())
	}
	if err := provider.Boot(filesystemProviderApp{registry: registry}); err != nil {
		t.Fatalf("provider boot: %v", err)
	}
	if ManagerCloseOption() == nil {
		t.Fatal("ManagerCloseOption returned nil binding option")
	}

	explicit := newTestManager(t, t.TempDir())
	if err := registry.Instance(serviceKey, explicit); err != nil {
		t.Fatalf("bind explicit manager: %v", err)
	}
	if err := provider.Register(filesystemProviderApp{registry: registry}); err != nil {
		t.Fatalf("provider register with existing manager: %v", err)
	}
	if got := Resolve(); got != explicit {
		t.Fatal("provider should preserve explicitly bound manager")
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

func TestBuildConfigParsesFilesystemLinks(t *testing.T) {
	// The application config should expose only valid link-to-target string pairs,
	// with relative paths resolved from the application base path.
	base := t.TempDir()
	c := useFilesystemTestContainer(t)
	if err := c.Instance("path.base", base); err != nil {
		t.Fatalf("bind base path: %v", err)
	}
	if err := c.Instance("path.storage", filepath.Join(base, "storage")); err != nil {
		t.Fatalf("bind storage path: %v", err)
	}
	absoluteLink := filepath.Join(t.TempDir(), "absolute-link")
	absoluteTarget := filepath.Join(t.TempDir(), "absolute-target")

	configpkg.Add("app", func() map[string]any {
		return map[string]any{
			"key": "filesystem-signing-key",
			"url": "http://app.test",
		}
	})
	configpkg.Add("filesystem", func() map[string]any {
		return map[string]any{
			"default": "local",
			"links": map[string]any{
				"public/storage": "storage/app/public",
				absoluteLink:     absoluteTarget,
				"ignored":        42,
				"":               "storage/app/empty-link",
				"blank-target":   " ",
			},
			"disks": map[string]any{
				"local": map[string]any{
					"driver": "local",
					"root":   "storage/app/private",
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
	if got, want := built.Links[filepath.Join(base, "public", "storage")], filepath.Join(base, "storage", "app", "public"); got != want {
		t.Fatalf("relative filesystem link target = %q, want %q", got, want)
	}
	if got := built.Links[absoluteLink]; got != absoluteTarget {
		t.Fatalf("absolute filesystem link target = %q, want %q", got, absoluteTarget)
	}
	if _, ok := built.Links[filepath.Join(base, "ignored")]; ok {
		t.Fatalf("non-string filesystem link target should be ignored: %#v", built.Links)
	}
	if _, ok := built.Links[filepath.Join(base, "blank-target")]; ok {
		t.Fatalf("blank filesystem link target should be ignored: %#v", built.Links)
	}
	if len(built.Links) != 2 {
		t.Fatalf("filesystem links should contain only valid pairs: %#v", built.Links)
	}
}

func TestBuildConfigRejectsEmptySigningKeyWhenServeEnabled(t *testing.T) {
	registry := useFilesystemTestContainer(t)
	base := t.TempDir()
	if err := registry.Instance("path.storage", filepath.Join(base, "storage")); err != nil {
		t.Fatalf("bind storage path: %v", err)
	}

	configpkg.Add("app", func() map[string]any {
		return map[string]any{
			"key": "", // 空的 app.key
			"url": "http://app.test",
		}
	})
	configpkg.Add("filesystem", func() map[string]any {
		return map[string]any{
			"default": "public",
			"temporary_url": map[string]any{
				"signing_key": "", // 空的 signing_key
			},
			"disks": map[string]any{
				"public": map[string]any{
					"driver":     "local",
					"root":       "storage/app/public",
					"url":        "http://app.test/storage",
					"visibility": "public",
					"serve":      true, // 启用了 serve
				},
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

	_, err := buildConfig()
	if err == nil {
		t.Fatal("expected error when signing key is empty and serve is enabled")
	}
	if !strings.Contains(err.Error(), "signing key") {
		t.Fatalf("expected signing key error, got: %v", err)
	}
}

func TestBuildConfigAllowsEmptySigningKeyWhenServeDisabled(t *testing.T) {
	registry := useFilesystemTestContainer(t)
	base := t.TempDir()
	if err := registry.Instance("path.storage", filepath.Join(base, "storage")); err != nil {
		t.Fatalf("bind storage path: %v", err)
	}

	configpkg.Add("app", func() map[string]any {
		return map[string]any{
			"key": "", // 空的 app.key
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
					"serve":  false, // 未启用 serve
				},
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

	built, err := buildConfig()
	if err != nil {
		t.Fatalf("buildConfig should succeed when serve is disabled: %v", err)
	}
	if built.TemporaryURL.SigningKey != "" {
		t.Fatalf("signing key should be empty when not configured: %q", built.TemporaryURL.SigningKey)
	}
}

type filesystemProviderApp struct{ registry containercontract.Container }

func (a filesystemProviderApp) Container() containercontract.Container { return a.registry }
