package filesystem

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type customFilesystemDriver struct {
	*fakeDriver
	closed atomic.Bool
}

func (d *customFilesystemDriver) Close() error {
	d.closed.Store(true)
	return d.fakeDriver.Close()
}

func TestCustomDriverRegisterBuildsRepositoryWithOptions(t *testing.T) {
	ctx := context.Background()
	driverName := "custom-memory-driver"
	var calls atomic.Int32
	var captured DriverFactoryContext
	var builtDriver *customFilesystemDriver

	Extend(driverName, func(factoryCtx DriverFactoryContext) (Driver, error) {
		calls.Add(1)
		captured = factoryCtx
		builtDriver = &customFilesystemDriver{
			fakeDriver: &fakeDriver{
				files:      make(map[string]fakeFile),
				pathPrefix: t.TempDir(),
				publicBase: "http://example.test/custom",
				tempBase:   "http://example.test/custom-temp",
				visibility: VisibilityPublic,
			},
		}
		return builtDriver, nil
	})

	manager, err := NewManager(Config{
		Default: "primary",
		Disks: map[string]DiskConfig{
			"primary": {
				Driver:     "CUSTOM-MEMORY-DRIVER",
				Visibility: VisibilityPublic,
				Options: map[string]any{
					"endpoint": "in-memory",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("new manager with custom driver: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	repo := manager.Default()
	if calls.Load() != 1 {
		t.Fatalf("custom driver factory calls = %d, want 1", calls.Load())
	}
	if repo != manager.Disk("primary") || calls.Load() != 1 {
		t.Fatalf("custom repository should be cached, calls=%d", calls.Load())
	}
	if captured.Name != "primary" || captured.Driver != driverName {
		t.Fatalf("unexpected custom context name/driver: %#v", captured)
	}
	if captured.Config.Visibility != VisibilityPublic {
		t.Fatalf("unexpected custom context visibility: %#v", captured.Config)
	}
	if captured.Config.Options["endpoint"] != "in-memory" {
		t.Fatalf("custom options not passed: %#v", captured.Config.Options)
	}
	captured.Config.Options["endpoint"] = "mutated"
	if manager.specs["primary"].Options["endpoint"] != "in-memory" {
		t.Fatalf("manager disk config should not share factory context options: %#v", manager.specs["primary"].Options)
	}

	if err := repo.Put(ctx, "docs/readme.txt", "hello custom"); err != nil {
		t.Fatalf("custom put: %v", err)
	}
	content, err := repo.Get(ctx, "docs/readme.txt")
	if err != nil {
		t.Fatalf("custom get: %v", err)
	}
	if string(content) != "hello custom" {
		t.Fatalf("custom get content = %q, want hello custom", string(content))
	}
	publicURL, err := repo.URL("docs/readme.txt")
	if err != nil || publicURL != "http://example.test/custom/docs/readme.txt" {
		t.Fatalf("custom url = %q err=%v", publicURL, err)
	}
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Second)
	tempURL, err := repo.TemporaryURL(ctx, "docs/readme.txt", expires)
	if err != nil || !strings.Contains(tempURL, "docs/readme.txt") {
		t.Fatalf("custom temporary url = %q err=%v", tempURL, err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("custom manager close: %v", err)
	}
	if !builtDriver.closed.Load() {
		t.Fatal("custom close should be called")
	}
}

func TestCustomDriverUnknownNilAndIgnoredRegistrations(t *testing.T) {
	Extend("", func(DriverFactoryContext) (Driver, error) {
		t.Fatal("empty custom driver registration should be ignored")
		return nil, nil
	})
	Extend("custom-ignored-nil", nil)

	unknown, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "custom-ignored-nil"},
		},
	})
	if err != nil {
		t.Fatalf("new manager with ignored custom driver: %v", err)
	}
	if err := unknown.Default().Put(context.Background(), "key", "value"); !errors.Is(err, ErrUnsupportedDriver) {
		t.Fatalf("ignored custom driver err = %v, want ErrUnsupportedDriver", err)
	}

	Extend("custom-nil-driver", func(DriverFactoryContext) (Driver, error) {
		return nil, nil
	})
	nilDriver, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "custom-nil-driver"},
		},
	})
	if err != nil {
		t.Fatalf("new manager with nil custom driver: %v", err)
	}
	if err := nilDriver.Default().Put(context.Background(), "key", "value"); err == nil || !strings.Contains(err.Error(), "custom driver") {
		t.Fatalf("nil custom driver err = %v, want custom driver error", err)
	}

	Extend("custom-error-driver", func(DriverFactoryContext) (Driver, error) {
		return nil, errors.New("factory failed")
	})
	broken, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "custom-error-driver"},
		},
	})
	if err != nil {
		t.Fatalf("new manager with error custom driver: %v", err)
	}
	if err := broken.Default().Put(context.Background(), "key", "value"); err == nil || !strings.Contains(err.Error(), "factory failed") {
		t.Fatalf("factory error driver err = %v, want factory failed", err)
	}
}

func TestCustomDriverExtendReplacesExistingFactory(t *testing.T) {
	driverName := "custom-replace-driver"
	Extend(driverName, func(DriverFactoryContext) (Driver, error) {
		return &fakeDriver{files: map[string]fakeFile{}, pathPrefix: t.TempDir()}, nil
	})
	Extend(driverName, func(DriverFactoryContext) (Driver, error) {
		return &fakeDriver{
			files: map[string]fakeFile{
				"key": {data: []byte("second")},
			},
			pathPrefix: t.TempDir(),
		}, nil
	})

	m, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: driverName},
		},
	})
	if err != nil {
		t.Fatalf("new manager with replacement custom driver: %v", err)
	}
	body, err := m.Default().Get(context.Background(), "key")
	if err != nil || string(body) != "second" {
		t.Fatalf("replacement custom driver body = %q, %v", body, err)
	}
}
