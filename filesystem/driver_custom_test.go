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

type reentrantCloseDriver struct {
	*fakeDriver
	manager *Manager
}

type failingCloseDriver struct {
	*fakeDriver
	err error
}

func (d *failingCloseDriver) Close() error { return d.err }

type managerClosingDriver struct {
	*fakeDriver
	manager *Manager
}

func (d *managerClosingDriver) Close() error { return d.manager.Close() }

type blockingCloseDriver struct {
	*fakeDriver
	entered chan struct{}
	release chan struct{}
	err     error
}

func (d *blockingCloseDriver) Close() error {
	close(d.entered)
	<-d.release
	return d.err
}

func (d *reentrantCloseDriver) Close() error {
	d.manager.Extend("registered-during-close", func(DriverFactoryContext) (Driver, error) {
		return nil, nil
	})
	return nil
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
	manager.Extend(driverName, func(factoryCtx DriverFactoryContext) (Driver, error) {
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

func TestCustomDriverFactoryCanExtendManager(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "adapter"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	manager.Extend("adapter", func(DriverFactoryContext) (Driver, error) {
		manager.Extend("registered-during-build", func(DriverFactoryContext) (Driver, error) {
			return nil, nil
		})
		return &fakeDriver{files: make(map[string]fakeFile)}, nil
	})

	done := make(chan struct{})
	go func() {
		manager.Default()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("custom driver factory deadlocked while extending its manager")
	}
}

func TestCustomDriverCloseCanExtendManager(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "adapter"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	manager.Extend("adapter", func(DriverFactoryContext) (Driver, error) {
		return &reentrantCloseDriver{
			fakeDriver: &fakeDriver{files: make(map[string]fakeFile)},
			manager:    manager,
		}, nil
	})
	manager.Default()

	done := make(chan struct{})
	go func() {
		_ = manager.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("custom driver Close deadlocked while extending its manager")
	}
}

func TestManagerRetainsCloseErrorFromInFlightDriver(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "adapter"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	factoryEntered := make(chan struct{})
	releaseFactory := make(chan struct{})
	wantErr := errors.New("close failed")
	manager.Extend("adapter", func(DriverFactoryContext) (Driver, error) {
		close(factoryEntered)
		<-releaseFactory
		return &failingCloseDriver{
			fakeDriver: &fakeDriver{files: make(map[string]fakeFile)},
			err:        wantErr,
		}, nil
	})
	diskDone := make(chan struct{})
	go func() {
		manager.Default()
		close(diskDone)
	}()
	<-factoryEntered
	if gotErr := manager.Close(); gotErr != nil {
		t.Fatalf("Close before driver construction = %v, want nil", gotErr)
	}
	close(releaseFactory)
	<-diskDone
	if gotErr := manager.Close(); !errors.Is(gotErr, wantErr) {
		t.Fatalf("Close after in-flight cleanup = %v, want retained %v", gotErr, wantErr)
	}
}

func TestCustomDriverCloseCanReenterManagerClose(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "adapter"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	manager.Extend("adapter", func(DriverFactoryContext) (Driver, error) {
		return &managerClosingDriver{
			fakeDriver: &fakeDriver{files: make(map[string]fakeFile)},
			manager:    manager,
		}, nil
	})
	manager.Default()

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- manager.Close()
	}()
	select {
	case gotErr := <-closeResult:
		if gotErr != nil {
			t.Fatalf("reentrant Manager.Close result = %v, want nil", gotErr)
		}
	case <-time.After(time.Second):
		t.Fatal("custom driver Close deadlocked while re-entering Manager.Close")
	}
	if gotErr := manager.Close(); gotErr != nil {
		t.Fatalf("completed Manager.Close result = %v, want nil", gotErr)
	}
}

func TestCustomDriverFactoryCanCloseManager(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "adapter"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	manager.Extend("adapter", func(DriverFactoryContext) (Driver, error) {
		if closeErr := manager.Close(); closeErr != nil {
			return nil, closeErr
		}
		return &fakeDriver{files: make(map[string]fakeFile)}, nil
	})

	done := make(chan struct{})
	go func() {
		manager.Default()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("custom driver factory deadlocked while closing its manager")
	}
}

func TestConcurrentManagerCloseReportsClosingThenFinalError(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "adapter"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	wantErr := errors.New("close failed")
	manager.Extend("adapter", func(DriverFactoryContext) (Driver, error) {
		return &blockingCloseDriver{
			fakeDriver: &fakeDriver{files: make(map[string]fakeFile)},
			entered:    entered,
			release:    release,
			err:        wantErr,
		}, nil
	})
	manager.Default()

	firstResult := make(chan error, 1)
	go func() { firstResult <- manager.Close() }()
	<-entered
	if gotErr := manager.Close(); !errors.Is(gotErr, ErrManagerClosing) {
		t.Fatalf("concurrent Close error = %v, want ErrManagerClosing", gotErr)
	}
	close(release)
	if gotErr := <-firstResult; !errors.Is(gotErr, wantErr) {
		t.Fatalf("first Close error = %v, want %v", gotErr, wantErr)
	}
	if gotErr := manager.Close(); !errors.Is(gotErr, wantErr) {
		t.Fatalf("completed Close error = %v, want retained %v", gotErr, wantErr)
	}
}

func TestCustomDriverUnknownNilAndIgnoredRegistrations(t *testing.T) {
	unknown, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "custom-ignored-nil"},
		},
	})
	if err != nil {
		t.Fatalf("new manager with ignored custom driver: %v", err)
	}
	unknown.Extend("", func(DriverFactoryContext) (Driver, error) {
		t.Fatal("empty custom driver registration should be ignored")
		return nil, nil
	})
	unknown.Extend("custom-ignored-nil", nil)
	if err := unknown.Default().Put(context.Background(), "key", "value"); !errors.Is(err, ErrUnsupportedDriver) {
		t.Fatalf("ignored custom driver err = %v, want ErrUnsupportedDriver", err)
	}

	nilDriver, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "custom-nil-driver"},
		},
	})
	if err != nil {
		t.Fatalf("new manager with nil custom driver: %v", err)
	}
	nilDriver.Extend("custom-nil-driver", func(DriverFactoryContext) (Driver, error) {
		return nil, nil
	})
	if err := nilDriver.Default().Put(context.Background(), "key", "value"); err == nil || !strings.Contains(err.Error(), "custom driver") {
		t.Fatalf("nil custom driver err = %v, want custom driver error", err)
	}

	broken, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: "custom-error-driver"},
		},
	})
	if err != nil {
		t.Fatalf("new manager with error custom driver: %v", err)
	}
	broken.Extend("custom-error-driver", func(DriverFactoryContext) (Driver, error) {
		return nil, errors.New("factory failed")
	})
	if err := broken.Default().Put(context.Background(), "key", "value"); err == nil || !strings.Contains(err.Error(), "factory failed") {
		t.Fatalf("factory error driver err = %v, want factory failed", err)
	}
}

func TestCustomDriverExtendReplacesExistingFactory(t *testing.T) {
	driverName := "custom-replace-driver"
	m, err := NewManager(Config{
		Default: "custom",
		Disks: map[string]DiskConfig{
			"custom": {Driver: driverName},
		},
	})
	if err != nil {
		t.Fatalf("new manager with replacement custom driver: %v", err)
	}
	m.Extend(driverName, func(DriverFactoryContext) (Driver, error) {
		return &fakeDriver{files: map[string]fakeFile{}, pathPrefix: t.TempDir()}, nil
	})
	m.Extend(driverName, func(DriverFactoryContext) (Driver, error) {
		return &fakeDriver{
			files: map[string]fakeFile{
				"key": {data: []byte("second")},
			},
			pathPrefix: t.TempDir(),
		}, nil
	})
	body, err := m.Default().Get(context.Background(), "key")
	if err != nil || string(body) != "second" {
		t.Fatalf("replacement custom driver body = %q, %v", body, err)
	}
}

func TestManagerDriverFactoriesAreApplicationLocal(t *testing.T) {
	newManager := func(t *testing.T) *Manager {
		t.Helper()
		manager, err := NewManager(Config{
			Default: "custom",
			Disks: map[string]DiskConfig{
				"custom": {Driver: "adapter"},
			},
		})
		if err != nil {
			t.Fatalf("NewManager failed: %v", err)
		}
		return manager
	}
	first := newManager(t)
	second := newManager(t)
	first.Extend("adapter", func(DriverFactoryContext) (Driver, error) {
		return &fakeDriver{files: map[string]fakeFile{"key": {data: []byte("first")}}}, nil
	})
	second.Extend("adapter", func(DriverFactoryContext) (Driver, error) {
		return &fakeDriver{files: map[string]fakeFile{"key": {data: []byte("second")}}}, nil
	})

	firstBody, err := first.Default().Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("first disk Get failed: %v", err)
	}
	secondBody, err := second.Default().Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("second disk Get failed: %v", err)
	}
	if string(firstBody) != "first" || string(secondBody) != "second" {
		t.Fatalf("application disk contents = (%q, %q), want (first, second)", firstBody, secondBody)
	}
}
