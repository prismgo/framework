package logger

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/sirupsen/logrus"
)

func TestFacadeLazyFactoryAndPackageHelpers(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	calls := 0
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		calls++
		return NewManager(Config{
			Default: "app",
			Channels: map[string]ChannelOptions{
				"app": {Driver: "null", Level: "debug"},
				"ops": {Driver: "null", Level: "info"},
			},
		})
	}, managerCloseOption()); err != nil {
		t.Fatalf("register logger factory: %v", err)
	}

	manager := Resolve()
	if manager == nil || calls != 1 {
		t.Fatalf("expected one lazy manager, got manager=%v calls=%d", manager, calls)
	}
	if Resolve() != manager {
		t.Fatal("Current should return the resolved manager")
	}
	if DefaultName() != "app" {
		t.Fatalf("expected facade DefaultName app, got %s", DefaultName())
	}

	Debug("debug")
	Debugf("debug %s", "fmt")
	Info("info")
	Infof("info %s", "fmt")
	Warn("warn")
	Warnf("warn %s", "fmt")
	Error("error")
	Errorf("error %s", "fmt")
	WithField("request_id", "r1").Info("with field")
	WithFields(map[string]any{"tenant": 7}).Warn("with fields")
	WithError(errors.New("boom")).Error("with error")
	Channel("ops").Info("ops")

	if got := Resolve(); got != manager || calls != 1 {
		t.Fatalf("expected cached manager, got manager=%v calls=%d", got, calls)
	}
	if err := Close(); err != nil {
		t.Fatalf("facade Close failed: %v", err)
	}
}

func TestFacadePanicsWhenFactoryFails(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return nil, errors.New("factory failed")
	}); err != nil {
		t.Fatalf("register failing factory: %v", err)
	}

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Resolve with failing factory did not panic")
		}
	}()

	_ = Resolve()
}

func TestApplicationManagerAndServiceProvider(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if err := registry.Instance("config.default", loggerTestConfig{
		store: map[string]any{
			"logging.default": "stack",
			"logging.channels": map[string]any{
				"app": map[string]any{"driver": "null", "level": "info"},
				"ops": map[string]any{"driver": "null", "level": "warn"},
				"stack": map[string]any{
					"driver":   "stack",
					"channels": []any{"app", "ops", ""},
				},
				"ignored": "not-a-map",
			},
		},
	}); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	closer, manager, err := NewManagerFromConfig()
	if err != nil {
		t.Fatalf("new application manager: %v", err)
	}
	if manager.DefaultName() != "stack" {
		t.Fatalf("expected stack default, got %s", manager.DefaultName())
	}
	if closer == nil {
		t.Fatal("expected closer")
	}
	if err := closer(); err != nil {
		t.Fatalf("close application manager: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}
	resolved := Resolve()
	if resolved == nil || resolved.DefaultName() != "stack" {
		t.Fatalf("unexpected registered manager: %#v", resolved)
	}
}

func TestApplicationManagerRejectsEmptyChannels(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("config.default", loggerTestConfig{}); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	_, _, err := NewManagerFromConfig()
	if err == nil {
		t.Fatal("expected empty logging.channels error")
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}
}

type loggerTestConfig struct {
	store map[string]any
}

func (c loggerTestConfig) GetString(path string, defaultValue ...any) string {
	if value, ok := c.store[path].(string); ok {
		return value
	}
	if len(defaultValue) > 0 {
		if value, ok := defaultValue[0].(string); ok {
			return value
		}
	}
	return ""
}

func (c loggerTestConfig) GetStringMap(path string) map[string]any {
	if value, ok := c.store[path].(map[string]any); ok {
		return value
	}
	return nil
}

func TestStackLoggerAndEntryConvenienceMethods(t *testing.T) {
	m, err := NewManager(Config{
		Default: "stack",
		Channels: map[string]ChannelOptions{
			"app":   {Driver: "null", Level: "debug"},
			"audit": {Driver: "null", Level: "debug"},
			"stack": {Driver: "stack", Channels: []string{"app", "audit"}},
		},
	})
	if err != nil {
		t.Fatalf("new stack manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	lg := m.Default()
	lg.Debug("debug")
	lg.Debugf("debug %s", "fmt")
	lg.Infof("info %s", "fmt")
	lg.Warn("warn")
	lg.Warnf("warn %s", "fmt")
	lg.Error("error")
	lg.Errorf("error %s", "fmt")
	lg.WithField("request_id", "r1").Info("with field")
	lg.WithFields(map[string]any{"tenant": 1}).Warn("with fields")
	lg.WithError(errors.New("stack error")).Error("with error")
	lg.Channel("app").Info("channel")

	entry := m.Channel("app").WithField("scope", "entry")
	entry.Debug("debug")
	entry.Debugf("debug %s", "fmt")
	entry.Infof("info %s", "fmt")
	entry.Warn("warn")
	entry.Warnf("warn %s", "fmt")
	entry.Errorf("error %s", "fmt")
	entry.WithField("next", true).Info("field")
	entry.WithFields(map[string]any{"next": "fields"}).Info("fields")
	entry.Channel("audit").Info("channel")
}

func TestManagerFallbackAndCustomRegistries(t *testing.T) {
	Extend("", func(ChannelOptions) (Driver, error) { return newNullDriver(), nil })
	Extend("test-null", func(ChannelOptions) (Driver, error) { return newNullDriver(), nil })
	Extend("test-replace", func(ChannelOptions) (Driver, error) { return newStderrDriver(), nil })
	Extend("test-replace", func(ChannelOptions) (Driver, error) { return newNullDriver(), nil })
	RegisterFormatter("", func(map[string]any) (Formatter, error) { return &logrus.TextFormatter{}, nil })
	RegisterFormatter("test-text", func(map[string]any) (Formatter, error) {
		return &logrus.TextFormatter{DisableColors: true}, nil
	})

	m, err := NewManager(Config{
		Default: "bad",
		Channels: map[string]ChannelOptions{
			"bad":      {Driver: "missing", Level: "debug"},
			"custom":   {Driver: "test-null", Formatter: "test-text", Level: "debug"},
			"replaced": {Driver: "test-replace", Level: "debug"},
		},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	m.Default().Info("falls back to null")
	m.Channel("custom").Info("custom")
	if _, ok := m.Channel("replaced").(*channel).driver.(*nullDriver); !ok {
		t.Fatal("custom driver replacement should use last factory")
	}
	if m.Channel("").Channel("missing") == nil {
		t.Fatal("expected fallback logger for missing channel")
	}

	if err := newStderrDriver().Close(); err != nil {
		t.Fatalf("stderr close: %v", err)
	}
	if _, err := io.WriteString(errWriter(), ""); err != nil {
		t.Fatalf("err writer: %v", err)
	}
}

func TestDriverFormatterAndFileErrorBranches(t *testing.T) {
	if _, err := buildDriver(ChannelOptions{}); err == nil {
		t.Fatal("expected empty driver error")
	}
	if _, err := buildFormatter("missing", nil); err == nil {
		t.Fatal("expected unknown formatter error")
	}
	if _, err := newChannel("bad-formatter", ChannelOptions{Driver: "null", Level: "info", Formatter: "missing"}, nil); err == nil {
		t.Fatal("expected channel formatter error")
	}
	if _, err := newDailyDriver(ChannelOptions{}); err == nil {
		t.Fatal("expected daily path error")
	}
	if _, err := newSingleDriver(ChannelOptions{}); err == nil {
		t.Fatal("expected single path error")
	}

	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	if _, err := newDailyDriver(ChannelOptions{Path: filepath.Join(blocker, "app.log")}); err == nil {
		t.Fatal("expected daily mkdir error")
	}
	if _, err := newSingleDriver(ChannelOptions{Path: filepath.Join(blocker, "app.log")}); err == nil {
		t.Fatal("expected single mkdir error")
	}
	if d, err := newSingleDriver(ChannelOptions{Path: dir}); err == nil {
		_ = d.Close()
		t.Fatal("expected single open directory error")
	}

	daily, err := newDailyDriver(ChannelOptions{Path: filepath.Join(dir, "daily-no-clock.log")})
	if err != nil {
		t.Fatalf("new daily without clock: %v", err)
	}
	if _, err := daily.Write([]byte("same-day\n")); err != nil {
		t.Fatalf("daily write: %v", err)
	}
	if err := daily.Close(); err != nil {
		t.Fatalf("daily close: %v", err)
	}
	if err := daily.Close(); err != nil {
		t.Fatalf("daily second close: %v", err)
	}

	single, err := newSingleDriver(ChannelOptions{Path: filepath.Join(dir, "single-close.log")})
	if err != nil {
		t.Fatalf("new single: %v", err)
	}
	if err := single.Close(); err != nil {
		t.Fatalf("single close: %v", err)
	}
	if err := single.Close(); err != nil {
		t.Fatalf("single second close: %v", err)
	}

	if got := datedLogPath(filepath.Join(dir, "plain"), "2026-04-30"); !strings.HasSuffix(got, "plain-2026-04-30.log") {
		t.Fatalf("unexpected no-ext dated path: %s", got)
	}
}

func TestManagerFallbackAndHelperBranches(t *testing.T) {
	if _, err := NewManager(Config{}); err == nil {
		t.Fatal("expected empty default error")
	}

	missingDefault := &Manager{
		defName:  "ghost",
		specs:    map[string]ChannelOptions{},
		resolved: make(map[string]Logger),
		building: make(map[string]bool),
	}
	if missingDefault.Channel("ghost") == nil {
		t.Fatal("expected null fallback for missing default")
	}

	m, err := NewManager(Config{
		Default: "app",
		Channels: map[string]ChannelOptions{
			"app": {Driver: "null", Level: "info"},
			"bad": {Driver: "missing", Level: "info"},
		},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.Channel("bad").Info("fallback to app")
	m.building["app"] = true
	if m.Channel("app") == nil {
		t.Fatal("expected null fallback while channel is building")
	}
	delete(m.building, "app")

	emptyStack, err := NewManager(Config{
		Default:  "stack",
		Channels: map[string]ChannelOptions{"stack": {Driver: "stack"}},
	})
	if err != nil {
		t.Fatalf("new empty stack manager: %v", err)
	}
	emptyStack.Default().Info("falls back to null")

	selfOnlyStack, err := NewManager(Config{
		Default:  "stack",
		Channels: map[string]ChannelOptions{"stack": {Driver: "stack", Channels: []string{"", "stack"}}},
	})
	if err != nil {
		t.Fatalf("new self stack manager: %v", err)
	}
	selfOnlyStack.Default().Info("falls back to null")
}

func TestNilManagerAndConversionBranches(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("DefaultName without manager did not panic")
			}
		}()
		_ = DefaultName()
	}()
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("Close without manager did not panic")
			}
		}()
		_ = Close()
	}()
	if err := (*channel)(nil).Close(); err != nil {
		t.Fatalf("nil channel close: %v", err)
	}
	if got := toLogrusFields(nil); len(got) != 0 {
		t.Fatalf("expected empty fields, got %#v", got)
	}

	c, err := newChannel("solo", ChannelOptions{Driver: "null", Level: "debug"}, nil)
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if c.Channel("other") != c {
		t.Fatal("channel without manager should return itself")
	}
	entryLogger, ok := c.WithField("scope", "entry").(*entry)
	if !ok {
		t.Fatal("expected entry logger")
	}
	if entryLogger.Channel("other") != entryLogger {
		t.Fatal("entry without manager should return itself")
	}
	stack := &stackLogger{}
	if stack.Channel("other") != stack {
		t.Fatal("stack without manager should return itself")
	}

	if got := castString(123); got != "" {
		t.Fatalf("expected non-string cast to empty, got %q", got)
	}
	if got := castStringSlice([]string{"app", "", "error"}); len(got) != 2 || got[0] != "app" || got[1] != "error" {
		t.Fatalf("unexpected string slice cast: %#v", got)
	}
	if got := castStringSlice(123); got != nil {
		t.Fatalf("expected unsupported slice cast to nil, got %#v", got)
	}
}

func TestManagerCloseReturnsFirstDriverError(t *testing.T) {
	Extend("close-error", func(ChannelOptions) (Driver, error) {
		return closeErrorDriver{}, nil
	})

	m, err := NewManager(Config{
		Default:  "bad-close",
		Channels: map[string]ChannelOptions{"bad-close": {Driver: "close-error", Level: "info"}},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.Default().Info("build channel")
	if err := m.Close(); err == nil {
		t.Fatal("expected close error")
	}
}

type closeErrorDriver struct{}

func (closeErrorDriver) Write(p []byte) (int, error) { return len(p), nil }
func (closeErrorDriver) Close() error                { return errors.New("close failed") }
