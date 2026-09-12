package logger

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/sirupsen/logrus"
)

func TestServiceProviderBridgesGlobalLogrusAfterResolve(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	path := filepath.Join(t.TempDir(), "global.log")
	if err := registry.Instance("config.default", loggerTestConfig{store: map[string]any{
		"logging.default": "demo",
		"logging.channels": map[string]any{"demo": map[string]any{
			"driver": "single", "level": "info", "path": path,
		}},
	}}); err != nil {
		t.Fatalf("bind logger configuration: %v", err)
	}
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register logger provider: %v", err)
	}
	t.Cleanup(func() {
		if err := registry.Close(context.Background()); err != nil {
			t.Errorf("close provider registry during cleanup: %v", err)
		}
	})
	if got := Resolve(); got == nil {
		t.Fatal("resolved manager = nil, want non-nil")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("log file before first write: stat error = %v, want not exist", err)
	}
	logrus.Info("provider bridge marker")
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "provider bridge marker") {
		t.Fatalf("global log output = %q, read error = %v; want provider bridge marker", data, err)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatalf("close provider registry: %v", err)
	}
}

func TestServiceProviderRegistersLazyLoggerFactory(t *testing.T) {
	registry := container.NewContainer()
	provider := ServiceProvider{}
	if err := provider.Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound("logger.manager") {
		t.Fatal("provider Register should bind logger manager factory")
	}
	if registry.Resolved("logger.manager") {
		t.Fatal("provider Register should not construct logger manager")
	}
}

func TestServiceProviderPreservesExplicitLoggerManager(t *testing.T) {
	registry := container.NewContainer()
	explicit, err := NewManager(Config{
		Default:  "null",
		Channels: map[string]ChannelOptions{"null": {Driver: "null"}},
	})
	if err != nil {
		t.Fatalf("new explicit manager: %v", err)
	}
	if err := registry.Instance("logger.manager", explicit); err != nil {
		t.Fatalf("seed logger manager: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("logger.manager")
	if err != nil {
		t.Fatalf("resolve logger manager: %v", err)
	}
	got, ok := raw.(*Manager)
	if !ok {
		t.Fatalf("resolve logger manager type = %T, want *Manager", raw)
	}
	if got != explicit {
		t.Fatal("service provider should preserve explicit logger manager")
	}
}

func TestServiceProviderIdentityAndBootAreStable(t *testing.T) {
	// Provider identity is used by application lifecycle bookkeeping and should stay stable.
	provider := ServiceProvider{}
	if got := provider.Name(); got != "logger" {
		t.Fatalf("provider name = %q, want logger", got)
	}

	// Boot intentionally has no side effects; channel construction remains lazy after Register.
	registry := container.NewContainer()
	if err := provider.Boot(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
	if registry.Bound("logger.manager") {
		t.Fatal("Boot should not register logger.manager")
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
