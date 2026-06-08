package logger

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func useIsolatedFacadeRegistry(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func bindLoggerManagerForTest(t *testing.T, registry *container.Container, manager *Manager) {
	t.Helper()
	if err := registry.Instance(serviceKey, manager, managerCloseOption(), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	syncLogrusStandard(manager)
}

func TestResolveRequiresCurrentRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Resolve without current registry did not panic")
		}
	}()

	_ = Resolve()
}

func TestDefaultResolvesRegisteredFactoryBeforeFallbackLogger(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	calls := 0
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		calls++
		return NewManager(Config{
			Default: "factory",
			Channels: map[string]ChannelOptions{
				"factory": {Driver: "null", Level: "debug"},
			},
		})
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	if got := Resolve(); got == nil {
		t.Fatal("Resolve before default access returned nil")
	}
	if got := DefaultName(); got != "factory" {
		t.Fatalf("DefaultName = %q, want factory", got)
	}
	defaultLogger().Info("factory default")
	Channel("factory").Info("factory channel")
	if calls != 1 {
		t.Fatalf("factory calls = %d, want 1", calls)
	}
}

func TestLoggerManagerFacadeUsesReportingCloseGroup(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	manager, err := NewManager(Config{
		Default: "null",
		Channels: map[string]ChannelOptions{
			"null": {Driver: "null", Level: "debug"},
		},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	bindLoggerManagerForTest(t, registry, manager)

	for _, info := range container.List() {
		if info.Key == "logger.manager" {
			if info.CloseGroup != container.CloseGroupReporting {
				t.Fatalf("logger.manager close group = %q, want %q", info.CloseGroup, container.CloseGroupReporting)
			}
			return
		}
	}
	t.Fatal("logger.manager not found in container.List()")
}
