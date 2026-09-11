package queue

import (
	"context"
	"path/filepath"
	"testing"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	queuecontract "github.com/prismgo/framework/contracts/queue"
)

func TestServiceProviderRegistersLazyQueueFactory(t *testing.T) {
	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound("queue.manager") {
		t.Fatal("provider Register should bind queue manager factory")
	}
	if !registry.Bound(queuecontract.DispatcherServiceKey) {
		t.Fatal("provider Register should bind queue dispatcher factory")
	}
	if registry.Resolved("queue.manager") {
		t.Fatal("provider Register should not construct queue manager")
	}
	if registry.Resolved(queuecontract.DispatcherServiceKey) {
		t.Fatal("provider Register should not construct queue dispatcher")
	}
}

func TestServiceProviderBuildsManagerFromApplicationConfigWithoutCurrentFacade(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })
	configpkg.Add("queue", func() map[string]any {
		return map[string]any{
			"default": "external",
			"connections": map[string]any{
				"external": map[string]any{"driver": "external"},
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
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	manager, err := ManagerFrom(registry)
	if err != nil {
		t.Fatalf("resolve manager without current facade: %v", err)
	}
	if _, err := manager.Queue(""); err == nil {
		t.Fatal("first Queue should report the unregistered external driver")
	}
}

func TestServiceProviderPreservesExplicitQueueManager(t *testing.T) {
	registry := container.NewContainer()
	explicit, err := NewManager(Config{Default: "sync"}, NewRegistry())
	if err != nil {
		t.Fatalf("new explicit queue manager: %v", err)
	}
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("queue.manager", explicit); err != nil {
		t.Fatalf("seed queue manager: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("queue.manager")
	if err != nil {
		t.Fatalf("resolve queue manager: %v", err)
	}
	got, _ := raw.(*Manager)
	if got != explicit {
		t.Fatal("service provider should preserve explicit queue manager")
	}
	rawDispatcher, err := registry.Make(queuecontract.DispatcherServiceKey)
	if err != nil {
		t.Fatalf("resolve queue dispatcher: %v", err)
	}
	dispatcher, _ := rawDispatcher.(queuecontract.Dispatcher)
	if dispatcher == nil {
		t.Fatal("service provider should expose queue dispatcher contract")
	}
}

func TestServiceProviderPreservesExplicitQueueDispatcher(t *testing.T) {
	registry := container.NewContainer()
	explicit := recordingQueueDispatcher{}
	if err := registry.Instance(queuecontract.DispatcherServiceKey, explicit); err != nil {
		t.Fatalf("seed dispatcher: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make(queuecontract.DispatcherServiceKey)
	if err != nil {
		t.Fatalf("resolve queue dispatcher: %v", err)
	}
	got, _ := raw.(queuecontract.Dispatcher)
	if got != explicit {
		t.Fatal("service provider should preserve explicit queue dispatcher")
	}
}

func TestServiceProviderBootInstallsEventSink(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	bus := &recordingDispatcher{}
	if err := registry.Instance("event.dispatcher", eventcontract.Dispatcher(bus)); err != nil {
		t.Fatalf("register event bus: %v", err)
	}

	if err := (ServiceProvider{}).Boot(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
	fire(context.Background(), JobQueued{Connection: "sync", Queue: "default"})
	if bus.count != 1 || bus.lastName != EventJobQueued {
		t.Fatalf("queue event sink dispatch = count:%d name:%q, want 1/%q", bus.count, bus.lastName, EventJobQueued)
	}
}

func TestServiceProviderEventDispatchSkipsMissingCurrentDispatcher(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	dispatchCurrentEvent(context.Background(), nil)
	dispatchCurrentEvent(context.Background(), JobQueued{Connection: "sync", Queue: "default"})

	container.SetProvider(nil)
	dispatchCurrentEvent(context.Background(), JobQueued{Connection: "sync", Queue: "default"})
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }

type recordingDispatcher struct {
	count    int
	lastName string
}

func (d *recordingDispatcher) Dispatch(_ context.Context, ev eventcontract.Event) {
	d.count++
	d.lastName = ev.Name()
}
func (d *recordingDispatcher) Forget(_ string)                           {}
func (d *recordingDispatcher) Listen(_ string, _ eventcontract.Listener) {}
func (d *recordingDispatcher) ListenFunc(_ string, _ func(context.Context, eventcontract.Event) error) {
}
func (d *recordingDispatcher) Subscribe(_ eventcontract.Subscriber) {}
func (d *recordingDispatcher) Has(_ string) bool                    { return false }

type recordingQueueDispatcher struct{}

func (recordingQueueDispatcher) DispatchJob(context.Context, queuecontract.Job, queuecontract.DispatchOptions) (string, error) {
	return "job-id", nil
}
func (recordingQueueDispatcher) RequestRestart(context.Context) error { return nil }
func (recordingQueueDispatcher) Close() error                         { return nil }
