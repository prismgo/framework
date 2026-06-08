package cache

import (
	"context"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
)

func TestServiceProviderRegistersLazyCacheFactory(t *testing.T) {
	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound("cache.manager") {
		t.Fatal("provider Register should bind cache manager factory")
	}
	if registry.Resolved("cache.manager") {
		t.Fatal("provider Register should not construct cache manager")
	}
}

func TestServiceProviderLazyFactoryPanicsWithoutConfigFacade(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	assertPanics(t, func() { _, _ = container.Make[*Manager]("cache.manager") })
}

func TestServiceProviderPreservesExplicitCacheManager(t *testing.T) {
	registry := container.NewContainer()
	explicit, err := NewManager(Config{Default: "memory", Stores: map[string]StoreConfig{"memory": {Driver: "memory"}}})
	if err != nil {
		t.Fatalf("new explicit cache manager: %v", err)
	}
	if err := registry.Instance("cache.manager", explicit); err != nil {
		t.Fatalf("seed cache manager: %v", err)
	}
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	got, err := container.Make[*Manager]("cache.manager")
	if err != nil {
		t.Fatalf("resolve cache manager: %v", err)
	}
	if got != explicit {
		t.Fatal("service provider should preserve explicit cache manager")
	}
}

func TestServiceProviderPreservesCustomCacheFactory(t *testing.T) {
	registry := container.NewContainer()
	custom, err := NewManager(Config{Default: "memory", Stores: map[string]StoreConfig{"memory": {Driver: "memory"}}})
	if err != nil {
		t.Fatalf("new custom cache manager: %v", err)
	}
	if err := registry.Singleton("cache.manager", func(containercontract.Resolver) (any, error) {
		return custom, nil
	}); err != nil {
		t.Fatalf("seed cache factory: %v", err)
	}
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	got, err := container.Make[*Manager]("cache.manager")
	if err != nil {
		t.Fatalf("resolve cache manager: %v", err)
	}
	if got != custom {
		t.Fatal("service provider should preserve custom cache factory")
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
	dispatchCacheEvent(context.Background(), EventCacheHit, CacheEvent{Store: "memory"})
	if bus.count != 1 || bus.lastName != EventCacheHit {
		t.Fatalf("cache event sink dispatch = count:%d name:%q, want 1/%q", bus.count, bus.lastName, EventCacheHit)
	}
}

func TestServiceProviderEventDispatchSkipsMissingCurrentDispatcher(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	dispatchCurrentEvent(context.Background(), nil)
	dispatchCurrentEvent(context.Background(), CacheEvent{Event: EventCacheHit})

	container.SetProvider(nil)
	dispatchCurrentEvent(context.Background(), CacheEvent{Event: EventCacheHit})
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

func (d *recordingDispatcher) Listen(string, eventcontract.Listener) {}

func (d *recordingDispatcher) ListenFunc(string, func(context.Context, eventcontract.Event) error) {}

func (d *recordingDispatcher) Subscribe(eventcontract.Subscriber) {}

func (d *recordingDispatcher) Forget(string) {}

func (d *recordingDispatcher) Has(string) bool { return false }
