package event

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestDefaultNilSafeBeforeSet(t *testing.T) {
	useIsolatedFacadeRegistry(t)

	// 直接派发不应 panic，也无副作用。
	Dispatch(context.Background(), fakeEvent{name: "noop"})
	Listen("noop", ListenerFunc(func(_ context.Context, _ Event) error { return nil }))
}

func TestContainerBindingRoundTrip(t *testing.T) {
	bus := New()
	bindEventDispatcherForTest(t, bus)
	if Resolve() != bus {
		t.Fatal("Resolve should return the bus bound in the current container")
	}
	var fired int32
	Listen("e", ListenerFunc(func(_ context.Context, _ Event) error {
		atomic.AddInt32(&fired, 1)
		return nil
	}))
	Dispatch(context.Background(), fakeEvent{name: "e"})
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatalf("listener via package-level Dispatch fired %d times, want 1", fired)
	}
}

func TestFacadeExposesDispatcherMethods(t *testing.T) {
	bindEventDispatcherForTest(t, New())

	var fired int32
	ListenFunc("facade.listen_func", func(_ context.Context, _ Event) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})
	if !Has("facade.listen_func") {
		t.Fatal("Has should report listeners registered via ListenFunc")
	}

	Dispatch(context.Background(), fakeEvent{name: "facade.listen_func"})
	Forget("facade.listen_func")
	if Has("facade.listen_func") {
		t.Fatal("Has should return false after Forget")
	}
	Dispatch(context.Background(), fakeEvent{name: "facade.listen_func"})
	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("ListenFunc listener fired %d times, want 1", got)
	}

	sub := &counterSubscriber{}
	Subscribe(sub)
	Dispatch(context.Background(), fakeEvent{name: "user.created"})
	Dispatch(context.Background(), fakeEvent{name: "user.updated"})
	if atomic.LoadInt32(&sub.created) != 1 {
		t.Fatalf("subscriber created listener fired %d times, want 1", sub.created)
	}
	if atomic.LoadInt32(&sub.updated) != 1 {
		t.Fatalf("subscriber updated listener fired %d times, want 1", sub.updated)
	}
}

func TestPackageHelpersFallBackToNoopBusWithoutBinding(t *testing.T) {
	useIsolatedFacadeRegistry(t)

	Dispatch(context.Background(), fakeEvent{name: "noop"})
}

func TestDefaultAndPackageHelpersResolveRegisteredFactoryBeforeNoopFallback(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	calls := 0
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		calls++
		return New(), nil
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	var fired int32
	Listen("factory.event", ListenerFunc(func(_ context.Context, _ Event) error {
		atomic.AddInt32(&fired, 1)
		return nil
	}))
	Dispatch(context.Background(), fakeEvent{name: "factory.event"})

	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("factory dispatcher fired %d times, want 1", got)
	}
	if calls != 1 {
		t.Fatalf("factory calls = %d, want 1", calls)
	}
}

func TestResolveReturnsNilWithoutCurrentRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	if got := Resolve(); got != nil {
		t.Fatalf("Resolve without current registry = %#v, want nil", got)
	}
}

func TestContainerBindingCompatibility(t *testing.T) {
	bus := New()
	bindEventDispatcherForTest(t, bus)
	if Resolve() != bus {
		t.Fatal("container binding should remain compatible with Resolve")
	}
}
