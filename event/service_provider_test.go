package event

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/queue"
)

func TestServiceProviderRegistersDispatcherFactory(t *testing.T) {
	registry := container.NewContainer()
	provider := ServiceProvider{}

	if err := provider.Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("event.dispatcher")
	if err != nil {
		t.Fatalf("resolve dispatcher failed: %v", err)
	}
	if dispatcher, ok := raw.(*Dispatcher); !ok || dispatcher == nil {
		t.Fatal("expected dispatcher from service provider")
	}
}

func TestServiceProviderPreservesExplicitDispatcher(t *testing.T) {
	registry := container.NewContainer()
	explicit := New()
	if err := registry.Instance("event.dispatcher", explicit); err != nil {
		t.Fatalf("seed dispatcher: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	raw, err := registry.Make("event.dispatcher")
	if err != nil {
		t.Fatalf("resolve dispatcher failed: %v", err)
	}
	got, _ := raw.(*Dispatcher)
	if got != explicit {
		t.Fatal("service provider should preserve explicit dispatcher")
	}
}

func TestServiceProviderBootRegistersQueuedListenerJob(t *testing.T) {
	if err := (ServiceProvider{}).Boot(providerTestApp{registry: container.NewContainer()}); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
	name, err := queue.JobTypeName(&queuedListenerJob{})
	if err != nil {
		t.Fatalf("job type name: %v", err)
	}
	if !queue.DefaultRegistry().Has(name) {
		t.Fatal("event provider should register queued listener job in default queue registry")
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
