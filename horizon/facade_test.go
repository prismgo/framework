package horizon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func useHorizonTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func TestHorizonFacadeGeneralInterfaces(t *testing.T) {
	registry := useHorizonTestContainer(t)

	manager, err := NewManager(Config{Store: "memory", HeartbeatTTL: time.Minute}, WithStoreFactory(integrationStaticStore{store: NewMemoryStore(StoreOptions{})}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := registry.Instance(managerFacadeKey, manager); err != nil {
		t.Fatalf("bind manager: %v", err)
	}
	if container.Value[*Manager](managerFacadeKey) != manager {
		t.Fatal("Current should return explicitly used manager")
	}
	resolved, err := container.Make[*Manager](managerFacadeKey)
	if err != nil {
		t.Fatalf("resolve manager: %v", err)
	}
	if resolved != manager || container.Value[*Manager](managerFacadeKey) != manager {
		t.Fatal("Resolve and Default should reuse explicitly used manager")
	}

	appScoped, err := NewManager(Config{Store: "memory", Prefix: "app"}, WithStoreFactory(integrationStaticStore{store: NewMemoryStore(StoreOptions{})}))
	if err != nil {
		t.Fatalf("new app scoped manager: %v", err)
	}
	if err := registry.Singleton(managerFacadeKey, func(containercontract.Resolver) (any, error) { return appScoped, nil }); err != nil {
		t.Fatalf("register factory in registry: %v", err)
	}
	resolvedIn, err := container.Make[*Manager](managerFacadeKey)
	if err != nil {
		t.Fatalf("resolve in registry: %v", err)
	}
	if resolvedIn != appScoped {
		t.Fatal("ResolveIn should return application scoped manager")
	}

	if _, err := container.Make[*Manager](managerFacadeKey); err != nil {
		t.Fatalf("ResolveIn nil registry should fail: %v", err)
	}
	container.SetProvider(nil)
	if _, err := container.Make[*Manager](managerFacadeKey); err == nil {
		t.Fatal("Make nil registry should fail")
	}
}

func TestServiceProviderSmallErrorBranches(t *testing.T) {
	if name := (ServiceProvider{}).Name(); name != "horizon" {
		t.Fatalf("provider name = %q, want horizon", name)
	}
	registry := container.NewContainer()
	if _, err := buildProviderManager(registry); err == nil || !strings.Contains(err.Error(), "queue manager is not configured") {
		t.Fatalf("buildProviderManager missing queue error = %v", err)
	}
	if _, err := resolveProviderQueueManager(nil); err == nil {
		t.Fatal("resolveProviderQueueManager nil registry should fail")
	}
	if _, err := resolveProviderEventDispatcher(nil); err == nil {
		t.Fatal("resolveProviderEventDispatcher nil registry should fail")
	}
}

func TestDefaultManagerUsesFacadeDefault(t *testing.T) {
	registry := useHorizonTestContainer(t)

	manager, err := NewManager(Config{Store: "memory"}, WithStoreFactory(integrationStaticStore{store: NewMemoryStore(StoreOptions{})}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := registry.Instance(managerFacadeKey, manager); err != nil {
		t.Fatalf("bind manager: %v", err)
	}

	got, err := defaultManager()
	if err != nil {
		t.Fatalf("defaultManager error: %v", err)
	}
	if got != manager {
		t.Fatal("defaultManager should return facade Default manager")
	}

	if _, err := got.ResolveStore(context.Background()); err != nil {
		t.Fatalf("facade default manager should keep injected store factory: %v", err)
	}
}

func TestDefaultReportsFactoryErrorBeforeFallbackManager(t *testing.T) {
	registry := useHorizonTestContainer(t)

	factoryErr := errors.New("horizon factory failed")

	if err := registry.Singleton(managerFacadeKey, func(containercontract.Resolver) (any, error) {
		return nil, factoryErr
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	_, err := container.Make[*Manager](managerFacadeKey)
	if !errors.Is(err, factoryErr) {
		t.Fatalf("Make error = %v, want factory error", err)
	}
	if got := container.Value[*Manager](managerFacadeKey); got != nil {
		t.Fatalf("Value after factory failure = %#v, want nil", got)
	}
}

func TestHorizonFacadeResolveWithoutCurrentContainer(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })
	if manager := Resolve(); manager != nil {
		t.Fatalf("Resolve without current container = %#v, want nil", manager)
	}
}
