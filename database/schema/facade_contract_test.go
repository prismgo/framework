package schema

import (
	"errors"
	"fmt"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestResolveRequiresCurrentRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Resolve without current registry did not panic")
		}
		if got := fmt.Sprint(recovered); got != `container "database.schema": no current application container` {
			t.Fatalf("panic = %q, want database.schema no current container", got)
		}
	}()

	_ = Resolve()
}

func TestDefaultResolvesRegisteredFactoryBeforeFallbackBuilder(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	builder := New(openSQLite(t))
	calls := 0
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		calls++
		return builder, nil
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	if got := Resolve(); got != builder {
		t.Fatalf("Resolve builder = %#v, want factory builder", got)
	}
	if calls != 1 {
		t.Fatalf("factory calls = %d, want 1", calls)
	}
	if err := Create("schema_facade_contract", func(table *Blueprint) { table.Id() }); err != nil {
		t.Fatalf("package Create via factory builder: %v", err)
	}
	if !builder.HasTable("schema_facade_contract") {
		t.Fatal("expected package Create to use factory builder")
	}
}

func TestServiceProviderAndFacadeSmallWrappers(t *testing.T) {
	currentRegistry := useIsolatedFacadeRegistry(t)
	if err := (ServiceProvider{}).Register(schemaProviderApp{registry: currentRegistry}); err != nil {
		t.Fatalf("register schema provider: %v", err)
	}
	if builder := Resolve(); builder == nil {
		t.Fatalf("resolve provider builder = nil")
	}

	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(schemaProviderApp{registry: registry}); err != nil {
		t.Fatalf("register explicit schema provider: %v", err)
	}
	raw, err := registry.Make("database.schema")
	if err != nil {
		t.Fatalf("resolve explicit registry builder: %v", err)
	}
	if builder, _ := raw.(*Builder); builder == nil {
		t.Fatalf("resolve explicit registry builder = %#v", raw)
	}

	container.SetProvider(nil)
	if _, err := container.Make[*Builder]("database.schema"); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatalf("Make nil error = %v, want ErrNoCurrentContainer", err)
	}
	container.SetProvider(func() *container.Container { return registry })

	DefaultMorphKeyType("uuid")
	Bind(openSQLite(t))
	if _, err := CreateDatabase(""); err == nil {
		t.Fatal("expected CreateDatabase validation error")
	}
	if _, err := DropDatabaseIfExists(""); err == nil {
		t.Fatal("expected DropDatabaseIfExists validation error")
	}
}

type schemaProviderApp struct{ registry containercontract.Container }

func (a schemaProviderApp) Container() containercontract.Container { return a.registry }
