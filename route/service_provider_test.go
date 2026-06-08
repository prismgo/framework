package route

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestServiceProviderName(t *testing.T) {
	if name := (ServiceProvider{}).Name(); name != "route" {
		t.Fatalf("Name() = %q, want %q", name, "route")
	}
}

func TestServiceProviderRegister(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if !registry.Bound("route.router") {
		t.Fatal("provider Register should bind route.router")
	}

	if registry.Resolved("route.router") {
		t.Fatal("provider Register should not construct Router eagerly")
	}
}

func TestServiceProviderRegisterPreservesExplicitRouter(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	explicit := New()
	if err := registry.Instance("route.router", explicit); err != nil {
		t.Fatalf("seed router: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	got, err := container.Make[*Router]("route.router")
	if err != nil {
		t.Fatalf("resolve router: %v", err)
	}
	if got != explicit {
		t.Fatal("service provider should preserve explicit router")
	}
}

func TestServiceProviderBoot(t *testing.T) {
	if err := (ServiceProvider{}).Boot(providerTestApp{registry: container.NewContainer()}); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
}

func TestServiceProviderResolveFromContainer(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	router, err := container.Make[*Router]("route.router")
	if err != nil {
		t.Fatalf("resolve router: %v", err)
	}
	if router == nil {
		t.Fatal("resolved router is nil")
	}

	router.Get("/test", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("GET /test = %d %q, want %d ok", w.Code, w.Body.String(), http.StatusOK)
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
