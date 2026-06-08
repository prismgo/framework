package foundation

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/console"
	providercontract "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	prismhttp "github.com/prismgo/framework/http"
	basickernel "github.com/prismgo/framework/kernel"
	"github.com/prismgo/framework/route"
	"github.com/prismgo/framework/timer"
)

func TestBuilderStoresConsoleCommandsOnApplicationRuntimeRegistry(t *testing.T) {

	first := Configure().WithCommands(
		func() console.Command { return runtimeRegistryCommand("app:first") },
	).WithRouting(func(r *Routing) {
		r.MigrationPaths("database/first")
		r.SeedPaths("database/seeders/first")
	}).Create()
	second := Configure().WithCommands(
		func() console.Command { return runtimeRegistryCommand("app:second") },
	).WithRouting(func(r *Routing) {
		r.MigrationPaths("database/second")
		r.SeedPaths("database/seeders/second")
	}).Create()

	kernel := basickernel.NewApplicationKernel("test", second.runtime)
	if err := kernel.Call(context.Background(), "app:second"); err != nil {
		t.Fatalf("second application command should be registered: %v", err)
	}
	if err := kernel.Call(context.Background(), "app:first"); err == nil {
		t.Fatal("first application command should not leak into second kernel")
	}
	if got := second.runtime.MigrationPaths(); len(got) != 1 || got[0] != "database/second" {
		t.Fatalf("second migration paths = %v, want [database/second]", got)
	}
	if got := second.runtime.SeedPaths(); len(got) != 1 || got[0] != "database/seeders/second" {
		t.Fatalf("second seed paths = %v, want [database/seeders/second]", got)
	}

	kernel = basickernel.NewApplicationKernel("test", first.runtime)
	if err := kernel.Call(context.Background(), "app:first"); err != nil {
		t.Fatalf("first application command should be registered through its runtime registry: %v", err)
	}
	if err := kernel.Call(context.Background(), "app:second"); err == nil {
		t.Fatal("second application command should not leak into first kernel")
	}
	if got := first.runtime.MigrationPaths(); len(got) != 1 || got[0] != "database/first" {
		t.Fatalf("first migration paths = %v, want [database/first]", got)
	}
	if got := first.runtime.SeedPaths(); len(got) != 1 || got[0] != "database/seeders/first" {
		t.Fatalf("first seed paths = %v, want [database/seeders/first]", got)
	}
}

func TestRoutingCommandsMountIntoApplicationKernel(t *testing.T) {
	app := Configure().WithRouting(func(r *Routing) {
		r.Commands(func() console.Command { return runtimeRegistryCommand("app:routing-command") })
	}).Create()

	if got := app.runtime.CommandFactories(); len(got) != 1 {
		t.Fatalf("Routing.Commands factories = %d, want 1", len(got))
	}

	kernel := basickernel.NewApplicationKernel("test", app.runtime)
	if err := kernel.Call(context.Background(), "app:routing-command"); err != nil {
		t.Fatalf("Routing.Commands command should mount into application kernel: %v", err)
	}
}

func TestBuilderWithCommandsMergesWithRoutingCommands(t *testing.T) {
	app := Configure().WithCommands(
		func() console.Command { return runtimeRegistryCommand("app:builder-command") },
	).WithRouting(func(r *Routing) {
		r.Commands(func() console.Command { return runtimeRegistryCommand("app:routing-command") })
	}).Create()

	if got := app.runtime.CommandFactories(); len(got) != 2 {
		t.Fatalf("merged command factories = %d, want 2", len(got))
	}

	kernel := basickernel.NewApplicationKernel("test", app.runtime)
	for _, name := range []string{"app:builder-command", "app:routing-command"} {
		if err := kernel.Call(context.Background(), name); err != nil {
			t.Fatalf("%s should mount into application kernel: %v", name, err)
		}
	}
}

func TestBuilderWithCommandsKeepsRoutingDeclarationsSeparate(t *testing.T) {

	app := Configure().WithCommands(
		func() console.Command { return runtimeRegistryCommand("app:with-commands") },
	).WithRouting(func(r *Routing) {
		r.MigrationPaths("database/migrations")
		r.SeedPaths("database/seeders")
		r.Schedules(func(*timer.Schedule) {})
	}).Create()

	if got := app.runtime.CommandFactories(); len(got) != 1 {
		t.Fatalf("WithCommands factories = %d, want 1", len(got))
	}
	if got := app.runtime.MigrationPaths(); len(got) != 1 || got[0] != "database/migrations" {
		t.Fatalf("migration paths = %v, want [database/migrations]", got)
	}
	if got := app.runtime.SeedPaths(); len(got) != 1 || got[0] != "database/seeders" {
		t.Fatalf("seed paths = %v, want [database/seeders]", got)
	}
	if got := app.runtime.ScheduleRegistrars(); len(got) != 1 {
		t.Fatalf("schedule registrars = %d, want 1", len(got))
	}

	kernel := basickernel.NewApplicationKernel("test", app.runtime)
	if err := kernel.Call(context.Background(), "app:with-commands"); err != nil {
		t.Fatalf("WithCommands command should mount into application kernel: %v", err)
	}
}

func TestBuilderStoresHTTPRoutesOnApplicationRuntimeRegistry(t *testing.T) {
	gin.SetMode(gin.TestMode)

	first := Configure().WithRouting(func(r *Routing) {
		r.Routes(func(_ *Application, engine *gin.Engine) error {
			engine.GET("/first", func(c *gin.Context) { c.String(http.StatusOK, "first") })
			return nil
		})
	}).WithMiddleware(func(m *Middleware) {
		m.Use(func(engine *gin.Engine) {
			engine.Use(func(c *gin.Context) {
				c.Header("X-Application", "first")
				c.Next()
			})
		})
	}).Create()

	second := Configure().WithRouting(func(r *Routing) {
		r.Routes(func(_ *Application, engine *gin.Engine) error {
			engine.GET("/second", func(c *gin.Context) { c.String(http.StatusAccepted, "second") })
			return nil
		})
	}).Create()

	defer second.Close()
	defer first.Close()

	second.Boot()

	server, err := second.runtime.NewHTTPServer(context.Background(), "8080")
	if err != nil {
		t.Fatalf("NewApplicationServer failed: %v", err)
	}
	engine := server.Handler.(*gin.Engine)
	if response := request(engine, "/second"); response.Code != http.StatusAccepted || response.Body.String() != "second" {
		t.Fatalf("GET /second = %d %q, want 202 second", response.Code, response.Body.String())
	}
	if response := request(engine, "/first"); response.Code != http.StatusNotFound {
		t.Fatalf("GET /first = %d, want 404", response.Code)
	}

	first.Boot()

	server, err = first.runtime.NewHTTPServer(context.Background(), "8080")
	if err != nil {
		t.Fatalf("NewApplicationServer with first runtime failed: %v", err)
	}
	engine = server.Handler.(*gin.Engine)
	if response := request(engine, "/first"); response.Code != http.StatusOK || response.Body.String() != "first" {
		t.Fatalf("GET /first = %d %q, want 200 first", response.Code, response.Body.String())
	}
	if response := request(engine, "/first"); response.Header().Get("X-Application") != "first" {
		t.Fatalf("GET /first header X-Application = %q, want first", response.Header().Get("X-Application"))
	}
	if response := request(engine, "/second"); response.Code != http.StatusNotFound {
		t.Fatalf("GET /second = %d, want 404", response.Code)
	}
}

func TestHTTPRoutesPreservesProviderBootRegisteredRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	app := Configure().
		WithProviders(bootRouteProvider{}).
		WithRouting(func(r *Routing) {
			r.Routes(func(_ *Application, _ *gin.Engine) error {
				route.Get("/from-registrar", func(c *gin.Context) { c.String(http.StatusOK, "registrar") })
				return nil
			})
		}).
		Create()
	if err := app.Boot(); err != nil {
		t.Fatalf("boot application failed: %v", err)
	}

	server, err := app.runtime.NewHTTPServer(context.Background(), "8080")
	if err != nil {
		t.Fatalf("NewApplicationServer failed: %v", err)
	}
	engine := server.Handler.(*gin.Engine)

	if response := request(engine, "/from-provider"); response.Code != http.StatusOK || response.Body.String() != "provider" {
		t.Fatalf("GET /from-provider = %d %q, want 200 provider", response.Code, response.Body.String())
	}
	if response := request(engine, "/from-registrar"); response.Code != http.StatusOK || response.Body.String() != "registrar" {
		t.Fatalf("GET /from-registrar = %d %q, want 200 registrar", response.Code, response.Body.String())
	}
}

func TestHTTPRoutesKeepsProviderBootRoutesAheadOfFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)

	app := Configure().
		WithProviders(bootRouteProvider{}).
		WithRouting(func(r *Routing) {
			r.Routes(func(_ *Application, _ *gin.Engine) error {
				route.Fallback(func(c *gin.Context) { c.String(http.StatusOK, "fallback") })
				return nil
			})
		}).
		Create()
	if err := app.Boot(); err != nil {
		t.Fatalf("boot application failed: %v", err)
	}

	server, err := app.runtime.NewHTTPServer(context.Background(), "8080")
	if err != nil {
		t.Fatalf("NewApplicationServer failed: %v", err)
	}
	engine := server.Handler.(*gin.Engine)

	if response := request(engine, "/from-provider"); response.Code != http.StatusOK || response.Body.String() != "provider" {
		t.Fatalf("GET /from-provider = %d %q, want 200 provider", response.Code, response.Body.String())
	}
	if response := request(engine, "/missing"); response.Code != http.StatusOK || response.Body.String() != "fallback" {
		t.Fatalf("GET /missing = %d %q, want 200 fallback", response.Code, response.Body.String())
	}
}

func TestHTTPRoutesPreservesMultipleProviderBootRegisteredRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	app := Configure().
		WithProviders(bootRouteProvider{}, secondBootRouteProvider{}).
		WithRouting(func(r *Routing) {
			r.Routes(func(_ *Application, _ *gin.Engine) error { return nil })
		}).
		Create()
	if err := app.Boot(); err != nil {
		t.Fatalf("boot application failed: %v", err)
	}

	server, err := app.runtime.NewHTTPServer(context.Background(), "8080")
	if err != nil {
		t.Fatalf("NewApplicationServer failed: %v", err)
	}
	engine := server.Handler.(*gin.Engine)

	if response := request(engine, "/from-provider"); response.Code != http.StatusOK || response.Body.String() != "provider" {
		t.Fatalf("GET /from-provider = %d %q, want 200 provider", response.Code, response.Body.String())
	}
	if response := request(engine, "/from-provider-2"); response.Code != http.StatusOK || response.Body.String() != "provider-2" {
		t.Fatalf("GET /from-provider-2 = %d %q, want 200 provider-2", response.Code, response.Body.String())
	}
}

func TestHTTPRoutesPreservesPostBootDirectRouteRegistrations(t *testing.T) {
	gin.SetMode(gin.TestMode)

	app := Configure().
		WithProviders(bootRouteProvider{}).
		WithRouting(func(r *Routing) {
			r.Routes(func(_ *Application, _ *gin.Engine) error { return nil })
		}).
		Create()
	if err := app.Boot(); err != nil {
		t.Fatalf("boot application failed: %v", err)
	}

	route.Get("/after-boot", func(c *gin.Context) { c.String(http.StatusOK, "after-boot") })

	server, err := app.runtime.NewHTTPServer(context.Background(), "8080")
	if err != nil {
		t.Fatalf("NewApplicationServer failed: %v", err)
	}
	engine := server.Handler.(*gin.Engine)

	if response := request(engine, "/from-provider"); response.Code != http.StatusOK || response.Body.String() != "provider" {
		t.Fatalf("GET /from-provider = %d %q, want 200 provider", response.Code, response.Body.String())
	}
	if response := request(engine, "/after-boot"); response.Code != http.StatusOK || response.Body.String() != "after-boot" {
		t.Fatalf("GET /after-boot = %d %q, want 200 after-boot", response.Code, response.Body.String())
	}
}

func TestRuntimeRegistriesNilAndEmptyBranches(t *testing.T) {
	var registries *runtimeRegistries
	if got := registries.CommandFactories(); got != nil {
		t.Fatalf("nil CommandFactories = %v, want nil", got)
	}
	if got := registries.ScheduleRegistrars(); got != nil {
		t.Fatalf("nil ScheduleRegistrars = %v, want nil", got)
	}
	if got := registries.MigrationPaths(); got != nil {
		t.Fatalf("nil MigrationPaths = %v, want nil", got)
	}
	if got := registries.SeedPaths(); got != nil {
		t.Fatalf("nil SeedPaths = %v, want nil", got)
	}
	if got := registries.HTTPMiddlewares(); got != nil {
		t.Fatal("nil HTTPMiddlewares should return nil registrar")
	}
	if got := registries.HTTPPreMiddlewares(); got != nil {
		t.Fatal("nil HTTPPreMiddlewares should return nil registrar")
	}
	if got := registries.HTTPRoutes(); got != nil {
		t.Fatal("nil HTTPRoutes should return nil registrar")
	}

	app := NewApplication()
	_ = route.ServiceProvider{}.Register(app)
	configureRuntimeRegistries(nil, nil, Routing{}, Middleware{})
	configureRuntimeRegistries(app, nil, Routing{routes: []func(*Application, *gin.Engine) error{nil}}, Middleware{preRegistrars: []func(*gin.Engine){nil}, registrars: []func(*gin.Engine){nil}})
	if got := app.runtime.HTTPPreMiddlewares(); got == nil {
		t.Fatal("pre middleware registrar should exist when pre middleware slice is declared")
	}
	if got := app.runtime.HTTPMiddlewares(); got == nil {
		t.Fatal("middleware registrar should exist when middleware slice is declared")
	}
	routes := app.runtime.HTTPRoutes()
	if routes == nil {
		t.Fatal("route registrar should exist when route slice is declared")
	}
	if err := routes(gin.New()); err != nil {
		t.Fatalf("empty route registrar returned error: %v", err)
	}
}

func TestRuntimeRegistriesLoadHTTPRoutesSilencesGinDebugAndRestoresMode(t *testing.T) {

	previousMode := gin.Mode()
	previousWriter := gin.DefaultWriter
	output := &bytes.Buffer{}
	gin.SetMode(gin.DebugMode)
	gin.DefaultWriter = output
	t.Cleanup(func() {
		gin.SetMode(previousMode)
		gin.DefaultWriter = previousWriter
	})

	app := NewApplication()
	_ = route.ServiceProvider{}.Register(app)
	configureRuntimeRegistries(app, nil, Routing{
		routes: []func(*Application, *gin.Engine) error{
			func(_ *Application, _ *gin.Engine) error {
				route.Get("/loaded", func(*gin.Context) {})
				return nil
			},
		},
	}, Middleware{})

	if err := app.runtime.LoadHTTPRoutes(); err != nil {
		t.Fatalf("LoadHTTPRoutes returned error: %v", err)
	}
	if gin.Mode() != gin.DebugMode {
		t.Fatalf("LoadHTTPRoutes should restore gin mode, got %q", gin.Mode())
	}
	if output.Len() != 0 {
		t.Fatalf("LoadHTTPRoutes should silence gin debug output, got %q", output.String())
	}
	if routes := route.List(); len(routes) != 1 || routes[0].GinPath != "/loaded" {
		t.Fatalf("LoadHTTPRoutes should populate route facade, got %#v", routes)
	}
}

func TestBuilderPrependMiddlewaresRunBeforeInternalMiddlewares(t *testing.T) {
	gin.SetMode(gin.TestMode)

	bus := event.New()
	var captured string
	bus.ListenFunc(event.EventRequestReceived, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.RequestReceived).RequestID
		return nil
	})
	app := Configure().
		WithMiddleware(func(m *Middleware) {
			m.Prepend(func(engine *gin.Engine) {
				engine.Use(func(c *gin.Context) {
					prismhttp.SetRequestID(c, "rid-prepend")
					c.Next()
				})
			})
			m.Use(func(engine *gin.Engine) {
				engine.Use(func(c *gin.Context) {
					c.Header("X-Normal-Middleware", "yes")
					c.Next()
				})
			})
		}).
		WithRouting(func(r *Routing) {
			r.Routes(func(_ *Application, engine *gin.Engine) error {
				engine.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })
				return nil
			})
		}).Create()

	_ = route.ServiceProvider{}.Register(app)

	if err := app.Container().Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("register event dispatcher: %v", err)
	}

	preRegistrar := app.runtime.HTTPPreMiddlewares()
	if preRegistrar == nil {
		t.Fatal("HTTPPreMiddlewares should expose prepend declarations")
	}

	server, err := app.runtime.NewHTTPServer(context.Background(), "8080")
	if err != nil {
		t.Fatalf("NewApplicationServer failed: %v", err)
	}
	engine := server.Handler.(*gin.Engine)
	response := request(engine, "/ok")
	if response.Code != http.StatusOK {
		t.Fatalf("GET /ok = %d, want 200", response.Code)
	}
	if got := response.Header().Get("X-Normal-Middleware"); got != "yes" {
		t.Fatalf("normal middleware header = %q, want yes", got)
	}
	if captured != "rid-prepend" {
		t.Fatalf("event RequestID = %q, want rid-prepend", captured)
	}
}

type runtimeRegistryCommand string

func (c runtimeRegistryCommand) Definition() *console.Definition {
	return console.MustDefinition(string(c), "runtime registry")
}

func (c runtimeRegistryCommand) Handle(_ console.CommandContext) error {
	return nil
}

func request(engine *gin.Engine, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}

type bootRouteProvider struct{}

func (bootRouteProvider) Register(providercontract.Application) error { return nil }

func (bootRouteProvider) Boot(providercontract.Application) error {
	route.Get("/from-provider", func(c *gin.Context) { c.String(http.StatusOK, "provider") })
	return nil
}

type secondBootRouteProvider struct{}

func (secondBootRouteProvider) Register(providercontract.Application) error { return nil }

func (secondBootRouteProvider) Boot(providercontract.Application) error {
	route.Get("/from-provider-2", func(c *gin.Context) { c.String(http.StatusOK, "provider-2") })
	return nil
}
