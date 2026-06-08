package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/exception"
	httpmiddleware "github.com/prismgo/framework/http/middleware"
)

func TestUseWithConfigHonorsServerSwitches(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindHTTPMiddlewareEventDispatcher(t)

	engine := gin.New()
	httpmiddleware.UseWithConfig(engine, ServerConfig{})

	if got := len(engine.Handlers); got != 2 {
		t.Fatalf("middleware count = %d, want 2", got)
	}
}

func TestUseUsesCurrentConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindHTTPMiddlewareEventDispatcher(t)

	engine := gin.New()
	httpmiddleware.Use(engine)

	if got := len(engine.Handlers); got == 0 {
		t.Fatal("middleware count = 0, want current config middlewares")
	}
}

func TestUseWithConfigRegistersExceptionHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindHTTPMiddlewareEventDispatcher(t)

	engine := gin.New()
	httpmiddleware.UseWithConfig(engine, ServerConfig{ExceptionHandler: true})

	if got := len(engine.Handlers); got != 3 {
		t.Fatalf("middleware count = %d, want 3", got)
	}
}

func TestUseWithConfigRegistersAccessLog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindHTTPMiddlewareEventDispatcher(t)

	engine := gin.New()
	httpmiddleware.UseWithConfig(engine, ServerConfig{AccessLog: true})

	if got := len(engine.Handlers); got != 3 {
		t.Fatalf("middleware count = %d, want 3", got)
	}
}

func bindHTTPMiddlewareEventDispatcher(t *testing.T) {
	t.Helper()
	registry := useHTTPTestContainer(t)
	if err := registry.Instance("event.dispatcher", event.New()); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}
	if err := registry.Instance("config.default", configpkg.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := registry.Instance("exception.handler", exception.New()); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
}

func TestUseWithConfigIgnoresNilEngine(t *testing.T) {
	httpmiddleware.UseWithConfig(nil, ServerConfig{AccessLog: true, ExceptionHandler: true})
}

func TestNewApplicationServerRegistersInternalMiddlewares(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindHTTPApplicationServerServices(t, nil)

	configure := testServerConfigurator(
		nil,
		func(engine *gin.Engine) {
			engine.Use(func(c *gin.Context) {
				c.Header("X-Application-Middleware", "yes")
				c.Next()
			})
		},
		func(engine *gin.Engine) error {
			engine.GET("/ok", func(c *gin.Context) {
				c.Status(http.StatusOK)
			})
			return nil
		},
	)
	server, err := NewApplicationServer("", configure, WithServerConfig(ServerConfig{
		Port:             "8080",
		ExceptionHandler: true,
	}))
	if err != nil {
		t.Fatalf("NewApplicationServer returned error: %v", err)
	}

	engine, ok := server.Handler.(*gin.Engine)
	if !ok {
		t.Fatalf("server handler = %T, want *gin.Engine", server.Handler)
	}
	if got := len(engine.Handlers); got != 4 {
		t.Fatalf("middleware count = %d, want 4", got)
	}
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if recorder.Header().Get("X-Application-Middleware") != "yes" {
		t.Fatalf("application middleware header = %q, want yes", recorder.Header().Get("X-Application-Middleware"))
	}
}

func TestNewApplicationServerUsesExplicitRegistrarsWithoutProcessState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindHTTPApplicationServerServices(t, nil)

	configure := testServerConfigurator(nil, nil, func(engine *gin.Engine) error {
		engine.GET("/second", func(c *gin.Context) { c.Status(http.StatusCreated) })
		return nil
	})
	server, err := NewApplicationServer("", configure, WithServerConfig(ServerConfig{Port: "8080"}))
	if err != nil {
		t.Fatalf("NewApplicationServer returned error: %v", err)
	}
	engine, ok := server.Handler.(*gin.Engine)
	if !ok {
		t.Fatalf("server handler = %T, want *gin.Engine", server.Handler)
	}

	secondRecorder := httptest.NewRecorder()
	engine.ServeHTTP(secondRecorder, httptest.NewRequest(http.MethodGet, "/second", nil))
	if secondRecorder.Code != http.StatusCreated {
		t.Fatalf("GET /second status = %d, want %d", secondRecorder.Code, http.StatusCreated)
	}
	firstRecorder := httptest.NewRecorder()
	engine.ServeHTTP(firstRecorder, httptest.NewRequest(http.MethodGet, "/first", nil))
	if firstRecorder.Code != http.StatusNotFound {
		t.Fatalf("GET /first status = %d, want %d", firstRecorder.Code, http.StatusNotFound)
	}
}

func TestNewApplicationServerHonorsBaseContextAndTimeoutOption(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindHTTPApplicationServerServices(t, nil)

	type contextKey string
	base := context.WithValue(context.Background(), contextKey("request-scope"), "base")
	server, err := NewApplicationServer("", testServerConfigurator(nil, nil, func(engine *gin.Engine) error {
		engine.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })
		return nil
	}), WithServerConfig(ServerConfig{Port: "8080"}), WithServerTimeout(3*time.Second), WithBaseContext(base))
	if err != nil {
		t.Fatalf("NewApplicationServer returned error: %v", err)
	}

	if server.ReadTimeout != 3*time.Second {
		t.Fatalf("ReadTimeout = %s, want 3s", server.ReadTimeout)
	}
	if server.WriteTimeout != 3*time.Second {
		t.Fatalf("WriteTimeout = %s, want 3s", server.WriteTimeout)
	}
	if got := server.BaseContext(nil).Value(contextKey("request-scope")); got != "base" {
		t.Fatalf("BaseContext value = %v, want base", got)
	}
}

func TestNewApplicationServerPreMiddlewareRequestIDBeforeEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)

	bus := event.New()
	var captured string
	bus.ListenFunc(event.EventRequestReceived, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.RequestReceived).RequestID
		return nil
	})
	bindHTTPApplicationServerServices(t, bus)

	server, err := NewApplicationServer("", testServerConfigurator(
		func(engine *gin.Engine) {
			engine.Use(httpmiddleware.RequestID())
		},
		nil,
		func(engine *gin.Engine) error {
			engine.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })
			return nil
		},
	), WithServerConfig(ServerConfig{Port: "8080"}))
	if err != nil {
		t.Fatalf("NewApplicationServer returned error: %v", err)
	}
	engine := server.Handler.(*gin.Engine)

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set(RequestIDHeader, "rid-app-server")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	if captured != "rid-app-server" {
		t.Fatalf("event RequestID = %q, want rid-app-server", captured)
	}
	if got := recorder.Header().Get(RequestIDHeader); got != "rid-app-server" {
		t.Fatalf("response RequestID header = %q, want rid-app-server", got)
	}
}

func TestNewApplicationServerNormalMiddlewareRequestIDStillAfterEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)

	bus := event.New()
	var captured string
	bus.ListenFunc(event.EventRequestReceived, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.RequestReceived).RequestID
		return nil
	})
	bindHTTPApplicationServerServices(t, bus)

	server, err := NewApplicationServer("", testServerConfigurator(
		nil,
		func(engine *gin.Engine) {
			engine.Use(httpmiddleware.RequestID())
		},
		func(engine *gin.Engine) error {
			engine.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })
			return nil
		},
	), WithServerConfig(ServerConfig{Port: "8080"}))
	if err != nil {
		t.Fatalf("NewApplicationServer returned error: %v", err)
	}
	engine := server.Handler.(*gin.Engine)

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set(RequestIDHeader, "rid-after-events")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	if captured != "" {
		t.Fatalf("event RequestID = %q, want empty because normal middleware runs after httpmiddleware.Event", captured)
	}
	if got := recorder.Header().Get(RequestIDHeader); got != "rid-after-events" {
		t.Fatalf("response RequestID header = %q, want rid-after-events", got)
	}
}

func TestNewApplicationServerPreMiddlewaresRunBeforeInternalMiddlewares(t *testing.T) {
	gin.SetMode(gin.TestMode)

	bus := event.New()
	var captured string
	bus.ListenFunc(event.EventRequestReceived, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.RequestReceived).RequestID
		return nil
	})
	bindHTTPApplicationServerServices(t, bus)

	server, err := NewApplicationServer("", testServerConfigurator(
		func(engine *gin.Engine) {
			engine.Use(func(c *gin.Context) {
				SetRequestID(c, "rid-from-pre")
				c.Next()
			})
		},
		nil,
		func(engine *gin.Engine) error {
			engine.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })
			return nil
		},
	), WithServerConfig(ServerConfig{Port: "8080"}))
	if err != nil {
		t.Fatalf("NewApplicationServer returned error: %v", err)
	}
	engine := server.Handler.(*gin.Engine)

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))

	if captured != "rid-from-pre" {
		t.Fatalf("event RequestID = %q, want rid-from-pre", captured)
	}
}

func TestNewApplicationServerRequiresRoutesRegistrar(t *testing.T) {
	bindHTTPApplicationServerServices(t, nil)
	_, err := NewApplicationServer("", nil, WithServerConfig(ServerConfig{Port: "8080"}))
	if err == nil {
		t.Fatal("expected missing routes registrar error")
	}
}

func bindHTTPApplicationServerServices(t *testing.T, bus *event.Dispatcher) {
	t.Helper()
	if bus == nil {
		bus = event.New()
	}
	registry := useHTTPTestContainer(t)
	if err := registry.Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}
	if err := registry.Instance("config.default", configpkg.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := registry.Instance("exception.handler", exception.New()); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
}

func testServerConfigurator(pre, middle func(*gin.Engine), routes func(*gin.Engine) error) ApplicationServerConfigurator {
	return func(engine *gin.Engine, useInternalMiddlewares func(*gin.Engine)) error {
		if pre != nil {
			pre(engine)
		}
		if useInternalMiddlewares != nil {
			useInternalMiddlewares(engine)
		}
		if middle != nil {
			middle(engine)
		}
		if routes == nil {
			return nil
		}
		return routes(engine)
	}
}
