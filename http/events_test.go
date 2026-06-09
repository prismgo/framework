package http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"

	prismconfig "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	"github.com/prismgo/framework/event"
	httpmiddleware "github.com/prismgo/framework/http/middleware"
)

func init() {
	prismconfig.Add("app", func() map[string]any {
		return map[string]any{
			"key":           prismconfig.Env("APP_KEY", ""),
			"cipher":        prismconfig.Env("APP_CIPHER", "AES-256-GCM"),
			"previous_keys": prismconfig.Env("APP_PREVIOUS_KEYS", ""),
			"debug":         prismconfig.Env("APP_DEBUG", false),
		}
	})
	gin.SetMode(gin.TestMode)
}

func useHTTPTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

// captureBus 收集派发到总线上的所有事件，按顺序返回。
type captureBus struct {
	mu     sync.Mutex
	events []event.Event
}

func (c *captureBus) Subscribe(d eventcontract.Dispatcher) {
	d.Listen("*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.events = append(c.events, ev)
		return nil
	}))
}

func (c *captureBus) names() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.events))
	for i, ev := range c.events {
		out[i] = ev.Name()
	}
	return out
}

func newTestEngine(d eventcontract.Dispatcher, handler gin.HandlerFunc) *gin.Engine {
	r := gin.New()
	r.Use(httpmiddleware.Event(d))
	r.GET("/ok", handler)
	return r
}

func TestEventRequestHandled(t *testing.T) {
	bus := event.New()
	cap := &captureBus{}
	bus.Subscribe(cap)

	engine := newTestEngine(bus, func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	got := cap.names()
	if len(got) != 3 || got[0] != event.EventRequestReceived || got[1] != event.EventRequestHandled || got[2] != event.EventRequestFinished {
		t.Fatalf("event sequence = %v, want [%s %s %s]", got, event.EventRequestReceived, event.EventRequestHandled, event.EventRequestFinished)
	}
}

func TestEventRequestFailed5xx(t *testing.T) {
	bus := event.New()
	cap := &captureBus{}
	bus.Subscribe(cap)

	engine := newTestEngine(bus, func(c *gin.Context) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "boom"})
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	got := cap.names()
	if len(got) != 3 || got[1] != event.EventRequestFailed || got[2] != event.EventRequestFinished {
		t.Fatalf("event sequence = %v, want failed then finished", got)
	}
}

func TestEventRequestFailedOnPanic(t *testing.T) {
	bus := event.New()
	cap := &captureBus{}
	bus.Subscribe(cap)

	engine := gin.New()
	engine.Use(httpmiddleware.Event(bus))
	// 在 httpmiddleware.Event 之外再挂一个 recovery，模拟生产中异常处理器的兜底。
	engine.Use(gin.RecoveryWithWriter(io.Discard))
	engine.GET("/panic", func(c *gin.Context) {
		panic("boom")
	})

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	got := cap.names()
	if len(got) != 3 || got[0] != event.EventRequestReceived || got[1] != event.EventRequestFailed || got[2] != event.EventRequestFinished {
		t.Fatalf("event sequence on panic = %v, want [%s %s %s]", got, event.EventRequestReceived, event.EventRequestFailed, event.EventRequestFinished)
	}
}

func TestEventRequestFailedPanicStackGatedByDebugFalse(t *testing.T) {
	useDebugConfig(t, false)

	bus := event.New()
	var failed event.RequestFailed
	bus.ListenFunc(event.EventRequestFailed, func(_ context.Context, ev event.Event) error {
		failed = ev.(event.RequestFailed)
		return nil
	})

	engine := gin.New()
	engine.Use(httpmiddleware.Event(bus))
	engine.GET("/panic", func(c *gin.Context) {
		panic("boom")
	})

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic to propagate after request.failed event")
			}
		}()
		engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/panic", nil))
	}()

	if failed.Error != "boom" || failed.Stack != "" {
		t.Fatalf("debug=false should omit panic stack from event: %+v", failed)
	}
}

func TestEventRequestFailedPanicStackGatedByDebugTrue(t *testing.T) {
	useDebugConfig(t, true)

	bus := event.New()
	var failed event.RequestFailed
	bus.ListenFunc(event.EventRequestFailed, func(_ context.Context, ev event.Event) error {
		failed = ev.(event.RequestFailed)
		return nil
	})

	engine := gin.New()
	engine.Use(httpmiddleware.Event(bus))
	engine.GET("/panic", func(c *gin.Context) {
		panic("boom")
	})

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic to propagate after request.failed event")
			}
		}()
		engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/panic", nil))
	}()

	if failed.Error != "boom" || failed.Stack == "" {
		t.Fatalf("debug=true should include panic stack in event: %+v", failed)
	}
}

func useDebugConfig(t *testing.T, enabled bool) {
	t.Helper()

	envPath := filepath.Join(t.TempDir(), ".env")
	content := "APP_DEBUG=false\n"
	if enabled {
		content = "APP_DEBUG=true\n"
	}
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write test env: %v", err)
	}
	cfg, err := prismconfig.NewFromFile(envPath)
	if err != nil {
		t.Fatalf("load test config: %v", err)
	}
	if err := useHTTPTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
}

func TestEventRequestFinishedPayload(t *testing.T) {
	bus := event.New()
	var captured event.RequestFinished
	bus.ListenFunc(event.EventRequestFinished, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.RequestFinished)
		return nil
	})

	engine := gin.New()
	engine.Use(httpmiddleware.RequestID())
	engine.Use(httpmiddleware.Event(bus))
	engine.GET("/ok", func(c *gin.Context) {
		c.JSON(http.StatusCreated, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set(RequestIDHeader, "rid-finished")
	engine.ServeHTTP(httptest.NewRecorder(), req)

	if captured.RequestID != "rid-finished" || captured.Status != http.StatusCreated || captured.Duration < 0 || captured.Error != "" {
		t.Fatalf("unexpected finished payload: %+v", captured)
	}
}

func TestEventRequestFinishedFailurePayload(t *testing.T) {
	bus := event.New()
	var captured event.RequestFinished
	bus.ListenFunc(event.EventRequestFinished, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.RequestFinished)
		return nil
	})

	engine := newTestEngine(bus, func(c *gin.Context) {
		_ = c.Error(errors.New("write failed"))
		c.Status(http.StatusInternalServerError)
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	engine.ServeHTTP(httptest.NewRecorder(), req)

	if captured.Status != http.StatusInternalServerError || captured.Error != "write failed" {
		t.Fatalf("unexpected failed finished payload: %+v", captured)
	}
}

func TestEventNilDispatcherFallsBackToDefault(t *testing.T) {
	// 临时设置当前容器事件总线，确保 nil dispatcher 时事件能落到容器绑定的 dispatcher。
	registry := useHTTPTestContainer(t)
	bus := event.New()
	cap := &captureBus{}
	bus.Subscribe(cap)
	if err := registry.Instance("event.dispatcher", eventcontract.Dispatcher(bus)); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}

	engine := newTestEngine(nil, func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	got := cap.names()
	if len(got) == 0 {
		t.Fatal("expected events on default bus when dispatcher is nil")
	}
}

func TestEnsureRequestIDPrefersHeader(t *testing.T) {
	bus := event.New()
	var captured string
	bus.ListenFunc(event.EventRequestReceived, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.RequestReceived).RequestID
		return nil
	})

	engine := gin.New()
	engine.Use(httpmiddleware.RequestID())
	engine.Use(httpmiddleware.Event(bus))
	engine.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set(RequestIDHeader, "rid-123")
	engine.ServeHTTP(httptest.NewRecorder(), req)

	if captured != "rid-123" {
		t.Fatalf("expected RequestID from header, got %q", captured)
	}
}

func TestEventDoesNotGenerateRequestID(t *testing.T) {
	bus := event.New()
	var captured string
	bus.ListenFunc(event.EventRequestReceived, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.RequestReceived).RequestID
		return nil
	})

	engine := newTestEngine(bus, func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if captured != "" {
		t.Fatalf("expected empty RequestID without RequestID middleware, got %q", captured)
	}
	if got := w.Header().Get(RequestIDHeader); got != "" {
		t.Fatalf("httpmiddleware.Event response RequestID header = %q, want empty", got)
	}
}
