package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/cache"
	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/cookie"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/http/internal/requestid"
	"github.com/prismgo/framework/logger"
	"github.com/prismgo/framework/ratelimit"
	"github.com/prismgo/framework/session"
)

func bindMiddlewareLoggerForTest(t *testing.T) {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
}

func TestDeferredRunsQueuedTasks(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager, err := cache.NewManager(cache.Config{
		Default: "memory",
		Stores:  map[string]cache.StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
		container.SetProvider(nil)
	})

	engine := gin.New()
	engine.Use(Deferred())
	engine.GET("/", func(c *gin.Context) {
		if _, err := cache.Flexible(c.Request.Context(), "mw-deferred", cache.FlexibleWindow{Fresh: time.Millisecond, Stale: time.Hour}, func(context.Context) (string, error) {
			return "fresh", nil
		}); err != nil {
			t.Fatalf("seed flexible: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
		if got, err := cache.Flexible(c.Request.Context(), "mw-deferred", cache.FlexibleWindow{Fresh: time.Millisecond, Stale: time.Hour}, func(context.Context) (string, error) {
			return "refreshed", nil
		}); err != nil || got != "fresh" {
			t.Fatalf("stale flexible got=%#v err=%v", got, err)
		}
	})

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	var got string
	for i := 0; i < 20; i++ {
		got, _ = cache.Flexible(context.Background(), "mw-deferred", cache.FlexibleWindow{Fresh: time.Hour, Stale: time.Hour}, func(context.Context) (string, error) {
			return "fallback", nil
		})
		if got == "refreshed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got != "refreshed" {
		t.Fatalf("deferred refresh = %#v, want refreshed", got)
	}
}

func TestUseWithConfigRegistersConfiguredChain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("event.dispatcher", event.New()); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}
	if err := registry.Instance("config.default", config.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := registry.Instance("exception.handler", exception.New(exception.WithLogging(false))); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}

	engine := gin.New()
	UseWithConfig(engine, middlewareTestConfig{accessLog: false, exceptionHandler: true})
	engine.GET("/panic", func(*gin.Context) { panic("boom") })

	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/panic", nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("panic response code = %d, want 500", recorder.Code)
	}

	Use(nil)
	if !(currentConfig{}).AccessLogEnabled() || !(currentConfig{}).ExceptionHandlerEnabled() {
		t.Fatal("currentConfig should keep built-in middleware enabled by default")
	}
}

func TestRequestIDPropagatesHeaderAndContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(RequestID())
	engine.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, requestid.Get(c))
	})

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(requestid.Header, "rid-middleware-test")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, request)

	if recorder.Body.String() != "rid-middleware-test" {
		t.Fatalf("request id body = %q, want propagated id", recorder.Body.String())
	}
	if got := recorder.Header().Get(requestid.Header); got != "rid-middleware-test" {
		t.Fatalf("request id header = %q, want propagated id", got)
	}
}

func TestEventMiddlewareDispatchesHandledFailedAndPanicEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("config.default", config.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	bus := event.New()
	seen := map[string]int{}
	bus.Listen("*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		seen[ev.Name()]++
		return nil
	}))

	engine := gin.New()
	engine.Use(Event(bus))
	engine.GET("/ok", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	engine.GET("/fail", func(c *gin.Context) {
		_ = c.Error(errors.New("handler failed"))
		c.Status(http.StatusInternalServerError)
	})
	engine.GET("/panic", func(*gin.Context) { panic("boom") })

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic to be re-raised after event dispatch")
			}
		}()
		engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/panic", nil))
	}()

	if seen[event.EventRequestReceived] != 3 {
		t.Fatalf("received events = %d, want 3", seen[event.EventRequestReceived])
	}
	if seen[event.EventRequestHandled] != 1 {
		t.Fatalf("handled events = %d, want 1", seen[event.EventRequestHandled])
	}
	if seen[event.EventRequestFailed] != 2 {
		t.Fatalf("failed events = %d, want 2", seen[event.EventRequestFailed])
	}
	if seen[event.EventRequestFinished] != 3 {
		t.Fatalf("finished events = %d, want 3", seen[event.EventRequestFinished])
	}
}

func TestQueuedCookiesFlushesAndAbortsOnError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(QueuedCookies())
	engine.GET("/ok", func(c *gin.Context) {
		if _, err := cookie.QueueMakeFrom(c, "notice", "ok", 5, cookie.Path("/")); err != nil {
			t.Fatalf("queue cookie: %v", err)
		}
		c.String(http.StatusCreated, "created")
	})
	engine.GET("/bad", func(c *gin.Context) {
		queue, ok := cookie.QueueFrom(c)
		if !ok {
			t.Fatal("queue missing")
		}
		queue.Make("bad name", "x", 5)
		c.String(http.StatusOK, "body")
	})

	ok := httptest.NewRecorder()
	engine.ServeHTTP(ok, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if ok.Code != http.StatusCreated || len(ok.Result().Header.Values("Set-Cookie")) != 1 {
		t.Fatalf("ok response code=%d cookies=%#v", ok.Code, ok.Result().Header.Values("Set-Cookie"))
	}

	bad := httptest.NewRecorder()
	engine.ServeHTTP(bad, httptest.NewRequest(http.MethodGet, "/bad", nil))
	if bad.Code != http.StatusInternalServerError || bad.Body.String() != "" {
		t.Fatalf("bad response code=%d body=%q", bad.Code, bad.Body.String())
	}
}

func TestStartSessionUsesResolvedManagerAndReportsMissingManager(t *testing.T) {
	gin.SetMode(gin.TestMode)
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	missing := gin.New()
	missing.Use(StartSession())
	missing.GET("/missing", func(c *gin.Context) { c.String(http.StatusOK, "should not run") })
	missingRecorder := httptest.NewRecorder()
	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatal("StartSession without current manager did not panic")
			}
			if got := fmt.Sprint(recovered); got != `container "session.manager": no current application container` {
				t.Fatalf("panic = %q, want session.manager no current container", got)
			}
		}()
		missing.ServeHTTP(missingRecorder, httptest.NewRequest(http.MethodGet, "/missing", nil))
	}()

	manager := newMiddlewareSessionManager(t)
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	if err := registry.Instance("session.manager", manager); err != nil {
		t.Fatalf("bind session manager: %v", err)
	}

	engine := gin.New()
	engine.Use(StartSession())
	engine.GET("/session", func(c *gin.Context) {
		if err := session.Put(c, "status", "saved"); err != nil {
			t.Fatalf("put session: %v", err)
		}
		c.String(http.StatusCreated, "%v", session.Get(c, "status"))
	})
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/session", nil))
	if recorder.Code != http.StatusCreated || recorder.Body.String() != "saved" {
		t.Fatalf("session response code=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if len(recorder.Result().Cookies()) == 0 {
		t.Fatal("expected StartSession to flush a session cookie")
	}
}

func TestExceptionRecoversReportsAndReraisesWhenDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindMiddlewareLoggerForTest(t)
	var reported bool
	h := exception.New(exception.WithReporter(func(_ any, err error, fields map[string]any) {
		reported = err != nil && fields["status"] == http.StatusInternalServerError
	}))

	engine := gin.New()
	engine.Use(Exception(h))
	engine.GET("/", func(*gin.Context) { panic("boom") })
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError || !reported {
		t.Fatalf("recovered code=%d reported=%v", rec.Code, reported)
	}

	noRecover := gin.New()
	noRecover.Use(Exception(exception.New(exception.WithRecovery(false))))
	noRecover.GET("/", func(*gin.Context) { panic("boom") })
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic to be re-raised")
		}
	}()
	noRecover.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestExceptionRendersContextErrorsAndWrittenResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := exception.New()
	engine := gin.New()
	engine.Use(Exception(h))
	engine.GET("/err", func(c *gin.Context) { _ = c.Error(errors.New("broken")) })
	engine.GET("/written", func(c *gin.Context) {
		c.String(http.StatusTeapot, "done")
		_ = c.Error(errors.New("after write"))
	})

	errRec := httptest.NewRecorder()
	engine.ServeHTTP(errRec, httptest.NewRequest(http.MethodGet, "/err", nil))
	if errRec.Code != http.StatusInternalServerError {
		t.Fatalf("error status = %d", errRec.Code)
	}

	written := httptest.NewRecorder()
	engine.ServeHTTP(written, httptest.NewRequest(http.MethodGet, "/written", nil))
	if written.Code != http.StatusTeapot || written.Body.String() != "done" {
		t.Fatalf("written response code=%d body=%q", written.Code, written.Body.String())
	}
}

func TestExceptionAbortWithStatus500Reports(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bindMiddlewareLoggerForTest(t)
	var capturedErr error
	var capturedFields map[string]any
	h := exception.New(exception.WithReporter(func(_ any, err error, fields map[string]any) {
		capturedErr = err
		capturedFields = fields
	}))

	engine := gin.New()
	engine.Use(Exception(h))
	engine.GET("/abort", func(c *gin.Context) {
		c.AbortWithStatus(http.StatusInternalServerError)
	})

	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/abort", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if capturedErr == nil {
		t.Fatal("expected status-only error to be reported")
	}
	if capturedFields["status"] != http.StatusInternalServerError {
		t.Fatalf("status field = %#v, want 500", capturedFields["status"])
	}
	if capturedFields["message"] != http.StatusText(http.StatusInternalServerError) {
		t.Fatalf("message field = %#v, want %q", capturedFields["message"], http.StatusText(http.StatusInternalServerError))
	}
	if got := capturedErr.Error(); got != "Internal Server Error: status 500" {
		t.Fatalf("reported error = %q, want synthetic status error", got)
	}
}

func TestThrottleAllowsBlocksAndFallsThroughMissingLimiter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager, err := cache.NewManager(cache.Config{
		Default: "memory",
		Stores:  map[string]cache.StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	defer manager.Close()
	limiter := ratelimit.New(manager.Default())
	limiter.For("api", func(*gin.Context) []ratelimit.Limit {
		return []ratelimit.Limit{ratelimit.PerMinute(1).By("same")}
	})

	engine := gin.New()
	engine.GET("/ok", ThrottleFor(limiter, "api"), func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	engine.GET("/missing", ThrottleFor(limiter, "missing"), func(c *gin.Context) { c.String(http.StatusOK, "open") })
	engine.GET("/nil", ThrottleFor(nil, "api"), func(c *gin.Context) { c.String(http.StatusOK, "nil") })

	first := httptest.NewRecorder()
	engine.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if first.Code != http.StatusOK || first.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("first code=%d remaining=%q", first.Code, first.Header().Get("X-RateLimit-Remaining"))
	}
	second := httptest.NewRecorder()
	engine.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if second.Code != http.StatusTooManyRequests || second.Header().Get("Retry-After") == "" {
		t.Fatalf("second code=%d retry=%q", second.Code, second.Header().Get("Retry-After"))
	}
	missing := httptest.NewRecorder()
	engine.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if missing.Code != http.StatusOK || missing.Body.String() != "open" {
		t.Fatalf("missing code=%d body=%q", missing.Code, missing.Body.String())
	}
	nilLimiter := httptest.NewRecorder()
	engine.ServeHTTP(nilLimiter, httptest.NewRequest(http.MethodGet, "/nil", nil))
	if nilLimiter.Code != http.StatusOK || nilLimiter.Body.String() != "nil" {
		t.Fatalf("nil code=%d body=%q", nilLimiter.Code, nilLimiter.Body.String())
	}
}

func TestThrottleUsesDefaultLimiterAndFallbackKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager, err := cache.NewManager(cache.Config{
		Default: "memory",
		Stores:  map[string]cache.StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	defer manager.Close()

	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}
	if err := registry.Instance("config.default", config.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	ratelimit.For("default", func(*gin.Context) []ratelimit.Limit {
		return []ratelimit.Limit{
			ratelimit.PerMinute(3),
			ratelimit.PerMinute(3),
			ratelimit.PerMinute(3).By("explicit"),
			ratelimit.PerMinute(3).By("explicit").FallbackKey("alternate"),
		}
	})

	engine := gin.New()
	engine.GET("/", Throttle("default"), func(c *gin.Context) {
		c.String(http.StatusOK, c.Writer.Header().Get("X-RateLimit-Limit"))
	})
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "3" {
		t.Fatalf("default throttle response code=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestThrottleCustomResponseAndAfter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager, err := cache.NewManager(cache.Config{
		Default: "memory",
		Stores:  map[string]cache.StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	defer manager.Close()
	limiter := ratelimit.New(manager.Default())
	limiter.For("custom", func(*gin.Context) []ratelimit.Limit {
		return []ratelimit.Limit{
			ratelimit.PerMinute(1).By("same").After(func(c *gin.Context) bool {
				return c.Writer.Status() >= http.StatusInternalServerError
			}).Response(func(c *gin.Context, result ratelimit.Result) {
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"custom": true, "retry_after": result.RetryAfter})
			}),
		}
	})

	engine := gin.New()
	engine.GET("/:status", ThrottleFor(limiter, "custom"), func(c *gin.Context) {
		if c.Param("status") == "fail" {
			c.String(http.StatusInternalServerError, "fail")
			return
		}
		c.String(http.StatusOK, "ok")
	})

	ok := httptest.NewRecorder()
	engine.ServeHTTP(ok, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if ok.Code != http.StatusOK {
		t.Fatalf("ok status = %d", ok.Code)
	}
	fail := httptest.NewRecorder()
	engine.ServeHTTP(fail, httptest.NewRequest(http.MethodGet, "/fail", nil))
	if fail.Code != http.StatusInternalServerError {
		t.Fatalf("fail status = %d", fail.Code)
	}
	blocked := httptest.NewRecorder()
	engine.ServeHTTP(blocked, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if blocked.Code != http.StatusTooManyRequests || !strings.Contains(blocked.Body.String(), "custom") {
		t.Fatalf("blocked code=%d body=%q", blocked.Code, blocked.Body.String())
	}
}

type middlewareTestConfig struct {
	accessLog        bool
	exceptionHandler bool
}

func (c middlewareTestConfig) AccessLogEnabled() bool { return c.accessLog }
func (c middlewareTestConfig) ExceptionHandlerEnabled() bool {
	return c.exceptionHandler
}

func newMiddlewareSessionManager(t *testing.T) *session.Manager {
	t.Helper()
	cfg := session.DefaultConfig()
	cfg.Files = t.TempDir()
	cfg.Cookie.Name = "middleware_session"
	cfg.Lifetime = time.Hour
	manager, err := session.NewManager(cfg, nil)
	if err != nil {
		t.Fatalf("new session manager: %v", err)
	}
	return manager
}
