package session

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	cookiepkg "github.com/prismgo/framework/cookie"
	"github.com/prismgo/framework/responsekit"
)

func TestStartSessionPersistsAndQueuesCookies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager, _ := newTestManager(t)

	engine := gin.New()
	engine.Use(startSessionTestMiddleware(WithManager(manager)))
	engine.GET("/", func(c *gin.Context) {
		if err := Put(c, "visits", "first"); err != nil {
			t.Fatalf("Put error = %v", err)
		}
		if _, err := cookiepkg.QueueMakeFrom(c, "notice", "ok", 5, cookiepkg.Path("/")); err != nil {
			t.Fatalf("QueueMakeFrom error = %v", err)
		}
		c.String(http.StatusCreated, "created")
	})

	first := httptest.NewRecorder()
	engine.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/", nil))
	if first.Code != http.StatusCreated || first.Body.String() != "created" {
		t.Fatalf("first response code=%d body=%q", first.Code, first.Body.String())
	}
	sessionCookie := findCookie(t, first.Result().Cookies(), manager.cfg.Cookie.Name)
	noticeCookie := findCookie(t, first.Result().Cookies(), "notice")
	if sessionCookie.Value == "" || noticeCookie.Value != "ok" {
		t.Fatalf("cookies session=%#v notice=%#v", sessionCookie, noticeCookie)
	}

	secondEngine := gin.New()
	secondEngine.Use(startSessionTestMiddleware(WithManager(manager)))
	secondEngine.GET("/", func(c *gin.Context) {
		if got := Get(c, "visits"); got != "first" {
			t.Fatalf("Get = %#v, want first", got)
		}
		c.String(http.StatusOK, "restored")
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(sessionCookie)
	second := httptest.NewRecorder()
	secondEngine.ServeHTTP(second, req)
	if second.Code != http.StatusOK || second.Body.String() != "restored" {
		t.Fatalf("second response code=%d body=%q", second.Code, second.Body.String())
	}
}

func TestStartSessionRequestCookieHelpersIsolateConcurrentRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager, _ := newTestManager(t)

	ready := make(chan string, 2)
	release := make(chan struct{})
	engine := gin.New()
	engine.Use(startSessionTestMiddleware(WithManager(manager)))
	engine.GET("/", func(c *gin.Context) {
		id := c.Query("id")
		ready <- id
		<-release
		if _, err := cookiepkg.QueueMakeFrom(c, "notice_"+id, "value_"+id, 5, cookiepkg.Path("/")); err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.String(http.StatusOK, id)
	})

	type result struct {
		id  string
		rec *httptest.ResponseRecorder
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	run := func(id string) {
		defer wg.Done()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/?id="+id, nil)
		engine.ServeHTTP(rec, req)
		results <- result{id: id, rec: rec}
	}

	wg.Add(2)
	go run("first")
	go run("second")
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("request did not reach queue phase")
		}
	}
	close(release)
	wg.Wait()
	close(results)

	for result := range results {
		other := "first"
		if result.id == "first" {
			other = "second"
		}
		headers := result.rec.Result().Header.Values("Set-Cookie")
		joined := strings.Join(headers, "\n")
		if result.rec.Code != http.StatusOK || strings.TrimSpace(result.rec.Body.String()) != result.id {
			t.Fatalf("%s response code=%d body=%q", result.id, result.rec.Code, result.rec.Body.String())
		}
		sessionCookie := findCookie(t, result.rec.Result().Cookies(), manager.cfg.Cookie.Name)
		if sessionCookie.Value == "" {
			t.Fatalf("%s session cookie is empty", result.id)
		}
		if !strings.Contains(joined, "notice_"+result.id+"=value_"+result.id) {
			t.Fatalf("%s Set-Cookie missing own queued cookie: %#v", result.id, headers)
		}
		if strings.Contains(joined, "notice_"+other+"=value_"+other) {
			t.Fatalf("%s Set-Cookie leaked %s queued cookie: %#v", result.id, other, headers)
		}
	}
}

func TestStartSessionFacadeHelpersAndFlash(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager, _ := newTestManager(t)
	engine := gin.New()
	engine.Use(startSessionTestMiddleware(WithManager(manager)))
	engine.GET("/flash", func(c *gin.Context) {
		if err := Flash(c, "status", "saved"); err != nil {
			t.Fatalf("Flash error = %v", err)
		}
		if !Has(c, "status") {
			t.Fatalf("expected flash value in current request")
		}
		c.Status(http.StatusNoContent)
	})
	engine.GET("/read", func(c *gin.Context) {
		c.String(http.StatusOK, "%v", Get(c, "status", "missing"))
	})

	first := httptest.NewRecorder()
	engine.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/flash", nil))
	sessionCookie := findCookie(t, first.Result().Cookies(), manager.cfg.Cookie.Name)

	readReq := httptest.NewRequest(http.MethodGet, "/read", nil)
	readReq.AddCookie(sessionCookie)
	second := httptest.NewRecorder()
	engine.ServeHTTP(second, readReq)
	if strings.TrimSpace(second.Body.String()) != "saved" {
		t.Fatalf("flash read body = %q", second.Body.String())
	}
}

func TestStartSessionSerializesConcurrentRequestsForSameSessionID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := testConfig(t)
	driver := newTestFileDriver(t, cfg)
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	manager.clock = testNow

	seed := gin.New()
	seed.Use(startSessionTestMiddleware(WithManager(manager)))
	seed.GET("/", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	seedRec := httptest.NewRecorder()
	seed.ServeHTTP(seedRec, httptest.NewRequest(http.MethodGet, "/", nil))
	sessionCookie := findCookie(t, seedRec.Result().Cookies(), manager.cfg.Cookie.Name)

	firstEntered := make(chan struct{})
	engine := gin.New()
	engine.Use(startSessionTestMiddleware(WithManager(manager)))
	engine.GET("/write", func(c *gin.Context) {
		key := c.Query("key")
		if key == "" {
			t.Fatalf("missing key")
		}
		if err := Put(c, key, "saved"); err != nil {
			t.Fatalf("Put %s error = %v", key, err)
		}
		if key == "first" {
			close(firstEntered)
			time.Sleep(120 * time.Millisecond)
		}
		c.Status(http.StatusNoContent)
	})

	var wg sync.WaitGroup
	responses := make(chan *httptest.ResponseRecorder, 2)
	run := func(path string) {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.AddCookie(sessionCookie)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)
		responses <- rec
	}

	wg.Add(1)
	go run("/write?key=first")
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatalf("first request did not enter handler")
	}
	wg.Add(1)
	go run("/write?key=second")
	wg.Wait()
	close(responses)

	for rec := range responses {
		if rec.Code != http.StatusNoContent {
			t.Fatalf("write response code = %d body = %q", rec.Code, rec.Body.String())
		}
	}

	read := gin.New()
	read.Use(startSessionTestMiddleware(WithManager(manager)))
	read.GET("/", func(c *gin.Context) {
		if got := Get(c, "first"); got != "saved" {
			t.Fatalf("first value = %#v", got)
		}
		if got := Get(c, "second"); got != "saved" {
			t.Fatalf("second value = %#v", got)
		}
		c.Status(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(sessionCookie)
	rec := httptest.NewRecorder()
	read.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("read response code = %d body = %q", rec.Code, rec.Body.String())
	}
}

func TestStartSessionReleasesLockWhenHandlerPanics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := testConfig(t)
	cfg.Lock.Wait = 50 * time.Millisecond
	driver := newTestFileDriver(t, cfg)
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	manager.clock = testNow

	seed := gin.New()
	seed.Use(startSessionTestMiddleware(WithManager(manager)))
	seed.GET("/", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	seedRec := httptest.NewRecorder()
	seed.ServeHTTP(seedRec, httptest.NewRequest(http.MethodGet, "/", nil))
	sessionCookie := findCookie(t, seedRec.Result().Cookies(), manager.cfg.Cookie.Name)

	engine := gin.New()
	engine.Use(gin.RecoveryWithWriter(io.Discard))
	engine.Use(startSessionTestMiddleware(WithManager(manager)))
	engine.GET("/panic", func(c *gin.Context) {
		if err := Put(c, "panic", "not-saved"); err != nil {
			t.Fatalf("Put panic error = %v", err)
		}
		panic("boom")
	})
	engine.GET("/ok", func(c *gin.Context) {
		if err := Put(c, "after", "saved"); err != nil {
			t.Fatalf("Put after error = %v", err)
		}
		c.Status(http.StatusNoContent)
	})

	panicReq := httptest.NewRequest(http.MethodGet, "/panic", nil)
	panicReq.AddCookie(sessionCookie)
	panicRec := httptest.NewRecorder()
	engine.ServeHTTP(panicRec, panicReq)
	if panicRec.Code != http.StatusInternalServerError {
		t.Fatalf("panic response code = %d", panicRec.Code)
	}

	okReq := httptest.NewRequest(http.MethodGet, "/ok", nil)
	okReq.AddCookie(sessionCookie)
	okRec := httptest.NewRecorder()
	engine.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusNoContent {
		t.Fatalf("ok response code = %d body = %q", okRec.Code, okRec.Body.String())
	}
}

func TestStartSessionReleasesLockWhenSaveFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := testConfig(t)
	cfg.Lock.Wait = 50 * time.Millisecond
	driver := newReleaseTrackingDriver()
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	manager.clock = testNow

	seed := newStore(manager, Payload{}, nil)
	seed.Put("seed", "value")
	if err := seed.Save(context.Background()); err != nil {
		t.Fatalf("initial Save error = %v", err)
	}
	sessionCookie := &http.Cookie{Name: manager.cfg.Cookie.Name, Value: seed.ID()}

	engine := gin.New()
	engine.Use(startSessionTestMiddleware(WithManager(manager)))
	engine.GET("/fail", func(c *gin.Context) {
		if err := Put(c, "failed", "value"); err != nil {
			t.Fatalf("Put failed error = %v", err)
		}
		driver.writeErr = ErrPayloadSerialize
		c.Status(http.StatusNoContent)
	})
	engine.GET("/ok", func(c *gin.Context) {
		if err := Put(c, "after", "saved"); err != nil {
			t.Fatalf("Put after error = %v", err)
		}
		c.Status(http.StatusNoContent)
	})

	failReq := httptest.NewRequest(http.MethodGet, "/fail", nil)
	failReq.AddCookie(sessionCookie)
	failRec := httptest.NewRecorder()
	engine.ServeHTTP(failRec, failReq)
	if failRec.Code != http.StatusInternalServerError {
		t.Fatalf("failed save response code = %d", failRec.Code)
	}

	driver.writeErr = nil
	okReq := httptest.NewRequest(http.MethodGet, "/ok", nil)
	okReq.AddCookie(sessionCookie)
	okRec := httptest.NewRecorder()
	engine.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusNoContent {
		t.Fatalf("ok response code = %d body = %q", okRec.Code, okRec.Body.String())
	}
}

func TestStartSessionRecordsStartError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(startSessionTestMiddleware(WithManager(&Manager{})))
	engine.GET("/", func(c *gin.Context) {
		t.Fatalf("handler should not run when session start fails")
	})

	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("response code = %d", rec.Code)
	}
}

func TestRecordMiddlewareErrorIgnoresNilContext(t *testing.T) {
	RecordMiddlewareError(nil, ErrInvalidConfig)
}

func TestStartSessionDefersRecordedErrorsToOuterMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager, _ := newTestManager(t)
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Next()
		if len(c.Errors) > 0 && !c.Writer.Written() {
			c.String(http.StatusTeapot, "handled")
		}
	})
	engine.Use(startSessionTestMiddleware(WithManager(manager)))
	engine.GET("/", func(c *gin.Context) {
		_ = c.Error(errors.New("handled later"))
	})

	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTeapot || rec.Body.String() != "handled" {
		t.Fatalf("response code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestBufferedResponseWriterTracksStateAndFlushes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(rec)
	writer := newBufferedResponseWriter(context.Writer)

	if writer.Status() != http.StatusOK || writer.Size() != -1 || writer.Written() {
		t.Fatalf("initial status=%d size=%d written=%v", writer.Status(), writer.Size(), writer.Written())
	}
	writer.WriteHeader(http.StatusCreated)
	if writer.Status() != http.StatusCreated {
		t.Fatalf("status after WriteHeader = %d", writer.Status())
	}
	n, err := writer.WriteString("created")
	if err != nil || n != len("created") {
		t.Fatalf("WriteString n=%d err=%v", n, err)
	}
	if !writer.Written() || writer.Size() != len("created") {
		t.Fatalf("after write size=%d written=%v", writer.Size(), writer.Written())
	}
	writer.FlushBuffered()
	if rec.Code != http.StatusCreated || rec.Body.String() != "created" {
		t.Fatalf("flushed code=%d body=%q", rec.Code, rec.Body.String())
	}

	flushRec := httptest.NewRecorder()
	flushContext, _ := gin.CreateTestContext(flushRec)
	flushWriter := newBufferedResponseWriter(flushContext.Writer)
	flushWriter.Flush()
	if !flushWriter.Written() || flushWriter.Size() != 0 {
		t.Fatalf("after Flush size=%d written=%v", flushWriter.Size(), flushWriter.Written())
	}
}

func TestStartSessionMissingStoreReturnsError(t *testing.T) {
	if err := Put(nil, "a", "b"); err == nil {
		t.Fatalf("expected missing store error")
	}
	if got := Get(nil, "a", "fallback"); got != "fallback" {
		t.Fatalf("missing store fallback = %#v", got)
	}
}

func findCookie(t *testing.T, cookies []*http.Cookie, name string) *http.Cookie {
	t.Helper()
	for _, item := range cookies {
		if item.Name == name {
			return item
		}
	}
	t.Fatalf("cookie %q not found in %#v", name, cookies)
	return nil
}

func startSessionTestMiddleware(options ...MiddlewareOption) gin.HandlerFunc {
	cfg := NewMiddlewareConfig(options...)
	return func(c *gin.Context) {
		manager := cfg.Manager
		if manager == nil {
			manager = Resolve()
		}
		if manager == nil {
			RecordMiddlewareError(c, ErrInvalidConfig)
			return
		}

		store, err := manager.Start(c.Request.Context(), c.Request, c.Writer)
		if err != nil {
			RecordMiddlewareError(c, err)
			return
		}

		committer := responsekit.NewDeferredResponseCommitter(c)
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = store.ReleaseRequestLock(c.Request.Context())
				committer.Restore()
				panic(recovered)
			}
		}()

		queue := cookiepkg.NewQueue()
		SetStore(c, store)
		c.Set(cookiepkg.QueueKey, queue)

		c.Next()

		if err := committer.Commit(func(w http.ResponseWriter) error {
			if err := store.Save(c.Request.Context()); err != nil {
				return err
			}
			return queue.Flush(w)
		}); err != nil {
			RecordMiddlewareError(c, err)
			return
		}
	}
}

func newBufferedResponseWriter(target gin.ResponseWriter) *responsekit.DeferredResponseWriter {
	return responsekit.NewDeferredResponseWriter(target)
}
