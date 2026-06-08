package cookie

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/responsekit"
)

func TestQueuedCookiesFlushesRequestQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(queuedCookiesTestMiddleware())
	engine.GET("/", func(c *gin.Context) {
		if _, err := QueueMakeFrom(c, "notice", "ok", 5, Path("/")); err != nil {
			t.Fatalf("QueueMakeFrom error = %v", err)
		}
		c.String(http.StatusCreated, "created")
	})

	rr := httptest.NewRecorder()
	engine.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if rr.Code != http.StatusCreated || rr.Body.String() != "created" {
		t.Fatalf("response code=%d body=%q", rr.Code, rr.Body.String())
	}
	headers := rr.Result().Header.Values("Set-Cookie")
	if len(headers) != 1 || !strings.Contains(headers[0], "notice=ok") {
		t.Fatalf("Set-Cookie headers = %#v", headers)
	}
}

func TestQueuedCookiesRequestHelpersIsolateConcurrentRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	ready := make(chan string, 2)
	release := make(chan struct{})
	engine.Use(queuedCookiesTestMiddleware())
	engine.GET("/", func(c *gin.Context) {
		id := c.Query("id")
		ready <- id
		<-release
		if _, err := QueueMakeFrom(c, "notice_"+id, "value_"+id, 5, Path("/")); err != nil {
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
		if !strings.Contains(joined, "notice_"+result.id+"=value_"+result.id) {
			t.Fatalf("%s Set-Cookie missing own cookie: %#v", result.id, headers)
		}
		if strings.Contains(joined, "notice_"+other+"=value_"+other) {
			t.Fatalf("%s Set-Cookie leaked %s cookie: %#v", result.id, other, headers)
		}
	}
}

func TestQueuedCookiesFlushErrorAborts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(queuedCookiesTestMiddleware())
	engine.GET("/", func(c *gin.Context) {
		queue, ok := QueueFrom(c)
		if !ok {
			t.Fatalf("queue missing")
		}
		queue.Make("bad name", "x", 5)
		c.String(http.StatusOK, "body")
	})

	rr := httptest.NewRecorder()
	engine.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rr.Code)
	}
	if rr.Body.String() != "" {
		t.Fatalf("body should remain buffered on flush error, got %q", rr.Body.String())
	}
}

func TestQueuedCookiesDefersEmptyErrorResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Next()
		if len(c.Errors) > 0 && !c.Writer.Written() {
			c.String(http.StatusTeapot, "handled")
		}
	})
	engine.Use(queuedCookiesTestMiddleware())
	engine.GET("/", func(c *gin.Context) {
		_ = c.Error(http.ErrAbortHandler)
	})

	rr := httptest.NewRecorder()
	engine.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if rr.Code != http.StatusTeapot || rr.Body.String() != "handled" {
		t.Fatalf("response code=%d body=%q", rr.Code, rr.Body.String())
	}
}

func TestQueueFromMissing(t *testing.T) {
	if queue, ok := QueueFrom(nil); ok || queue != nil {
		t.Fatalf("nil context should not resolve queue")
	}
}

func queuedCookiesTestMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		committer := responsekit.NewDeferredResponseCommitter(c)
		defer func() {
			if recovered := recover(); recovered != nil {
				committer.Restore()
				panic(recovered)
			}
		}()

		queue := NewQueue()
		c.Set(QueueKey, queue)
		c.Next()

		if err := committer.Commit(func(w http.ResponseWriter) error {
			return queue.Flush(w)
		}); err != nil {
			_ = c.Error(err)
			c.Status(http.StatusInternalServerError)
			c.Abort()
		}
	}
}
