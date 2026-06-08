package cookie

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestCookieCompatibilityContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(queuedCookiesTestMiddleware())
	engine.GET("/", func(c *gin.Context) {
		queue, ok := QueueFrom(c)
		if !ok {
			t.Fatal("expected request cookie queue")
		}
		queue.Queue(Make("notice", "ok", 5, SameSite(SameSiteLax)))
		queue.Queue(Make("notice", "new", 5, SameSite(SameSiteLax)))
		queue.Expire("legacy")
		if !queue.HasQueued("notice") {
			t.Fatal("expected queued cookie in handler")
		}
		c.Status(http.StatusAccepted)
	})

	rr := httptest.NewRecorder()
	engine.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if rr.Code != http.StatusAccepted {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
	headers := setCookieHeaders(t, rr)
	if len(headers) != 2 {
		t.Fatalf("expected replaced notice and expired legacy headers, got %#v", headers)
	}
	joined := strings.Join(headers, "\n")
	if !strings.Contains(joined, "notice=new") || strings.Contains(joined, "notice=ok") {
		t.Fatalf("replacement contract failed: %#v", headers)
	}
	if !strings.Contains(joined, "legacy=") || !strings.Contains(joined, "Max-Age=0") {
		t.Fatalf("expire contract failed: %#v", headers)
	}
}
