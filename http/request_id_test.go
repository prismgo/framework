package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	httpmiddleware "github.com/prismgo/framework/http/middleware"
)

func TestRequestIDGeneratesIDWhenHeaderMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(httpmiddleware.RequestID())
	engine.GET("/ok", func(c *gin.Context) {
		id := GetRequestID(c)
		if id == "" {
			t.Fatal("request id should be available in gin context")
		}
		c.String(http.StatusOK, id)
	})

	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ok", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	bodyID := recorder.Body.String()
	if bodyID == "" {
		t.Fatal("response body should contain generated request id")
	}
	if got := recorder.Header().Get(RequestIDHeader); got != bodyID {
		t.Fatalf("response request id header = %q, want %q", got, bodyID)
	}
	if strings.Contains(bodyID, "192.0.2.") {
		t.Fatalf("request id should not expose remote address: %q", bodyID)
	}
}

func TestRequestIDReusesValidIncomingHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(httpmiddleware.RequestID())
	engine.GET("/ok", func(c *gin.Context) {
		c.String(http.StatusOK, GetRequestID(c))
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set(RequestIDHeader, "rid-from-gateway")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	if body := recorder.Body.String(); body != "rid-from-gateway" {
		t.Fatalf("request id = %q, want incoming header", body)
	}
	if got := recorder.Header().Get(RequestIDHeader); got != "rid-from-gateway" {
		t.Fatalf("response request id header = %q, want incoming header", got)
	}
}

func TestRequestIDReusesExistingContextValue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		SetRequestID(c, "rid-from-context")
		c.Next()
	})
	engine.Use(httpmiddleware.RequestID())
	engine.GET("/ok", func(c *gin.Context) {
		c.String(http.StatusOK, GetRequestID(c))
	})

	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ok", nil))

	if body := recorder.Body.String(); body != "rid-from-context" {
		t.Fatalf("request id = %q, want existing context value", body)
	}
	if got := recorder.Header().Get(RequestIDHeader); got != "rid-from-context" {
		t.Fatalf("response request id header = %q, want context value", got)
	}
}

func TestRequestIDHeaderTakesPrecedenceOverExistingContextValue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		SetRequestID(c, "rid-from-context")
		c.Next()
	})
	engine.Use(httpmiddleware.RequestID())
	engine.GET("/ok", func(c *gin.Context) {
		c.String(http.StatusOK, GetRequestID(c))
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set(RequestIDHeader, "rid-from-header")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	if body := recorder.Body.String(); body != "rid-from-header" {
		t.Fatalf("request id = %q, want header value", body)
	}
	if got := recorder.Header().Get(RequestIDHeader); got != "rid-from-header" {
		t.Fatalf("response request id header = %q, want header value", got)
	}
}

func TestRequestIDIgnoresInvalidExistingContextValue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		SetRequestID(c, "bad\r\nid")
		c.Next()
	})
	engine.Use(httpmiddleware.RequestID(WithRequestIDGenerator(func(*gin.Context) string { return "generated-id" })))
	engine.GET("/ok", func(c *gin.Context) {
		c.String(http.StatusOK, GetRequestID(c))
	})

	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ok", nil))

	if body := recorder.Body.String(); body != "generated-id" {
		t.Fatalf("request id = %q, want generated replacement", body)
	}
	if got := recorder.Header().Get(RequestIDHeader); got != "generated-id" {
		t.Fatalf("response request id header = %q, want generated replacement", got)
	}
}

func TestRequestIDRejectsInvalidIncomingHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(httpmiddleware.RequestID())
	engine.GET("/ok", func(c *gin.Context) {
		c.String(http.StatusOK, GetRequestID(c))
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set(RequestIDHeader, "bad\r\nid")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	bodyID := recorder.Body.String()
	if bodyID == "" || bodyID == "bad\r\nid" {
		t.Fatalf("request id = %q, want generated replacement", bodyID)
	}
	if strings.ContainsAny(bodyID, "\r\n") {
		t.Fatalf("generated request id contains control characters: %q", bodyID)
	}
	if got := recorder.Header().Get(RequestIDHeader); got != bodyID {
		t.Fatalf("response request id header = %q, want generated replacement %q", got, bodyID)
	}
}

func TestRequestIDOptionsCustomizeBehavior(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(httpmiddleware.RequestID(
		WithRequestIDHeader("X-Correlation-ID"),
		WithRequestIDGenerator(func(*gin.Context) string { return "generated-correlation" }),
		WithRequestIDValidator(func(id string) bool { return strings.HasPrefix(id, "trusted-") }),
		WithRequestIDResponseHeader(false),
	))
	engine.GET("/ok", func(c *gin.Context) {
		c.String(http.StatusOK, GetRequestID(c))
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set("X-Correlation-ID", "untrusted-upstream")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	if body := recorder.Body.String(); body != "generated-correlation" {
		t.Fatalf("request id = %q, want custom generated id", body)
	}
	if got := recorder.Header().Get("X-Correlation-ID"); got != "" {
		t.Fatalf("response correlation header = %q, want empty when disabled", got)
	}
}

func TestRequestIDContextHelpers(t *testing.T) {
	gin.SetMode(gin.TestMode)

	if got := GetRequestID(nil); got != "" {
		t.Fatalf("nil context request id = %q, want empty", got)
	}
	SetRequestID(nil, "ignored")

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	SetRequestID(c, "")
	if got := GetRequestID(c); got != "" {
		t.Fatalf("empty request id should not be stored, got %q", got)
	}

	c.Set(RequestIDContextKey, 123)
	if got := GetRequestID(c); got != "" {
		t.Fatalf("non-string request id = %q, want empty", got)
	}

	SetRequestID(c, "rid-helper")
	if got := GetRequestID(c); got != "rid-helper" {
		t.Fatalf("request id = %q, want rid-helper", got)
	}
}
