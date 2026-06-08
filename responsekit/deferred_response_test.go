package responsekit

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestDeferredResponseCommitterFlushesBufferedResponseAfterHook(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	original := c.Writer

	committer := NewDeferredResponseCommitter(c)

	c.Header("X-Test", "queued")
	c.String(http.StatusCreated, "created")

	if err := committer.Commit(func(w http.ResponseWriter) error {
		http.SetCookie(w, &http.Cookie{Name: "notice", Value: "ok", Path: "/"})
		return nil
	}); err != nil {
		t.Fatalf("Commit error = %v", err)
	}

	if rec.Code != http.StatusCreated || rec.Body.String() != "created" {
		t.Fatalf("response code=%d body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Test"); got != "queued" {
		t.Fatalf("header X-Test = %q", got)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != "notice" || cookies[0].Value != "ok" {
		t.Fatalf("cookies = %#v", cookies)
	}
	if c.Writer != original {
		t.Fatalf("writer should be restored after commit")
	}
}

func TestDeferredResponseCommitterDefersEmptyErrorResponsesToOuterMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	original := c.Writer

	committer := NewDeferredResponseCommitter(c)
	_ = c.Error(errors.New("handled later"))

	if err := committer.Commit(nil); err != nil {
		t.Fatalf("Commit error = %v", err)
	}

	if rec.Body.Len() != 0 {
		t.Fatalf("body should stay empty, got %q", rec.Body.String())
	}
	if c.Writer != original {
		t.Fatalf("writer should be restored after commit")
	}
}

func TestDeferredResponseCommitterRestoresWriterOnHookError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	original := c.Writer

	committer := NewDeferredResponseCommitter(c)
	want := errors.New("boom")
	if err := committer.Commit(func(http.ResponseWriter) error {
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("Commit error = %v", err)
	}
	if c.Writer != original {
		t.Fatalf("writer should be restored after hook error")
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body should stay empty, got %q", rec.Body.String())
	}
}

func TestDeferredResponseCommitterRestoreRestoresOriginalWriter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	original := c.Writer

	committer := NewDeferredResponseCommitter(c)
	committer.Restore()

	if c.Writer != original {
		t.Fatalf("writer should be restored")
	}
}

func TestDeferredResponseCommitterNilBranch(t *testing.T) {
	var committer *DeferredResponseCommitter
	called := false
	if err := committer.Commit(func(w http.ResponseWriter) error {
		called = true
		if w != nil {
			t.Fatalf("nil committer should pass nil writer")
		}
		return nil
	}); err != nil {
		t.Fatalf("Commit error = %v", err)
	}
	if !called {
		t.Fatalf("hook should run for nil committer fallback")
	}
}

func TestDeferredResponseWriterTracksHeadersAndFlushes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)

	writer := NewDeferredResponseWriter(c.Writer)
	writer.WriteHeader(http.StatusAccepted)
	writer.Header().Set("X-Test", "queued")
	if _, err := writer.WriteString("queued"); err != nil {
		t.Fatalf("WriteString error = %v", err)
	}
	writer.FlushBuffered()

	if rec.Code != http.StatusAccepted || rec.Body.String() != "queued" {
		t.Fatalf("flushed code=%d body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Test"); got != "queued" {
		t.Fatalf("header X-Test = %q", got)
	}
}

func TestDeferredResponseWriterFlushMarksResponseWrittenWithoutStreaming(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)

	writer := NewDeferredResponseWriter(c.Writer)
	writer.Flush()

	if !writer.Written() || writer.Size() != 0 {
		t.Fatalf("after Flush size=%d written=%v", writer.Size(), writer.Written())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("flush should not stream body, got %q", rec.Body.String())
	}
}
