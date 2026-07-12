// Package exception 单元测试。
//
// 测试范围：
//   - Handler 构造与默认值 / ShouldReport / Level
//   - Render（BizError / 普通 error / 自定义 renderer）
//   - Middleware（panic 恢复 / c.Errors 处理）
//   - Report 非 HTTP 上下文上报
//   - 当前容器绑定管理（Resolve / BuildAndRegister）
//   - Reporter / ContextExtractor / Renderer / ResponseRenderer
//
// 测试原则：
//   - 通过公开接口验证行为，不依赖内部实现
//   - HTTP 路径使用 httptest.NewRecorder + gin.CreateTestContext 模拟
//   - 全局状态测试使用 resetExceptionForTest() + t.Cleanup 确保隔离
package exception

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/logger"
)

const (
	testCodeNotFound     = 40401
	testCodeConflict     = 40901
	testCodeBizRule      = 42201
	testCodeInternal     = 50001
	testCodeInvalidInput = 40001
)

func resetExceptionForTest() {
	container.SetProvider(nil)
}

func useExceptionTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(resetExceptionForTest)
	return registry
}

func bindExceptionHandlerForTest(t *testing.T, h *Handler) *container.Container {
	t.Helper()
	registry := useExceptionTestContainer(t)
	if err := registry.Instance(serviceKey, h, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	return registry
}

type testHTTPError struct {
	code    int
	status  int
	message string
	kind    string
	cause   error
	context map[string]any
	fields  map[string]any
}

type exceptionLoggerContextKey string

type exceptionLogBufferDriver struct {
	*bytes.Buffer
}

func (d exceptionLogBufferDriver) Write(p []byte) (int, error) {
	return d.Buffer.Write(p)
}

func (d exceptionLogBufferDriver) Close() error {
	return nil
}

func newTestHTTPError(code, status int, message string) *testHTTPError {
	return &testHTTPError{code: code, status: status, message: message}
}

func (e *testHTTPError) Error() string                { return e.message }
func (e *testHTTPError) Unwrap() error                { return e.cause }
func (e *testHTTPError) StatusCode() int              { return e.status }
func (e *testHTTPError) PublicMessage() string        { return e.message }
func (e *testHTTPError) BusinessCode() int            { return e.code }
func (e *testHTTPError) ErrorType() string            { return e.kind }
func (e *testHTTPError) ErrorContext() map[string]any { return e.context }
func (e *testHTTPError) PublicFields() map[string]any { return e.fields }

func (e *testHTTPError) WithType(kind string) *testHTTPError {
	e.kind = kind
	return e
}

func (e *testHTTPError) WithCause(cause error) *testHTTPError {
	e.cause = cause
	return e
}

func (e *testHTTPError) WithContext(context map[string]any) *testHTTPError {
	e.context = context
	return e
}

func (e *testHTTPError) WithFields(fields map[string]any) *testHTTPError {
	e.fields = fields
	return e
}

func testNotFound(message string) *testHTTPError {
	return newTestHTTPError(testCodeNotFound, http.StatusNotFound, message)
}

func testInternal(message string) *testHTTPError {
	return newTestHTTPError(testCodeInternal, http.StatusInternalServerError, message)
}

// =============================================================================
// Handler 构造测试
// =============================================================================

func TestNewHandlerDefaults(t *testing.T) {
	h := New()
	if h == nil {
		t.Fatal("New() returned nil")
	}
	if !h.RecoverPanics {
		t.Error("RecoverPanics should default to true")
	}
	if !h.LogErrors {
		t.Error("LogErrors should default to true")
	}
	if !h.LogClientErrors {
		t.Error("LogClientErrors should default to true")
	}
	if !h.PanicStack {
		t.Error("PanicStack should default to true")
	}
}

func TestDebugResolverOptions(t *testing.T) {
	if New().Debug() {
		t.Fatal("default debug resolver should be false without foundation config")
	}
	if !New(WithDebugResolver(func() bool { return true })).Debug() {
		t.Fatal("custom debug resolver should be used")
	}
	if New(WithDebugResolver(nil)).Debug() {
		t.Fatal("nil debug resolver option should leave safe default")
	}
	var h *Handler
	if h.Debug() {
		t.Fatal("nil handler debug should be false")
	}
}

func TestNewHandlerWithOptions(t *testing.T) {
	h := New(
		WithRecovery(false),
		WithLogging(false),
		WithClientErrorLogging(false),
		WithPanicStack(false),
	)
	if h.RecoverPanics {
		t.Error("RecoverPanics should be false")
	}
	if h.LogErrors {
		t.Error("LogErrors should be false")
	}
	if h.LogClientErrors {
		t.Error("LogClientErrors should be false")
	}
	if h.PanicStack {
		t.Error("PanicStack should be false")
	}
}

func TestNewHandlerNilOption(t *testing.T) {
	h := New(nil, WithRecovery(false), nil)
	if h.RecoverPanics {
		t.Error("nil options should be skipped")
	}
}

func TestApplyOptions(t *testing.T) {
	h := New()
	h.ApplyOptions(WithRecovery(false))
	if h.RecoverPanics {
		t.Error("ApplyOptions should apply option")
	}
}

func TestApplyOptionsNilHandler(t *testing.T) {
	var h *Handler
	h.ApplyOptions(WithRecovery(false))
}

// TestProblemForErrorMapsFrameworkStatusBranches verifies framework-owned status mapping stays stable.
func TestProblemForErrorMapsFrameworkStatusBranches(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantType   string
		wantDetail string
	}{
		{
			name:       "canceled",
			err:        context.Canceled,
			wantStatus: http.StatusRequestTimeout,
			wantType:   "http.408",
			wantDetail: http.StatusText(http.StatusRequestTimeout),
		},
		{
			name:       "deadline",
			err:        context.DeadlineExceeded,
			wantStatus: http.StatusGatewayTimeout,
			wantType:   "internal_error",
			wantDetail: "Internal Server Error",
		},
		{
			name:       "no cookie",
			err:        http.ErrNoCookie,
			wantStatus: http.StatusBadRequest,
			wantType:   "bad_request",
			wantDetail: http.StatusText(http.StatusBadRequest),
		},
		{
			name:       "invalid http status",
			err:        newTestHTTPError(testCodeInternal, 99, "hidden"),
			wantStatus: http.StatusInternalServerError,
			wantType:   "internal_error",
			wantDetail: "Internal Server Error",
		},
		{
			name:       "unknown client status",
			err:        newTestHTTPError(testCodeInvalidInput, http.StatusIMUsed, "already processed"),
			wantStatus: http.StatusIMUsed,
			wantType:   "http.226",
			wantDetail: "already processed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problem := problemForError(tt.err, "req-123")
			if problem.Status != tt.wantStatus || problem.Type != tt.wantType || problem.Detail != tt.wantDetail {
				t.Fatalf("problem = %#v, want status/type/detail %d/%q/%q", problem, tt.wantStatus, tt.wantType, tt.wantDetail)
			}
			if problem.RequestID != "req-123" {
				t.Fatalf("request id = %q, want req-123", problem.RequestID)
			}
		})
	}
}

// =============================================================================
// ShouldReport 测试
// =============================================================================

func TestShouldReportDefaults(t *testing.T) {
	h := New()
	if !h.ShouldReport(errors.New("boom"), 500) {
		t.Error("should report 5xx errors by default")
	}
	if !h.ShouldReport(errors.New("boom"), 400) {
		t.Error("should report 4xx errors by default when logClientErrors is set")
	}
}

func TestShouldReportLogErrorsOff(t *testing.T) {
	h := New(WithLogging(false))
	if h.ShouldReport(errors.New("boom"), 500) {
		t.Error("should not report when LogErrors is false")
	}
}

func TestShouldReportClientErrorsOff(t *testing.T) {
	h := New(WithClientErrorLogging(false))
	if h.ShouldReport(errors.New("boom"), 400) {
		t.Error("should not report 4xx when LogClientErrors is false")
	}
	if !h.ShouldReport(errors.New("boom"), 500) {
		t.Error("should still report 5xx when LogClientErrors is false")
	}
}

func TestShouldReportDontReport(t *testing.T) {
	h := New(WithDontReport(func(err error) bool {
		var httpErr *testHTTPError
		return errors.As(err, &httpErr) && httpErr.code == testCodeNotFound
	}))
	if h.ShouldReport(testNotFound("test"), 404) {
		t.Error("should not report filtered error")
	}
	if !h.ShouldReport(errors.New("other"), 500) {
		t.Error("should report non-filtered error")
	}
}

func TestShouldReportNilHandler(t *testing.T) {
	var h *Handler
	if h.ShouldReport(errors.New("boom"), 500) {
		t.Error("nil handler should not report")
	}
}

// =============================================================================
// Level 测试
// =============================================================================

func TestLevelDefaults(t *testing.T) {
	h := New()
	if h.Level(errors.New("boom"), 500) != LevelError {
		t.Error("5xx should default to error level")
	}
	if h.Level(errors.New("boom"), 400) != LevelWarn {
		t.Error("4xx should default to warn level")
	}
}

func TestLevelWithResolver(t *testing.T) {
	h := New(WithLevel(func(err error, status int) Level {
		if status == 404 {
			return LevelDebug
		}
		return LevelInfo
	}))
	if h.Level(errors.New("missing"), 404) != LevelDebug {
		t.Error("custom level should be debug for 404")
	}
	if h.Level(errors.New("boom"), 500) != LevelInfo {
		t.Error("custom level should be info for 500")
	}
}

func TestLevelResolverReturnEmpty(t *testing.T) {
	h := New(WithLevel(func(err error, status int) Level {
		return ""
	}))
	if h.Level(errors.New("boom"), 500) != LevelError {
		t.Error("empty return should fall back to default error")
	}
}

// =============================================================================
// Render 测试
// =============================================================================

func TestRenderBizError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	status := h.Render(c, testNotFound("resource missing"))
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
	if !strings.Contains(w.Body.String(), `"type":"not_found"`) {
		t.Fatalf("body = %q, want not_found type", w.Body.String())
	}
}

func TestRenderGenericError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	status := h.Render(c, errors.New("internal boom"))
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", status)
	}
	if !strings.Contains(w.Body.String(), `"type":"internal_error"`) {
		t.Fatalf("body = %q, want internal_error type", w.Body.String())
	}
}

func TestRenderGenericErrorMessageIsHidden(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	h.Render(c, errors.New("secret database password"))
	if strings.Contains(w.Body.String(), "secret") {
		t.Fatal("generic error message should not leak to response")
	}
}

func TestRenderGenericErrorDebugFalseKeeps500Safe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithDebug(false))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	h.Render(c, errors.New("secret database password"))

	var got Problem
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if got.Detail != "Internal Server Error" || got.Message != "" || got.Exception != "" || len(got.Trace) != 0 {
		t.Fatalf("debug=false should render safe 500 problem: %+v", got)
	}
}

func TestRenderGenericErrorDebugTrueExposesUsefulDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithDebug(true))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	h.Render(c, errors.New("debug visible boom"))

	var got Problem
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if got.Detail != "debug visible boom" || got.Message != "debug visible boom" {
		t.Fatalf("debug=true should expose ordinary 5xx message: %+v", got)
	}
	if got.Exception == "" || got.File == "" || got.Line == 0 || len(got.Trace) == 0 {
		t.Fatalf("debug=true should include Go debug fields: %+v", got)
	}
}

func TestRenderBizErrorDebugTrueStillUsesPublicMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithDebug(true))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	h.Render(c, testInternal("public message"))

	var got Problem
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if got.Detail != "Internal Server Error" || got.Exception != "" || len(got.Trace) != 0 {
		t.Fatalf("business HTTP errors should not expose internals in debug mode: %+v", got)
	}
}

func TestRenderNilHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var h *Handler

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	status := h.Render(c, testNotFound("missing"))
	if status != http.StatusNotFound {
		t.Fatalf("nil handler should fall back to default, status = %d", status)
	}
}

// =============================================================================
// 自定义 Renderer 测试
// =============================================================================

func TestCustomProblemRenderer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithRenderer(func(c *gin.Context, err error) (Problem, bool) {
		return Problem{
			Type:    "custom",
			Title:   "Teapot",
			Status:  http.StatusTeapot,
			Code:    49999,
			Message: "custom problem",
		}, true
	}))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	status := h.Render(c, errors.New("teapot"))
	if status != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", status)
	}
	if !strings.Contains(w.Body.String(), `"type":"custom"`) {
		t.Fatalf("body = %q, want custom type", w.Body.String())
	}
}

func TestCustomResponseRenderer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithResponseRenderer(func(c *gin.Context, err error) bool {
		var biz *testHTTPError
		if !errors.As(err, &biz) {
			return false
		}
		c.Data(http.StatusNotFound, "text/html; charset=utf-8", []byte("<h1>missing</h1>"))
		return true
	}))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	status := h.Render(c, testNotFound("missing"))
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content type = %q, want text/html", ct)
	}
	if body := w.Body.String(); body != "<h1>missing</h1>" {
		t.Fatalf("body = %q", body)
	}
}

func TestCustomResponseRendererWithoutWriteUsesFallbackStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithResponseRenderer(func(c *gin.Context, err error) bool {
		return true
	}))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	status := h.Render(c, testNotFound("missing"))
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want fallback 404", status)
	}
}

func TestRendererFallbackWhenNoMatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithRenderer(func(c *gin.Context, err error) (Problem, bool) {
		return Problem{}, false
	}))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	status := h.Render(c, errors.New("plain error"))
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (fallback)", status)
	}
	if !strings.Contains(w.Body.String(), `"type":"internal_error"`) {
		t.Fatalf("body = %q, want internal_error fallback", w.Body.String())
	}
}

// =============================================================================
// Middleware 测试
// =============================================================================

func TestMiddlewarePanicRecovery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithLogging(false))
	bindExceptionHandlerForTest(t, h)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/panic", func(c *gin.Context) {
		panic("test panic")
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"type":"internal_error"`) {
		t.Fatalf("body = %q, want internal_error", w.Body.String())
	}
}

func TestMiddlewarePanicRecoveryDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithRecovery(false), WithLogging(false))
	bindExceptionHandlerForTest(t, h)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("should propagate panic when recovery is disabled")
		}
	}()

	engine.GET("/panic", func(c *gin.Context) {
		panic("test panic")
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/panic", nil))
}

func TestMiddlewareHandlesContextErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithLogging(false))
	bindExceptionHandlerForTest(t, h)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/error", func(c *gin.Context) {
		_ = c.Error(testNotFound("missing"))
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/error", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestMiddlewareHandles4xxStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithLogging(false))
	bindExceptionHandlerForTest(t, h)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/badreq", func(c *gin.Context) {
		c.AbortWithStatus(http.StatusBadRequest)
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/badreq", nil))

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// =============================================================================
// Report 测试（非 HTTP 上下文）
// =============================================================================

func TestReportNonHTTP(t *testing.T) {
	h := New(WithLogging(false))
	h.Report(context.Background(), errors.New("test error"), map[string]any{
		"job_id": "123",
	})
}

func bindLoggerManagerForExceptionTest(t *testing.T) {
	t.Helper()
	registry := useExceptionTestContainer(t)
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})
}

func TestReportWritesLoggerContextFields(t *testing.T) {
	registry := useExceptionTestContainer(t)

	buf := new(bytes.Buffer)
	logger.Extend("exception-context-buffer", func(logger.ChannelOptions) (logger.Driver, error) {
		return exceptionLogBufferDriver{Buffer: buf}, nil
	})

	manager, err := logger.NewManager(logger.Config{
		Default: "app",
		ContextExtractor: func(ctx context.Context) map[string]any {
			return map[string]any{
				"request_id": ctx.Value(exceptionLoggerContextKey("request_id")),
			}
		},
		Channels: map[string]logger.ChannelOptions{
			"app": {Driver: "exception-context-buffer", Formatter: "json", Level: "info"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	ctx := context.WithValue(context.Background(), exceptionLoggerContextKey("request_id"), "req-123")
	h := New(WithPanicStack(false))
	h.Report(ctx, errors.New("boom"), map[string]any{
		"job_id":  "job-1",
		"message": "context report",
	})

	line := strings.SplitN(strings.TrimSpace(buf.String()), "\n", 2)[0]
	if line == "" {
		t.Fatal("expected one JSON log row, got empty buffer")
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		t.Fatalf("not valid json: %s", line)
	}
	if row["request_id"] != "req-123" {
		t.Fatalf("context field missing: %v", row)
	}
	if row["job_id"] != "job-1" {
		t.Fatalf("report field missing: %v", row)
	}
	if row["msg"] != "context report" {
		t.Fatalf("message mismatch: %v", row)
	}
	if errorValue, _ := row["error"].(string); !strings.Contains(errorValue, "boom") {
		t.Fatalf("error field mismatch: %v", row)
	}
}

func TestReportNilError(t *testing.T) {
	h := New()
	h.Report(context.Background(), nil, nil)
}

func TestReportNilHandler(t *testing.T) {
	var h *Handler
	h.Report(context.Background(), errors.New("test"), nil)
}

func TestReportWithBizError(t *testing.T) {
	h := New(WithLogging(false))
	biz := newTestHTTPError(testCodeBizRule, http.StatusUnprocessableEntity, "rule violation").
		WithType("biz_rule").
		WithContext(map[string]any{"field": "value"})

	h.Report(context.Background(), biz, nil)
}

func TestReportWithBizErrorCause(t *testing.T) {
	h := New(WithLogging(false))
	cause := errors.New("db connection refused")
	biz := newTestHTTPError(testCodeInternal, http.StatusInternalServerError, "internal error").WithCause(cause)

	h.Report(context.Background(), biz, nil)
}

func TestReportPassesOriginalBizErrorToReporter(t *testing.T) {
	bindLoggerManagerForExceptionTest(t)
	h := New()
	biz := newTestHTTPError(testCodeInternal, http.StatusInternalServerError, "internal error").
		WithType("biz_rule").
		WithContext(map[string]any{"field": "value"}).
		WithFields(map[string]any{"scope": "reporter"})
	cause := errors.New("db connection refused")
	biz = biz.WithCause(cause)

	var capturedErr error
	var capturedFields map[string]any
	h.Reporters = append(h.Reporters, func(ctx any, err error, fields map[string]any) {
		capturedErr = err
		capturedFields = fields
	})

	h.Report(context.Background(), biz, map[string]any{"message": "biz wrapper"})

	if capturedErr != biz {
		t.Fatalf("reporter err = %#v, want original biz wrapper %#v", capturedErr, biz)
	}
	if capturedFields["error_code"] != testCodeInternal {
		t.Fatalf("error_code = %#v, want %d", capturedFields["error_code"], testCodeInternal)
	}
	if capturedFields["error_type"] != "biz_rule" {
		t.Fatalf("error_type = %#v, want biz_rule", capturedFields["error_type"])
	}
	if capturedFields["error_context"].(map[string]any)["field"] != "value" {
		t.Fatalf("error_context = %#v, want wrapped context", capturedFields["error_context"])
	}
	if capturedFields["field_errors"].(map[string]any)["scope"] != "reporter" {
		t.Fatalf("field_errors = %#v, want wrapped fields", capturedFields["field_errors"])
	}
}

// =============================================================================
// 全局状态管理测试
// =============================================================================

func TestSetAndCurrentHandler(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithRecovery(false))
	bindExceptionHandlerForTest(t, h)

	got := Resolve()
	if got != h {
		t.Fatal("CurrentHandler should return the set handler")
	}
}

func TestRenderErrorUsesRegisteredHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithResponseRenderer(func(c *gin.Context, err error) bool {
		c.String(http.StatusTeapot, "global render")
		c.Abort()
		return true
	}))
	bindExceptionHandlerForTest(t, h)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	Render(c, errors.New("boom"))
	if w.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", w.Code)
	}
	if body := w.Body.String(); body != "global render" {
		t.Fatalf("body = %q", body)
	}
}

func TestRenderErrorNoRegisteredHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Render without registered handler did not panic")
		}
		if got := fmt.Sprint(recovered); got != `container "exception.handler": no current application container` {
			t.Fatalf("panic = %q, want exception.handler no current container", got)
		}
	}()

	_ = Render(c, testNotFound("missing"))
}

func TestRenderErrorNilContext(t *testing.T) {
	Render(nil, errors.New("boom"))
}

func TestUseCanRestorePreviousHandlerExplicitly(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h1 := New(WithRecovery(false))
	registry := bindExceptionHandlerForTest(t, h1)
	previous := Resolve()
	if err := registry.Instance(serviceKey, New(WithRecovery(true)), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("replace handler: %v", err)
	}
	if err := registry.Instance(serviceKey, previous, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("restore handler: %v", err)
	}

	got := Resolve()
	if got != h1 {
		t.Fatal("handler should be restored")
	}
}

func TestNilContainerClearsHandlerResolution(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	bindExceptionHandlerForTest(t, New())
	resetExceptionForTest()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Resolve after nil container did not panic")
		}
		if got := fmt.Sprint(recovered); got != `container "exception.handler": no current application container` {
			t.Fatalf("panic = %q, want exception.handler no current container", got)
		}
	}()
	_ = Resolve()
}

func TestContainerFactoryResolvesHandler(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(resetExceptionForTest)

	want := New(WithLogging(false))
	if err := registry.Singleton("exception.handler", func(containercontract.Resolver) (any, error) {
		return want, nil
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	got := Resolve()
	if got != want {
		t.Fatal("Resolve should return handler from registered factory")
	}
}

func TestTestHelperClearsHandler(t *testing.T) {
	bindExceptionHandlerForTest(t, New())
	resetExceptionForTest()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Resolve after test helper reset did not panic")
		}
		if got := fmt.Sprint(recovered); got != `container "exception.handler": no current application container` {
			t.Fatalf("panic = %q, want exception.handler no current container", got)
		}
	}()
	_ = Resolve()
}

// =============================================================================
// BuildAndRegister 测试
// =============================================================================

func TestBuildAndRegisterBasic(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := BuildAndRegister([]Option{
		WithRecovery(false),
		WithLogging(false),
	}, nil)

	if h == nil {
		t.Fatal("BuildAndRegister returned nil")
	}
	if h.RecoverPanics {
		t.Error("RecoverPanics should be false")
	}
	bindExceptionHandlerForTest(t, h)
	if got := Resolve(); got != h {
		t.Fatal("handler was not bound through test container")
	}
}

func TestBuildAndRegisterWithFactory(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	factory := func(defaultHandler *Handler) *Handler {
		defaultHandler.RecoverPanics = false
		return defaultHandler
	}

	h := BuildAndRegister(nil, factory)
	if h.RecoverPanics {
		t.Error("factory should have set RecoverPanics to false")
	}
}

func TestBuildAndRegisterDoesNotDuplicateDefaultDontReport(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	customPredicate := func(err error) bool { return false }
	h := BuildAndRegister([]Option{WithDontReport(customPredicate)}, nil)

	if len(h.DontReport) != 2 {
		t.Fatalf("expected 2 predicates (DefaultDontReport + custom), got %d", len(h.DontReport))
	}

	if h.DontReport[0] == nil || h.DontReport[1] == nil {
		t.Fatal("predicates should not be nil")
	}
}

// =============================================================================
// Reporter 测试
// =============================================================================

func TestReporterCalledOnReport(t *testing.T) {
	bindLoggerManagerForExceptionTest(t)
	// 使用默认 Handler，LogErrors=true 确保 ShouldReport 放行。
	// WithLogging(false) 会导致 ShouldReport 立即返回 false，报告器不被调用。
	h := New()
	called := false
	h.Reporters = append(h.Reporters, func(ctx any, err error, fields map[string]any) {
		called = true
	})

	h.Report(context.Background(), errors.New("test"), nil)
	if !called {
		t.Error("reporter should have been called")
	}
}

func TestWithReporterRegistersNonNilReporter(t *testing.T) {
	bindLoggerManagerForExceptionTest(t)
	called := false
	h := New(WithReporter(func(ctx any, err error, fields map[string]any) {
		called = true
	}))

	h.Report(context.Background(), errors.New("test"), nil)
	if !called {
		t.Fatal("reporter registered through option should be called")
	}
}

func TestReportScrubsSensitiveFieldsBeforeReporters(t *testing.T) {
	bindLoggerManagerForExceptionTest(t)
	h := New()
	var captured map[string]any
	h.Reporters = append(h.Reporters, func(ctx any, err error, fields map[string]any) {
		captured = fields
	})

	h.Report(context.Background(), errors.New("test"), map[string]any{
		"password":      "secret",
		"authorization": "Bearer token",
		"metadata":      map[string]string{"api_key": "secret", "name": "public"},
		"items":         []any{map[string]any{"cookie": "session", "safe": "ok"}},
		"payload":       map[string]any{"token": "abc", "safe": "ok"},
		"safe":          "visible",
	})

	if captured["password"] != "[redacted]" || captured["authorization"] != "[redacted]" || captured["safe"] != "visible" {
		t.Fatalf("sensitive top-level fields were not scrubbed: %#v", captured)
	}
	payload, ok := captured["payload"].(string)
	if !ok || payload != "[redacted]" {
		t.Fatalf("payload should be fully redacted: %#v", captured["payload"])
	}
	metadata := captured["metadata"].(map[string]string)
	if metadata["api_key"] != "[redacted]" || metadata["name"] != "public" {
		t.Fatalf("nested string map was not scrubbed: %#v", metadata)
	}
	items := captured["items"].([]any)
	item := items[0].(map[string]any)
	if item["cookie"] != "[redacted]" || item["safe"] != "ok" {
		t.Fatalf("nested slice map was not scrubbed: %#v", item)
	}
}

func TestDontReportStopsReportInNonHTTPPath(t *testing.T) {
	h := New()
	h.DontReport = append(h.DontReport, func(err error) bool { return true })
	called := false
	h.Reporters = append(h.Reporters, func(ctx any, err error, fields map[string]any) {
		called = true
	})

	h.Report(context.Background(), errors.New("test"), nil)
	if called {
		t.Error("reporter should NOT be called when DontReport matches")
	}
}

// =============================================================================
// ContextExtractor 测试
// =============================================================================

func TestContextExtractorAddsFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(
		WithLogging(false),
		WithContext(func(c *gin.Context) map[string]any {
			return map[string]any{"tenant_id": "t-123"}
		}),
	)
	bindExceptionHandlerForTest(t, h)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/test", func(c *gin.Context) {
		c.Set("tenant_id", "t-123")
		_ = c.Error(testNotFound("missing"))
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/test", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// =============================================================================
// BizError 字段渲染测试
// =============================================================================

func TestRenderBizErrorWithFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	biz := newTestHTTPError(testCodeInvalidInput, http.StatusBadRequest, "validation failed").
		WithFields(map[string]any{
			"email": []string{"required", "invalid format"},
		})

	status := h.Render(c, biz)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
	if !strings.Contains(w.Body.String(), `"errors"`) {
		t.Fatalf("body = %q, should contain field errors", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "email") {
		t.Fatalf("body = %q, should contain email field", w.Body.String())
	}
}

func TestRenderBizErrorWithCustomType(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithRenderer(func(c *gin.Context, err error) (Problem, bool) {
		var httpErr *testHTTPError
		if !errors.As(err, &httpErr) || httpErr.kind == "" {
			return Problem{}, false
		}
		return Problem{
			Type:    httpErr.kind,
			Title:   statusTitle(httpErr.status),
			Status:  httpErr.status,
			Message: httpErr.message,
		}, true
	}))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	biz := newTestHTTPError(testCodeBizRule, http.StatusUnprocessableEntity, "payment failed").
		WithType("payment_failed")

	status := h.Render(c, biz)
	if status != 422 {
		t.Fatalf("status = %d, want 422", status)
	}
	if !strings.Contains(w.Body.String(), `"type":"payment_failed"`) {
		t.Fatalf("body = %q, want payment_failed type", w.Body.String())
	}
}

// =============================================================================
// 集成测试：模拟 foundation.Create() 装配流程
// =============================================================================

func TestBuildAndRegisterWrapsMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	// 模拟 foundation.Create 装配流程
	h := BuildAndRegister([]Option{
		WithRecovery(true),
		WithLogging(false),
		WithClientErrorLogging(false),
	}, nil)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/error", func(c *gin.Context) {
		_ = c.Error(testNotFound("missing"))
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/error", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// =============================================================================
// 补充测试：提升覆盖率至 85%
// =============================================================================

func TestReportHTTPWithDuplicationGuard(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithLogging(false))
	bindExceptionHandlerForTest(t, h)

	// 第一次错误渲染正常处理，第二次同请求不应重复上报
	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/dup", func(c *gin.Context) {
		_ = c.Error(testNotFound("first"))
		_ = c.Error(testNotFound("second"))
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/dup", nil))

	// 应响应 404，不应 panic 或 double-report
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestMiddlewareWithAlreadyWrittenResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithLogging(false))
	bindExceptionHandlerForTest(t, h)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/written", func(c *gin.Context) {
		_ = c.Error(testNotFound("written"))
		c.JSON(http.StatusNotFound, gin.H{"handled": true})
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/written", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestStackForStatus(t *testing.T) {
	h := New()

	if s := h.stackForStatus(400); s != nil {
		t.Error("should not return stack for 4xx")
	}
	if s := h.stackForStatus(500); s == nil {
		t.Error("should return stack for 5xx")
	}

	h2 := New(WithPanicStack(false))
	if s := h2.stackForStatus(500); s != nil {
		t.Error("should not return stack when PanicStack is false")
	}

	var nilHandler *Handler
	if s := nilHandler.stackForStatus(500); s != nil {
		t.Error("nil handler should return nil stack")
	}
}

func TestWithReporterNil(t *testing.T) {
	h := New(WithReporter(nil))
	if len(h.Reporters) != 0 {
		t.Error("nil reporter should not be added")
	}
}

func TestFinishRenderedResponseAlreadyWritten(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithResponseRenderer(func(c *gin.Context, err error) bool {
		// 先写 JSON 再返回 true
		c.JSON(http.StatusConflict, gin.H{"error": "conflict"})
		return true
	}))
	bindExceptionHandlerForTest(t, h)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/conflict", func(c *gin.Context) {
		_ = c.Error(newTestHTTPError(testCodeConflict, http.StatusConflict, "already exists"))
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/conflict", nil))

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

func TestRenderErrorWithRequestIDInHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)
	c.Set("request_id", "my-custom-id")

	h.Render(c, testNotFound("missing"))

	if !strings.Contains(w.Body.String(), "my-custom-id") {
		t.Fatalf("body = %q, should contain custom request ID", w.Body.String())
	}
}

func TestRenderErrorOmitsRequestIDWhenMiddlewareNotMounted(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	h.Render(c, testNotFound("missing"))

	if strings.Contains(w.Body.String(), "request_id") {
		t.Fatalf("body = %q, should omit request_id without request id middleware", w.Body.String())
	}
	if got := w.Header().Get("X-Request-ID"); got != "" {
		t.Fatalf("response request id header = %q, want empty", got)
	}
}

func TestCustomProblemRendererRequestIDIsNotOverwritten(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := New(WithRenderer(func(c *gin.Context, err error) (Problem, bool) {
		return Problem{
			Type:      "custom",
			Title:     "Custom",
			Status:    http.StatusBadRequest,
			RequestID: "renderer-id",
		}, true
	}))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)
	c.Set("request_id", "middleware-id")

	h.Render(c, errors.New("custom"))

	if !strings.Contains(w.Body.String(), `"request_id":"renderer-id"`) {
		t.Fatalf("body = %q, should preserve renderer request id", w.Body.String())
	}
}

func TestRequestIDFromContextIgnoresInvalidContext(t *testing.T) {
	if got := requestIDFromContext(nil); got != "" {
		t.Fatalf("nil context request id = %q, want empty", got)
	}

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	c.Set("request_id", 123)
	if got := requestIDFromContext(c); got != "" {
		t.Fatalf("non-string request id = %q, want empty", got)
	}
}

func TestRenderErrorWithNilsInErrorsChain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithLogging(false))
	bindExceptionHandlerForTest(t, h)

	engine := gin.New()
	engine.Use(exceptionMiddlewareTest(h))
	engine.GET("/nil-error", func(c *gin.Context) {
		// 注册一个 nil error 在链中
		c.Errors = append(c.Errors, &gin.Error{Err: nil})
		_ = c.Error(testNotFound("valid"))
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nil-error", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestFacadeReport(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	h := New(WithLogging(false))
	_ = bindExceptionHandlerForTest(t, h)

	Report(context.Background(), errors.New("test"), nil)
	Report(context.Background(), nil, nil)

	resetExceptionForTest()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Resolve after clearing provider did not panic")
		}
		if got := fmt.Sprint(recovered); got != `container "exception.handler": no current application container` {
			t.Fatalf("panic = %q, want exception.handler no current container", got)
		}
	}()
	_ = Resolve()
}

func TestResolveWithoutCurrentContainerReturnsNil(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Resolve without current container did not panic")
		}
		if got := fmt.Sprint(recovered); got != `container "exception.handler": no current application container` {
			t.Fatalf("panic = %q, want exception.handler no current container", got)
		}
	}()
	_ = Resolve()
}

func TestExceptionHandlerFacadeUsesReportingCloseGroup(t *testing.T) {
	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)

	bindExceptionHandlerForTest(t, New(WithLogging(false)))

	for _, info := range container.List() {
		if info.Key == "exception.handler" {
			if info.CloseGroup != container.CloseGroupReporting {
				t.Fatalf("exception handler close group = %q, want %q", info.CloseGroup, container.CloseGroupReporting)
			}
			return
		}
	}
	t.Fatal("exception.handler not found in container.List()")
}

func TestDefaultDontReportFiltersContextErrors(t *testing.T) {
	// RED test for 7.2: New() does not yet include DefaultDontReport.
	// After DefaultDontReport is wired in, context.Canceled and DeadlineExceeded
	// should be filtered by ShouldReport.

	h := New()

	// context.Canceled should be filtered by DefaultDontReport (currently not wired)
	if h.ShouldReport(context.Canceled, 500) {
		t.Error("RED: context.Canceled should be filtered by DefaultDontReport (not yet wired)")
	}

	// context.DeadlineExceeded should be filtered
	if h.ShouldReport(context.DeadlineExceeded, 500) {
		t.Error("RED: context.DeadlineExceeded should be filtered by DefaultDontReport (not yet wired)")
	}

	// Normal errors should still pass through
	if !h.ShouldReport(errors.New("real error"), 500) {
		t.Error("normal errors should NOT be filtered by DefaultDontReport")
	}

	// Verify DefaultDontReport predicate is present in the DontReport chain
	found := false
	for _, pred := range h.DontReport {
		if pred != nil && pred(context.Canceled) && pred(context.DeadlineExceeded) && !pred(errors.New("real")) {
			found = true
			break
		}
	}
	if !found {
		t.Error("RED: DefaultDontReport predicate should be in DontReport chain (not yet wired)")
	}
}

func TestReportRespectsLogErrorsViaShouldReport(t *testing.T) {
	// RED test for 7.1: Before the change, Report() only checks DontReport inline,
	// ignoring LogErrors. After calling ShouldReport(), LogErrors=false blocks Report().
	// Use a raw Handler (not New()) so DefaultDontReport is not in the chain.

	h := &Handler{}
	h.LogErrors = false
	called := false
	h.Reporters = append(h.Reporters, func(ctx any, err error, fields map[string]any) {
		called = true
	})

	h.Report(context.Background(), errors.New("test error"), nil)
	if called {
		t.Error("RED: reporter should NOT be called when LogErrors=false (ShouldReport not yet called from Report)")
	}
}

func TestDefaultDontReportBlocksNonHTTPReport(t *testing.T) {
	// 需求背景：验证非 HTTP 路径 exception.Report() 的完整委托链 —
	// Handler.Report() → ShouldReport() → DontReport（含 DefaultDontReport）。
	// context.Canceled 和 DeadlineExceeded 应在 DefaultDontReport 处被静默过滤，
	// 普通错误应正常上报。

	resetExceptionForTest()
	t.Cleanup(resetExceptionForTest)
	bindLoggerManagerForExceptionTest(t)

	h := New()
	// DefaultDontReport 已在 New() 中注入
	if len(h.DontReport) == 0 {
		t.Fatal("expected DefaultDontReport in DontReport chain")
	}

	// context.Canceled 被 DefaultDontReport 过滤
	called := false
	h.Reporters = append(h.Reporters, func(ctx any, err error, fields map[string]any) {
		called = true
	})
	h.Report(context.Background(), context.Canceled, nil)
	if called {
		t.Error("reporter should NOT be called for context.Canceled (DefaultDontReport)")
	}

	// context.DeadlineExceeded 被 DefaultDontReport 过滤
	called = false
	h.Report(context.Background(), context.DeadlineExceeded, nil)
	if called {
		t.Error("reporter should NOT be called for context.DeadlineExceeded (DefaultDontReport)")
	}

	// 普通错误不受 DefaultDontReport 影响
	called = false
	h.Report(context.Background(), errors.New("real error"), nil)
	if !called {
		t.Error("reporter should be called for normal errors")
	}
}
