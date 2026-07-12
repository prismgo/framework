package exception

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/internal/stackx"
)

// exceptionReportedKey 在 gin.Context 中标记异常已被上报，防止同一请求重复记录日志。
// 仅用于测试 helper，生产实现在 http/middleware/exception.go 中。
const exceptionReportedKey = "_exception_reported"

func exceptionMiddlewareTest(h *Handler) gin.HandlerFunc {
	if h == nil {
		h = New()
	}
	return func(c *gin.Context) {
		start := time.Now()

		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			if !h.RecoverPanics {
				panic(recovered)
			}

			err := fmt.Errorf("panic recovered: %v", recovered)
			_ = c.Error(err)
			c.Set(panicLoggedKey, true)
			status := h.renderResponse(c, err)
			stack := []byte(stackx.Capture(0).Filter(nil).Format())
			h.reportHTTPTest(c, err, status, start, recovered, stack)
		}()

		c.Next()

		status := c.Writer.Status()
		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err
			if !c.Writer.Written() {
				status = h.renderResponse(c, err)
			}
			h.reportHTTPTest(c, err, status, start, nil, h.stackForStatusTest(status))
			return
		}

		if status >= http.StatusBadRequest {
			h.reportHTTPTest(c, nil, status, start, nil, h.stackForStatusTest(status))
		}
	}
}

func (h *Handler) stackForStatusTest(status int) []byte {
	if h == nil || !h.PanicStack || status < http.StatusInternalServerError {
		return nil
	}
	return []byte(stackx.Capture(0).Filter(nil).Format())
}

func (h *Handler) stackForStatus(status int) []byte {
	return h.stackForStatusTest(status)
}

func (h *Handler) reportHTTPTest(c *gin.Context, err error, status int, start time.Time, recovered any, stack []byte) {
	if c == nil {
		return
	}
	statusMessage, reportErr := reportErrorForStatusTest(err, status)
	if !h.ShouldReport(reportErr, status) {
		return
	}
	if v, ok := c.Get(exceptionReportedKey); ok && v == true {
		return
	}
	c.Set(exceptionReportedKey, true)

	fields := map[string]any{
		"status":      status,
		"method":      c.Request.Method,
		"path":        c.FullPath(),
		"url":         c.Request.URL.Path,
		"query":       c.Request.URL.RawQuery,
		"client_ip":   c.ClientIP(),
		"duration_ms": time.Since(start).Milliseconds(),
	}
	if statusMessage != "" {
		fields["message"] = statusMessage
	}
	if requestID := requestIDFromContext(c); requestID != "" {
		fields["request_id"] = requestID
	}
	if h.ContextExtractor != nil {
		for key, value := range h.ContextExtractor(c) {
			fields[key] = value
		}
	}
	if len(c.Errors) > 0 {
		fields["errors"] = joinContextErrors(c.Errors)
	}
	if recovered != nil {
		fields["panic"] = fmt.Sprintf("%v", recovered)
	}
	if h.PanicStack && len(stack) > 0 {
		fields["stack"] = string(stack)
	}

	h.Report(c, reportErr, fields)
}

func reportErrorForStatusTest(err error, status int) (string, error) {
	if err != nil {
		return "", err
	}
	message := "http request rejected"
	if status >= http.StatusInternalServerError {
		message = "http request failed"
	}
	return message, fmt.Errorf("%s: status %d", message, status)
}
