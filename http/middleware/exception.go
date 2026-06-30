package middleware

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/http/internal/requestid"
	"github.com/prismgo/framework/internal/stackx"
)

const exceptionReportedKey = "_exception_reported"

// Exception returns Gin middleware that recovers panics, renders errors and
// reports HTTP failures through the given exception handler.
func Exception(handler *exception.Handler) gin.HandlerFunc {
	h := handler
	if h == nil {
		h = exception.New()
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
			c.Set("_panic_logged", true)
			status := h.Render(c, err)
			reportHTTP(h, c, err, status, start, recovered, stackx.Capture())
		}()

		c.Next()

		status := c.Writer.Status()
		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err
			if !c.Writer.Written() {
				status = h.Render(c, err)
			}
			reportHTTP(h, c, err, status, start, nil, stackForStatus(h, status))
			return
		}

		if status >= http.StatusBadRequest {
			reportHTTP(h, c, nil, status, start, nil, stackForStatus(h, status))
		}
	}
}

func stackForStatus(h *exception.Handler, status int) []byte {
	if h == nil || !h.PanicStack || status < http.StatusInternalServerError {
		return nil
	}
	return stackx.Capture()
}

func reportHTTP(h *exception.Handler, c *gin.Context, err error, status int, start time.Time, recovered any, stack []byte) {
	if h == nil || c == nil {
		return
	}
	statusMessage, reportErr := reportErrorForStatus(err, status)
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
	if requestID := requestid.Get(c); requestID != "" {
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

func reportErrorForStatus(err error, status int) (string, error) {
	if err != nil {
		return "", err
	}
	message := http.StatusText(status)
	if message == "" && status >= http.StatusInternalServerError {
		message = "http request failed"
	}
	if message == "" {
		message = "http request rejected"
	}
	return message, fmt.Errorf("%s: status %d", message, status)
}
