package middleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/config"
	eventcontract "github.com/prismgo/framework/contracts/event"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/http/internal/requestid"
	"github.com/prismgo/framework/internal/stackx"
)

// Event dispatches HTTP request lifecycle events around a Gin request.
func Event(dispatcher eventcontract.Dispatcher) gin.HandlerFunc {
	return func(c *gin.Context) {
		bus := dispatcher
		if bus == nil {
			bus = event.Resolve()
		}
		if bus == nil {
			bus = event.New()
		}

		requestID := requestid.Get(c)
		start := time.Now()

		bus.Dispatch(c.Request.Context(), event.RequestReceived{
			Method:     c.Request.Method,
			Path:       c.FullPath(),
			ClientIP:   c.ClientIP(),
			RequestID:  requestID,
			ReceivedAt: start,
		})

		defer func() {
			duration := time.Since(start)
			if r := recover(); r != nil {
				errText := fmt.Sprintf("%v", r)
				stack := ""
				if config.GetBool("app.debug", false) {
					stack = stackx.Capture(0).Filter(nil).Format()
				}
				bus.Dispatch(c.Request.Context(), event.RequestFailed{
					Method:    c.Request.Method,
					Path:      c.FullPath(),
					RequestID: requestID,
					Status:    http.StatusInternalServerError,
					Duration:  duration,
					Error:     errText,
					Stack:     stack,
				})
				bus.Dispatch(c.Request.Context(), event.RequestFinished{
					Method:    c.Request.Method,
					Path:      c.FullPath(),
					RequestID: requestID,
					Status:    http.StatusInternalServerError,
					Duration:  duration,
					Error:     errText,
				})
				panic(r)
			}

			status := c.Writer.Status()
			if status >= http.StatusInternalServerError {
				errText := joinContextErrors(c.Errors)
				bus.Dispatch(c.Request.Context(), event.RequestFailed{
					Method:    c.Request.Method,
					Path:      c.FullPath(),
					RequestID: requestID,
					Status:    status,
					Duration:  duration,
					Error:     errText,
				})
				bus.Dispatch(c.Request.Context(), event.RequestFinished{
					Method:    c.Request.Method,
					Path:      c.FullPath(),
					RequestID: requestID,
					Status:    status,
					Duration:  duration,
					Error:     errText,
				})
				return
			}
			bus.Dispatch(c.Request.Context(), event.RequestHandled{
				Method:    c.Request.Method,
				Path:      c.FullPath(),
				RequestID: requestID,
				Status:    status,
				Duration:  duration,
			})
			bus.Dispatch(c.Request.Context(), event.RequestFinished{
				Method:    c.Request.Method,
				Path:      c.FullPath(),
				RequestID: requestID,
				Status:    status,
				Duration:  duration,
			})
		}()

		c.Next()
	}
}

func joinContextErrors(errors []*gin.Error) string {
	messages := make([]string, 0, len(errors))
	for _, current := range errors {
		if current == nil || current.Err == nil {
			continue
		}
		messages = append(messages, current.Err.Error())
	}
	return strings.Join(messages, " | ")
}
