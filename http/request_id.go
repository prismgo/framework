package http

import (
	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/http/internal/requestid"
)

// RequestIDHeader is the default HTTP header used to propagate request IDs.
const RequestIDHeader = requestid.Header

// RequestIDContextKey is the gin.Context key used to store the request ID.
const RequestIDContextKey = requestid.ContextKey

// RequestIDOptions configures the RequestID middleware.
type RequestIDOptions = requestid.Options

// RequestIDOption customizes RequestIDOptions.
type RequestIDOption = requestid.Option

// WithRequestIDHeader changes the header used to read and write request IDs.
func WithRequestIDHeader(header string) RequestIDOption {
	return requestid.WithHeader(header)
}

// WithRequestIDGenerator changes the fallback request ID generator.
func WithRequestIDGenerator(generator func(*gin.Context) string) RequestIDOption {
	return requestid.WithGenerator(generator)
}

// WithRequestIDValidator changes validation for incoming request IDs.
func WithRequestIDValidator(validator func(string) bool) RequestIDOption {
	return requestid.WithValidator(validator)
}

// WithRequestIDResponseHeader controls whether the chosen ID is written back.
func WithRequestIDResponseHeader(enabled bool) RequestIDOption {
	return requestid.WithResponseHeader(enabled)
}

// GetRequestID returns the request ID stored in gin.Context.
func GetRequestID(c *gin.Context) string {
	return requestid.Get(c)
}

// SetRequestID stores a request ID in gin.Context.
func SetRequestID(c *gin.Context, id string) {
	requestid.Set(c, id)
}
