package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/http/internal/requestid"
)

// RequestID returns middleware that propagates a request ID through gin.Context
// and, by default, the response header. It is intentionally opt-in.
func RequestID(opts ...requestid.Option) gin.HandlerFunc {
	return requestid.Middleware(opts...)
}
