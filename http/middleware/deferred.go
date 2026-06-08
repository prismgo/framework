package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/cache"
)

// Deferred installs a per-request cache deferred task queue.
func Deferred() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, run := cache.WithDeferred(c.Request.Context())
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		run()
	}
}
