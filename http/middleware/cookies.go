package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/cookie"
	"github.com/prismgo/framework/responsekit"
)

// QueuedCookies installs a per-request cookie queue and flushes it after the
// handler has run.
func QueuedCookies() gin.HandlerFunc {
	return func(c *gin.Context) {
		committer := responsekit.NewDeferredResponseCommitter(c)
		defer func() {
			if recovered := recover(); recovered != nil {
				committer.Restore()
				panic(recovered)
			}
		}()

		queue := cookie.NewQueue()
		c.Set(cookie.QueueKey, queue)

		c.Next()

		if err := committer.Commit(func(w http.ResponseWriter) error {
			return queue.Flush(w)
		}); err != nil {
			_ = c.Error(err)
			c.Status(http.StatusInternalServerError)
			c.Abort()
		}
	}
}
