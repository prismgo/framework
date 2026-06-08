package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/cookie"
	"github.com/prismgo/framework/responsekit"
	"github.com/prismgo/framework/session"
)

// StartSession returns route-level session lifecycle middleware.
func StartSession(options ...session.MiddlewareOption) gin.HandlerFunc {
	cfg := session.NewMiddlewareConfig(options...)
	return func(c *gin.Context) {
		manager := cfg.Manager
		if manager == nil {
			manager = session.Resolve()
		}
		if manager == nil {
			session.RecordMiddlewareError(c, session.ErrInvalidConfig)
			return
		}

		store, err := manager.Start(c.Request.Context(), c.Request, c.Writer)
		if err != nil {
			session.RecordMiddlewareError(c, err)
			return
		}

		committer := responsekit.NewDeferredResponseCommitter(c)
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = store.ReleaseRequestLock(c.Request.Context())
				committer.Restore()
				panic(recovered)
			}
		}()

		queue := cookie.NewQueue()
		session.SetStore(c, store)
		c.Set(cookie.QueueKey, queue)

		c.Next()

		if err := committer.Commit(func(w http.ResponseWriter) error {
			if err := store.Save(c.Request.Context()); err != nil {
				return err
			}
			return queue.Flush(w)
		}); err != nil {
			session.RecordMiddlewareError(c, err)
			return
		}
	}
}
