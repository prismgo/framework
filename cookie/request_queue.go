package cookie

import "github.com/gin-gonic/gin"

// requestQueue returns the Request Cookie Queue installed on the current Gin
// request. It intentionally does not fall back to the process-level facade.
func requestQueue(c *gin.Context) (*Queue, error) {
	queue, ok := QueueFrom(c)
	if !ok {
		return nil, ErrQueueNotFound
	}
	return queue, nil
}
