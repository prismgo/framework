package route

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	httpmiddleware "github.com/prismgo/framework/http/middleware"
	"github.com/prismgo/framework/ratelimit"
)

// Limit 描述 route 包兼容层的一次限流窗口。
type Limit struct {
	Max     int
	Every   time.Duration
	KeyFunc func(*gin.Context) string
}

// PerMinute 构造每分钟限流规则。
func PerMinute(max int) Limit {
	return Limit{Max: max, Every: time.Minute}
}

// By 设置限流 key 的来源。
func (l Limit) By(fn func(*gin.Context) string) Limit {
	l.KeyFunc = fn
	return l
}

// RateLimiterFunc 根据当前请求返回限流规则。
type RateLimiterFunc func(*gin.Context) []Limit

// RateLimiter 注册命名限流器。
func RateLimiter(name string, limiter RateLimiterFunc) {
	name = strings.TrimSpace(name)
	if name == "" || limiter == nil {
		return
	}
	ratelimit.For(name, func(c *gin.Context) []ratelimit.Limit {
		limits := limiter(c)
		out := make([]ratelimit.Limit, 0, len(limits))
		for _, limit := range limits {
			key := c.ClientIP()
			if limit.KeyFunc != nil {
				if value := strings.TrimSpace(limit.KeyFunc(c)); value != "" {
					key = value
				}
			}
			out = append(out, ratelimit.Every(limit.Every, limit.Max).By(key))
		}
		return out
	})
}

// Throttle 返回可挂载到路由或分组上的 Gin 中间件。
func Throttle(name string) gin.HandlerFunc {
	return httpmiddleware.Throttle(name)
}
