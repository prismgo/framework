package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/ratelimit"
)

// Throttle returns a named rate limiting middleware using the default limiter.
func Throttle(name string) gin.HandlerFunc {
	return ThrottleFor(ratelimit.Resolve(), name)
}

// ThrottleFor returns a named rate limiting middleware using limiter.
func ThrottleFor(limiter *ratelimit.RateLimiter, name string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if limiter == nil {
			c.Next()
			return
		}
		resolver := limiter.Limiter(name)
		if resolver == nil {
			c.Next()
			return
		}
		limits := resolver(c)
		if len(limits) == 0 {
			c.Next()
			return
		}
		results, blocked := inspectLimits(limiter, c, name, limits)
		if blocked {
			return
		}
		writeSuccessHeaders(c, results)
		c.Next()
		hitAfterLimits(limiter, c, results)
	}
}

func inspectLimits(limiter *ratelimit.RateLimiter, c *gin.Context, name string, limits []ratelimit.Limit) ([]ratelimit.Result, bool) {
	results := make([]ratelimit.Result, 0, len(limits))
	seen := map[string]int{}
	for i, limit := range limits {
		if !limitEnabled(limit) {
			continue
		}
		key := resolvedKey(limiter, c, name, limit, seen, i)
		if blocked, result := blocked(limiter, c, limit, key); blocked {
			writeBlockedHeaders(c, result)
			if limit.ResponseFunc != nil {
				limit.ResponseFunc(c, result)
			} else {
				defaultThrottleResponse(c, result)
			}
			return nil, true
		}
		result := hitBefore(limiter, c, limit, key)
		results = append(results, result)
	}
	return results, false
}

func hitBefore(limiter *ratelimit.RateLimiter, c *gin.Context, limit ratelimit.Limit, key string) ratelimit.Result {
	if limit.AfterFunc != nil {
		remaining, _ := limiter.Remaining(c.Request.Context(), key, limit.MaxAttempts)
		attempts, _ := limiter.Attempts(c.Request.Context(), key)
		return result(limiter, c, limit, key, attempts, remaining)
	}
	attempts, _ := limiter.Hit(c.Request.Context(), key, limit.Decay)
	remaining := limit.MaxAttempts - int(attempts)
	if remaining < 0 {
		remaining = 0
	}
	return result(limiter, c, limit, key, attempts, remaining)
}

func hitAfterLimits(limiter *ratelimit.RateLimiter, c *gin.Context, results []ratelimit.Result) {
	for i := range results {
		if results[i].Limit.AfterFunc == nil || !results[i].Limit.AfterFunc(c) {
			continue
		}
		attempts, _ := limiter.Hit(c.Request.Context(), results[i].Key, results[i].Limit.Decay)
		results[i].Attempts = attempts
		results[i].Remaining = results[i].MaxAttempts - int(attempts)
		if results[i].Remaining < 0 {
			results[i].Remaining = 0
		}
	}
}

func blocked(limiter *ratelimit.RateLimiter, c *gin.Context, limit ratelimit.Limit, key string) (bool, ratelimit.Result) {
	blocked, err := limiter.TooManyAttempts(c.Request.Context(), key, limit.MaxAttempts)
	if err != nil || !blocked {
		return false, ratelimit.Result{}
	}
	attempts, _ := limiter.Attempts(c.Request.Context(), key)
	remaining, _ := limiter.Remaining(c.Request.Context(), key, limit.MaxAttempts)
	return true, result(limiter, c, limit, key, attempts, remaining)
}

func result(limiter *ratelimit.RateLimiter, c *gin.Context, limit ratelimit.Limit, key string, attempts int64, remaining int) ratelimit.Result {
	retryAfter, _ := limiter.AvailableIn(c.Request.Context(), key)
	return ratelimit.Result{
		Limit:       limit,
		Key:         key,
		MaxAttempts: limit.MaxAttempts,
		Attempts:    attempts,
		Remaining:   remaining,
		RetryAfter:  retryAfter,
		ResetAt:     time.Now().Add(time.Duration(retryAfter) * time.Second).Unix(),
	}
}

func resolvedKey(limiter *ratelimit.RateLimiter, c *gin.Context, name string, limit ratelimit.Limit, seen map[string]int, index int) string {
	raw := strings.TrimSpace(limitKey(limit, c.ClientIP()))
	if raw == "" {
		raw = c.ClientIP()
	}
	if seen[raw] > 0 {
		if strings.TrimSpace(limit.Fallback) != "" {
			raw = strings.TrimSpace(limit.Fallback)
		} else {
			raw += ":" + strconv.Itoa(index)
		}
	}
	seen[raw]++
	return limiter.MiddlewareKey(name, raw)
}

func writeSuccessHeaders(c *gin.Context, results []ratelimit.Result) {
	if len(results) == 0 || c.Writer.Written() {
		return
	}
	selected := results[0]
	for _, result := range results[1:] {
		if result.Remaining < selected.Remaining {
			selected = result
		}
	}
	c.Header("X-RateLimit-Limit", strconv.Itoa(selected.MaxAttempts))
	c.Header("X-RateLimit-Remaining", strconv.Itoa(selected.Remaining))
}

func writeBlockedHeaders(c *gin.Context, result ratelimit.Result) {
	c.Header("X-RateLimit-Limit", strconv.Itoa(result.MaxAttempts))
	c.Header("X-RateLimit-Remaining", "0")
	c.Header("Retry-After", strconv.Itoa(result.RetryAfter))
	c.Header("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt, 10))
	c.Status(http.StatusTooManyRequests)
}

func limitEnabled(limit ratelimit.Limit) bool {
	return limit.MaxAttempts > 0 && limit.Decay > 0
}

func limitKey(limit ratelimit.Limit, fallback string) string {
	if limit.Key != "" {
		return limit.Key
	}
	if limit.Fallback != "" {
		return limit.Fallback
	}
	return fallback
}

func defaultThrottleResponse(c *gin.Context, result ratelimit.Result) {
	err := tooManyRequestsError{}
	_ = c.Error(err)
	problem := exception.Problem{
		Type:    "too_many_requests",
		Title:   "Too Many Requests",
		Status:  429,
		Detail:  err.PublicMessage(),
		Message: err.PublicMessage(),
	}
	c.AbortWithStatusJSON(problem.Status, problem)
	_ = result
}

type tooManyRequestsError struct{}

func (tooManyRequestsError) Error() string { return "too many requests" }

func (tooManyRequestsError) StatusCode() int { return 429 }

func (tooManyRequestsError) PublicMessage() string { return "too many requests" }
