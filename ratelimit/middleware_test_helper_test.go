package ratelimit

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (r *RateLimiter) throttleTest(name string) gin.HandlerFunc {
	return func(c *gin.Context) {
		limiter := r.Limiter(name)
		if limiter == nil {
			c.Next()
			return
		}
		limits := limiter(c)
		if len(limits) == 0 {
			c.Next()
			return
		}
		results, blocked := r.inspectLimitsTest(c, name, limits)
		if blocked {
			return
		}
		writeSuccessHeadersTest(c, results)
		c.Next()
		r.hitAfterLimitsTest(c, results)
	}
}

func (r *RateLimiter) inspectLimitsTest(c *gin.Context, name string, limits []Limit) ([]Result, bool) {
	results := make([]Result, 0, len(limits))
	seen := map[string]int{}
	for i, limit := range limits {
		if !limit.enabled() {
			continue
		}
		key := r.resolvedKeyTest(c, name, limit, seen, i)
		if blocked, result := r.blockedTest(c, limit, key); blocked {
			writeBlockedHeadersTest(c, result)
			if limit.ResponseFunc != nil {
				limit.ResponseFunc(c, result)
			} else {
				defaultResponse(c, result)
			}
			return nil, true
		}
		result := r.hitBeforeTest(c, limit, key)
		results = append(results, result)
	}
	return results, false
}

func (r *RateLimiter) hitBeforeTest(c *gin.Context, limit Limit, key string) Result {
	if limit.AfterFunc != nil {
		remaining, _ := r.Remaining(c.Request.Context(), key, limit.MaxAttempts)
		attempts, _ := r.Attempts(c.Request.Context(), key)
		return r.resultTest(c, limit, key, attempts, remaining)
	}
	attempts, _ := r.Hit(c.Request.Context(), key, limit.Decay)
	remaining := limit.MaxAttempts - int(attempts)
	if remaining < 0 {
		remaining = 0
	}
	return r.resultTest(c, limit, key, attempts, remaining)
}

func (r *RateLimiter) hitAfterLimitsTest(c *gin.Context, results []Result) {
	for _, result := range results {
		if result.Limit.AfterFunc == nil || !result.Limit.AfterFunc(c) {
			continue
		}
		attempts, _ := r.Hit(c.Request.Context(), result.Key, result.Limit.Decay)
		result.Attempts = attempts
		result.Remaining = result.MaxAttempts - int(attempts)
		if result.Remaining < 0 {
			result.Remaining = 0
		}
	}
}

func (r *RateLimiter) blockedTest(c *gin.Context, limit Limit, key string) (bool, Result) {
	blocked, err := r.TooManyAttempts(c.Request.Context(), key, limit.MaxAttempts)
	if err != nil || !blocked {
		return false, Result{}
	}
	attempts, _ := r.Attempts(c.Request.Context(), key)
	remaining, _ := r.Remaining(c.Request.Context(), key, limit.MaxAttempts)
	return true, r.resultTest(c, limit, key, attempts, remaining)
}

func (r *RateLimiter) resultTest(c *gin.Context, limit Limit, key string, attempts int64, remaining int) Result {
	retryAfter, _ := r.AvailableIn(c.Request.Context(), key)
	return Result{
		Limit:       limit,
		Key:         key,
		MaxAttempts: limit.MaxAttempts,
		Attempts:    attempts,
		Remaining:   remaining,
		RetryAfter:  retryAfter,
		ResetAt:     time.Now().Add(time.Duration(retryAfter) * time.Second).Unix(),
	}
}

func (r *RateLimiter) resolvedKeyTest(c *gin.Context, name string, limit Limit, seen map[string]int, index int) string {
	raw := strings.TrimSpace(limit.key(c.ClientIP()))
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
	return r.MiddlewareKey(name, raw)
}

func writeSuccessHeadersTest(c *gin.Context, results []Result) {
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

func writeBlockedHeadersTest(c *gin.Context, result Result) {
	c.Header("X-RateLimit-Limit", strconv.Itoa(result.MaxAttempts))
	c.Header("X-RateLimit-Remaining", "0")
	c.Header("Retry-After", strconv.Itoa(result.RetryAfter))
	c.Header("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt, 10))
	c.Status(http.StatusTooManyRequests)
}
