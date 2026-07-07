package ratelimit

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/exception"
)

// AfterFunc 在业务 handler 执行后判断本次请求是否应该计入限流次数。
//
// 该回调用于复刻 Laravel Limit::after 的语义，例如只统计 404、5xx 或失败响应。
type AfterFunc func(*gin.Context) bool

// ResponseFunc 在命中限流上限时生成自定义响应。
//
// 调用方可以通过 Result 读取剩余时间、上限和当前尝试次数，并自行写入 JSON 或文本响应。
type ResponseFunc func(*gin.Context, Result)

// Limit 描述一次固定窗口限流规则。
//
// MaxAttempts 是窗口内允许的最大尝试次数，Decay 是窗口长度，Key/Fallback 用于生成缓存 key。
// After 和 Response 分别对应 Laravel 的 after 与 response 自定义能力。
//
// 使用方式：
//   - 推荐使用构造函数：PerMinute(10)、PerHour(100) 等
//   - 使用 Builder 方法链式配置：PerMinute(10).By("user:123").After(fn).Response(fn)
//   - 直接初始化结构体也是合法的，但 Builder 方法提供更好的可读性
type Limit struct {
	MaxAttempts  int
	Decay        time.Duration
	Key          string
	Fallback     string
	AfterFunc    AfterFunc
	ResponseFunc ResponseFunc
}

// Result 描述一次限流检查或命中的运行时结果。
type Result struct {
	Limit       Limit
	Key         string
	MaxAttempts int
	Attempts    int64
	Remaining   int
	RetryAfter  int
	ResetAt     int64
}

// Every 构造指定窗口长度的限流规则。
func Every(decay time.Duration, maxAttempts int) Limit {
	return Limit{MaxAttempts: maxAttempts, Decay: decay}
}

// PerSecond 构造每秒限流规则。
func PerSecond(maxAttempts int) Limit {
	return Every(time.Second, maxAttempts)
}

// PerMinute 构造每分钟限流规则。
func PerMinute(maxAttempts int) Limit {
	return Every(time.Minute, maxAttempts)
}

// PerMinutes 构造多分钟限流规则，参数顺序与 Laravel Limit::perMinutes 保持一致。
func PerMinutes(decayMinutes, maxAttempts int) Limit {
	return Every(time.Duration(decayMinutes)*time.Minute, maxAttempts)
}

// PerHour 构造每小时限流规则。
func PerHour(maxAttempts int) Limit {
	return Every(time.Hour, maxAttempts)
}

// PerDay 构造每日限流规则。
func PerDay(maxAttempts int) Limit {
	return Every(24*time.Hour, maxAttempts)
}

// None 构造不限制的规则，常用于按条件关闭某个命名限流器。
func None() Limit {
	return Limit{}
}

// By 设置当前规则的限流维度 key。
func (l Limit) By(key string) Limit {
	l.Key = key
	return l
}

// FallbackKey 设置当前规则未显式指定 key 时使用的兜底 key。
func (l Limit) FallbackKey(key string) Limit {
	l.Fallback = key
	return l
}

// After 设置后置计数判断逻辑。
func (l Limit) After(fn AfterFunc) Limit {
	l.AfterFunc = fn
	return l
}

// Response 设置命中限流后的自定义响应。
func (l Limit) Response(fn ResponseFunc) Limit {
	l.ResponseFunc = fn
	return l
}

func (l Limit) enabled() bool {
	return l.MaxAttempts > 0 && l.Decay > 0
}

func (l Limit) key(fallback string) string {
	if l.Key != "" {
		return l.Key
	}
	if l.Fallback != "" {
		return l.Fallback
	}
	return fallback
}

func defaultResponse(c *gin.Context, result Result) {
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
