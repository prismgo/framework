package ratelimit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/cache"
	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
)

func newTestLimiter(t *testing.T) *RateLimiter {
	t.Helper()
	m, err := cache.NewManager(cache.Config{
		Default: "memory",
		Prefix:  "ratelimit_test",
		Stores: map[string]cache.StoreConfig{
			"memory": {Driver: "memory", CleanupInterval: time.Millisecond},
		},
		Lock: cache.LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return New(m.Default())
}

func setTestLimiter(t *testing.T, limiter *RateLimiter) {
	t.Helper()
	defaultLimiter.mu.Lock()
	previous := defaultLimiter.current
	defaultLimiter.current = limiter
	defaultLimiter.mu.Unlock()
	t.Cleanup(func() {
		defaultLimiter.mu.Lock()
		defaultLimiter.current = previous
		defaultLimiter.mu.Unlock()
	})
}

func clearTestLimiter(t *testing.T) {
	t.Helper()
	defaultLimiter.mu.Lock()
	defaultLimiter.current = nil
	defaultLimiter.mu.Unlock()
	t.Cleanup(func() {
		defaultLimiter.mu.Lock()
		defaultLimiter.current = nil
		defaultLimiter.mu.Unlock()
	})
}

func useRatelimitTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func TestCoreCountersWindowAndCleanup(t *testing.T) {
	ctx := context.Background()
	limiter := newTestLimiter(t)

	attempts, err := limiter.Attempts(ctx, " login\nkey ")
	if err != nil || attempts != 0 {
		t.Fatalf("initial attempts = %d err=%v, want 0 nil", attempts, err)
	}
	if got := CleanRateLimiterKey(" a\nb\t "); got != "ab" {
		t.Fatalf("clean key = %q, want ab", got)
	}

	count, err := limiter.Hit(ctx, "login:key", 60*time.Millisecond)
	if err != nil || count != 1 {
		t.Fatalf("hit = %d err=%v, want 1 nil", count, err)
	}
	remaining, err := limiter.Remaining(ctx, "login:key", 2)
	if err != nil || remaining != 1 {
		t.Fatalf("remaining = %d err=%v, want 1 nil", remaining, err)
	}
	left, err := limiter.RetriesLeft(ctx, "login:key", 2)
	if err != nil || left != remaining {
		t.Fatalf("retries left = %d err=%v, want %d nil", left, err, remaining)
	}
	limited, err := limiter.TooManyAttempts(ctx, "login:key", 2)
	if err != nil || limited {
		t.Fatalf("limited after one hit = %v err=%v, want false nil", limited, err)
	}
	if _, err := limiter.Hit(ctx, "login:key", 60*time.Millisecond); err != nil {
		t.Fatalf("second hit: %v", err)
	}
	limited, err = limiter.TooManyAttempts(ctx, "login:key", 2)
	if err != nil || !limited {
		t.Fatalf("limited after two hits = %v err=%v, want true nil", limited, err)
	}
	wait, err := limiter.AvailableIn(ctx, "login:key")
	if err != nil || wait < 0 {
		t.Fatalf("available in = %d err=%v, want non-negative nil", wait, err)
	}

	if err := limiter.Clear(ctx, "login:key"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	limited, err = limiter.TooManyAttempts(ctx, "login:key", 2)
	if err != nil || limited {
		t.Fatalf("limited after clear = %v err=%v, want false nil", limited, err)
	}

	if _, err := limiter.Increment(ctx, "short", 20*time.Millisecond, 2); err != nil {
		t.Fatalf("short increment: %v", err)
	}
	time.Sleep(35 * time.Millisecond)
	limited, err = limiter.TooManyAttempts(ctx, "short", 1)
	if err != nil || limited {
		t.Fatalf("limited after ttl = %v err=%v, want false nil", limited, err)
	}
}

func TestAttemptIncrementDecrementAndReset(t *testing.T) {
	ctx := context.Background()
	limiter := newTestLimiter(t)

	value, allowed, err := limiter.Attempt(ctx, "attempt", 2, time.Minute, func(context.Context) (any, error) {
		return "ok", nil
	})
	if err != nil || !allowed || value != "ok" {
		t.Fatalf("attempt result = %#v allowed=%v err=%v", value, allowed, err)
	}
	if attempts, _ := limiter.Attempts(ctx, "attempt"); attempts != 1 {
		t.Fatalf("attempts after success = %d, want 1", attempts)
	}

	expectedErr := errors.New("boom")
	_, allowed, err = limiter.Attempt(ctx, "attempt-error", 2, time.Minute, func(context.Context) (any, error) {
		return nil, expectedErr
	})
	if !allowed || !errors.Is(err, expectedErr) {
		t.Fatalf("attempt error allowed=%v err=%v", allowed, err)
	}
	if attempts, _ := limiter.Attempts(ctx, "attempt-error"); attempts != 0 {
		t.Fatalf("failed callback should not hit, got %d", attempts)
	}
	if _, allowed, err := limiter.Attempt(ctx, "bad", 1, time.Minute, nil); err == nil || allowed {
		t.Fatalf("nil callback allowed=%v err=%v, want false error", allowed, err)
	}

	if _, err := limiter.Increment(ctx, "counter", time.Minute, 5); err != nil {
		t.Fatalf("increment: %v", err)
	}
	got, err := limiter.Decrement(ctx, "counter", 2)
	if err != nil || got != 3 {
		t.Fatalf("decrement = %d err=%v, want 3 nil", got, err)
	}
	if err := limiter.ResetAttempts(ctx, "counter"); err != nil {
		t.Fatalf("reset attempts: %v", err)
	}
	if attempts, _ := limiter.Attempts(ctx, "counter"); attempts != 0 {
		t.Fatalf("attempts after reset = %d, want 0", attempts)
	}
}

func TestLimiterCountersUseCacheCounterSemanticsWithMsgpackCache(t *testing.T) {
	ctx := context.Background()
	limiter := newTestLimiter(t)

	// 测试目的：验证限流尝试次数只走 cache counter 语义，不再先写普通 cache value。
	//
	// 需求背景：cache 默认 Payload Encoding 是 msgpack；如果 ratelimit 把同一个 key 同时当
	// cache value 和 counter 使用，底层 Increment 会无法解析旧 value。这里使用默认测试 cache
	// 的 msgpack 配置，覆盖 Hit、Attempts、Remaining、AvailableIn 的完整操作方式。
	if count, err := limiter.Hit(ctx, "msgpack-counter", time.Minute); err != nil || count != 1 {
		t.Fatalf("first hit = %d err=%v, want 1 nil", count, err)
	}
	if count, err := limiter.Increment(ctx, "msgpack-counter", time.Minute, 2); err != nil || count != 3 {
		t.Fatalf("increment = %d err=%v, want 3 nil", count, err)
	}
	if attempts, err := limiter.Attempts(ctx, "msgpack-counter"); err != nil || attempts != 3 {
		t.Fatalf("attempts = %d err=%v, want 3 nil", attempts, err)
	}
	if remaining, err := limiter.Remaining(ctx, "msgpack-counter", 5); err != nil || remaining != 2 {
		t.Fatalf("remaining = %d err=%v, want 2 nil", remaining, err)
	}
	if wait, err := limiter.AvailableIn(ctx, "msgpack-counter"); err != nil || wait < 0 {
		t.Fatalf("available in = %d err=%v, want non-negative nil", wait, err)
	}
}

func TestFacadeUsesExplicitLimiterInstance(t *testing.T) {
	limiter := newTestLimiter(t)
	limiter.ShouldHashKeys(true)
	limiter.For("api", func(*gin.Context) []Limit {
		return []Limit{PerMinute(1).By("user")}
	})
	setTestLimiter(t, limiter)

	// 测试目的：验证 ratelimit facade 只依赖显式注册的限流器实例，不再需要 RuntimeGlobal 快照。
	//
	// 设计思路：通过公开 Use/Limiter/MiddlewareKey 路径验证 facade 行为；测试隔离只保存并恢复
	// 当前显式实例，避免把内部 limiters map 暴露成生产诊断 API。
	if Limiter("api") == nil {
		t.Fatal("expected named limiter from explicit facade instance")
	}
	if got := Resolve().MiddlewareKey("api", "tenant:1"); len(got) != len("ratelimit:api:")+40 {
		t.Fatalf("hashed middleware key = %q, want sha1 suffix", got)
	}
}

func TestLimitConstructorsAndRegistration(t *testing.T) {
	limiter := newTestLimiter(t)
	limiter.For("api", func(*gin.Context) []Limit {
		return []Limit{
			PerSecond(1).By("second"),
			PerMinute(2).FallbackKey("minute"),
			PerMinutes(3, 4),
			PerHour(5),
			PerDay(6),
			None(),
		}
	})
	registered := limiter.Limiter("api")
	if registered == nil {
		t.Fatal("expected registered limiter")
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	limits := registered(c)
	if len(limits) != 6 {
		t.Fatalf("limits len = %d, want 6", len(limits))
	}
	if limits[0].Decay != time.Second || limits[1].Decay != time.Minute || limits[2].Decay != 3*time.Minute {
		t.Fatalf("unexpected short decays: %#v", limits[:3])
	}
	if limits[3].Decay != time.Hour || limits[4].Decay != 24*time.Hour || limits[5].enabled() {
		t.Fatalf("unexpected long decays or none limit: %#v", limits[3:])
	}
}

func TestThrottleMiddlewareHeadersAndDefaultResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	limiter := newTestLimiter(t)
	limiter.For("api", func(*gin.Context) []Limit {
		return []Limit{PerMinute(1).By("user:1")}
	})
	engine := gin.New()
	engine.GET("/ok", limiter.throttleTest("api"), func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	w := perform(engine, "/ok")
	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("first response = %d %q", w.Code, w.Body.String())
	}
	if w.Header().Get("X-RateLimit-Limit") != "1" || w.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("first headers = %#v", w.Header())
	}

	w = perform(engine, "/ok")
	if w.Code != http.StatusTooManyRequests || !strings.Contains(w.Body.String(), "too many requests") {
		t.Fatalf("blocked response = %d %q", w.Code, w.Body.String())
	}
	if w.Header().Get("Retry-After") == "" || w.Header().Get("X-RateLimit-Reset") == "" {
		t.Fatalf("missing blocked headers: %#v", w.Header())
	}
}

func TestThrottleAfterCustomResponseFallbackAndHash(t *testing.T) {
	gin.SetMode(gin.TestMode)
	limiter := newTestLimiter(t)
	limiter.ShouldHashKeys(true)
	limiter.For("errors", func(*gin.Context) []Limit {
		return []Limit{
			PerMinute(1).By("same"),
			PerMinute(2).By("same").FallbackKey("same:fallback").After(func(c *gin.Context) bool {
				return c.Writer.Status() >= http.StatusInternalServerError
			}).Response(func(c *gin.Context, result Result) {
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"retry_after": result.RetryAfter,
					"custom":      true,
				})
			}),
		}
	})

	engine := gin.New()
	engine.GET("/status/:code", limiter.throttleTest("errors"), func(c *gin.Context) {
		code, _ := strconvAtoi(c.Param("code"))
		c.String(code, "status")
	})

	w := perform(engine, "/status/500")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("first 500 = %d", w.Code)
	}
	hashedPrimary := limiter.MiddlewareKey("errors", "same")
	hashedFallback := limiter.MiddlewareKey("errors", "same:fallback")
	if attempts, _ := limiter.Attempts(context.Background(), "ratelimit:errors:same"); attempts != 0 {
		t.Fatalf("plain key should not be used when hashing, attempts=%d", attempts)
	}
	if attempts, _ := limiter.Attempts(context.Background(), hashedPrimary); attempts != 1 {
		t.Fatalf("primary attempts = %d, want 1", attempts)
	}
	if attempts, _ := limiter.Attempts(context.Background(), hashedFallback); attempts != 1 {
		t.Fatalf("fallback attempts = %d, want 1", attempts)
	}

	w = perform(engine, "/status/200")
	if w.Code != http.StatusTooManyRequests || !strings.Contains(w.Body.String(), "too many requests") {
		t.Fatalf("second response = %d %q", w.Code, w.Body.String())
	}
}

func TestFacadeUsesConfiguredLimiter(t *testing.T) {
	limiter := newTestLimiter(t)
	setTestLimiter(t, limiter)
	For("facade", func(*gin.Context) []Limit {
		return []Limit{PerMinute(1).By("facade")}
	})
	if Limiter("facade") == nil || Resolve() != limiter {
		t.Fatal("expected facade to use configured limiter")
	}
}

func TestFacadeCurrentUsesLimiterCacheDriver(t *testing.T) {
	clearTestLimiter(t)
	registry := useRatelimitTestContainer(t)

	manager, err := cache.NewManager(cache.Config{
		Default: "memory",
		Prefix:  "ratelimit_config",
		Stores: map[string]cache.StoreConfig{
			"memory":  {Driver: "memory", CleanupInterval: time.Millisecond},
			"limiter": {Driver: "memory", CleanupInterval: time.Millisecond},
		},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}

	configpkg.Add("cache", func() map[string]any {
		return map[string]any{
			"limiter": map[string]any{"driver": "limiter"},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	if got := Resolve().repo.Name(); got != "limiter" {
		t.Fatalf("expected limiter cache store limiter, got %s", got)
	}
}

func TestFacadeCurrentFallsBackToDefaultCacheStore(t *testing.T) {
	clearTestLimiter(t)
	registry := useRatelimitTestContainer(t)

	manager, err := cache.NewManager(cache.Config{
		Default: "shared",
		Stores: map[string]cache.StoreConfig{
			"memory": {Driver: "memory", CleanupInterval: time.Millisecond},
			"shared": {Driver: "memory", CleanupInterval: time.Millisecond},
		},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}
	if err := registry.Instance("config.default", configpkg.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	if got := Resolve().repo.Name(); got != "shared" {
		t.Fatalf("expected fallback cache store shared, got %s", got)
	}
}

func TestFacadeWrappersAndFallbackCurrent(t *testing.T) {
	ctx := context.Background()
	limiter := newTestLimiter(t)
	setTestLimiter(t, limiter)

	ShouldHashKeys(false)
	value, allowed, err := Attempt(ctx, "facade:attempt", 2, time.Minute, func(context.Context) (any, error) {
		return "facade-ok", nil
	})
	if err != nil || !allowed || value != "facade-ok" {
		t.Fatalf("facade attempt = %#v allowed=%v err=%v", value, allowed, err)
	}
	if _, err := Hit(ctx, "facade:hit", time.Minute); err != nil {
		t.Fatalf("facade hit: %v", err)
	}
	if _, err := Increment(ctx, "facade:hit", time.Minute, 2); err != nil {
		t.Fatalf("facade increment: %v", err)
	}
	if attempts, err := Attempts(ctx, "facade:hit"); err != nil || attempts != 3 {
		t.Fatalf("facade attempts = %d err=%v, want 3 nil", attempts, err)
	}
	if got, err := Decrement(ctx, "facade:hit"); err != nil || got != 2 {
		t.Fatalf("facade decrement = %d err=%v, want 2 nil", got, err)
	}
	if remaining, err := Remaining(ctx, "facade:hit", 4); err != nil || remaining != 2 {
		t.Fatalf("facade remaining = %d err=%v, want 2 nil", remaining, err)
	}
	if retries, err := RetriesLeft(ctx, "facade:hit", 4); err != nil || retries != 2 {
		t.Fatalf("facade retries = %d err=%v, want 2 nil", retries, err)
	}
	if limited, err := TooManyAttempts(ctx, "facade:hit", 2); err != nil || !limited {
		t.Fatalf("facade limited = %v err=%v, want true nil", limited, err)
	}
	if wait, err := AvailableIn(ctx, "facade:hit"); err != nil || wait < 0 {
		t.Fatalf("facade available = %d err=%v", wait, err)
	}
	if err := ResetAttempts(ctx, "facade:hit"); err != nil {
		t.Fatalf("facade reset: %v", err)
	}
	if err := Clear(ctx, "facade:hit"); err != nil {
		t.Fatalf("facade clear: %v", err)
	}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.GET("/open", Resolve().throttleTest("missing"), func(c *gin.Context) {
		c.String(http.StatusOK, "open")
	})
	if w := perform(engine, "/open"); w.Code != http.StatusOK || w.Body.String() != "open" {
		t.Fatalf("missing limiter response = %d %q", w.Code, w.Body.String())
	}

	registry := useRatelimitTestContainer(t)
	manager, err := cache.NewManager(cache.Config{
		Default: "memory",
		Stores:  map[string]cache.StoreConfig{"memory": {Driver: "memory", CleanupInterval: time.Millisecond}},
	})
	if err != nil {
		t.Fatalf("new fallback cache manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind fallback cache manager: %v", err)
	}
	if err := registry.Instance("config.default", configpkg.New()); err != nil {
		t.Fatalf("bind fallback config: %v", err)
	}
	clearTestLimiter(t)
	if Resolve() == nil {
		t.Fatal("expected fallback current limiter")
	}
}

func TestThrottleEmptyDisabledAndCustomResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	limiter := newTestLimiter(t)
	limiter.For("empty", func(*gin.Context) []Limit { return nil })
	limiter.For("disabled", func(*gin.Context) []Limit { return []Limit{None()} })
	limiter.For("custom", func(*gin.Context) []Limit {
		return []Limit{
			PerMinute(1).By("custom").Response(func(c *gin.Context, result Result) {
				c.JSON(http.StatusTooManyRequests, gin.H{"custom": result.MaxAttempts})
				c.Abort()
			}),
		}
	})

	engine := gin.New()
	engine.GET("/empty", limiter.throttleTest("empty"), func(c *gin.Context) { c.String(http.StatusOK, "empty") })
	engine.GET("/disabled", limiter.throttleTest("disabled"), func(c *gin.Context) { c.String(http.StatusOK, "disabled") })
	engine.GET("/custom", limiter.throttleTest("custom"), func(c *gin.Context) { c.String(http.StatusOK, "custom") })

	if w := perform(engine, "/empty"); w.Code != http.StatusOK || w.Body.String() != "empty" {
		t.Fatalf("empty response = %d %q", w.Code, w.Body.String())
	}
	if w := perform(engine, "/disabled"); w.Code != http.StatusOK || w.Body.String() != "disabled" {
		t.Fatalf("disabled response = %d %q", w.Code, w.Body.String())
	}
	if w := perform(engine, "/custom"); w.Code != http.StatusOK {
		t.Fatalf("first custom = %d", w.Code)
	}
	w := perform(engine, "/custom")
	if w.Code != http.StatusTooManyRequests || !strings.Contains(w.Body.String(), `"custom":1`) {
		t.Fatalf("custom blocked = %d %q", w.Code, w.Body.String())
	}
}

func TestIntegerCoercionBranches(t *testing.T) {
	cases := []any{int64(1), int(2), int32(3), float64(4), float32(5), "6"}
	for i, value := range cases {
		got, err := asInt64(value)
		if err != nil || got != int64(i+1) {
			t.Fatalf("asInt64(%T) = %d err=%v, want %d nil", value, got, err, i+1)
		}
	}
	if _, err := asInt64(struct{}{}); err == nil {
		t.Fatal("expected unsupported integer type error")
	}
	if got := normalizeDecay(0); got != defaultDecay {
		t.Fatalf("normalize default = %s, want %s", got, defaultDecay)
	}
}

func perform(engine *gin.Engine, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	return w
}

func strconvAtoi(value string) (int, error) {
	n := 0
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, errors.New("not a number")
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}
