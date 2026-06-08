package session

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

const storeContextKey = "prismgo.session.store"

type middlewareConfig struct {
	Manager *Manager
}

// MiddlewareOption 配置 StartSession 中间件。
//
// 需求背景：多数业务路由直接使用全局 session manager 即可，但测试或局部隔离场景需要显式传入独立 manager。
// 设计思路：使用 option 保持入口稳定，后续增加锁、驱动或 cookie 行为开关时不需要改动 StartSession 的签名。
type MiddlewareOption func(*middlewareConfig)

// WithManager 为当前中间件指定显式 session manager。
//
// 参数 manager 是已经完成配置的 session 管理器；传入 nil 时 StartSession 会回退到 session.Default()。
func WithManager(manager *Manager) MiddlewareOption {
	return func(cfg *middlewareConfig) {
		cfg.Manager = manager
	}
}

// NewMiddlewareConfig applies StartSession options for HTTP middleware.
func NewMiddlewareConfig(options ...MiddlewareOption) middlewareConfig {
	cfg := middlewareConfig{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	return cfg
}

// RecordMiddlewareError 把 session/cookie 生命周期错误交给 Gin 错误链路。
//
// 设计原因：中间件内部错误不能静默忽略；这里统一写入 c.Errors 并中止请求，外层
// httpkit.ExceptionHandler 可继续按项目规则渲染安全错误响应。
func RecordMiddlewareError(c *gin.Context, err error) {
	if c == nil {
		return
	}
	_ = c.Error(err)
	c.Status(http.StatusInternalServerError)
	c.Abort()
}

// SetStore installs a request-level Store into gin.Context.
func SetStore(c *gin.Context, store *Store) {
	if c == nil || store == nil {
		return
	}
	c.Set(storeContextKey, store)
}
