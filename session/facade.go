package session

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "session.manager"

// Resolve 从当前 Application 容器解析 session manager。
func Resolve() *Manager {
	return facade.Resolve[*Manager](serviceKey)
}

// NewManagerFromConfig 根据当前 config facade 创建默认 Manager。
func NewManagerFromConfig() (*Manager, error) {
	cfg, err := ConfigFromFacadeStrict()
	if err != nil {
		return nil, err
	}
	manager, err := NewManager(cfg, nil)
	if err != nil {
		return nil, err
	}
	return manager, nil
}

// Start 通过全局 Manager 启动标准 net/http 请求的 session。
func Start(ctx context.Context, r *http.Request, w http.ResponseWriter) (*Store, error) {
	manager := Resolve()
	if manager == nil {
		return nil, ErrInvalidConfig
	}
	return manager.Start(ctx, r, w)
}

// StoreFrom 从 Gin 上下文读取当前请求级 Store。
func StoreFrom(c *gin.Context) (*Store, bool) {
	if c == nil {
		return nil, false
	}
	store, ok := c.Get(storeContextKey)
	if !ok {
		return nil, false
	}
	typed, ok := store.(*Store)
	return typed, ok
}

// Get 从当前 Gin 请求的 Store 读取值，缺失时返回默认值或 nil。
func Get(c *gin.Context, key string, def ...any) any {
	store, ok := StoreFrom(c)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return nil
	}
	return store.Get(key, def...)
}

// Put 写入当前 Gin 请求的 Store。
func Put(c *gin.Context, key string, value any) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	store.Put(key, value)
	return nil
}

// Has 判断当前 Store 中 key 是否存在且值不为 nil。
func Has(c *gin.Context, key string) bool {
	store, ok := StoreFrom(c)
	return ok && store.Has(key)
}

// Exists 判断当前 Store 中 key 是否存在，包含 nil 值。
func Exists(c *gin.Context, key string) bool {
	store, ok := StoreFrom(c)
	return ok && store.Exists(key)
}

// Missing 判断当前 Store 中 key 是否不存在。
func Missing(c *gin.Context, key string) bool {
	store, ok := StoreFrom(c)
	return !ok || store.Missing(key)
}

// Pull 读取当前 Store 中的值并删除该 key。
func Pull(c *gin.Context, key string, def ...any) any {
	store, ok := StoreFrom(c)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return nil
	}
	return store.Pull(key, def...)
}

// Forget 从当前 Store 删除一个或多个 key。
func Forget(c *gin.Context, keys ...string) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	store.Forget(keys...)
	return nil
}

// Flush 清空当前 Store 的业务数据和 flash 元数据。
func Flush(c *gin.Context) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	store.Flush()
	return nil
}

// Flash 写入当前请求和下一次请求可读的临时值。
func Flash(c *gin.Context, key string, value any) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	store.Flash(key, value)
	return nil
}

// Now 写入仅当前请求可读的临时值。
func Now(c *gin.Context, key string, value any) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	store.Now(key, value)
	return nil
}

// Reflash 延长全部当前 flash 数据一个请求周期。
func Reflash(c *gin.Context) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	store.Reflash()
	return nil
}

// Keep 延长指定 flash key 一个请求周期。
func Keep(c *gin.Context, keys ...string) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	store.Keep(keys...)
	return nil
}

// Regenerate 为当前 Store 换发新的 session ID，并保留当前数据。
func Regenerate(c *gin.Context) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	return store.Regenerate(contextFrom(c))
}

// Invalidate 清空当前 Store 并换发新的 session ID。
func Invalidate(c *gin.Context) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	return store.Invalidate(contextFrom(c))
}

// Save 立即保存当前 Store。
func Save(c *gin.Context) error {
	store, ok := StoreFrom(c)
	if !ok {
		return ErrInvalidConfig
	}
	return store.Save(contextFrom(c))
}

func contextFrom(c *gin.Context) context.Context {
	if c != nil && c.Request != nil {
		return c.Request.Context()
	}
	return context.Background()
}
