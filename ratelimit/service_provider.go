package ratelimit

import (
	"strings"

	"github.com/prismgo/framework/cache"
	configpkg "github.com/prismgo/framework/config"
	cachecontract "github.com/prismgo/framework/contracts/cache"
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// ServiceProvider 把 ratelimit 限流器注册到当前 Application Container。
//
// 设计原因：遵循框架约定，每个组件包应包含 service_provider.go 用于注册到容器，
// 使限流器可以通过 foundation.Application 生命周期自动初始化。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "ratelimit" }

// Register 注册限流器 lazy factory。
//
// 实现细节：使用 Singleton 绑定 "ratelimit.default" key，factory 直接创建 RateLimiter 实例。
// 如果已绑定则直接返回，避免重复注册。
func (ServiceProvider) Register(app providercontract.Application) error {
	c := app.Container()
	if c.Bound("ratelimit.default") {
		return nil
	}
	return c.Singleton("ratelimit.default", func(containercontract.Resolver) (any, error) {
		return New(configuredRepository()), nil
	})
}

// Boot 当前为空实现，预留扩展点。
func (sp ServiceProvider) Boot(app providercontract.Application) error {
	return nil
}

// configuredRepository 返回配置的限流器缓存仓库。
//
// 设计原因：支持通过 config 中的 cache.limiter.driver 指定专用缓存 store，
// 如果未配置则使用默认缓存 store。
func configuredRepository() cachecontract.Repository {
	driver := strings.TrimSpace(configpkg.GetString("cache.limiter.driver", ""))
	if driver == "" {
		return cache.Resolve().Default()
	}
	return cache.Store(driver)
}
