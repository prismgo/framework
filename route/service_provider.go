package route

import (
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 route 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把 Router 注册到当前 Application Container。
//
// 需求背景：Router 是应用级资源，应通过容器管理生命周期，与 config/cache/logger 等包保持一致。
// Register 阶段只注册 lazy factory，Router 在首次 facade.Resolve 或容器 Make 时创建。
// 如果调用方已显式注入 Router，本 provider 保留该实例，不覆盖。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "route" }

// Register 注册 Router 的 lazy factory。
//
// 参数 app 是当前 Application 的最小 facade 视图；使用接口而不是 foundation.Application
// 是为了避免 route 包反向 import foundation 形成循环依赖。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("route.router") {
		return nil
	}
	return c.Singleton("route.router", func(containercontract.Resolver) (any, error) {
		return New(), nil
	})
}

// Boot 保持无副作用；Router 由 lazy factory 在首次 Resolve 时创建。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
