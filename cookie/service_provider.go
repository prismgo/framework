package cookie

import (
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 cookie 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把进程级 cookie queue 注册到当前 Application Container。
//
// 需求背景：cookie 是 framework default provider。Register 阶段只绑定默认
// Process Cookie Queue 的 lazy factory，不创建请求级 Request Cookie Queue。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "cookie" }

// Register 注册 cookie queue lazy factory，并保留显式注入的 queue。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("cookie.queue") {
		return nil
	}
	return c.Singleton("cookie.queue", func(containercontract.Resolver) (any, error) {
		return NewQueue(), nil
	})
}

// Boot 保持无副作用；请求级 cookie 队列由 middleware 在请求生命周期内创建。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
