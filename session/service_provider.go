package session

import (
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 session 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把 session manager 注册到当前 Application Container。
//
// 需求背景：session 是 framework default provider。Register 阶段只绑定 lazy
// factory，不创建 session store、文件、Redis 连接或写出 session cookie。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "session" }

// Register 注册 session manager lazy factory，并保留显式注入的 manager。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("session.manager") {
		return nil
	}
	return c.Singleton("session.manager", func(containercontract.Resolver) (any, error) {
		return NewManagerFromConfig()
	})
}

// Boot 保持无副作用；请求级 session 生命周期由 middleware 负责。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
