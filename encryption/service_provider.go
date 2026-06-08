package encryption

import (
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 encryption 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把默认应用加密器注册到当前 Application Container。
//
// 需求背景：encryption 是 framework default provider。Register 阶段只绑定 lazy
// singleton factory，不读取或校验 APP_KEY；缺失或无效 APP_KEY 必须在首次 Resolve 时显式失败。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "encryption" }

// Register 注册默认 encryption lazy singleton factory，并保留显式注入的自定义加密器。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound(serviceKey) {
		return nil
	}
	return c.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return NewFromConfig(nil)
	})
}

// Boot 保持无副作用；加密器在首次 facade/container 解析时才会读取配置并构造。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
