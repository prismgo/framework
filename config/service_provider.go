package config

import (
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 config 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把配置访问器注册到当前 Application Container。
//
// 需求背景：配置是 Application base provider。Register 阶段只做 lazy factory
// 绑定，不读取 .env；真正的 Reload 错误在首次严格 Resolve 时返回给调用方。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "config" }

// Register 注册当前 Application 专属的配置 lazy factory。
//
// 如果测试或应用已经显式注入 Config，本 provider 会保留该实例，避免 provider
// 化迁移覆盖调用方的手工装配。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("config.default") {
		return nil
	}
	return c.Singleton("config.default", func(containercontract.Resolver) (any, error) {
		cfg := New()
		if err := cfg.Reload(); err != nil {
			return nil, err
		}
		return cfg, nil
	})
}

// Boot 保持无副作用；配置加载由 lazy factory 在首次 Resolve 时完成。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
