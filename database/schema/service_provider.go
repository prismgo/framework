package schema

import (
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 schema 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把 schema builder lazy factory 注册到 Application Container。
//
// 需求背景：schema 是 framework default provider。Register 阶段只声明 builder factory，
// 不设置项目级 DefaultStringLength、MorphUsingUuids 等 schema DSL 策略。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "database.schema" }

// Register 声明 schema builder factory，并保留调用方显式注入的 builder。
//
// 设计思路：框架 provider 只负责 `database.schema` slot 的 lazy factory；
// 项目级 DSL 默认值继续由 app/providers/AppServiceProvider 调用。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("database.schema") {
		return nil
	}
	return c.Singleton("database.schema", func(containercontract.Resolver) (any, error) {
		return New(), nil
	})
}

// Boot 保持无副作用；项目级 schema 默认策略属于 Application Provider。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
