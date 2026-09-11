package database

import (
	"fmt"

	configpkg "github.com/prismgo/framework/config"
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 database 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把默认数据库连接 lazy factory 注册到 Application Container。
//
// 需求背景：database 是 framework default provider。Register 阶段只声明 factory，
// 不提前打开 GORM 连接，配置或连接错误由后续 strict Resolve 暴露。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "database" }

// Register 声明默认 database factory，并保留调用方显式注入的 *gorm.DB。
//
// 设计思路：默认 factory 直接调用 OpenDefaultConnection，复用原有 DSN、GORM
// 和连接池配置逻辑，避免 provider 复制数据库内部构造细节。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if !c.Bound("database.manager") {
		if err := c.Singleton("database.manager", func(resolver containercontract.Resolver) (any, error) {
			if !resolver.Bound("config.default") {
				return NewManager(), nil
			}
			cfg, err := configFromResolver(resolver)
			if err != nil {
				return nil, err
			}
			return newManager(cfg.GetBool("app.debug", false)), nil
		}); err != nil {
			return err
		}
	}
	if c.Bound("database.default") {
		return nil
	}
	return c.Singleton("database.default", func(resolver containercontract.Resolver) (any, error) {
		manager, err := ManagerFrom(resolver)
		if err != nil {
			return nil, err
		}
		cfg, err := configFromResolver(resolver)
		if err != nil {
			return nil, err
		}
		return openConnectionWithManagerAndConfig(manager, cfg, "")
	}, DBCloseOption())
}

func configFromResolver(resolver containercontract.Resolver) (*configpkg.Config, error) {
	raw, err := resolver.Make("config.default")
	if err != nil {
		return nil, fmt.Errorf("database: resolve config: %w", err)
	}
	cfg, ok := raw.(*configpkg.Config)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("database: config resolved %T, want *config.Config", raw)
	}
	return cfg, nil
}

// Boot 保持无副作用；数据库连接只在 strict Resolve 或真实入口使用时打开。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
