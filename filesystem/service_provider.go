package filesystem

import (
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 filesystem 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把 filesystem manager lazy factory 注册到 Application Container。
//
// 需求背景：filesystem 是 framework default provider。Register 阶段只声明 lazy factory，
// 不提前构造 Manager，也不打开本地目录、OSS bucket 或自定义 driver。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "filesystem" }

// Register 声明 filesystem manager factory，并保留调用方显式注入的 manager。
//
// 设计思路：默认 factory 直接复用包内 buildConfig + NewManager，错误由 strict
// Resolve 返回；Register 阶段不读取磁盘实例，保持启动装配轻量。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("filesystem.manager") {
		return nil
	}
	return c.Singleton("filesystem.manager", func(containercontract.Resolver) (any, error) {
		cfg, err := buildConfig()
		if err != nil {
			return nil, err
		}
		return NewManager(cfg)
	}, container.WithCloser(func(m *Manager) error {
		return m.Close()
	}))
}

// Boot 保持无副作用；具体 disk 仍由 Manager.Disk 在真实访问时惰性构造。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
