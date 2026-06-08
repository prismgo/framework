package logger

import (
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 logger 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把日志 Manager 注册到当前 Application Container。
//
// 需求背景：logger 是 Application base provider，但 Register 阶段只能绑定
// lazy factory，不能打开日志文件或构造具体通道；配置读取也延迟到首次严格
// logger.Resolve()。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "logger" }

// Register 注册日志 Manager lazy factory，并保留当前 Application 已显式注入的实例。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("logger.manager") {
		return nil
	}
	return c.Singleton("logger.manager", func(containercontract.Resolver) (any, error) {
		cfg, err := buildConfig()
		if err != nil {
			return nil, err
		}
		return NewManager(cfg)
	}, container.WithCloser(func(m *Manager) error {
		return m.Close()
	}), container.WithCloseGroup(container.CloseGroupReporting))
}

// Boot 保持无副作用；日志通道仍在首次写入或 Resolve 后按需构造。
func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
