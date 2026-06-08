package event

import (
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/queue"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 event 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把事件总线注册到当前 Application Container。
//
// 需求背景：事件总线是 Application base provider，必须在其它 provider register
// 前尽早提供 dispatcher factory，但 Register 阶段不能把 dispatcher 写入进程级
// fallback，也不能覆盖调用方已经显式注入到当前 Application 的 dispatcher。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "event" }

// Register 注册事件总线 lazy factory。
//
// 参数 app 是当前 Application 的最小 facade 视图；使用接口而不是 foundation.Application
// 是为了避免 event 包反向 import foundation 形成循环依赖。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("event.dispatcher") {
		return nil
	}
	return c.Singleton("event.dispatcher", func(containercontract.Resolver) (any, error) {
		return New(), nil
	})
}

// Boot 注册 queued listener 内部 Job，供默认 queue worker 恢复 payload。
func (ServiceProvider) Boot(providerApplication) error {
	RegisterQueuedListenerJobs(queue.DefaultRegistry())
	return nil
}
