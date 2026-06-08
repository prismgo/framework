// Package provider 声明 Prismgo service provider 的公共 contracts。
package provider

import (
	"context"

	"github.com/prismgo/framework/contracts/container"
)

// ServiceProvider 表示可进入 Application provider repository 的服务提供者。
//
// 需求背景：provider 的 Register/Boot 生命周期是启动链路核心契约，必须在编译期约束签名。
// 设计思路：方法参数使用本包的 Application 最小接口，避免 framework provider 反向 import
// foundation；foundation.Application 通过 Container 方法自然满足该接口。
type ServiceProvider interface {
	Register(Application) error
	Boot(Application) error
}

// Application 是 provider 生命周期阶段可依赖的最小应用视图。
//
// 参数用途：Register/Boot 通过它访问当前应用拥有的 container，provider 不应依赖 foundation
// 的完整实现类型，从而保持 contracts/provider 与 foundation 之间没有循环依赖。
type Application interface {
	// Container 返回当前 Application 拥有的完整容器。
	//
	// 设计原因：Application 自身有业务语义的 Close(...time.Duration)，不能嵌入完整
	// container.Container，否则会和容器 Close(context.Context) 方法签名冲突。
	Container() container.Container
}

// NamedProvider 声明 provider 的稳定生命周期 identity。
//
// 用途：多数 provider 可以使用完整 Go 类型路径去重；需要多实例或适配器身份时，
// Name 返回非空字符串即可覆盖默认类型 identity。
type NamedProvider interface {
	Name() string
}

// DeferrableProvider 标记可在首次 strict facade Resolve 时加载的延迟 provider。
//
// 需求背景：Laravel 13 通过 provides() 声明延迟服务 key；Prismgo 直接使用 facade
// service key 对齐，不额外引入字符串类名发现或扫描式 discovery。
type DeferrableProvider interface {
	Provides() []string
}

// TerminableProvider 标记拥有应用关闭钩子的 provider。
//
// 关闭语义：Application.CloseContext 会在 AppTerminating 之后、RegisterCleanup 之前，
// 按 provider repository 反序调用已完成 Register 的 provider Terminate。
type TerminableProvider interface {
	Terminate(ctx context.Context) error
}
