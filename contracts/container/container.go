// Package container 定义 PrismGo 服务容器的公共契约。
//
// 设计背景：
// Laravel 的 Application 以 Service Container 作为服务解析底座，facade 只是从容器解析服务的快捷代理。
// PrismGo 的框架包也应通过契约依赖容器能力，而不是依赖具体实现包或全局注册中心。
//
// 使用方式：
// Provider 的 Register 阶段通常只绑定 factory，不创建数据库连接、Redis client 等重资源：
//
//	app.Container().Singleton("cache.manager", func(r container.Resolver) (any, error) {
//		return cache.NewManager(cfg)
//	})
//
// 业务或 facade 快捷入口解析服务时使用稳定字符串 key。具体实现包可以再提供泛型 helper，
// 例如 container.Make[*cache.Manager](app.Container(), "cache.manager")。
package container

import "context"

// CloseGroup 标识共享服务在 Application 关闭链路中的释放阶段。
//
// 默认服务使用 CloseGroupNormal。日志、异常处理器、错误上报 client 等“关闭期仍要使用”的资源
// 应使用 CloseGroupReporting，使普通资源关闭失败时仍能被记录或上报。
type CloseGroup string

const (
	// CloseGroupNormal 是默认关闭分组，会在关闭期错误上报前释放。
	CloseGroupNormal CloseGroup = "normal"
	// CloseGroupReporting 是上报资源关闭分组，会在普通关闭错误上报后释放。
	CloseGroupReporting CloseGroup = "reporting"
)

// Factory 是容器服务工厂。
//
// resolver 参数只暴露 Resolver 契约，工厂可以通过它解析依赖，但不应在 factory 内注册新绑定。
// Singleton 工厂只会在首次成功解析时执行一次；Bind 工厂会在每次 Make 时执行。
//
// 示例：
//
//	app.Container().Singleton("queue.manager", func(r container.Resolver) (any, error) {
//		events, _ := r.Make("event.dispatcher")
//		return queue.NewManager(events)
//	})
type Factory func(Resolver) (any, error)

// Closer 释放已解析的共享服务实例。
//
// Close 和 CloseGroup 会按绑定顺序的反序调用 closer。closer 返回错误时，该实例会保留在容器中，
// 调用方可以稍后再次 CloseContext 重试释放。
type Closer func(context.Context, any) error

// BindingOption 配置绑定的生命周期元数据。
//
// 说明：
// contracts 子包只声明契约，不提供 WithCloser 这类 helper。具体实现包可以提供自己的 option
// 构造函数，也可以由调用方直接传入满足该类型的函数。
type BindingOption func(*Binding)

// Binding 保存绑定元数据。
//
// 字段语义：
//   - Closer：释放 Instance 或 Singleton 已持有实例的函数。Bind 返回的瞬时对象不由容器持有，
//     因此不会在 Close 时释放。
//   - CloseGroup：释放阶段。空值由实现包按 CloseGroupNormal 处理。
//
// 示例：
//
//	err := app.Container().Singleton("cache.manager", newCacheManager,
//		func(binding *container.Binding) {
//			binding.Closer = func(ctx context.Context, value any) error {
//				return value.(*cache.Manager).Close()
//			}
//		},
//	)
type Binding struct {
	Closer     Closer
	CloseGroup CloseGroup
}

// Resolver 是容器的解析侧契约。
//
// Provider 的 Boot 阶段、facade 快捷入口和框架桥接代码通常只需要 Resolver。这样可以避免
// 解析服务的代码意外修改容器绑定。
type Resolver interface {
	// Bound 判断 key 是否已经在当前容器中绑定。
	//
	// 语义对齐 Laravel Container::bound：只检查当前绑定表和 alias，不触发 deferred provider 加载。
	// 如果需要“按需加载后是否可解析”的判断，使用 Has。
	//
	// 示例：
	//
	//	if app.Container().Bound("cache.manager") {
	//		// 当前 Application 已经显式绑定 cache manager。
	//	}
	Bound(key string) bool

	// Has 判断 key 是否可解析。
	//
	// 与 Bound 不同，Has 可以触发容器实现的 deferred loader。适合 facade 在 Resolve 前做宽松探测，
	// 也适合框架内部判断某个默认 provider 是否会在首次 Make 时加载。
	//
	// 示例：
	//
	//	if resolver.Has("event.dispatcher") {
	//		bus, _ := resolver.Make("event.dispatcher")
	//		_ = bus
	//	}
	Has(key string) bool

	// Make 按字符串 key 解析服务。
	//
	// Bind 绑定会每次调用 factory；Singleton 绑定会在首次成功解析后复用同一个实例；
	// Instance 绑定会直接返回已注册实例。调用方需要对返回值做类型断言，或使用具体实现包提供的泛型 helper。
	//
	// 示例：
	//
	//	raw, err := resolver.Make("cache.manager")
	//	if err != nil {
	//		return err
	//	}
	//	manager := raw.(*cache.Manager)
	Make(key string) (any, error)

	// Factory 返回一个可调用的解析闭包。
	//
	// 返回的闭包遵循绑定本身的生命周期语义：transient 仍会创建新实例，singleton 仍会复用共享实例。
	// 适合把容器解析能力延迟传给不应直接持有容器的组件。
	//
	// 示例：
	//
	//	makeQueue, err := resolver.Factory("queue.manager")
	//	if err != nil {
	//		return err
	//	}
	//	raw, err := makeQueue()
	Factory(key string) (func() (any, error), error)

	// Call 调用函数，并为未显式传入的参数从容器解析依赖。
	//
	// 显式 args 按参数顺序优先使用；剩余参数使用参数类型字符串作为服务 key 解析。第一版不实现
	// Laravel 的完整 method injection 语义，也不做 contextual binding。
	//
	// 示例：
	//
	//	_, err := resolver.Call(func(bus event.Dispatcher) error {
	//		return bus.Dispatch(ctx, userRegistered)
	//	})
	Call(callback any, args ...any) ([]any, error)

	// Resolved 判断 key 是否已经产出过实例。
	//
	// 语义对齐 Laravel Container::resolved：Instance 注册后立即视为 resolved；Singleton 首次 Make
	// 成功后视为 resolved；Bind 每次 Make 成功后也会标记为 resolved。
	//
	// 示例：
	//
	//	if resolver.Resolved("logger.manager") {
	//		// logger 已经被解析过，关闭顺序或热更新逻辑可以据此判断。
	//	}
	Resolved(key string) bool
}

// Binder 是容器的绑定侧契约。
//
// ServiceProvider.Register 阶段主要依赖 Binder。Register 应只描述如何创建服务，不应立即创建重资源；
// 依赖已经可解析后的桥接、监听器、命令注册应放在 Boot 阶段。
type Binder interface {
	// Bind 注册瞬时服务。
	//
	// 每次 Make 都会重新执行 factory，容器不保存返回实例，也不会在 Close 时释放瞬时实例。
	//
	// 示例：
	//
	//	app.Container().Bind("uuid.generator", func(container.Resolver) (any, error) {
	//		return uuid.NewString, nil
	//	})
	Bind(key string, factory Factory, options ...BindingOption) error

	// Singleton 注册共享服务。
	//
	// factory 在首次 Make 成功时执行，返回实例会保存在容器中。适合 manager、dispatcher、logger、
	// database connection pool 等应用级服务。
	//
	// 示例：
	//
	//	app.Container().Singleton("event.dispatcher", func(container.Resolver) (any, error) {
	//		return event.NewDispatcher(), nil
	//	})
	Singleton(key string, factory Factory, options ...BindingOption) error

	// Instance 注册已构造好的共享实例。
	//
	// 适合测试替换、启动阶段已有对象注入，或用户显式 Use 某个 facade 底层服务。
	//
	// 示例：
	//
	//	app.Container().Instance("config.default", cfg)
	Instance(key string, value any, options ...BindingOption) error

	// Alias 为已有服务 key 注册别名。
	//
	// alias 会在 Make、Has、Bound、Resolved 等入口解析到原始 key。别名不复制绑定，也不改变关闭顺序。
	//
	// 示例：
	//
	//	app.Container().Alias("cache.manager", "cache")
	Alias(key, alias string) error
}

// Container 是完整应用服务容器契约。
//
// foundation.Application 应暴露该契约或具体实现，让框架包把服务绑定到 Application 容器中。
// 普通业务代码通常不需要依赖完整 Container；只读路径使用 Resolver，注册路径使用 Binder。
type Container interface {
	Resolver
	Binder

	// CloseGroup 关闭指定分组中已经解析并持有的共享服务。
	//
	// Application.CloseContext 会先关闭 CloseGroupNormal，再进行关闭期错误上报，最后关闭
	// CloseGroupReporting。关闭失败的服务会保留，后续可重试。
	CloseGroup(ctx context.Context, group CloseGroup) error

	// Close 关闭所有已经解析并持有的共享服务。
	//
	// 关闭顺序是绑定顺序的反序，匹配资源依赖“后创建先释放”的常见约束。
	Close(ctx context.Context) error
}
