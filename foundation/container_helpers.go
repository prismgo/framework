package foundation

import (
	containercontract "github.com/prismgo/framework/contracts/container"
)

// Make 通过当前 Application 拥有的容器解析服务。
//
// 参数 key 是容器服务标识，返回值由调用方按 contract 或具体实现做类型断言。
func (a *Application) Make(key string) (any, error) {
	return a.Container().Make(key)
}

// Bind 在 Application 容器中注册瞬时服务。
//
// 参数 factory 描述每次解析时如何创建服务；options 用于配置 closer 等生命周期元数据。
func (a *Application) Bind(key string, factory containercontract.Factory, options ...containercontract.BindingOption) error {
	return a.Container().Bind(key, factory, options...)
}

// Singleton 在 Application 容器中注册共享服务。
//
// 参数 factory 只在首次成功解析时执行，适合 manager、dispatcher 和连接池等应用级资源。
func (a *Application) Singleton(key string, factory containercontract.Factory, options ...containercontract.BindingOption) error {
	return a.Container().Singleton(key, factory, options...)
}

// Instance 在 Application 容器中注册已经构造好的共享实例。
//
// 参数 value 会立即标记为 resolved；options 可为该实例声明 Application 关闭时的释放函数。
func (a *Application) Instance(key string, value any, options ...containercontract.BindingOption) error {
	return a.Container().Instance(key, value, options...)
}

// Alias 为 Application 容器中的服务 key 注册别名。
//
// 参数 key 是原始服务名，alias 是调用方后续可用于 Make/Has/Resolved 的替代名称。
func (a *Application) Alias(key, alias string) error {
	return a.Container().Alias(key, alias)
}

// Bound 判断 Application 容器是否已经显式绑定 key。
//
// 该方法不触发 deferred provider 加载，适合 Register/Boot 阶段做低成本探测。
func (a *Application) Bound(key string) bool {
	return a.Container().Bound(key)
}

// Has 判断 Application 容器中的 key 是否可解析。
//
// 与 Bound 不同，Has 可以触发 deferred provider 加载，适合 facade 和启动桥接逻辑使用。
func (a *Application) Has(key string) bool {
	return a.Container().Has(key)
}

// Resolved 判断 Application 容器中的 key 是否已经产出过实例。
//
// 该状态用于生命周期测试和关闭顺序判断，不会主动创建服务。
func (a *Application) Resolved(key string) bool {
	return a.Container().Resolved(key)
}

// Factory 返回延迟解析指定服务的闭包。
//
// 返回闭包内部仍调用 Application 容器 Make，因此保留 singleton/transient 原有生命周期语义。
func (a *Application) Factory(key string) (func() (any, error), error) {
	return a.Container().Factory(key)
}

// Call 调用函数，并为未显式传入的参数从 Application 容器解析依赖。
//
// 参数 args 按位置优先匹配函数入参，剩余参数使用容器的类型字符串解析规则。
func (a *Application) Call(callback any, args ...any) ([]any, error) {
	return a.Container().Call(callback, args...)
}
