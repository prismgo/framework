package kernel

import (
	"context"
	"fmt"
)

// ApplicationLifecycle 描述 Console Kernel 需要的应用生命周期能力。
//
// 用途：让 Kernel 可以编排“应用启动后执行 CLI，再由应用统一关闭资源”的流程，
// 同时避免 kernel 包直接依赖 foundation 包而形成循环依赖。
// 设计思路：接口只暴露 RunContext 这一条窄入口，Application 仍然拥有根 context、
// provider、cleanup 和 facade registry；Kernel 只负责把当前 CLI runner 交给应用。
// 需求背景：对齐 Laravel Console Kernel 的应用级入口，使 main.go 保持“创建 Application、
// 创建 Kernel、运行 Kernel”的极简形态。
type ApplicationLifecycle interface {
	RunContext(run func(context.Context) error, contexts ...context.Context) error
}

// RunApplication 在应用生命周期内运行当前 Console Kernel。
//
// 参数说明：
// application 是当前项目 Application 或兼容实现，负责 boot、信号监听和 close；
// contexts 是可选外部运行边界，会继续透传给 Application.RunContext。
// 逻辑说明：Kernel 不持有 Application，也不接管 facade registry 或资源关闭；
// 它只把自身 RunContext 作为 runner 传给 Application，保持生命周期归属清晰。
func (k *Kernel) RunApplication(application ApplicationLifecycle, contexts ...context.Context) error {
	if k == nil {
		return fmt.Errorf("kernel run application: kernel is not initialized")
	}
	if application == nil {
		return fmt.Errorf("kernel run application: application is not initialized")
	}
	return application.RunContext(k.RunContext, contexts...)
}
