package kernel

import (
	"errors"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

// NewApplicationKernel 创建已装配应用命令与调度任务的 Kernel。
//
// 设计说明：
// 1. 内置命令由 prismgo/kernel 自动注册；
// 2. 业务命令与调度来自 bootstrap 阶段注册到 prismgo/kernel 的应用注册表；
// 3. main.go 只需创建 Application 并运行该 Kernel，不再依赖 app/console 桥接层。
func NewApplicationKernel(name string, application ApplicationRegistrySource, dependencies ...BuiltinDependencies) *Kernel {
	if name == "" {
		name = "app"
	}
	deps := BuiltinDependencies{}
	if len(dependencies) > 0 {
		deps = dependencies[0]
	}
	if deps.Application == nil {
		deps.Application = application
	}
	k := New(name, WithBuiltins(deps), WithApplicationRegistry(application))
	// 需求背景：应用 Kernel 创建完成后，provider/service/job 才能通过 artisan facade
	// 解析到同一个 Kernel；绑定失败说明 Application 启动顺序错误，应在构造阶段直接暴露。
	var registry containercontract.Container
	if source, ok := application.(applicationContainerSource); ok {
		registry = source.Container()
	}
	if err := BindApplicationKernel(registry, k); err != nil {
		if errors.Is(err, container.ErrNoCurrentContainer) {
			panic("Artisan Kernel binding missing current Application container: " + err.Error())
		}
		panic("Artisan Kernel binding failed: " + err.Error())
	}
	return k
}
