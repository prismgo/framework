package kernel

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prismgo/framework/console"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/timer"
)

// StartingCallback 描述 Console application 初始化时执行的回调。
//
// 用途：对齐 Laravel Artisan::starting，在 Application boot 成功后、CLI 命令解析和执行前，
// 允许 provider、bootstrap 或可选模块把延迟命令注册到当前 Kernel。
// 设计思路：回调只拿到当前 Kernel 视图，命令仍必须通过 ResolveCommand/ResolveCommands
// 进入同一套 Definition 校验和重复注册校验，避免绕过命令模型。
type StartingCallback func(*Kernel) error

// StartingRegistrarKey 是 Application 容器中保存 Console starting registrar 的固定键。
const StartingRegistrarKey = "kernel.starting.registrar"

// StartingRegistrar 把 provider 声明的 Console starting callbacks 写入当前 Application runtime。
type StartingRegistrar func(...StartingCallback) error

// ApplicationRegistrySource 描述 Application 级运行时注册表向 Kernel 暴露的只读能力。
//
// 需求背景：主启动路径需要从当前 Application 读取命令、调度、migration 和 seed 声明，避免多个
// Configure().Create() 之间共享进程级可变状态。
type ApplicationRegistrySource interface {
	CommandFactories() []console.CommandFactory
	StartingCallbacks() []StartingCallback
	ScheduleRegistrars() []func(*timer.Schedule)
	MigrationPaths() []string
	SeedPaths() []string
}

type applicationContainerSource interface {
	Container() containercontract.Container
}

type applicationHTTPServerSource interface {
	NewHTTPServer(context.Context, string) (*http.Server, error)
	LoadHTTPRoutes() error
}

// WithApplicationRegistry 在 Kernel 初始化阶段挂载当前应用已声明的命令与定时任务。
func WithApplicationRegistry(source ApplicationRegistrySource) Option {
	return func(k *Kernel) {
		RegisterApplication(k, source)
	}
}

// RegisterApplication 将当前应用注册表中的命令与定时任务一次性装配到 Kernel。
func RegisterApplication(k *Kernel, source ApplicationRegistrySource) {
	if k == nil {
		panic("kernel register application: kernel is nil")
	}
	k.application = source
	if source == nil {
		return
	}
	k.RegisterLazy(source.CommandFactories()...)
	applySchedules(k.schedule, source.ScheduleRegistrars())
}

// ApplicationNewHTTPServer 通过当前 Application registry source 创建 HTTP Server。
//
// 设计说明：HTTP 声明仍只来自 foundation.WithRouting/WithMiddleware；Kernel 只在框架内部把
// 当前 Application 暴露的 server factory 接给 serve 命令，main.go 不需要传递 HTTP 配置。
func ApplicationNewHTTPServer(source ApplicationRegistrySource, ctx context.Context, port string) (*http.Server, error) {
	httpSource, ok := source.(applicationHTTPServerSource)
	if !ok || httpSource == nil {
		return nil, fmt.Errorf("httpkit: HTTP routes registrar is not configured")
	}
	return httpSource.NewHTTPServer(ctx, port)
}

// ApplicationLoadHTTPRoutes 通过当前 Application registry source 加载 route facade。
func ApplicationLoadHTTPRoutes(source ApplicationRegistrySource) error {
	httpSource, ok := source.(applicationHTTPServerSource)
	if !ok || httpSource == nil {
		return fmt.Errorf("httpkit: HTTP routes registrar is not configured")
	}
	return httpSource.LoadHTTPRoutes()
}

func applySchedules(schedule *timer.Schedule, registrars []func(*timer.Schedule)) {
	if schedule == nil {
		return
	}
	for _, registrar := range registrars {
		if registrar != nil {
			registrar(schedule)
		}
	}
}
