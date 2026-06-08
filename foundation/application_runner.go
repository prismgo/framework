package foundation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/prismgo/framework/kernel"
	"github.com/prismgo/framework/version"
)

// Runner 描述 Application 生命周期内执行的运行函数。
//
// 用途：由 HTTP、CLI、队列、定时任务等入口把自己的阻塞运行逻辑交给 Application 托管。
// 参数说明：ctx 是已经合并应用生命周期信号后的运行 context，调用方应把它继续传给下游组件。
type Runner func(ctx context.Context) error

// NewConsoleKernel creates the console kernel for this Application runtime.
func (a *Application) NewConsoleKernel() *kernel.Kernel {
	if a == nil {
		return nil
	}
	return kernel.NewApplicationKernel(version.Name, a.runtime)
}

// NewHTTPServer builds an HTTP server from this Application runtime.
func (a *Application) NewHTTPServer(ctx context.Context, port string) (*http.Server, error) {
	if a == nil || a.runtime == nil {
		return nil, fmt.Errorf("foundation application: application is not initialized")
	}
	return a.runtime.NewHTTPServer(ctx, port)
}

// HandleCommand runs argv through an Application-owned Console Kernel.
func (a *Application) HandleCommand(ctx context.Context, argv []string) error {
	if a == nil {
		return fmt.Errorf("foundation application: application is not initialized")
	}
	if len(argv) == 0 {
		argv = os.Args
	}
	k := a.NewConsoleKernel()
	if k == nil {
		return fmt.Errorf("foundation application: console kernel is not initialized")
	}
	return a.RunContext(func(runCtx context.Context) error {
		return k.RunContextArgv(runCtx, argv)
	}, ctx)
}

func (a *Application) registerConsoleStarting(callbacks ...kernel.StartingCallback) error {
	if a == nil || a.runtime == nil {
		return fmt.Errorf("foundation application: application is not initialized")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runtime.RegisterStarting(callbacks...)
	return nil
}

// RunContext 启动应用生命周期并运行指定入口函数。
//
// 用途：统一封装 Boot、退出信号监听、应用根 context 传递以及 Close 资源释放，让 main.go
// 和各类入口 Kernel 不需要重复维护生命周期模板代码。
// 设计说明：Application 是 provider、cleanup 和根生命周期 context 的归属者，因此通用生命周期
// 运行逻辑放在 foundation 层；具体运行内容通过 Runner 注入，避免 foundation 反向依赖 HTTP、
// CLI 或业务包。
// 参数说明：不传 context 时使用 app.Context()；传入 context 时会与 app.Context() 合并，
// 任意一侧取消都会让 Runner 感知退出信号。
func (a *Application) RunContext(run func(context.Context) error, contexts ...context.Context) error {
	if a == nil {
		return fmt.Errorf("foundation application: application is not initialized")
	}
	if run == nil {
		return fmt.Errorf("foundation application: runner is not configured")
	}

	if err := a.Boot(); err != nil {
		return errors.Join(err, a.Close())
	}
	a.RegisterShutdownSignals()

	ctx := a.Context()
	release := func() {}
	if len(contexts) > 0 && contexts[0] != nil {
		ctx, release = mergeRunContext(a.Context(), contexts[0])
	}

	err := run(ctx)
	release()
	if isRunContextCancellation(ctx, err) {
		err = nil
	}
	closeErr := a.Close()
	if err != nil {
		return errors.Join(err, closeErr)
	}
	return closeErr
}

func isRunContextCancellation(ctx context.Context, err error) bool {
	return err != nil && ctx != nil && ctx.Err() != nil && errors.Is(err, context.Canceled)
}

// mergeRunContext 合并应用生命周期 context 与外部运行 context。
//
// 用途：支持外部调用方传入自己的运行边界，同时保证 Application Shutdown 仍能中断运行函数。
// 设计说明：外部 ctx 表示调用方的运行周期，appCtx 表示应用进程生命周期；两者任意一个结束，
// 合并后的 context 都应该结束，长运行任务才能可靠响应退出信号。
// 返回值说明：第一个返回值是传给 Runner 的合并 context；第二个返回值用于在 Runner 结束后
// 释放 appCtx 到合并 context 的桥接，避免 Close 阶段继续回头取消已经结束用途的运行 context。
func mergeRunContext(appCtx context.Context, runCtx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(runCtx)
	stop := context.AfterFunc(appCtx, func() {
		cancel(context.Cause(appCtx))
	})
	return ctx, func() {
		stop()
	}
}
