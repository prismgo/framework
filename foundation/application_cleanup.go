package foundation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prismgo/framework/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	providerpkg "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/internal/runtimex"
)

// DefaultCloseTimeout 是 Close 兼容入口使用的默认资源释放上限。
const DefaultCloseTimeout = 15 * time.Second

// DefaultCloseReportTimeout 是关闭期错误上报的默认上限。
const DefaultCloseReportTimeout = 3 * time.Second

// RegisterCleanup 注册应用退出时执行的清理函数。
//
// 用途：供 provider 或运行时组件把自身资源释放逻辑挂入 Application 关闭链路。
// 设计原因：资源通常按初始化顺序建立依赖，关闭时需要反向释放，因此后注册的清理函数会先执行。
func (a *Application) RegisterCleanup(cleanup func(*Application) error) {
	if cleanup == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closing {
		return
	}
	a.cleanups = append(a.cleanups, cleanup)
}

// Close 使用独立的超时 context 关闭应用。
//
// 用途：保留原有调用方式，适配 main.go 中 defer cleanup() 以及历史测试代码。
// 设计原因：新增生命周期 context 后不能破坏既有 API；同时资源释放阶段不能复用已经被
// Shutdown 取消的应用根 context，因此 Close 使用独立的默认超时 context 委托给 CloseContext。
//
// 未传 timeout 或传入非正数时使用 DefaultCloseTimeout。
func (a *Application) Close(timeout ...time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout(timeout...))
	defer cancel()
	return a.CloseContext(ctx)
}

func closeTimeout(timeout ...time.Duration) time.Duration {
	if len(timeout) == 0 || timeout[0] <= 0 {
		return DefaultCloseTimeout
	}
	return timeout[0]
}

// CloseContext 取消应用生命周期并释放全部注册资源。
//
// 用途：允许调用方为资源释放阶段提供独立 context，例如带超时的关闭流程。
// 设计原因：应用根 context 在关闭开始时已经被取消，清理阶段仍需要一个未取消的 context
// 完成事件派发、container 资源释放和其他 cleanup 逻辑。
//
// 重试语义：首次关闭会执行 Shutdown、AppTerminating、provider Terminate 和 cleanup；
// 如果 container 资源关闭失败，后续 CloseContext 只重试仍 registered 的 container resources，
// 直到成功后才派发 AppTerminated。
func (a *Application) CloseContext(ctx context.Context) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if a == nil {
		return nil
	}

	reentrant, err := a.acquireCloseSlot(ctx)
	if err != nil {
		return err
	}
	if reentrant {
		return nil
	}
	return a.runCloseAttempt(ctx)
}

// acquireCloseSlot 获取关闭执行权限，处理并发协调。
//
// 返回值：
//   - (false, nil): 成功获取权限，可以继续执行关闭
//   - (true, nil): owner 在生命周期回调中重入，应直接返回 nil
//   - (false, error): 无法执行关闭（正在启动或 context 取消）
func (a *Application) acquireCloseSlot(ctx context.Context) (reentrant bool, err error) {
	gid := runtimex.GoroutineID()

	for {
		a.mu.Lock()

		if a.terminated {
			a.mu.Unlock()
			return false, nil
		}

		if a.booting {
			a.mu.Unlock()
			return false, fmt.Errorf("application boot is in progress")
		}

		if !a.closeActive {
			a.mu.Unlock()
			return false, nil
		}

		// closeActive 为 true，检查是否为 owner 重入
		if a.closeOwner == gid {
			a.mu.Unlock()
			return true, nil
		}

		// 非 owner 等待当前关闭完成
		done := a.closeDone
		a.mu.Unlock()

		// 等待 closeDone 或 context 取消
		select {
		case <-done:
			// 关闭完成，继续循环重试获取权限
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

// runCloseAttempt 执行单次关闭尝试，协调并发并管理状态。
func (a *Application) runCloseAttempt(ctx context.Context) (err error) {
	gid := runtimex.GoroutineID()

	// closing 一旦置位，说明关闭副作用已经执行过，后续调用只能推进 container remaining resources。
	firstAttempt := !a.closing

	a.mu.Lock()
	if firstAttempt {
		a.closing = true
	}
	a.closeActive = true
	a.closeOwner = gid
	a.closeDone = make(chan struct{})
	done := a.closeDone
	firstErr := a.closeErr
	cleanups := append([]func(*Application) error(nil), a.cleanups...)
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.closeResult = err
		a.closeActive = false
		a.closeOwner = 0
		close(done)
		a.mu.Unlock()
	}()

	eventCtx := context.WithoutCancel(ctx)
	closeCtx := ctx
	if ctx.Err() != nil {
		// Application CloseContext 历史上允许传入已取消 context；资源关闭仍使用未取消的事件 context 推进。
		closeCtx = eventCtx
	}

	if firstAttempt {
		firstErr = a.runFirstCloseAttempt(eventCtx, closeCtx, cleanups)
	}

	a.mu.Lock()
	bus := a.closeBus
	a.mu.Unlock()

	return a.drainContainerResources(ctx, eventCtx, closeCtx, firstAttempt, firstErr, bus)
}

// runFirstCloseAttempt 执行首次关闭的完整流程：Shutdown、事件派发、provider 终止、cleanup。
func (a *Application) runFirstCloseAttempt(eventCtx, closeCtx context.Context, cleanups []func(*Application) error) error {
	a.Shutdown(ErrApplicationShutdown)

	bus, _ := resolveEventDispatcher(a.container)
	a.mu.Lock()
	a.closeBus = bus
	a.mu.Unlock()

	if bus != nil {
		bus.Dispatch(eventCtx, event.AppTerminating{Reason: a.shutdownReason()})
	}

	firstErr := a.closeErr
	firstErr = errors.Join(firstErr, a.terminateProviders(closeCtx))
	for i := len(cleanups) - 1; i >= 0; i-- {
		if err := cleanups[i](a); err != nil {
			firstErr = errors.Join(firstErr, err)
		}
	}
	a.mu.Lock()
	a.closeErr = firstErr
	a.mu.Unlock()

	return firstErr
}

// drainContainerResources 关闭容器资源并派发终止事件。
func (a *Application) drainContainerResources(ctx, eventCtx, closeCtx context.Context, firstAttempt bool, firstErr error, bus eventcontract.Dispatcher) error {
	closeStartedAt := time.Now()

	if closeCtx.Err() != nil {
		closeCtx = eventCtx
	}

	var normalContainerErr error
	if a.container != nil {
		// Container.CloseGroup 自身会保留失败或未执行资源；这里先关闭普通资源，
		// 再利用仍存活的 reporting 资源上报普通关闭错误。
		normalContainerErr = a.container.CloseGroup(closeCtx, container.CloseGroupNormal)
	}

	var reportErr error
	ordinaryErr := errors.Join(firstErr, normalContainerErr)
	if firstAttempt && ordinaryErr != nil {
		reportErr = reportCloseError(ctx, ordinaryErr)
	}

	var reportingContainerErr error
	if a.container != nil {
		reportingContainerErr = a.container.CloseGroup(closeCtx, container.CloseGroupReporting)
	}

	if normalContainerErr != nil || reportErr != nil || reportingContainerErr != nil {
		return errors.Join(firstErr, normalContainerErr, reportErr, reportingContainerErr)
	}

	a.mu.Lock()
	if a.terminated {
		a.mu.Unlock()
		return firstErr
	}
	a.terminated = true
	startedAt := a.startedAt
	a.mu.Unlock()

	if bus != nil {
		bus.Dispatch(eventCtx, event.AppTerminated{
			Duration:      time.Since(startedAt),
			CloseDuration: time.Since(closeStartedAt),
			Error:         errorString(firstErr),
		})
	}
	if App == a {
		container.SetProvider(nil)
	}
	return firstErr
}

// reportCloseError 在 reporting container 资源释放前上报普通关闭错误。
//
// 设计原因：关闭期错误只能上报一次，且必须发生在 logger、exception handler 以及
// 业务 reporter 依赖的 reporting client 仍然存活时；reporter 自身 panic 只进入返回值，
// 不再递归触发异常上报。
func reportCloseError(ctx context.Context, err error) (reportErr error) {
	if err == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultCloseReportTimeout)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			reportErr = fmt.Errorf("report close error: %v", recovered)
		}
	}()
	exception.Report(reportCtx, err, map[string]any{
		"status":  500,
		"phase":   "application.close",
		"message": "application close failed",
	})
	return nil
}

// terminateProviders 执行已注册 provider 的关闭钩子。
//
// 顺序说明：provider repository 按依赖建立顺序保存 provider，关闭时反序执行，保证后注册的
// provider 可以先释放自身资源；该阶段位于 RegisterCleanup 之前，因此 provider 仍可访问
// Application 和 container state。
func (a *Application) terminateProviders(ctx context.Context) error {
	var errs []error
	entries := a.registeredProviderSnapshot()
	for i := len(entries) - 1; i >= 0; i-- {
		terminable, ok := entries[i].provider.(providerpkg.TerminableProvider)
		if !ok {
			continue
		}
		if err := terminable.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("provider %s terminate: %w", entries[i].identity, err))
		}
	}
	return errors.Join(errs...)
}

// registeredProviderSnapshot 返回已完成 Register 阶段的 provider 快照。
//
// 设计原因：Terminate 不能持有 Application 锁执行用户代码；这里先复制已注册 provider，
// 再由调用方在锁外执行关闭钩子。未加载 deferred provider 不会进入该快照。
func (a *Application) registeredProviderSnapshot() []providerEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.initProviderRepositoryLocked()

	out := make([]providerEntry, 0, len(a.providers))
	for _, entry := range a.providers {
		if entry.identity == "" || !a.registeredProviders[entry.identity] {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
