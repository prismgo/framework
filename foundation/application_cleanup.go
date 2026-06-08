package foundation

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/prismgo/framework/container"
	providerpkg "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/exception"
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

	gid := currentGoroutineID()
	a.mu.Lock()
	if a.terminated {
		a.mu.Unlock()
		return nil
	}
	if a.booting {
		a.mu.Unlock()
		return fmt.Errorf("application boot is in progress")
	}
	if a.closeActive {
		if a.closeOwner == gid {
			err := a.closeErr
			a.mu.Unlock()
			return err
		}
		done := a.closeDone
		a.mu.Unlock()
		if done != nil {
			<-done
		}
		a.mu.Lock()
		err := a.closeResult
		terminated := a.terminated
		a.mu.Unlock()
		if terminated {
			return err
		}
		return a.CloseContext(ctx)
	}
	// closing 一旦置位，说明关闭副作用已经执行过，后续调用只能推进 container remaining resources。
	firstAttempt := !a.closing
	if firstAttempt {
		a.closing = true
	}
	a.closeActive = true
	a.closeOwner = gid
	a.closeDone = make(chan struct{})
	done := a.closeDone
	firstErr := a.closeErr
	bus := a.closeBus
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
	start := time.Now()
	if firstAttempt {
		a.Shutdown(ErrApplicationShutdown)
		bus, _ = resolveEventDispatcher(a.container)
		a.mu.Lock()
		a.closeBus = bus
		a.mu.Unlock()
		if bus != nil {
			bus.Dispatch(eventCtx, event.AppTerminating{Reason: a.shutdownReason()})
		}

		firstErr = errors.Join(firstErr, a.terminateProviders(closeCtx))
		for i := len(cleanups) - 1; i >= 0; i-- {
			if err := cleanups[i](a); err != nil {
				firstErr = errors.Join(firstErr, err)
			}
		}
		a.mu.Lock()
		a.closeErr = firstErr
		a.mu.Unlock()
	}
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
	a.mu.Unlock()
	if bus != nil {
		bus.Dispatch(eventCtx, event.AppTerminated{
			Duration: time.Since(start),
			Error:    errorString(firstErr),
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
	for _, provider := range a.providers {
		identity := providerIdentity(provider)
		if identity == "" || !a.registeredProviders[identity] {
			continue
		}
		out = append(out, providerEntry{identity: identity, provider: provider})
	}
	return out
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// currentGoroutineID 返回当前 goroutine 的运行期编号，仅用于区分 CloseContext 的内部重入与外部并发调用。
//
// 需求背景：AppTerminating、AppTerminated 和 cleanup 回调允许在同一调用栈内重入 CloseContext；
// 如果所有 closeActive 调用都等待 closeDone，内部重入会等待自己完成而死锁。Go 标准库没有公开
// goroutine id，这里从 runtime.Stack 头部解析，作用范围限定在 foundation 关闭期并发协调。
func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	header := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	idText := header
	if index := strings.IndexByte(header, ' '); index >= 0 {
		idText = header[:index]
	}
	id, err := strconv.ParseUint(idText, 10, 64)
	if err != nil {
		return 0
	}
	return id
}
