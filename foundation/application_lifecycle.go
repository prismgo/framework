package foundation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/prismgo/framework/routine"
)

// ErrApplicationShutdown 是应用生命周期默认关闭原因。
//
// 用途：当调用方没有提供明确关闭原因时，作为根 context 的取消原因。
// 设计原因：统一关闭原因便于测试、日志和监听器识别应用是正常进入退出流程，
// 而不是因为某个请求或任务自己的局部 context 被取消。
var ErrApplicationShutdown = errors.New("application shutdown")

// Context 返回当前应用实例的根生命周期 context。
//
// 用途：为 HTTP 服务、定时任务、后台 goroutine 等进程级组件提供统一退出信号。
// 设计原因：Application 是当前进程内 provider、cleanup 和核心资源的统一门面，
// 由它持有根 context 可以避免各命令入口重复维护取消信号。
//
// 注意：该 context 只表达应用生命周期，不用于存储用户、租户、数据库等业务值；
// 请求和任务应基于它派生自己的局部 context。
func (a *Application) Context() context.Context {
	if a == nil || a.ctx == nil {
		return context.Background()
	}
	return a.ctx
}

// Shutdown 取消应用根生命周期 context。
//
// 用途：让 HTTP 服务、定时任务和后台任务感知应用即将退出，并主动进入优雅停止流程。
// 设计原因：关闭意图应从 Application 统一广播，而不是散落在 serve、cron 等命令内部。
//
// 传入 nil 时会使用 ErrApplicationShutdown；多次调用是幂等的，首次取消原因会保留。
func (a *Application) Shutdown(reason error) {
	if a == nil || a.cancel == nil {
		return
	}
	if reason == nil {
		reason = ErrApplicationShutdown
	}
	a.cancel(reason)
}

// RegisterShutdownSignals 监听进程退出信号并触发应用关闭。
//
// 用途：把 SIGINT/SIGTERM 统一转换为应用根 context 的取消原因，供 HTTP 服务、
// 定时任务和后台任务共享同一条生命周期链路。
// 设计原因：系统信号属于通用进程生命周期能力，应由 foundation 统一承接，避免 main.go
// 或各命令入口重复维护各自的 signal 监听逻辑。
// 幂等性：多次调用只会注册一次信号监听，避免重复创建 goroutine 和信号通道。
func (a *Application) RegisterShutdownSignals() {
	if a == nil {
		return
	}

	a.mu.Lock()
	if a.signalsRegistered {
		a.mu.Unlock()
		return
	}
	a.signalsRegistered = true
	a.mu.Unlock()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	routine.Task(a.Context(), func(context.Context) error {
		select {
		case sig := <-sigCh:
			a.Shutdown(fmt.Errorf("received signal %s", sig))
		case <-a.Context().Done():
		}
		signal.Stop(sigCh)
		return nil
	}).
		Component("foundation").
		Name("shutdown.signals").
		Go()
}

func (a *Application) shutdownReason() string {
	if a == nil {
		return ErrApplicationShutdown.Error()
	}
	if cause := context.Cause(a.Context()); cause != nil {
		return cause.Error()
	}
	return ErrApplicationShutdown.Error()
}
