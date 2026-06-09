package cmd

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/timer"
)

type cronKernel interface {
	Schedule() *timer.Schedule
	Start(context.Context)
	Stop()
}

// CronCommand 启动定时任务调度器。
//
// 用途：在命令启动时注册业务定时任务，并阻塞运行直到收到应用关闭信号或系统退出信号。
// 设计原因：调度器属于长生命周期进程组件，需要和 Application 根 context 使用同一条取消链路。
type CronCommand struct {
	Kernel   cronKernel
	Register func(*timer.Schedule)
}

// NewCronCommand 创建定时任务命令。
//
// 用途：由业务装配层注入通用 Kernel 和定时任务注册回调。
// 设计原因：prismgo/cmd 只负责通用调度生命周期，不直接依赖业务任务列表。
func NewCronCommand(k cronKernel, register func(*timer.Schedule)) *CronCommand {
	return &CronCommand{Kernel: k, Register: register}
}

// Definition 返回定时任务命令定义。
func (c *CronCommand) Definition() *console.Definition {
	return console.MustDefinition("cron", "Start the cron scheduler")
}

// Run 注册并启动定时任务调度器。
//
// 用途：将 cobra 命令 context 作为父 context，同时监听 SIGINT/SIGTERM 以兼容独立使用。
// 设计原因：当 Application 触发 Shutdown 时，cmd.Context() 会被取消；当命令独立运行时，
// 系统信号也能触发同样的调度停止流程。
func (c *CronCommand) Handle(commandCtx console.CommandContext) error {
	if c.Register != nil {
		c.Register(c.Kernel.Schedule())
	}

	parent := commandCtx.Context()
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c.Kernel.Start(ctx)
	commandCtx.IO().Success("cron scheduler started")
	commandCtx.IO().Success(c.Kernel.Schedule().Summary())

	<-ctx.Done()
	c.Kernel.Stop()
	commandCtx.IO().Success("cron scheduler stopped")
	return nil
}
