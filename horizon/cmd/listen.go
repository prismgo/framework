package cmd

import (
	"strconv"
	"time"

	"github.com/prismgo/framework/console"
)

// NewListenCommand 创建本地开发用的 horizon:listen 辅助命令。
//
// 需求背景：Laravel Horizon 提供 listen 命令用于本地开发阶段监听文件变化并重启 Horizon；
// Prismgo 当前切片先提供命令面和参数解析，并明确标记为非生产用途，避免用户把它当作
// 生产进程管理器使用。
func NewListenCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:listen {--environment=} {--poll=1000}", "Run local Horizon watcher helper", load, runListenCommand)
}

// runListenCommand 启动本地监听流程，并把真实 watcher/restart 交给 runtime。
//
// 参数说明：ctx 提供命令输入输出；runtime 由父包注入，负责读取 horizon.watch、启动 horizon 子进程并处理重启。
func runListenCommand(ctx console.CommandContext, runtime Runtime) error {
	ctx.IO().Warn("horizon:listen is intended for local development and is not recommended for production.")
	if runtime == nil {
		return ErrRuntimeNotConfigured
	}
	environment := ctx.Input().Option("environment")
	if environment == "" {
		environment = "local"
	}
	poll := ctx.Input().OptionInt("poll")
	if poll <= 0 {
		poll = 1000
	}
	ctx.IO().Info("Environment: " + environment)
	ctx.IO().Info("Poll Interval: " + (time.Duration(poll) * time.Millisecond).String())
	summary, err := runtime.Listen(ctx.Context(), ListenOptions{Environment: environment, Poll: time.Duration(poll) * time.Millisecond})
	if err != nil {
		return err
	}
	ctx.IO().Info("Watch Paths: " + strconv.Itoa(summary.WatchPathCount))
	ctx.IO().Info("Starts: " + strconv.Itoa(summary.Starts))
	ctx.IO().Info("Restarts: " + strconv.Itoa(summary.Restarts))
	return nil
}
