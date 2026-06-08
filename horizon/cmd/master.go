package cmd

import "github.com/prismgo/framework/console"

// NewMasterCommand 创建 horizon master 入口。
//
// 需求背景：该命令以公开 Laravel 对齐签名 `horizon {--environment=}` 运行 master process，
// 负责派生当前环境下的 supervisor 子进程；命令文件独立存放，便于 issue 08 的 CLI 表面清单逐项审计。
func NewMasterCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon {--environment=}", "Run Horizon master process", load, runMasterCommand)
}

// runMasterCommand 把 CLI environment 选项传递给 Runtime。
//
// 参数说明：environment 为空时由 Runtime 根据已加载 Horizon 配置决定实际运行环境。
func runMasterCommand(ctx console.CommandContext, runtime Runtime) error {
	return runtime.RunMaster(ctx.Context(), MasterOptions{Environment: ctx.Input().Option("environment")})
}
