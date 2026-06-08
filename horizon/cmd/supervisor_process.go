package cmd

import "github.com/prismgo/framework/console"

// NewSupervisorProcessCommand 创建 horizon:supervisor 进程入口。
//
// 需求背景：issue 08 要求公开签名保持 Laravel 对齐的 `{name} {connection}`，并暴露当前
// Prismgo supervisor 配置可表达的运行选项；`environment/master-id` 仅保留给内部 bootstrap 兼容。
func NewSupervisorProcessCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:supervisor {name} {connection} {--balance=} {--backoff=} {--max-jobs=0} {--max-time=0} {--force} {--max-processes=0} {--min-processes=0} {--memory=0} {--nice=0} {--paused} {--queue=default} {--sleep=3} {--timeout=60} {--tries=1} {--workers-name=} {--parent-id=} {--environment=} {--master-id=}", "Run one Horizon supervisor process", load, runSupervisorProcessCommand)
}

// runSupervisorProcessCommand 绑定 supervisor 进程参数并交给 Runtime 执行。
//
// 参数说明：name/connection 来自公开位置参数；parent-id 是 Laravel 对齐名称，master-id 是旧内部名称，
// 二者都映射到 MasterID，避免内部 bootstrap 参数替代公开签名。
func runSupervisorProcessCommand(ctx console.CommandContext, runtime Runtime) error {
	return runtime.RunSupervisor(ctx.Context(), SupervisorProcessOptions{
		Name:         ctx.Input().Argument("name"),
		Connection:   ctx.Input().Argument("connection"),
		Environment:  ctx.Input().Option("environment"),
		MasterID:     firstCommandOption(ctx, "master-id", "parent-id"),
		ParentID:     ctx.Input().Option("parent-id"),
		Balance:      ctx.Input().Option("balance"),
		Backoff:      ctx.Input().Option("backoff"),
		MaxJobs:      intCommandOption(ctx, "max-jobs", 0),
		MaxTime:      intCommandOption(ctx, "max-time", 0),
		Force:        boolCommandOption(ctx, "force"),
		MaxProcesses: intCommandOption(ctx, "max-processes", 0),
		MinProcesses: intCommandOption(ctx, "min-processes", 0),
		Memory:       intCommandOption(ctx, "memory", 0),
		Nice:         intCommandOption(ctx, "nice", 0),
		Paused:       boolCommandOption(ctx, "paused"),
		Queue:        ctx.Input().Option("queue"),
		Sleep:        intCommandOption(ctx, "sleep", 3),
		Timeout:      intCommandOption(ctx, "timeout", 60),
		Tries:        intCommandOption(ctx, "tries", 1),
		WorkersName:  ctx.Input().Option("workers-name"),
	})
}
