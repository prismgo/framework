package cmd

import "github.com/prismgo/framework/console"

// NewWorkCommand 创建 horizon:work 进程入口。
//
// 需求背景：该命令是 supervisor 派生 worker 的公开入口，签名使用 Laravel 对齐的 `{connection?}`
// argument，并保留 Prismgo 当前 worker loop 能表达的安全选项绑定。
func NewWorkCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:work {connection?} {--name=} {--backoff=0} {--retry-after=90} {--max-jobs=0} {--max-time=0} {--force} {--memory=0} {--once} {--stop-when-empty} {--queue=default} {--sleep=3} {--rest=0} {--supervisor=} {--timeout=60} {--tries=1} {--json} {--environment=} {--prefix=}", "Run one Horizon worker process", load, runWorkCommand)
}

// runWorkCommand 绑定 worker 运行参数并交给 Runtime 执行。
//
// 参数说明：connection 来自位置参数；queue 允许逗号分隔队列；once/json/rest 等选项先完整绑定，
// Runtime 可按当前能力使用或忽略，但命令层不得静默丢失用户输入。
func runWorkCommand(ctx console.CommandContext, runtime Runtime) error {
	// 逻辑说明：retry-after 使用 Horizon 配置中的秒级语义，命令层只负责解析和转交。
	// 设计思路：所有 supervisor 派生 worker 的消费边界都必须通过 horizon:work 参数显式传递，
	// 避免 runtime adapter 使用默认值导致 UI 配置、supervisor 配置和实际 queue worker 行为不一致。
	return runtime.RunWorker(ctx.Context(), WorkerOptions{
		Name:          ctx.Input().Option("name"),
		Supervisor:    ctx.Input().Option("supervisor"),
		Environment:   ctx.Input().Option("environment"),
		Prefix:        ctx.Input().Option("prefix"),
		Connection:    ctx.Input().Argument("connection"),
		Queue:         ctx.Input().Option("queue"),
		Sleep:         intCommandOption(ctx, "sleep", 3),
		Rest:          intCommandOption(ctx, "rest", 0),
		Timeout:       intCommandOption(ctx, "timeout", 60),
		Tries:         intCommandOption(ctx, "tries", 1),
		Backoff:       ctx.Input().Option("backoff"),
		RetryAfter:    intCommandOption(ctx, "retry-after", 90),
		MaxJobs:       intCommandOption(ctx, "max-jobs", 0),
		MaxTime:       intCommandOption(ctx, "max-time", 0),
		Force:         boolCommandOption(ctx, "force"),
		Memory:        intCommandOption(ctx, "memory", 0),
		Once:          boolCommandOption(ctx, "once"),
		JSON:          boolCommandOption(ctx, "json"),
		StopWhenEmpty: boolCommandOption(ctx, "stop-when-empty"),
	})
}
