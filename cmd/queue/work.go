package queue

import (
	"github.com/prismgo/framework/console"
	queuecore "github.com/prismgo/framework/queue"
)

// WorkCommand 启动队列 worker，负责把 console 参数映射成 queue.WorkerOptions。
type WorkCommand struct{}

// NewWorkCommand 构造 queue:work 命令；同时保留 queue 作为兼容入口。
func NewWorkCommand() *WorkCommand { return &WorkCommand{} }

// Definition 返回 queue worker 的命令签名和运行参数。
func (c *WorkCommand) Definition() *console.Definition {
	def := console.MustDefinition("queue {connection?} {--queue=default} {--once} {--stop-when-empty} {--sleep=3} {--timeout=60} {--tries=1} {--backoff=0} {--max-jobs=0} {--max-time=0} {--retry-after=90}", "启动队列消费者")
	def.Aliases = []string{"queue:work"}
	return def
}

// Run 解析 worker 选项并开始消费队列。
func (c *WorkCommand) Handle(ctx console.CommandContext) error {
	manager, err := resolveManager()
	if err != nil {
		return err
	}
	options := queuecore.WorkerOptions{
		Connection:    ctx.Input().Argument("connection"),
		Queues:        splitQueueNames(ctx.Input().Option("queue")),
		Once:          ctx.Input().OptionBool("once"),
		StopWhenEmpty: ctx.Input().OptionBool("stop-when-empty"),
		Sleep:         secondsOption(ctx, "sleep", 3),
		Timeout:       secondsOption(ctx, "timeout", 60),
		Tries:         intOption(ctx, "tries", 1),
		Backoff:       parseBackoff(ctx.Input().Option("backoff")),
		MaxJobs:       intOption(ctx, "max-jobs", 0),
		MaxTime:       secondsOption(ctx, "max-time", 0),
		RetryAfter:    secondsOption(ctx, "retry-after", 90),
	}
	ctx.IO().Success("queue worker started")
	return queuecore.NewWorker(manager).Work(ctx.Context(), options)
}
