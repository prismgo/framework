package queue

import (
	"context"

	"github.com/prismgo/framework/console"
	queuecore "github.com/prismgo/framework/queue"
)

// RetryCommand 把失败任务重新投递回原队列。
type RetryCommand struct{}

// NewRetryCommand 构造 queue:retry 命令。
func NewRetryCommand() *RetryCommand { return &RetryCommand{} }

// Definition 返回失败任务重试命令定义。
func (c *RetryCommand) Definition() *console.Definition {
	return console.MustDefinition("queue:retry {ids*}", "Retry failed queue jobs")
}

// Run 按传入的失败任务 ID 逐个重新投递。
func (c *RetryCommand) Handle(ctx console.CommandContext) error {
	manager := resolveManager()
	for _, id := range ctx.Input().Arguments("ids") {
		if err := retryFailed(ctx.Context(), manager, id); err != nil {
			return err
		}
		ctx.IO().Success("retried failed job: " + id)
	}
	return nil
}

// retryFailed 通过 queue runtime 的 failed-job retry flow 重新入队。
func retryFailed(ctx context.Context, manager *queuecore.Manager, id string) error {
	return queuecore.NewDispatcher(manager).RetryFailed(ctx, id)
}
