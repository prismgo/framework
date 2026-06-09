package queue

import "github.com/prismgo/framework/console"

// FlushCommand 清空全部失败队列任务。
type FlushCommand struct{}

// NewFlushCommand 构造 queue:flush 命令。
func NewFlushCommand() *FlushCommand { return &FlushCommand{} }

// Definition 返回失败任务清空命令定义。
func (c *FlushCommand) Definition() *console.Definition {
	return console.MustDefinition("queue:flush", "Delete all failed queue jobs")
}

// Run 清空当前失败任务存储。
func (c *FlushCommand) Handle(ctx console.CommandContext) error {
	manager, err := resolveManager()
	if err != nil {
		return err
	}
	if err := manager.Failed().Flush(ctx.Context()); err != nil {
		return err
	}
	ctx.IO().Success("flushed failed jobs")
	return nil
}
