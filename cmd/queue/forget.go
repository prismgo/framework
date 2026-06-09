package queue

import "github.com/prismgo/framework/console"

// ForgetCommand 删除指定失败任务。
type ForgetCommand struct{}

// NewForgetCommand 构造 queue:forget 命令。
func NewForgetCommand() *ForgetCommand { return &ForgetCommand{} }

// Definition 返回失败任务删除命令定义。
func (c *ForgetCommand) Definition() *console.Definition {
	return console.MustDefinition("queue:forget {id}", "Delete a failed queue job")
}

// Run 从失败任务存储中删除指定 ID。
func (c *ForgetCommand) Handle(ctx console.CommandContext) error {
	id := ctx.Input().Argument("id")
	manager, err := resolveManager()
	if err != nil {
		return err
	}
	if err := manager.Failed().Forget(ctx.Context(), id); err != nil {
		return err
	}
	ctx.IO().Success("forgot failed job: " + id)
	return nil
}
