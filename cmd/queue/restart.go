package queue

import "github.com/prismgo/framework/console"

// RestartCommand 写入队列 worker 重启信号。
type RestartCommand struct{}

// NewRestartCommand 构造 queue:restart 命令。
func NewRestartCommand() *RestartCommand { return &RestartCommand{} }

// Definition 返回队列重启信号命令定义。
func (c *RestartCommand) Definition() *console.Definition {
	return console.MustDefinition("queue:restart", "Signal queue workers to restart")
}

// Run 记录重启时间戳；worker 会在当前任务结束后的下一轮检查时退出。
func (c *RestartCommand) Handle(ctx console.CommandContext) error {
	manager := resolveManager()
	if err := manager.RequestRestart(ctx.Context()); err != nil {
		return err
	}
	ctx.IO().Success("queue restart signal sent")
	return nil
}
