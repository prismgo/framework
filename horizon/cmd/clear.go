package cmd

import (
	"fmt"

	"github.com/prismgo/framework/console"
)

// NewClearCommand 创建 horizon:clear 命令。
func NewClearCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:clear {connection?} {--queue=} {--force}", "Clear a monitored queue", load, runClearCommand)
}

// runClearCommand 清空一个由当前环境 supervisor 配置明确覆盖的队列。
//
// 逻辑说明：参数不足时只在候选目标唯一时自动选择；否则返回带候选列表的错误，避免误清不属于 Horizon 管理范围的队列。
func runClearCommand(ctx console.CommandContext, runtime Runtime) error {
	if !boolCommandOption(ctx, "force") {
		return fmt.Errorf("horizon: clear requires --force")
	}
	target, err := resolveQueueTarget(runtime.QueueTargets(), ctx.Input().Argument("connection"), ctx.Input().Option("queue"))
	if err != nil {
		return err
	}
	if err := runtime.ClearQueue(ctx.Context(), target); err != nil {
		return err
	}
	ctx.IO().Success("Queue cleared: " + target.Connection + ":" + target.Queue)
	return nil
}
