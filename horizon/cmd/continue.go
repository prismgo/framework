package cmd

import "github.com/prismgo/framework/console"

// NewContinueCommand 创建 horizon:continue 命令。
func NewContinueCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:continue", "Continue Horizon globally", load, runContinueCommand)
}

// runContinueCommand 清除全局暂停标记；它不会清理 terminate 请求。
func runContinueCommand(ctx console.CommandContext, runtime Runtime) error {
	if err := runtime.SetGlobalPaused(ctx.Context(), false); err != nil {
		return err
	}
	ctx.IO().Success("Horizon continued.")
	return nil
}
