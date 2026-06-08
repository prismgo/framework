package cmd

import "github.com/prismgo/framework/console"

// NewPauseCommand 创建 horizon:pause 命令。
func NewPauseCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:pause", "Pause Horizon globally", load, runPauseCommand)
}

// runPauseCommand 写入全局暂停标记。
func runPauseCommand(ctx console.CommandContext, runtime Runtime) error {
	if err := runtime.SetGlobalPaused(ctx.Context(), true); err != nil {
		return err
	}
	ctx.IO().Success("Horizon paused.")
	return nil
}
