package cmd

import (
	"strings"

	"github.com/prismgo/framework/console"
)

// NewPauseSupervisorCommand 创建 horizon:pause-supervisor 命令。
func NewPauseSupervisorCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:pause-supervisor {name}", "Pause one Horizon supervisor", load, runPauseSupervisorCommand)
}

// runPauseSupervisorCommand 写入指定 supervisor 的暂停标记。
func runPauseSupervisorCommand(ctx console.CommandContext, runtime Runtime) error {
	name := strings.TrimSpace(ctx.Input().Argument("name"))
	if err := runtime.SetSupervisorPaused(ctx.Context(), name, true); err != nil {
		return err
	}
	ctx.IO().Success("Supervisor paused: " + name)
	return nil
}
