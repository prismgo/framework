package cmd

import (
	"strings"

	"github.com/prismgo/framework/console"
)

// NewContinueSupervisorCommand 创建 horizon:continue-supervisor 命令。
func NewContinueSupervisorCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:continue-supervisor {name}", "Continue one Horizon supervisor", load, runContinueSupervisorCommand)
}

// runContinueSupervisorCommand 清除指定 supervisor 的暂停标记。
func runContinueSupervisorCommand(ctx console.CommandContext, runtime Runtime) error {
	name := strings.TrimSpace(ctx.Input().Argument("name"))
	if err := runtime.SetSupervisorPaused(ctx.Context(), name, false); err != nil {
		return err
	}
	ctx.IO().Success("Supervisor continued: " + name)
	return nil
}
