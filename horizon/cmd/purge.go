package cmd

import (
	"strconv"
	"time"

	"github.com/prismgo/framework/console"
)

// NewPurgeCommand 创建 Laravel 对齐的 horizon:purge orphan process 清理命令。
func NewPurgeCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:purge {--signal=SIGTERM}", "Purge orphan Horizon worker processes", load, runPurgeCommand)
}

func runPurgeCommand(ctx console.CommandContext, runtime Runtime) error {
	summary, err := runtime.Purge(ctx.Context(), time.Now(), ctx.Input().Option("signal"))
	if err != nil {
		return err
	}
	ctx.IO().Info("Orphans Discovered: " + strconv.Itoa(summary.OrphansDiscovered))
	ctx.IO().Info("Terminate Requests: " + strconv.Itoa(summary.TerminateRequests))
	ctx.IO().Info("Orphans Forgotten: " + strconv.Itoa(summary.OrphansForgotten))
	return nil
}
