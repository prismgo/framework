package cmd

import (
	"fmt"
	"strconv"
	"time"

	"github.com/prismgo/framework/console"
)

// NewStatusCommand 创建 horizon:status 命令。
func NewStatusCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:status", "Show Horizon runtime status", load, runStatusCommand)
}

// runStatusCommand 输出全局 Horizon 状态快照。
//
// 输出边界：只展示 global status、control flags、supervisor/worker 总数与 stale 计数，不展示队列健康或 metrics。
func runStatusCommand(ctx console.CommandContext, runtime Runtime) error {
	snapshot, err := runtime.StatusSnapshot(ctx.Context(), time.Now())
	if err != nil {
		return err
	}
	ctx.IO().Info("Status: " + snapshot.Status)
	ctx.IO().Info("Global Paused: " + strconv.FormatBool(snapshot.GlobalPaused))
	ctx.IO().Info("Terminate Requested: " + strconv.FormatBool(snapshot.TerminateRequested))
	ctx.IO().Info("Supervisors: " + strconv.Itoa(snapshot.SupervisorCount))
	ctx.IO().Info("Workers: " + strconv.Itoa(snapshot.WorkerCount))
	ctx.IO().Info("Stale Supervisors: " + strconv.Itoa(snapshot.StaleSupervisorCount))
	ctx.IO().Info("Stale Workers: " + strconv.Itoa(snapshot.StaleWorkerCount))
	switch snapshot.Status {
	case "inactive":
		return fmt.Errorf("horizon: status is %s", snapshot.Status)
	}
	return nil
}
