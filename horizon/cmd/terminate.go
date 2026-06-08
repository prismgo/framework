package cmd

import (
	"errors"
	"time"

	"github.com/prismgo/framework/console"
)

// NewTerminateCommand 创建 horizon:terminate 命令。
func NewTerminateCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:terminate {--wait}", "Request Horizon graceful termination", load, runTerminateCommand)
}

// runTerminateCommand 写入一次性的优雅退出请求。
func runTerminateCommand(ctx console.CommandContext, runtime Runtime) error {
	hasProcesses, err := hasFreshTerminateTargets(ctx, runtime)
	if err != nil {
		return err
	}
	if err := runtime.RequestTerminate(ctx.Context(), time.Now(), boolCommandOption(ctx, "wait")); err != nil {
		if errors.Is(err, ErrNoProcessesToTerminate) {
			ctx.IO().Info("No processes to terminate.")
			return nil
		}
		return err
	}
	if !hasProcesses {
		ctx.IO().Info("No processes to terminate.")
		return nil
	}
	ctx.IO().Success("Horizon termination requested.")
	return nil
}

// hasFreshTerminateTargets 判断当前是否存在 terminate 可通知的 fresh master/supervisor。
//
// 逻辑说明：该检查只影响 CLI 文案；Runtime.RequestTerminate 仍负责写入 terminate flag 和 queue restart，
// 因此没有运行中进程时命令也不会跳过控制语义。
func hasFreshTerminateTargets(ctx console.CommandContext, runtime Runtime) (bool, error) {
	now := time.Now()
	masters, err := runtime.Masters(ctx.Context(), now)
	if err != nil {
		return false, err
	}
	for _, master := range masters {
		if master.Status != "stale" && master.PID > 0 {
			return true, nil
		}
	}
	supervisors, err := runtime.Supervisors(ctx.Context(), now)
	if err != nil {
		return false, err
	}
	for _, supervisor := range supervisors {
		if supervisor.Status != "stale" && supervisor.PID > 0 {
			return true, nil
		}
	}
	return false, nil
}
