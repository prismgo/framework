package cmd

import "github.com/prismgo/framework/console"

// NewClearMetricsCommand 创建 horizon:clear-metrics 命令。
func NewClearMetricsCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:clear-metrics", "Clear Horizon metrics", load, runClearMetricsCommand)
}

// runClearMetricsCommand 清理 Store metrics 和当前 collector 内存聚合。
//
// 逻辑说明：该命令只清理事件派生 metrics，不清 supervisor/worker heartbeat、pause/terminate 控制标记、
// queue payload、queue failed records、queue length snapshot 或 queue restart signal。
func runClearMetricsCommand(ctx console.CommandContext, runtime Runtime) error {
	if err := runtime.ClearMetrics(ctx.Context()); err != nil {
		return err
	}
	ctx.IO().Success("Horizon metrics cleared.")
	return nil
}
