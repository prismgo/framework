package cmd

import (
	"strconv"

	"github.com/prismgo/framework/console"
)

// NewTimeoutCommand 创建 Laravel 对齐的 horizon:timeout 配置查询命令。
//
// 需求背景：该命令只报告当前已加载 environment 下 supervisor 配置里的最大 worker timeout，
// 不读取 heartbeat，也不承担 stale 诊断或进程清理职责。
func NewTimeoutCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:timeout {environment=production}", "Show Horizon maximum worker timeout", load, runTimeoutCommand)
}

// runTimeoutCommand 输出最大 worker timeout 秒数。
//
// 参数说明：environment 是可选参数；当前实现只允许查询已加载的 environment，避免为了该只读命令扩大配置模型。
func runTimeoutCommand(ctx console.CommandContext, runtime Runtime) error {
	timeout, err := runtime.MaxWorkerTimeout(ctx.Input().Argument("environment"))
	if err != nil {
		return err
	}
	ctx.IO().Info(strconv.Itoa(timeout))
	return nil
}
