package cmd

import (
	"sort"
	"time"

	"github.com/prismgo/framework/console"
)

// NewSupervisorsCommand 创建 horizon:supervisors 命令。
func NewSupervisorsCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:supervisors", "List Horizon supervisors", load, runSupervisorsCommand)
}

// runSupervisorsCommand 输出 supervisor 基础状态表。
//
// 设计原因：该命令贴近 Laravel Horizon 的 supervisor 表，同时只追加 issue 02 已有的基础字段。
func runSupervisorsCommand(ctx console.CommandContext, runtime Runtime) error {
	supervisors, err := runtime.Supervisors(ctx.Context(), time.Now())
	if err != nil {
		return err
	}
	sort.Slice(supervisors, func(i, j int) bool { return supervisors[i].Name < supervisors[j].Name })
	if len(supervisors) == 0 {
		ctx.IO().Info("No supervisors are running.")
		return nil
	}
	return ctx.IO().Table(supervisorStateHeaders(), supervisorStateRows(supervisors))
}
