package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prismgo/framework/console"
)

// NewSupervisorStatusCommand 创建 horizon:supervisor-status 命令。
func NewSupervisorStatusCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:supervisor-status {name}", "Show Horizon supervisor status", load, runSupervisorStatusCommand)
}

// runSupervisorStatusCommand 输出同名 supervisor 的所有运行实例。
//
// 参数说明：name 来自命令位置参数；找不到任何同名实例时输出提示并返回 not found 错误，便于脚本感知失败。
func runSupervisorStatusCommand(ctx console.CommandContext, runtime Runtime) error {
	name := strings.TrimSpace(ctx.Input().Argument("name"))
	supervisors, err := runtime.Supervisors(ctx.Context(), time.Now())
	if err != nil {
		return err
	}
	matches := make([]SupervisorState, 0)
	for _, supervisor := range supervisors {
		if supervisor.Name != name {
			continue
		}
		matches = append(matches, supervisor)
	}
	sort.Slice(matches, func(i, j int) bool {
		if !matches[i].LastHeartbeatAt.Equal(matches[j].LastHeartbeatAt) {
			return matches[i].LastHeartbeatAt.After(matches[j].LastHeartbeatAt)
		}
		if matches[i].Host != matches[j].Host {
			return matches[i].Host < matches[j].Host
		}
		return matches[i].PID < matches[j].PID
	})
	rows := make([][]string, 0, len(matches))
	for _, supervisor := range matches {
		rows = append(rows, []string{
			supervisor.Name,
			supervisor.Status,
			strconv.Itoa(supervisor.PID),
			supervisor.Host,
			strconv.Itoa(supervisor.WorkerCount),
			formatProcessPools(supervisor.Pools),
			supervisor.Connection,
			strings.Join(supervisor.Queues, ","),
			formatTime(supervisor.StartedAt),
			formatTime(supervisor.LastHeartbeatAt),
		})
	}
	if len(rows) == 0 {
		ctx.IO().Error("Unable to find a supervisor with this name.")
		return fmt.Errorf("horizon: supervisor %q not found", name)
	}
	ctx.IO().Info(fmt.Sprintf("%s has %d instance(s)", name, len(rows)))
	return ctx.IO().Table([]string{"Name", "Status", "PID", "Host", "Workers", "Pools", "Connection", "Queues", "Started At", "Last Heartbeat"}, rows)
}
