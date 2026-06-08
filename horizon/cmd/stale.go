package cmd

import (
	"sort"
	"strconv"
	"time"

	"github.com/prismgo/framework/console"
)

// NewStaleCommand 创建 Prismgo 扩展的 heartbeat stale 诊断命令。
//
// 需求背景：Laravel 对齐的 horizon:timeout 是配置查询；Prismgo 需要单独的只读命令展示
// heartbeat TTL 派生出的 master/supervisor/worker 失联对象。
func NewStaleCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:stale", "List stale Horizon processes", load, runStaleCommand)
}

func runStaleCommand(ctx console.CommandContext, runtime Runtime) error {
	now := time.Now()
	rows, err := staleRows(ctx, runtime, now)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		ctx.IO().Info("No stale Horizon processes.")
		return nil
	}
	return ctx.IO().Table([]string{"Type", "ID", "PID", "Host", "Status", "Last Heartbeat", "Stale For"}, rows)
}

// staleRows 聚合三类 heartbeat 记录，只保留读取路径已经派生为 stale 的对象。
//
// 逻辑说明：该函数不修改 Store、不发送 signal、不调用 purge；stale 判断完全来自 Runtime 读取 Store 后的状态投影。
// 参数说明：now 用于计算展示时长，测试可传入固定时间减少时间敏感断言。
func staleRows(ctx console.CommandContext, runtime Runtime, now time.Time) ([][]string, error) {
	masters, err := runtime.Masters(ctx.Context(), now)
	if err != nil {
		return nil, err
	}
	supervisors, err := runtime.Supervisors(ctx.Context(), now)
	if err != nil {
		return nil, err
	}
	workers, err := runtime.Workers(ctx.Context(), now)
	if err != nil {
		return nil, err
	}
	rows := make([][]string, 0)
	for _, master := range masters {
		if master.Status == "stale" {
			rows = append(rows, staleRow("master", master.ID, master.PID, master.Host, master.Status, master.LastHeartbeatAt, now))
		}
	}
	for _, supervisor := range supervisors {
		if supervisor.Status == "stale" {
			rows = append(rows, staleRow("supervisor", supervisor.Name, supervisor.PID, supervisor.Host, supervisor.Status, supervisor.LastHeartbeatAt, now))
		}
	}
	for _, worker := range workers {
		if worker.Status == "stale" {
			rows = append(rows, staleRow("worker", worker.ID, worker.PID, worker.Host, worker.Status, worker.LastHeartbeatAt, now))
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i][0] == rows[j][0] {
			return rows[i][1] < rows[j][1]
		}
		return rows[i][0] < rows[j][0]
	})
	return rows, nil
}

func staleRow(kind string, id string, pid int, host string, status string, lastHeartbeat time.Time, now time.Time) []string {
	return []string{
		kind,
		id,
		strconv.Itoa(pid),
		host,
		status,
		formatTime(lastHeartbeat),
		formatDurationSince(lastHeartbeat, now),
	}
}

func formatDurationSince(start time.Time, now time.Time) string {
	if start.IsZero() {
		return ""
	}
	if now.IsZero() {
		now = time.Now()
	}
	if now.Before(start) {
		return "0s"
	}
	return now.Sub(start).Round(time.Second).String()
}
