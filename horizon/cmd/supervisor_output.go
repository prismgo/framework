package cmd

import (
	"strconv"
	"strings"
)

// supervisorStateHeaders 返回 supervisor 表头，集中维护以避免命令输出字段漂移。
func supervisorStateHeaders() []string {
	return []string{"Name", "PID", "Status", "Workers", "Pools", "Balancing", "Host", "Connection", "Queues", "Last Heartbeat"}
}

// supervisorStateRows 把 supervisor 状态投影为 console 表格行。
//
// 逻辑说明：当前 Workers 显示总数，worker 明细和按队列分布留给后续 horizon:workers 或 UI/API。
func supervisorStateRows(supervisors []SupervisorState) [][]string {
	rows := make([][]string, 0, len(supervisors))
	for _, supervisor := range supervisors {
		rows = append(rows, []string{
			supervisor.Name,
			strconv.Itoa(supervisor.PID),
			supervisor.Status,
			strconv.Itoa(supervisor.WorkerCount),
			formatProcessPools(supervisor.Pools),
			"",
			supervisor.Host,
			supervisor.Connection,
			strings.Join(supervisor.Queues, ","),
			formatTime(supervisor.LastHeartbeatAt),
		})
	}
	return rows
}

func formatProcessPools(pools []ProcessPoolState) string {
	if len(pools) == 0 {
		return ""
	}
	parts := make([]string, 0, len(pools))
	for _, pool := range pools {
		name := pool.Queue
		if name == "" {
			name = strings.Join(pool.Queues, ",")
		}
		parts = append(parts, name+":"+strconv.Itoa(pool.CurrentWorkers)+"/"+strconv.Itoa(pool.TargetWorkers))
	}
	return strings.Join(parts, " ")
}
