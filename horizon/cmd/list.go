package cmd

import (
	"strconv"
	"time"

	"github.com/prismgo/framework/console"
)

// ConfigView 是 Horizon 静态配置的只读投影。
type ConfigView struct {
	// Environment 是解析出的 Horizon 环境名。
	Environment string
	// Store 是 Horizon Store 类型。
	Store string
	// Connection 是 Horizon Store 使用的连接名。
	Connection string
	// Prefix 是 Horizon Store key 前缀。
	Prefix string
	// HeartbeatTTL 是 heartbeat 存活窗口。
	HeartbeatTTL time.Duration
	// Trim 是静态保留时间配置。
	// Supervisors 是合并后的 supervisor 配置投影。
	Supervisors map[string]SupervisorView
}

// SupervisorView 是单个 supervisor 的静态配置投影。
type SupervisorView struct {
	Name                string
	Connection          string
	Queues              []string
	Balance             string
	MinProcesses        int
	MaxProcesses        int
	Tries               int
	Timeout             int
	Sleep               int
	MaxJobs             int
	MaxTime             int
	RetryAfter          int
	Backoff             []int
	StopWhenEmpty       bool
	Memory              int
	Nice                int
	AutoScalingStrategy string
	BalanceMaxShift     int
	BalanceCooldown     int
}

// ListCommand 展示正在运行的 Horizon master machines。
//
// 需求背景：Laravel 对齐的 `horizon:list` 是运行时视图，必须读取 Horizon Store 中的 master
// heartbeat，而不是展示静态配置、store 配置、trim 配置或 queue targets。
// NewListCommand 构造 horizon:list 命令。
//
// 参数说明：load 负责解析 Runtime；通过统一 runtime command 包装复用稳定的配置错误、nil runtime
// 防护和 memory store 警告边界。
func NewListCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:list", "List running Horizon master machines", load, func(ctx console.CommandContext, runtime Runtime) error {
		masters, err := runtime.Masters(ctx.Context(), time.Now())
		if err != nil {
			return err
		}
		if len(masters) == 0 {
			ctx.IO().Info("No machines are running.")
			return nil
		}
		return ctx.IO().Table([]string{"Name", "PID", "Supervisors", "Status"}, masterMachineRows(masters))
	})
}

// masterMachineRows 把 master heartbeat 投影为 horizon:list 的固定核心列。
func masterMachineRows(masters []MasterState) [][]string {
	rows := make([][]string, 0, len(masters))
	for _, master := range masters {
		rows = append(rows, []string{
			master.ID,
			strconv.Itoa(master.PID),
			strconv.Itoa(master.SupervisorCount),
			master.Status,
		})
	}
	return rows
}
