package horizon

import (
	horizoncmd "github.com/prismgo/framework/horizon/cmd"

	"github.com/prismgo/framework/console"
)

// CommandFactories 返回 Horizon 当前切片真实可执行的命令工厂。
//
// 需求背景：issue 01 只允许注册 horizon:list，避免 no-op 命令让使用者误以为 worker、status、
// publish 等运行期能力已经可用。标准应用侧通过 horizon.ServiceProvider 接入；该工厂保留给
// 低层兼容、独立测试或特殊装配场景。
func CommandFactories() []console.CommandFactory {
	return []console.CommandFactory{
		func() console.Command { return horizoncmd.NewMasterCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewListCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewStatusCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewSupervisorsCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewSupervisorStatusCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command {
			return horizoncmd.NewSupervisorProcessCommand(newRuntimeLoader(defaultManager))
		},
		func() console.Command { return horizoncmd.NewWorkCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewPauseCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewContinueCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewPauseSupervisorCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command {
			return horizoncmd.NewContinueSupervisorCommand(newRuntimeLoader(defaultManager))
		},
		func() console.Command { return horizoncmd.NewTerminateCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewTimeoutCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewStaleCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewSnapshotCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewClearCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewForgetCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewClearMetricsCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewPurgeCommand(newRuntimeLoader(defaultManager)) },
		func() console.Command { return horizoncmd.NewInstallCommand() },
		func() console.Command { return horizoncmd.NewListenCommand(newRuntimeLoader(defaultManager)) },
	}
}

// toCommandConfigView 把 Horizon 核心配置复制为 cmd 包的只读展示模型。
//
// 设计原因：cmd 包不反向依赖父包，避免循环依赖；这里复制切片和 map，防止命令输出层意外修改
// 核心配置对象。
func toCommandConfigView(cfg Config) horizoncmd.ConfigView {
	supervisors := make(map[string]horizoncmd.SupervisorView, len(cfg.Supervisors))
	for name, supervisor := range cfg.Supervisors {
		supervisors[name] = horizoncmd.SupervisorView{
			Name:                supervisor.Name,
			Connection:          supervisor.Connection,
			Queues:              append([]string(nil), supervisor.Queues...),
			Balance:             supervisor.Balance,
			MinProcesses:        supervisor.MinProcesses,
			MaxProcesses:        supervisor.MaxProcesses,
			Tries:               supervisor.Tries,
			Timeout:             supervisor.Timeout,
			Sleep:               supervisor.Sleep,
			MaxJobs:             supervisor.MaxJobs,
			MaxTime:             supervisor.MaxTime,
			RetryAfter:          supervisor.RetryAfter,
			Backoff:             append([]int(nil), supervisor.Backoff...),
			StopWhenEmpty:       supervisor.StopWhenEmpty,
			Memory:              supervisor.Memory,
			Nice:                supervisor.Nice,
			AutoScalingStrategy: supervisor.AutoScalingStrategy,
			BalanceMaxShift:     supervisor.BalanceMaxShift,
			BalanceCooldown:     supervisor.BalanceCooldown,
		}
	}
	return horizoncmd.ConfigView{
		Environment:  cfg.Environment,
		Store:        cfg.Store,
		Connection:   cfg.Connection,
		Prefix:       cfg.Prefix,
		HeartbeatTTL: cfg.HeartbeatTTL,
		Supervisors:  supervisors,
	}
}

// cloneIntMap 复制 int map，供命令展示投影隔离原始配置。
func cloneIntMap(input map[string]int) map[string]int {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]int, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
