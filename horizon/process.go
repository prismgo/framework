package horizon

import (
	"context"
	"os"
	"os/exec"
	"strings"
)

const (
	// HorizonProcessWorker 表示 OS 扫描到的 horizon:work 子进程。
	HorizonProcessWorker = "worker"
)

// ProcessSpec 描述 Horizon 父进程启动子进程所需的最小命令行信息。
//
// 需求背景：issue 05 要求 master/supervisor/worker 通过同一个项目可执行文件重新进入 main.go，
// 因此父进程只能传递命令行参数、环境变量和 OS 进程信息，不能共享 Go 对象实例。
type ProcessSpec struct {
	// Args 是传给当前可执行文件的参数，第一项应为 horizon:supervisor 或 horizon:work。
	Args []string
	// Env 是追加到子进程环境中的键值对。
	Env []string
	// NewProcessGroup 表示子进程是否脱离调用方终端进程组，避免 Ctrl+C 绕过 Horizon control flag。
	NewProcessGroup bool
}

// ManagedProcess 表示已经启动的 Horizon 子进程。
type ManagedProcess interface {
	// PID 返回真实 OS 进程 ID，供状态记录和测试断言使用。
	PID() int
	// Wait 等待子进程退出。
	Wait() error
}

// HorizonProcess 是 ProcessInspector 扫描到的 Horizon 子进程摘要。
//
// 需求背景：horizon:purge 必须通过可注入接口封装 OS process scanning，测试不能依赖真实系统进程表。
type HorizonProcess struct {
	PID         int
	Kind        string
	Command     string
	Prefix      string
	Environment string
	WorkerID    string
}

// applyHorizonProcessArgs 从 horizon:work 命令行提取 purge 所需的命名空间字段。
//
// 设计原因：purge 只能清理当前 Horizon prefix/environment 下的 orphan worker；无法识别
// namespace 的进程保持只读，避免同一机器上其他应用或环境的 worker 被误杀。
func applyHorizonProcessArgs(process *HorizonProcess, args []string) {
	if process == nil {
		return
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		key, value, ok := strings.Cut(arg, "=")
		if !ok && strings.HasPrefix(arg, "--") && i+1 < len(args) {
			key = arg
			value = args[i+1]
			i++
		}
		switch key {
		case "--name":
			process.WorkerID = strings.TrimSpace(value)
		case "--environment":
			process.Environment = strings.TrimSpace(value)
		case "--prefix":
			process.Prefix = strings.TrimSpace(value)
		}
	}
}

// ProcessInspector 封装 Horizon 子进程扫描和终止请求。
type ProcessInspector interface {
	HorizonProcesses(context.Context) ([]HorizonProcess, error)
	Terminate(context.Context, int, bool) error
}

// ControlTarget 表示需要被 Horizon control signal 唤醒的运行中进程。
//
// 需求背景：pause/continue/terminate 的事实源是 Store control flag；control signal 只用于唤醒
// fresh master/supervisor 进程尽快重读 Store，不能作为 worker job 取消机制。
type ControlTarget struct {
	Type string
	ID   string
	Name string
	PID  int
}

// ControlNotifier 唤醒 fresh Horizon master/supervisor 进程。
//
// 设计思路：接口只接收已筛选的 master/supervisor 目标，不暴露 worker PID，避免调用方误把
// 进程通知实现成直接中断正在执行 job 的 worker。
type ControlNotifier interface {
	Notify(context.Context, []ControlTarget) error
}

// ProcessRunner 启动 Horizon 子进程，测试可以注入 fake runner 验证命令行边界。
type ProcessRunner interface {
	Start(context.Context, ProcessSpec) (ManagedProcess, error)
}

// NoopControlNotifier 是默认 control notifier。
//
// 逻辑说明：Unix 实现用 SIGUSR1 唤醒 master/supervisor；Windows 保留 Store 轮询兼容语义。
type NoopControlNotifier struct{}

// runtimeControlWake 把平台信号转换为 runtime loop 可 select 的轻量唤醒事件。
type runtimeControlWake struct {
	C    <-chan struct{}
	stop func()
}

// Stop 释放平台 signal.Notify 注册，避免测试或短命令泄漏 signal handler。
func (w runtimeControlWake) Stop() {
	if w.stop != nil {
		w.stop()
	}
}

// newRuntimeControlWake 可在测试中替换，用于验证 control signal 不再依赖固定 ticker。
var newRuntimeControlWake = platformRuntimeControlWake

// OSProcessRunner 使用当前可执行文件启动真实 OS 子进程。
type OSProcessRunner struct{}

// Start 通过 os.Executable 解析当前程序，并启动独立 Horizon 子进程。
func (OSProcessRunner) Start(ctx context.Context, spec ProcessSpec) (ManagedProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, spec.Args...)
	if spec.NewProcessGroup {
		platformConfigureProcessCommand(cmd)
	}
	cmd.Env = append(os.Environ(), spec.Env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &osManagedProcess{cmd: cmd}, nil
}

type osManagedProcess struct {
	cmd *exec.Cmd
}

func (p *osManagedProcess) PID() int {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *osManagedProcess) Wait() error {
	if p == nil || p.cmd == nil {
		return nil
	}
	return p.cmd.Wait()
}

// OSProcessInspector 提供 production 路径的 best-effort OS 进程扫描和终止请求。
type OSProcessInspector struct{}
