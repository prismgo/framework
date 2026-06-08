//go:build windows

package horizon

import (
	"context"
	"os"
	"os/exec"
)

func platformConfigureProcessCommand(*exec.Cmd) {}

// HorizonProcesses 在 Windows 下保持可运行 fallback，不伪造 POSIX 进程扫描结果。
func (OSProcessInspector) HorizonProcesses(context.Context) ([]HorizonProcess, error) {
	return nil, nil
}

// Terminate 在 Windows 下使用 Go 标准库进程控制 fallback；不承诺 POSIX SIGTERM 等价语义。
func (OSProcessInspector) Terminate(_ context.Context, pid int, force bool) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if force {
		return process.Kill()
	}
	return process.Signal(os.Interrupt)
}

func (NoopControlNotifier) Notify(context.Context, []ControlTarget) error {
	return nil
}

func platformRuntimeControlWake(context.Context) runtimeControlWake {
	return runtimeControlWake{}
}
