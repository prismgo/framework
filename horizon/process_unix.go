//go:build !windows

package horizon

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

func platformConfigureProcessCommand(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// HorizonProcesses 使用 ps 扫描命令行中的 horizon:work 子进程。
func (OSProcessInspector) HorizonProcesses(ctx context.Context) ([]HorizonProcess, error) {
	cmd := exec.CommandContext(ctx, "ps", "-eo", "pid=,command=")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var processes []HorizonProcess
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.Contains(line, "horizon:work") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 0 || pid == os.Getpid() {
			continue
		}
		process := HorizonProcess{PID: pid, Kind: HorizonProcessWorker, Command: line}
		applyHorizonProcessArgs(&process, fields[1:])
		processes = append(processes, process)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return processes, nil
}

// Terminate 在 POSIX 路径使用 SIGTERM，force=true 时使用 Kill。
func (OSProcessInspector) Terminate(_ context.Context, pid int, force bool) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if force {
		return process.Kill()
	}
	return process.Signal(syscall.SIGTERM)
}

// Notify 用 SIGUSR1 唤醒 master/supervisor，让它们立即重读 Store control flag。
func (NoopControlNotifier) Notify(_ context.Context, targets []ControlTarget) error {
	for _, target := range targets {
		if target.PID <= 0 || target.PID == os.Getpid() {
			continue
		}
		process, err := os.FindProcess(target.PID)
		if err != nil {
			return err
		}
		if err := process.Signal(syscall.SIGUSR1); err != nil {
			if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			return err
		}
	}
	return nil
}

// platformRuntimeControlWake 将 SIGUSR1 合并成单个非阻塞 wake 事件，避免高频命令堆积。
func platformRuntimeControlWake(ctx context.Context) runtimeControlWake {
	signals := make(chan os.Signal, 1)
	wake := make(chan struct{}, 1)
	signal.Notify(signals, syscall.SIGUSR1)
	done := make(chan struct{})
	startRecoveringGoroutine(ctx, "runtime_control_wake", nil, func() {
		defer close(wake)
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-signals:
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
	})
	return runtimeControlWake{
		C: wake,
		stop: func() {
			signal.Stop(signals)
			close(done)
		},
	}
}
