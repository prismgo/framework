package http

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/prismgo/framework/exception"
)

// ProcessManager 管理 HTTP 服务器进程的生命周期。
type ProcessManager struct {
	pidFile string
}

const (
	listenerFDEnv   = "PRISMGO_HTTP_LISTENER_FD"
	reloadSignalEnv = "PRISMGO_HTTP_RELOAD_SIGNAL_PID"
	inheritedFD     = 3
)

// NewProcessManager 创建进程管理器，pidFile 用于存储进程 ID。
func NewProcessManager(pidFile string) *ProcessManager {
	return &ProcessManager{pidFile: pidFile}
}

// SavePID 保存当前进程 ID 到文件。
func (pm *ProcessManager) SavePID() error {
	if err := os.MkdirAll(filepath.Dir(pm.pidFile), 0755); err != nil {
		return fmt.Errorf("create pid dir failed: %w", err)
	}
	return os.WriteFile(pm.pidFile, []byte(strconv.Itoa(os.Getpid())), 0644)
}

// RemovePID 删除 PID 文件。
func (pm *ProcessManager) RemovePID() error {
	return os.Remove(pm.pidFile)
}

// ReadPID 从文件读取进程 ID。
func (pm *ProcessManager) ReadPID() (int, error) {
	data, err := os.ReadFile(pm.pidFile)
	if err != nil {
		return 0, fmt.Errorf("read pid file failed: %w", err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, fmt.Errorf("parse pid failed: %w", err)
	}
	return pid, nil
}

// Kill 强制杀死指定 PID 的进程。
func (pm *ProcessManager) Kill(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find process failed: %w", err)
	}
	return process.Kill()
}

// Stop 发送 SIGTERM 信号进行优雅关闭，最多等待 shutdownTimeout 秒。
// 如果进程在超时后仍未退出，将强制杀死。
func (pm *ProcessManager) Stop(pid int, shutdownTimeout time.Duration) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find process failed: %w", err)
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("send SIGTERM failed: %w", err)
	}

	// 等待进程关闭，最多 shutdownTimeout
	pollInterval := time.Duration(500) * time.Millisecond
	deadline := time.Now().Add(shutdownTimeout)

	for time.Now().Before(deadline) {
		time.Sleep(pollInterval)
		// 用信号 0 检查进程是否存在，不会实际发送信号
		if err := process.Signal(syscall.Signal(0)); err != nil {
			// 进程已退出
			return nil
		}
	}

	// 超时后强制杀死进程
	return process.Kill()
}

// Restart 重启：强制杀死旧进程，然后启动新进程。
// args 是启动新进程的命令行参数。
func (pm *ProcessManager) Restart(pid int, executable string, args []string) (int, error) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return 0, fmt.Errorf("find process failed: %w", err)
	}

	// 强制杀死旧进程
	if err := process.Kill(); err != nil {
		return 0, fmt.Errorf("kill process failed: %w", err)
	}

	// 轮询等待进程退出，确认端口释放
	if err := waitProcessExit(process, 3*time.Second); err != nil {
		// 轮询超时后短暂等待作为 fallback
		time.Sleep(500 * time.Millisecond)
	}

	// 启动新进程
	cmd := exec.Command(executable, args...)
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start new process failed: %w", err)
	}

	return cmd.Process.Pid, nil
}

func waitProcessExit(process *os.Process, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := process.Signal(syscall.Signal(0)); err != nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("process still alive after %s", timeout)
}

// InheritedListener 返回由父进程传入的监听器；若当前进程不是 reload 子进程则返回 nil。
//
// 设计说明：平滑重载的新进程通过约定 FD 3 继承监听套接字，这里统一负责识别环境变量并转换成 net.Listener。
func InheritedListener() (net.Listener, error) {
	fdValue := os.Getenv(listenerFDEnv)
	if fdValue == "" {
		return nil, nil
	}
	fd, err := strconv.Atoi(fdValue)
	if err != nil {
		return nil, fmt.Errorf("parse inherited listener fd failed: %w", err)
	}
	if fd < 0 {
		return nil, fmt.Errorf("invalid inherited listener fd %d", fd)
	}
	file := os.NewFile(uintptr(fd), "prismgo-inherited-listener")
	if file == nil {
		return nil, fmt.Errorf("open inherited listener fd failed")
	}
	listener, err := net.FileListener(file)
	if closeErr := file.Close(); closeErr != nil {
		exception.Report(context.Background(), closeErr, map[string]any{
			"component": "http",
			"operation": "inherited_listener_file_close",
		})
	}
	if err != nil {
		return nil, fmt.Errorf("inherit listener failed: %w", err)
	}
	return listener, nil
}

// NotifyReloadParent 在 reload 子进程持有监听资源后通知父进程开始优雅退出。
func NotifyReloadParent() error {
	pidValue := os.Getenv(reloadSignalEnv)
	if pidValue == "" {
		return nil
	}
	pid, err := strconv.Atoi(pidValue)
	if err != nil {
		return fmt.Errorf("parse reload parent pid failed: %w", err)
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find reload parent failed: %w", err)
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("notify reload parent failed: %w", err)
	}
	return nil
}
