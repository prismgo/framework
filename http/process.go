package http

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
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

// Reload 请求旧服务进程执行真正的平滑重载。
//
// 设计说明：监听 FD 只存在于旧服务进程内，外部控制进程无法直接把该 FD 传给新进程，
// 因此这里改为向旧进程发送用户态信号，由旧进程在自身上下文里 fork 子进程并完成监听接管。
func (pm *ProcessManager) Reload(pid int, executable string, args []string, shutdownTimeout time.Duration) (int, error) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return 0, fmt.Errorf("find process failed: %w", err)
	}
	if shutdownTimeout < 0 {
		shutdownTimeout = 0
	}
	_ = executable
	_ = args
	_ = shutdownTimeout
	if err := process.Signal(syscall.SIGUSR2); err != nil {
		return 0, fmt.Errorf("send reload signal failed: %w", err)
	}
	return pid, nil
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

	// 等待一下确保端口释放
	time.Sleep(1 * time.Second)

	// 启动新进程
	cmd := exec.Command(executable, args...)
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start new process failed: %w", err)
	}

	return cmd.Process.Pid, nil
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
	_ = file.Close()
	if err != nil {
		return nil, fmt.Errorf("inherit listener failed: %w", err)
	}
	return listener, nil
}

// WatchReloadSignal 监听用户态 reload 信号，并在收到信号后执行监听器接管。
//
// 用途：旧服务进程持有真实监听资源，只有它自己能把 listener 作为 ExtraFiles 传给新进程。
// 设计说明：收到 SIGUSR2 后由旧进程 fork 新 serve 子进程，待新进程持有监听 FD 后再触发当前 ctx 取消，
// 使旧进程走既有优雅关闭路径。
func WatchReloadSignal(ctx context.Context, listener net.Listener, executable string, args []string, onReady func()) func() {
	if listener == nil {
		return func() {}
	}
	if executable == "" {
		executable = os.Args[0]
	}
	if len(args) == 0 {
		args = append([]string(nil), os.Args[1:]...)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR2)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-ctx.Done():
				return
			case <-signals:
				_ = spawnReloadChild(listener, executable, args, onReady)
			}
		}
	}()
	return func() {
		signal.Stop(signals)
		close(signals)
		<-stopped
	}
}

func spawnReloadChild(listener net.Listener, executable string, args []string, onReady func()) error {
	fileListener, ok := listener.(*net.TCPListener)
	if !ok {
		return fmt.Errorf("reload requires TCP listener")
	}
	listenerFile, err := fileListener.File()
	if err != nil {
		return fmt.Errorf("listener file failed: %w", err)
	}
	defer listenerFile.Close()
	cmd := exec.Command(executable, args...)
	cmd.ExtraFiles = []*os.File{listenerFile}
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%d", listenerFDEnv, inheritedFD))
	if onReady != nil {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%d", reloadSignalEnv, os.Getpid()))
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start reload child failed: %w", err)
	}
	go func() {
		_ = cmd.Wait()
	}()
	return nil
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
