//go:build !windows

package http

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prismgo/framework/exception"
)

// Reload 请求旧服务进程执行真正的平滑重载。
//
// 设计说明：监听 FD 只存在于旧服务进程内，外部控制进程无法直接把该 FD 传给新进程，
// 因此这里改为向旧进程发送用户态信号，由旧进程在自身上下文里 fork 子进程并完成监听接管。
func (pm *ProcessManager) Reload(pid int) (int, error) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return 0, fmt.Errorf("find process failed: %w", err)
	}
	if err := process.Signal(syscall.SIGUSR2); err != nil {
		return 0, fmt.Errorf("send reload signal failed: %w", err)
	}
	if pm.pidFile == "" {
		// 无法等待新 PID，仅确认信号已发送
		return 0, nil
	}
	return pm.waitForNewPID(pid, 10*time.Second)
}

func (pm *ProcessManager) waitForNewPID(oldPID int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		newPID, err := pm.ReadPID()
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if newPID == oldPID || newPID <= 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return newPID, nil
	}
	if lastErr != nil {
		return 0, fmt.Errorf("reload: new process did not write pid file within %s: last read error: %w", timeout, lastErr)
	}
	return 0, fmt.Errorf("reload: new process did not write pid file within %s", timeout)
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
	var reloading sync.Mutex
	go func() {
		defer close(stopped)
		for {
			select {
			case <-ctx.Done():
				return
			case sig, ok := <-signals:
				if !ok {
					// channel 已关闭，cleanup 正在执行，退出 goroutine
					return
				}
				_ = sig // 消费信号
				if !reloading.TryLock() {
					// 已有 reload 在执行，丢弃本次信号
					continue
				}
				go func() {
					defer reloading.Unlock()
					if err := spawnReloadChild(listener, executable, args, onReady); err != nil {
						exception.Report(context.Background(), err, map[string]any{
							"component":  "http",
							"operation":  "watch_reload_signal",
							"executable": executable,
						})
					}
				}()
			}
		}
	}()
	return func() {
		signal.Stop(signals)
		close(signals)
		<-stopped
	}
}

// spawnReloadChild 启动子进程并传递监听器文件描述符。
//
// 副作用说明：调用 fileListener.File() 会将底层 TCPListener 切换为阻塞模式（Go 标准库行为），
// 这会导致 listener.Accept() 绕过 netpoller，在高并发场景下可能阻塞操作系统线程。
// 因此 reload 触发后应尽快通过 context 取消使旧进程停止 Accept()，进入优雅关闭流程。
func spawnReloadChild(listener net.Listener, executable string, args []string, onReady func()) error {
	fileListener, ok := listener.(*net.TCPListener)
	if !ok {
		return fmt.Errorf("reload requires TCP listener")
	}
	listenerFile, err := fileListener.File()
	if err != nil {
		return fmt.Errorf("listener file failed: %w", err)
	}
	defer func() {
		if err := listenerFile.Close(); err != nil {
			exception.Report(context.Background(), err, map[string]any{
				"component": "http",
				"operation": "reload_listener_file_close",
			})
		}
	}()
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
		if err := cmd.Wait(); err != nil {
			exception.Report(context.Background(), err, map[string]any{
				"component": "http",
				"operation": "reload_child_wait",
				"pid":       cmd.Process.Pid,
			})
		}
	}()
	return nil
}
