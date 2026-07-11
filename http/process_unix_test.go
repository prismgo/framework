//go:build !windows

package http

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
)

// setupExceptionHandlerForTest 设置测试所需的容器环境。
// spawnReloadChild 内部 goroutine 调用 exception.Report，需要 exception handler 和 logger。
func setupExceptionHandlerForTest(t *testing.T) {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	mgr, err := logger.NewManager(logger.Config{
		Default: "null",
		Channels: map[string]logger.ChannelOptions{
			"null": {Driver: "null", Level: "debug"},
		},
	})
	if err != nil {
		t.Fatalf("create logger manager failed: %v", err)
	}
	if err := registry.Instance("logger.manager", mgr); err != nil {
		t.Fatalf("bind logger manager failed: %v", err)
	}

	handler := exception.New()
	if err := registry.Instance("exception.handler", handler); err != nil {
		t.Fatalf("bind exception handler failed: %v", err)
	}
}

func TestProcessManagerSignalsChildProcesses(t *testing.T) {
	manager := NewProcessManager("")

	killCmd := startSleepProcess(t)
	if err := manager.Kill(killCmd.Process.Pid); err != nil {
		t.Fatalf("Kill returned error: %v", err)
	}
	_ = killCmd.Wait()

	stopCmd := startSleepProcess(t)
	if err := manager.Stop(stopCmd.Process.Pid, time.Millisecond); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	_ = stopCmd.Wait()
}

func TestProcessManagerReloadAndRestart(t *testing.T) {
	manager := NewProcessManager("")
	truePath := trueExecutable(t)

	reloadCmd := startSleepProcess(t)
	newPID, err := manager.Reload(reloadCmd.Process.Pid)
	if err != nil {
		t.Fatalf("Reload returned error: %v", err)
	}
	// pidFile 为空时，Reload 返回 0 表示信号已发送但无法确认新进程 PID
	if newPID != 0 {
		t.Fatalf("Reload new PID = %d, want 0 (pidFile is empty)", newPID)
	}
	_ = reloadCmd.Wait()

	restartCmd := startSleepProcess(t)
	newPID, err = manager.Restart(restartCmd.Process.Pid, truePath, nil)
	if err != nil {
		t.Fatalf("Restart returned error: %v", err)
	}
	if newPID <= 0 {
		t.Fatalf("Restart new PID = %d, want positive", newPID)
	}
	_ = restartCmd.Wait()
}

func TestProcessManagerUnixSignalErrors(t *testing.T) {
	manager := NewProcessManager("")

	if err := manager.Kill(999999); err == nil {
		t.Fatal("Kill missing process error = nil, want error")
	}

	if err := manager.Stop(999999, time.Millisecond); err == nil || err.Error() == "" {
		t.Fatalf("Stop missing process error = %v, want error", err)
	}

	if _, err := manager.Reload(999999); err == nil || err.Error() == "" {
		t.Fatalf("Reload missing process error = %v, want error", err)
	}
}

func TestProcessManagerRestartStartError(t *testing.T) {
	manager := NewProcessManager("")
	cmd := startSleepProcess(t)

	_, err := manager.Restart(cmd.Process.Pid, "/path/to/missing/executable", nil)
	if err == nil {
		t.Fatal("Restart missing executable error = nil, want error")
	}
	_ = cmd.Wait()
}

func TestNotifyReloadParentSignalsConfiguredParent(t *testing.T) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	t.Cleanup(func() {
		signal.Stop(signals)
	})

	t.Setenv(reloadSignalEnv, strconv.Itoa(os.Getpid()))
	if err := NotifyReloadParent(); err != nil {
		t.Fatalf("NotifyReloadParent returned error: %v", err)
	}

	select {
	case <-signals:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SIGTERM")
	}
}

func startSleepProcess(t *testing.T) *exec.Cmd {
	t.Helper()

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep failed: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	return cmd
}

func trueExecutable(t *testing.T) string {
	t.Helper()

	path, err := exec.LookPath("true")
	if err != nil {
		t.Fatalf("find true executable failed: %v", err)
	}
	return path
}

// TestWaitForNewPIDReturnsNewPIDWhenFileChanges 验证 waitForNewPID 在新 PID 写入后正确返回。
//
// 设计说明：waitForNewPID 是 Unix 平台特有的方法，Windows 上返回 stub 错误，
// 因此此测试放在 process_unix_test.go 中避免跨平台编译失败。
func TestWaitForNewPIDReturnsNewPIDWhenFileChanges(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "process.pid")
	oldPID := 12345
	newPID := 54321

	// Initialize pid file with old PID
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(oldPID)), 0644); err != nil {
		t.Fatalf("write init pid file: %v", err)
	}

	manager := NewProcessManager(pidFile)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(newPID)), 0644); err != nil {
			t.Errorf("write new pid failed: %v", err)
		}
	}()

	got, err := manager.waitForNewPID(oldPID, 5*time.Second)
	wg.Wait()

	if err != nil {
		t.Fatalf("waitForNewPID returned error: %v", err)
	}
	if got != newPID {
		t.Fatalf("waitForNewPID = %d, want %d", got, newPID)
	}
}

// TestWaitForNewPIDReturnsErrorOnTimeout 验证 waitForNewPID 在超时时返回错误。
//
// 设计说明：waitForNewPID 是 Unix 平台特有的方法，Windows 上返回 stub 错误，
// 因此此测试放在 process_unix_test.go 中避免跨平台编译失败。
func TestWaitForNewPIDReturnsErrorOnTimeout(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "process-timeout.pid")
	oldPID := 12345

	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(oldPID)), 0644); err != nil {
		t.Fatalf("write init pid file: %v", err)
	}

	manager := NewProcessManager(pidFile)

	got, err := manager.waitForNewPID(oldPID, 50*time.Millisecond)
	if err == nil {
		t.Fatalf("waitForNewPID returned pid=%d, want error", got)
	}
	if !strings.Contains(err.Error(), "reload: new process did not write pid file") {
		t.Fatalf("waitForNewPID error = %q, want timeout error", err.Error())
	}
	if got != 0 {
		t.Fatalf("waitForNewPID pid on timeout = %d, want 0", got)
	}
}
