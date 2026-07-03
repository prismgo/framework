//go:build !windows

package http

import (
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"testing"
	"time"
)

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
	if newPID <= 0 {
		t.Fatalf("Reload new PID = %d, want positive", newPID)
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
