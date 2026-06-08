//go:build !windows

package http

import (
	"os/exec"
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

	reloadCmd := startSleepProcess(t)
	newPID, err := manager.Reload(reloadCmd.Process.Pid, "/bin/true", nil, time.Millisecond)
	if err != nil {
		t.Fatalf("Reload returned error: %v", err)
	}
	if newPID <= 0 {
		t.Fatalf("Reload new PID = %d, want positive", newPID)
	}
	_ = reloadCmd.Wait()

	restartCmd := startSleepProcess(t)
	newPID, err = manager.Restart(restartCmd.Process.Pid, "/bin/true", nil)
	if err != nil {
		t.Fatalf("Restart returned error: %v", err)
	}
	if newPID <= 0 {
		t.Fatalf("Restart new PID = %d, want positive", newPID)
	}
	_ = restartCmd.Wait()
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
