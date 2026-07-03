package http

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProcessManagerPIDFileLifecycle(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "run", "server.pid")
	manager := NewProcessManager(pidFile)

	if err := manager.SavePID(); err != nil {
		t.Fatalf("SavePID returned error: %v", err)
	}
	pid, err := manager.ReadPID()
	if err != nil {
		t.Fatalf("ReadPID returned error: %v", err)
	}
	if pid != os.Getpid() {
		t.Fatalf("pid = %d, want %d", pid, os.Getpid())
	}
	if err := manager.RemovePID(); err != nil {
		t.Fatalf("RemovePID returned error: %v", err)
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Fatalf("pid file still exists or stat failed unexpectedly: %v", err)
	}
}

func TestProcessManagerReadPIDErrors(t *testing.T) {
	missing := NewProcessManager(filepath.Join(t.TempDir(), "missing.pid"))
	if _, err := missing.ReadPID(); err == nil || !strings.Contains(err.Error(), "read pid file failed") {
		t.Fatalf("ReadPID missing error = %v, want read pid file failed", err)
	}

	invalidPath := filepath.Join(t.TempDir(), "invalid.pid")
	if err := os.WriteFile(invalidPath, []byte("not-a-pid"), 0644); err != nil {
		t.Fatalf("write invalid pid file failed: %v", err)
	}
	invalid := NewProcessManager(invalidPath)
	if _, err := invalid.ReadPID(); err == nil || !strings.Contains(err.Error(), "parse pid failed") {
		t.Fatalf("ReadPID invalid error = %v, want parse pid failed", err)
	}
}

func TestProcessManagerSavePIDCreateDirError(t *testing.T) {
	parentFile := filepath.Join(t.TempDir(), "server.pid")
	if err := os.WriteFile(parentFile, []byte("not a directory"), 0644); err != nil {
		t.Fatalf("write parent file failed: %v", err)
	}

	manager := NewProcessManager(filepath.Join(parentFile, "child.pid"))
	if err := manager.SavePID(); err == nil || !strings.Contains(err.Error(), "create pid dir failed") {
		t.Fatalf("SavePID error = %v, want create pid dir failed", err)
	}
}

func TestInheritedListenerWithoutEnvReturnsNil(t *testing.T) {
	t.Setenv(listenerFDEnv, "")
	listener, err := InheritedListener()
	if err != nil {
		t.Fatalf("InheritedListener returned error: %v", err)
	}
	if listener != nil {
		t.Fatalf("InheritedListener = %v, want nil", listener)
	}
}

func TestNotifyReloadParentWithoutEnvReturnsNil(t *testing.T) {
	t.Setenv(reloadSignalEnv, "")
	if err := NotifyReloadParent(); err != nil {
		t.Fatalf("NotifyReloadParent returned error: %v", err)
	}
}

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