package http

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
