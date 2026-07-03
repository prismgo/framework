package cmd

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
)

func TestServeCommandStartServerRoutesError(t *testing.T) {
	setupCommandConfigContainer(t)

	cmd := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{routesErr: errors.New("routes failed")}))
	err := cmd.startServer(context.Background(), "8051", console.NewIO(strings.NewReader(""), io.Discard, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "routes failed") {
		t.Fatalf("expected routes failure, got %v", err)
	}
}

func TestServeCommandProcessErrorBranches(t *testing.T) {
	cmd := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{}))
	killErr := errors.New("kill failed")
	stopErr := errors.New("stop failed")
	reloadErr := errors.New("reload failed")
	restartErr := errors.New("restart failed")
	manager := &errorProcessManager{pid: 100, killErr: killErr, stopErr: stopErr, reloadErr: reloadErr, restartErr: restartErr}
	ioo := console.NewIO(strings.NewReader(""), io.Discard, io.Discard)

	if err := cmd.killServer(manager, 100, ioo); err == nil || !strings.Contains(err.Error(), "kill failed") {
		t.Fatalf("expected kill error, got %v", err)
	}
	if err := cmd.stopServer(manager, 100, 0, ioo); err == nil || !strings.Contains(err.Error(), "stop failed") {
		t.Fatalf("expected stop error, got %v", err)
	}
	if err := cmd.reloadServer(manager, "8051", 100, 0, ioo); err == nil || !strings.Contains(err.Error(), "reload failed") {
		t.Fatalf("expected reload error, got %v", err)
	}
	if err := cmd.restartServer(manager, "8051", 100, ioo); err == nil || !strings.Contains(err.Error(), "restart failed") {
		t.Fatalf("expected restart error, got %v", err)
	}
}

type errorProcessManager struct {
	pid        int
	killErr    error
	stopErr    error
	reloadErr  error
	restartErr error
}

func (pm *errorProcessManager) SavePID() error                { return nil }
func (pm *errorProcessManager) RemovePID() error              { return nil }
func (pm *errorProcessManager) ReadPID() (int, error)         { return pm.pid, nil }
func (pm *errorProcessManager) Kill(int) error                { return pm.killErr }
func (pm *errorProcessManager) Stop(int, time.Duration) error { return pm.stopErr }
func (pm *errorProcessManager) Reload(int) (int, error) {
	return 0, pm.reloadErr
}
func (pm *errorProcessManager) Restart(int, string, []string) (int, error) {
	return 0, pm.restartErr
}

var _ processManager = (*errorProcessManager)(nil)
