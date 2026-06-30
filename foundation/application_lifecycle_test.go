package foundation

import (
	"context"
	"os"
	"syscall"
	"testing"
)

func TestRegisterShutdownSignalsIdempotency(t *testing.T) {
	app := NewApplication()
	defer func() {
		if err := app.Close(); err != nil {
			t.Fatalf("app.Close() error = %v", err)
		}
	}()

	// 第一次调用应该注册信号监听
	app.RegisterShutdownSignals()

	// 检查 signalsRegistered 标志
	if !app.signalsRegistered {
		t.Error("signalsRegistered should be true after first call")
	}

	// 第二次调用应该是幂等的，不会重复注册
	app.RegisterShutdownSignals()

	// 再次检查标志，应该仍然是 true
	if !app.signalsRegistered {
		t.Error("signalsRegistered should still be true after second call")
	}
}

func TestRegisterShutdownSignalsNilApplication(t *testing.T) {
	var app *Application
	// 对 nil Application 调用应该安全返回
	app.RegisterShutdownSignals()
}

func TestRegisterShutdownSignalsTriggersShutdown(t *testing.T) {
	app := NewApplication()
	defer func() {
		if err := app.Close(); err != nil {
			t.Fatalf("app.Close() error = %v", err)
		}
	}()

	app.RegisterShutdownSignals()

	// 发送 SIGTERM 信号
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("failed to find process: %v", err)
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("failed to send signal: %v", err)
	}

	// 等待应用关闭
	<-app.Context().Done()

	// 验证关闭原因包含信号信息
	cause := context.Cause(app.Context())
	if cause == nil {
		t.Error("expected shutdown cause to be set")
	}
}
