package foundation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
)

// TestAcquireCloseSlotNoDoubleUnlockOnRetry 验证当非 owner goroutine 等待关闭完成后
// 发现 terminated 仍为 false（container 资源关闭失败场景），重试路径不会触发 double-unlock panic。
//
// 触发条件：
//  1. Goroutine A 执行 CloseContext，facade 关闭失败，terminated 保持 false，closeDone 被关闭
//  2. Goroutine B 在 A 执行期间调用 CloseContext，阻塞在 closeDone channel
//  3. B 被唤醒后发现 terminated=false，进入重试路径
//  4. 旧代码在递归调用返回后 defer Unlock 再次执行导致 panic
func TestAcquireCloseSlotNoDoubleUnlockOnRetry(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()

	closeErr := errors.New("facade close failed")
	var closeCalls int
	if err := app.Container().Instance("foundation.double.unlock.probe", &closeContextProbe{}, container.WithCloser(func(*closeContextProbe) error {
		closeCalls++
		if closeCalls == 1 {
			return closeErr
		}
		return nil
	})); err != nil {
		t.Fatalf("register probe: %v", err)
	}

	// 启动第一个 CloseContext（goroutine A）
	firstDone := make(chan error, 1)
	go func() { firstDone <- app.CloseContext(context.Background()) }()

	// 等待 A 进入关闭流程
	time.Sleep(50 * time.Millisecond)

	// 启动第二个 CloseContext（goroutine B），它会等待 A 完成
	// 使用 recover 捕获可能的 double-unlock panic
	secondDone := make(chan error, 1)
	var panicValue any
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicValue = r
				secondDone <- errors.New("panic recovered")
			}
		}()
		secondDone <- app.CloseContext(context.Background())
	}()

	// 等待两个 CloseContext 完成
	select {
	case err := <-firstDone:
		if !errors.Is(err, closeErr) {
			t.Fatalf("first CloseContext error = %v, want %v", err, closeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first CloseContext timed out")
	}

	select {
	case err := <-secondDone:
		if panicValue != nil {
			t.Fatalf("second CloseContext panicked: %v", panicValue)
		}
		if err != nil {
			t.Fatalf("second CloseContext error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second CloseContext timed out")
	}
}

// TestAcquireCloseSlotRespectsContextCancel 验证非 owner goroutine 在等待关闭完成时
// 能够响应 context 取消，而不是无限阻塞。
func TestAcquireCloseSlotRespectsContextCancel(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()

	// 让第一个 CloseContext 阻塞在 cleanup 中
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	app.RegisterCleanup(func(*Application) error {
		close(entered)
		<-release
		return nil
	})

	// 启动第一个 CloseContext（goroutine A）
	go func() { firstDone <- app.CloseContext(context.Background()) }()
	<-entered

	// 第二个 CloseContext 使用带超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := app.CloseContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second CloseContext error = %v, want context.DeadlineExceeded", err)
	}

	// 释放第一个 CloseContext 并等待其完成
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first CloseContext error = %v", err)
	}
}
