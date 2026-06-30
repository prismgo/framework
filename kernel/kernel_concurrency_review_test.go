package kernel

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/prismgo/framework/console"
)

// TestResolveRegisteredCommandConcurrentWithRegister 验证 resolveRegisteredCommand 与 mustRegister
// 并发执行不会导致 map 数据竞争（Critical #1）
func TestResolveRegisteredCommandConcurrentWithRegister(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// 并发注册命令
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("cmd%d", idx)
			k.Register(&simpleTestCommand{name: name})
		}(i)
	}

	// 并发解析命令（模拟调度器）
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("cmd%d", idx)
			// resolveRegisteredCommand 应该能安全读取 k.commands
			_, err := k.resolveRegisteredCommand(name, nil)
			if err != nil {
				// 命令可能还没注册完成，这是预期的
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// 验证所有命令都已注册
	cmds := k.Commands()
	if len(cmds) < 10 {
		t.Errorf("expected at least 10 commands, got %d", len(cmds))
	}
}

// TestExecuteSignatureConcurrentWithRunContext 验证 executeSignature 与 RunContext
// 并发执行不会导致状态竞争（High #2）
func TestExecuteSignatureConcurrentWithRunContext(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")
	k.Register(&simpleTestCommand{name: "test:cmd"})

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// 并发执行 Call（使用 executeSignature）
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := k.Call(context.Background(), "test:cmd"); err != nil {
				errCh <- err
			}
		}()
	}

	// 并发执行 RunContextArgv
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := k.RunContextArgv(context.Background(), []string{"test", "test:cmd"}); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent execution failed: %v", err)
	}
}

// TestRunStartingCallbacksPanicRecovery 验证 callback panic 不会导致状态不一致（High #3）
func TestRunStartingCallbacksPanicRecovery(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")

	// 注册一个会 panic 的 callback
	panicCallback := func(kernel *Kernel) error {
		panic("callback panic")
	}
	_ = k.Starting(panicCallback)

	// 第一次调用应该 panic（重新抛出）
	panicked := false
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				panicked = true
			}
		}()
		_ = k.runStartingCallbacks(context.Background())
	}()

	if !panicked {
		t.Fatal("expected panic to be re-thrown")
	}

	// 验证状态已清理，后续调用可以重试
	// 注册一个正常的 callback
	normalCalled := false
	_ = k.Starting(func(kernel *Kernel) error {
		normalCalled = true
		return nil
	})

	// 第二次调用应该成功
	err := k.runStartingCallbacks(context.Background())
	if err != nil {
		t.Fatalf("second call should succeed after panic recovery, got: %v", err)
	}
	if !normalCalled {
		t.Fatal("normal callback should have been called")
	}
}

// TestCallValuesRecursionDepthLimit 验证 callValues 递归深度限制（Medium #4）
func TestCallValuesRecursionDepthLimit(t *testing.T) {
	// 构造 10 层嵌套的切片：叶子字符串在 depth=10 被访问，depth > 10 为 false，应该成功
	var nested10 any = []any{"leaf"}
	for i := 0; i < 9; i++ {
		nested10 = []any{nested10}
	}
	_, err := callValues(nested10, 0)
	if err != nil {
		t.Errorf("10 levels should succeed: %v", err)
	}

	// 构造 11 层嵌套的切片：叶子字符串在 depth=11 被访问，depth > 10 为 true，应该失败
	var nested11 any = []any{"leaf"}
	for i := 0; i < 10; i++ {
		nested11 = []any{nested11}
	}
	_, err = callValues(nested11, 0)
	if err == nil {
		t.Error("11 levels should fail with recursion limit error")
	}
}

// panickingCallbackCommand 用于测试 callback panic
type panickingCallbackCommand struct {
	name string
}

func (c *panickingCallbackCommand) Definition() *console.Definition {
	return &console.Definition{
		Name:        c.name,
		Description: "panicking callback command",
	}
}

func (c *panickingCallbackCommand) Handle(ctx console.CommandContext) error {
	return nil
}
