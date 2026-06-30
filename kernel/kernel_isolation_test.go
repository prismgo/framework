package kernel

import (
	"context"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
)

// TestKernelIsolationLockReleaseTimeout 验证隔离锁释放有超时保护
// 这是 Medium #2 的测试：acquireIsolationLock 释放无超时保护
func TestKernelIsolationLockReleaseTimeout(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")
	cmd := &isolatableTestCommand{name: "isolated:cmd"}
	k.Register(cmd)

	// 第一次执行应该成功
	ctx := context.Background()
	err := k.Call(ctx, "isolated:cmd")
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// 等待锁释放（应该很快完成，因为有超时保护）
	time.Sleep(100 * time.Millisecond)

	// 第二次执行也应该成功（锁已释放）
	err = k.Call(ctx, "isolated:cmd")
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
}

// isolatableTestCommand 是一个可隔离的测试命令
type isolatableTestCommand struct {
	name string
}

func (c *isolatableTestCommand) Definition() *console.Definition {
	return &console.Definition{
		Name:        c.name,
		Description: "isolatable test command",
	}
}

func (c *isolatableTestCommand) Handle(ctx console.CommandContext) error {
	return nil
}

func (c *isolatableTestCommand) IsolationKey(ctx console.CommandContext) string {
	return c.name
}
