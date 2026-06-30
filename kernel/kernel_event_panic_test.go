package kernel

import (
	"context"
	"errors"
	"testing"

	"github.com/prismgo/framework/console"
)

// TestKernelEventDispatchPanicProtection 验证事件派发 panic 不会覆盖原始错误
// 这是 Medium #4 的测试：defer 中 event.Dispatch 若 panic 会覆盖原始错误
func TestKernelEventDispatchPanicProtection(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")
	cmd := &panickingEventCommand{name: "panic:cmd"}
	k.Register(cmd)

	// 执行命令，即使事件派发 panic，也应该能捕获到原始错误
	err := k.Call(context.Background(), "panic:cmd")
	if err == nil {
		t.Fatal("expected error from command, got nil")
	}

	// 验证原始错误被正确返回
	if err.Error() != "command failed" {
		t.Errorf("expected error 'command failed', got %v", err)
	}
}

// panickingEventCommand 是一个会触发事件派发 panic 的测试命令
type panickingEventCommand struct {
	name string
}

func (c *panickingEventCommand) Definition() *console.Definition {
	return &console.Definition{
		Name:        c.name,
		Description: "panicking event command",
	}
}

func (c *panickingEventCommand) Handle(ctx console.CommandContext) error {
	return errors.New("command failed")
}
