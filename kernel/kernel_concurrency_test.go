package kernel

import (
	"context"
	"sync"
	"testing"

	"github.com/prismgo/framework/console"
)

// TestKernelConcurrentRegisterAndCommands 验证并发注册和读取命令列表不会 panic
// 这是 Critical #1 的测试：k.commands map 和 k.commandList slice 的并发读写
func TestKernelConcurrentRegisterAndCommands(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")

	var wg sync.WaitGroup
	// 并发注册不同命令
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := string(rune('a'+idx)) + "cmd"
			cmd := &simpleTestCommand{name: name}
			k.Register(cmd)
		}(i)
	}

	// 并发读取 Commands()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = k.Commands()
		}()
	}

	wg.Wait()

	// 验证所有命令都已注册
	cmds := k.Commands()
	if len(cmds) < 10 {
		t.Errorf("expected at least 10 commands, got %d", len(cmds))
	}
}

// simpleTestCommand 是一个简单的测试命令实现
type simpleTestCommand struct {
	name string
}

func (c *simpleTestCommand) Definition() *console.Definition {
	return &console.Definition{
		Name:        c.name,
		Description: "test command",
	}
}

func (c *simpleTestCommand) Handle(ctx console.CommandContext) error {
	return nil
}

// TestKernelConcurrentRunContext 验证并发 RunContextArgv 不会 panic
// 这是 High #1 的测试：RunContext 中 resetCommandFlags 与 ExecuteContext 数据竞争
func TestKernelConcurrentRunContext(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")
	k.Register(&simpleTestCommand{name: "cmd1"})

	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := k.RunContextArgv(context.Background(), []string{"test", "cmd1"}); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent RunContext failed: %v", err)
	}
}
