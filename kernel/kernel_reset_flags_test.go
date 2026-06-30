package kernel

import (
	"context"
	"testing"

	"github.com/prismgo/framework/console"
)

// TestKernelResetFlagsPerformance 验证 resetCommandFlags 的性能
// 这是 Medium #3 的测试：resetCommandFlags 每次执行都递归整棵树，应该优化
func TestKernelResetFlagsPerformance(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")

	// 注册多个带选项的命令
	for i := 0; i < 50; i++ {
		name := string(rune('a'+i/26)) + string(rune('a'+i%26)) + "cmd"
		cmd := &commandWithOptions{
			name:    name,
			options: []string{"opt1", "opt2", "opt3"},
		}
		k.Register(cmd)
	}

	// 执行命令多次，验证 resetCommandFlags 不会成为性能瓶颈
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err := k.Call(ctx, "aacmd")
		if err != nil {
			t.Fatalf("call failed: %v", err)
		}
	}
}

// commandWithOptions 是一个带多个选项的测试命令
type commandWithOptions struct {
	name    string
	options []string
}

func (c *commandWithOptions) Definition() *console.Definition {
	def := &console.Definition{
		Name:        c.name,
		Description: "command with options",
	}
	for _, opt := range c.options {
		def.Options = append(def.Options, console.Option{
			Name:        opt,
			Description: "option " + opt,
		})
	}
	return def
}

func (c *commandWithOptions) Handle(ctx console.CommandContext) error {
	return nil
}
