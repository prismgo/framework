package kernel

import (
	"context"
	"strings"
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

// stringArrayCommand 测试 stringArray 选项在多次调用间正确重置。
// 回归测试 #20260710：resetCommandFlags 对 stringArray 调用 Set("[]") 会把字面值
// "[]" 追加为数组元素，导致选项值在多次执行间泄漏。
type stringArrayCommand struct {
	values []string
}

func (c *stringArrayCommand) Definition() *console.Definition {
	return console.MustDefinition("array:reset {--item=*} {--flag}", "test stringArray reset")
}

func (c *stringArrayCommand) Handle(ctx console.CommandContext) error {
	c.values = ctx.Input().OptionStrings("item")
	return nil
}

// TestResetCommandFlagsStringArray 验证 stringArray 选项在多次调用间正确重置。
func TestResetCommandFlagsStringArray(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")
	cmd := &stringArrayCommand{}
	k.Register(cmd)

	// 第一次调用：传入两个值
	ctx := context.Background()
	err := k.Call(ctx, "array:reset --item=a --item=b")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got := strings.Join(cmd.values, ","); got != "a,b" {
		t.Fatalf("first call values = %q, want a,b", got)
	}

	// 第二次调用：不传 --item，应重置为空
	err = k.Call(ctx, "array:reset")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if len(cmd.values) != 0 {
		t.Fatalf("second call values = %q, want empty (got %d elements)", cmd.values, len(cmd.values))
	}

	// 第三次调用：传一个不同的值
	err = k.Call(ctx, "array:reset --item=x --item=y --item=z")
	if err != nil {
		t.Fatalf("third call: %v", err)
	}
	if got := strings.Join(cmd.values, ","); got != "x,y,z" {
		t.Fatalf("third call values = %q, want x,y,z", got)
	}
}
