package console

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"

	consolecontract "github.com/prismgo/framework/contracts/console"
	"github.com/prismgo/framework/routine"
	"github.com/spf13/cobra"
)

// CommandCaller 定义命令互调所需的最小能力。
type CommandCaller = consolecontract.CommandCaller

// Isolatable 允许命令声明自己的并发互斥 key。
type Isolatable = consolecontract.Isolatable

// IsolationContext 是隔离 key 生成时暴露给命令的只读上下文。
type IsolationContext = consolecontract.CommandContext

// CommandContext 是命令运行期统一上下文。
type CommandContext = consolecontract.CommandContext

// CommandContext 是命令运行期统一上下文。
//
// 用途：把原始 Cobra 命令、解析后的输入、统一 IO 与命令互调能力收敛到一个对象里，
// 供新风格命令直接消费。
// 设计原因：减少命令实现对 Cobra 细节的直接依赖，提升复用性、可测试性与后续扩展性。
type runtimeCommandContext struct {
	baseContext context.Context
	command     Command
	definition  Definition
	input       Input
	io          IO
	caller      CommandCaller
	cobra       *cobra.Command
	trapMu      sync.Mutex
	trapRelease []func()
}

type commandContextKey struct{}

// NewCommandContext 构造命令运行期上下文。
func NewCommandContext(ctx context.Context, command Command, definition Definition, input Input, io IO, caller CommandCaller, cobraCmd *cobra.Command) CommandContext {
	if ctx == nil {
		ctx = context.Background()
	}
	commandCtx := &runtimeCommandContext{
		baseContext: ctx,
		command:     command,
		definition:  definition,
		input:       input,
		io:          io,
		caller:      caller,
		cobra:       cobraCmd,
	}
	commandCtx.baseContext = WithContext(ctx, commandCtx)
	return commandCtx
}

// WithContext 把 CommandContext 放入标准 context。
func WithContext(parent context.Context, commandCtx CommandContext) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithValue(parent, commandContextKey{}, commandCtx)
}

// FromContext 从标准 context 中提取 CommandContext。
func FromContext(ctx context.Context) (CommandContext, bool) {
	if ctx == nil {
		return nil, false
	}
	commandCtx, ok := ctx.Value(commandContextKey{}).(CommandContext)
	return commandCtx, ok
}

// FromCommand 从 Cobra 命令中提取 CommandContext。
func FromCommand(cmd *cobra.Command) (CommandContext, bool) {
	if cmd == nil {
		return nil, false
	}
	return FromContext(cmd.Context())
}

// MustFromCommand 从 Cobra 命令中提取 CommandContext；若上下文缺失则返回错误。
func MustFromCommand(cmd *cobra.Command) (CommandContext, error) {
	commandCtx, ok := FromCommand(cmd)
	if !ok {
		return nil, fmt.Errorf("console command context is not available")
	}
	return commandCtx, nil
}

type cobraContext interface {
	CobraCommand() *cobra.Command
}

// CobraCommand 返回运行时绑定的 Cobra 命令；非 console runtime context 返回 nil。
func CobraCommand(commandCtx CommandContext) *cobra.Command {
	if commandCtx == nil {
		return nil
	}
	withCobra, ok := commandCtx.(cobraContext)
	if !ok {
		return nil
	}
	return withCobra.CobraCommand()
}

// Call 以当前命令的 context 调用另一个命令。
func (c *runtimeCommandContext) Call(signature string, input ...CallInput) error {
	if c == nil || c.caller == nil {
		return fmt.Errorf("console command caller is not configured")
	}
	return c.caller.Call(c.Context(), signature, input...)
}

// CallSilently 以静默方式调用另一个命令。
func (c *runtimeCommandContext) CallSilently(signature string, input ...CallInput) error {
	if c == nil || c.caller == nil {
		return fmt.Errorf("console command caller is not configured")
	}
	return c.caller.CallSilently(c.Context(), signature, input...)
}

func (c *runtimeCommandContext) Fail(messageOrErr ...any) error {
	return Fail(messageOrErr...)
}

func (c *runtimeCommandContext) Trap(signals []os.Signal, callback func(os.Signal)) (func(), error) {
	if c == nil {
		return nil, fmt.Errorf("console trap: command context is nil")
	}
	if len(signals) == 0 {
		return nil, fmt.Errorf("console trap: at least one signal is required")
	}
	if callback == nil {
		return nil, fmt.Errorf("console trap: callback is nil")
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, signals...)
	done := make(chan struct{})
	var once sync.Once
	release := func() {
		once.Do(func() {
			signal.Stop(ch)
			close(ch)
			close(done)
		})
	}
	routine.Task(c.Context(), func(ctx context.Context) error {
		for {
			select {
			case sig := <-ch:
				callback(sig)
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}).
		Component("console").
		Name("command.trap").
		Fields(map[string]any{
			"command": c.CommandName(),
			"signals": signals,
		}).
		Go()
	c.trapMu.Lock()
	c.trapRelease = append(c.trapRelease, release)
	c.trapMu.Unlock()
	return release, nil
}

// ReleaseTraps 释放运行期上下文注册的所有命令级 signal trap。
func ReleaseTraps(commandCtx CommandContext) {
	runtimeCtx, ok := commandCtx.(*runtimeCommandContext)
	if !ok || runtimeCtx == nil {
		return
	}
	runtimeCtx.trapMu.Lock()
	releases := append([]func(){}, runtimeCtx.trapRelease...)
	runtimeCtx.trapRelease = nil
	runtimeCtx.trapMu.Unlock()
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i] != nil {
			releases[i]()
		}
	}
}

// Context 返回当前命令绑定的标准 Go context。
func (c *runtimeCommandContext) Context() context.Context {
	if c == nil || c.baseContext == nil {
		return context.Background()
	}
	return c.baseContext
}

// CommandName 返回当前命令名称。
func (c *runtimeCommandContext) CommandName() string {
	if c == nil {
		return ""
	}
	return c.definition.Name
}

func (c *runtimeCommandContext) Definition() *Definition {
	if c == nil {
		return nil
	}
	definition := CloneDefinition(c.definition)
	return &definition
}

func (c *runtimeCommandContext) Input() Input {
	if c == nil {
		return nil
	}
	return c.input
}

func (c *runtimeCommandContext) IO() IO {
	if c == nil {
		return nil
	}
	return c.io
}

func (c *runtimeCommandContext) CobraCommand() *cobra.Command {
	if c == nil {
		return nil
	}
	return c.cobra
}

func (c *runtimeCommandContext) Argument(name string) string {
	if c == nil || c.input == nil {
		return ""
	}
	return c.input.Argument(name)
}

func (c *runtimeCommandContext) Arguments(name string) []string {
	if c == nil || c.input == nil {
		return nil
	}
	return c.input.Arguments(name)
}

func (c *runtimeCommandContext) Option(name string) string {
	if c == nil || c.input == nil {
		return ""
	}
	return c.input.Option(name)
}

func (c *runtimeCommandContext) OptionStrings(name string) []string {
	if c == nil || c.input == nil {
		return nil
	}
	return c.input.OptionStrings(name)
}

func (c *runtimeCommandContext) OptionBool(name string) bool {
	if c == nil || c.input == nil {
		return false
	}
	return c.input.OptionBool(name)
}

func (c *runtimeCommandContext) OptionInt(name string) (int, error) {
	if c == nil || c.input == nil {
		return 0, nil
	}
	return c.input.OptionInt(name)
}

func (c *runtimeCommandContext) HasOption(name string) bool {
	if c == nil || c.input == nil {
		return false
	}
	return c.input.HasOption(name)
}
