// Package console 定义命令行系统可跨包依赖的公共契约。
//
// 本包声明命令、命令定义、命令互调、并发隔离和交互式 I/O 的公共接口。
// Cobra 绑定、Kernel 调度和参数解析由 prismgo/console 和 prismgo/kernel 实现包提供。
package console

import (
	"context"
	"os"
)

// OptionValueMode 描述命令选项是否需要携带值。
type OptionValueMode uint8

const (
	// OptionValueNone 表示选项不接收值，等价于布尔开关。
	OptionValueNone OptionValueMode = iota
	// OptionValueRequired 表示选项必须接收值。
	OptionValueRequired
	// OptionValueOptional 表示选项可以显式出现，但值可省略。
	OptionValueOptional
)

// Argument 描述一个位置参数的声明。
type Argument struct {
	Name         string
	Description  string
	Required     bool
	IsArray      bool
	DefaultValue *string
	Suggestions  []string
}

// Option 描述一个命令选项的声明。
type Option struct {
	Name         string
	Shortcut     string
	Description  string
	ValueMode    OptionValueMode
	IsArray      bool
	DefaultValue *string
	Suggestions  []string
}

// Definition 描述一个命令的完整静态定义。
type Definition struct {
	Name        string
	Description string
	Arguments   []Argument
	Options     []Option
	Aliases     []string
	Hidden      bool
	Examples    []string
	Help        string
	UsageText   string
}

// CallInput 描述结构化 programmatic command 调用输入。
type CallInput struct {
	Arguments map[string]any
	Options   map[string]any
}

// MissingArgumentPrompt 允许命令自定义缺失必填参数的交互式补问。
type MissingArgumentPrompt struct {
	Question string
	Default  string
	Ask      func(ctx CommandContext, argument Argument) (string, error)
}

// PromptsForMissingInput 是命令可选实现的缺参补问契约。
type PromptsForMissingInput interface {
	PromptForMissingArgumentsUsing() map[string]MissingArgumentPrompt
	AfterPromptingForMissingArguments(ctx CommandContext) error
}

// Input 提供统一的命令输入读取接口。
type Input interface {
	Argument(name string) string
	Arguments(name string) []string
	Option(name string) string
	OptionStrings(name string) []string
	OptionBool(name string) bool
	// OptionInt 读取整数型命令行选项，解析失败时返回错误。
	OptionInt(name string) (int, error)
	// HasOption 判断当前命令定义中是否存在该选项。
	HasOption(name string) bool
}

// Command 描述统一命令格式。
type Command interface {
	Definition() *Definition
	Handle(ctx CommandContext) error
}

// CommandFactory 用于延迟创建命令实例。
type CommandFactory func() Command

// CommandCaller 是命令互调能力的契约。
//
// 用途：让一个命令在执行过程中调用另一个已注册的命令。
// 命令通过 CommandContext.Call 和 CommandContext.CallSilently 使用此能力。
type CommandCaller interface {
	// Call 以当前上下文调用另一个命令。
	//
	// 参数 signature 是目标命令的签名（如 "area:sync"）；input 存在时 signature 应为命令名。
	Call(ctx context.Context, signature string, input ...CallInput) error

	// CallSilently 以静默方式调用另一个命令（不输出到控制台）。
	CallSilently(ctx context.Context, signature string, input ...CallInput) error
}

// Isolatable 是命令并发隔离的可选契约。
//
// 用途：命令实现此接口后，Kernel 在多个进程同时执行该命令时，
// 只允许一个实例进入关键区，防止定时任务重入或人工重复触发。
//
// 使用方式：
//
//	func (c *ExportCommand) IsolationKey(ctx console.CommandContext) string {
//	    return "export:" + ctx.Option("tenant")
//	}
type Isolatable interface {
	// IsolationKey 返回并发互斥的唯一标识。
	//
	// 同一时刻只有一个持有相同 key 的命令实例可以执行。
	IsolationKey(ctx CommandContext) string
}

// CommandContext 是命令运行期统一上下文。
//
// 用途：把原始 Cobra 命令、解析后的输入、统一 IO 与命令互调能力收敛到一个对象里，
// 供新风格命令直接消费。
//
// 主要能力：
//   - 参数读取：Argument/Option/OptionBool/OptionInt/HasOption
//   - IO 操作：IO() 返回统一 IO 接口
//   - 命令互调：Call/CallSilently 调用其他命令
//   - 信号处理：Trap 注册命令级信号处理器
//   - 错误处理：Fail 主动失败并返回可识别的错误
//
// 设计原因：减少命令实现对 Cobra 细节的直接依赖，提升复用性、可测试性与后续扩展性。
//
// 使用示例：
//
//	func (c *MyCommand) Handle(ctx console.CommandContext) error {
//	    name := ctx.Argument("name")
//	    count, err := ctx.OptionInt("count")
//	    if err != nil {
//	        return ctx.Fail("invalid count:", err)
//	    }
//	    ctx.IO().Info("Processing " + name)
//	    return nil
//	}
type CommandContext interface {
	// Context 返回标准 Go context。
	Context() context.Context

	// CommandName 返回当前命令名称。
	CommandName() string

	// Definition 返回当前命令定义快照。
	Definition() *Definition

	// Input 返回解析后的命令输入。
	Input() Input

	// IO 返回命令交互与输出接口。
	IO() IO

	// Call 以当前命令的 context 调用另一个命令。
	Call(signature string, input ...CallInput) error

	// CallSilently 以静默方式调用另一个命令。
	CallSilently(signature string, input ...CallInput) error

	// Fail 构造主动失败错误，供命令 return ctx.Fail(...) 使用。
	Fail(messageOrErr ...any) error

	// Trap 注册命令级 signal 回调；命令结束后 Kernel 自动释放。
	Trap(signals []os.Signal, callback func(os.Signal)) (func(), error)

	// Argument 读取单值位置参数。
	Argument(name string) string

	// Arguments 读取多值位置参数。
	Arguments(name string) []string

	// Option 读取字符串型命令行选项。
	Option(name string) string

	// OptionStrings 读取字符串数组型命令行选项。
	OptionStrings(name string) []string

	// OptionBool 读取布尔型命令行选项。
	OptionBool(name string) bool

	// OptionInt 读取整数型命令行选项，解析失败时返回错误。
	OptionInt(name string) (int, error)

	// HasOption 判断当前命令定义中是否存在该选项。
	HasOption(name string) bool
}

// IO 是命令行交互与输出的完整契约。
//
// 用途：向命令提供统一的输入询问和输出渲染能力，屏蔽底层终端细节。
// 命令作者使用此接口而非直接操作 stdin/stdout/stderr。
//
// 使用方式：
//
//	name, _ := ctx.IO().Ask("请输入名称", "默认值")
//	ctx.IO().Success("操作完成")
//	ctx.IO().Table([]string{"ID", "名称"}, [][]string{{"1", "张三"}})
type IO interface {
	// Line 输出一行消息。
	//
	// style 可选值与 Laravel/Symfony 传统输出对齐：
	// info、comment、question、success、warn/warning、error。
	// style 为空或未知时按无颜色普通文本输出。
	Line(message string, style ...string)

	// Info 输出普通信息消息（绿色）。
	Info(message string)

	// Comment 输出注释消息（黄色）。
	Comment(message string)

	// Question 输出问题消息（黑字青底）。
	Question(message string)

	// Success 输出成功消息（白字绿底）。
	Success(message string)

	// Warn 输出警告消息（黄色）。
	Warn(message string)

	// Error 输出错误消息（白字红底）。
	Error(message string)

	// Alert 输出 Laravel 风格的黄色警示块。
	Alert(message string)

	// Ask 询问用户输入文本。
	//
	// 参数 defaultValue 是可选默认值，用户直接回车时使用。
	Ask(question string, defaultValue ...string) (string, error)

	// Confirm 询问用户确认 yes/no。
	//
	// 参数 defaultYes 为 true 时默认答案为 yes。
	Confirm(question string, defaultYes bool) (bool, error)

	// Choice 让用户从选项列表中选择一项。
	//
	// 参数 defaultValue 是可选默认选项。
	Choice(question string, options []string, defaultValue ...string) (string, error)

	// ChoiceWithOptions 让用户选择单个或多个选项，并支持默认值和最大尝试次数。
	ChoiceWithOptions(question string, options []string, config ChoiceOptions) ([]string, error)

	// Anticipate 询问文本输入，设计意图是提供基于 choices 的自动补全。
	//
	// 当前实现限制：
	//   - TTY 和非 TTY 环境下降级为 Ask 行为，不实际提供自动补全 UI。
	//   - choices 参数在当前版本中未使用，仅为保持接口语义预留。
	//   - 未来可能引入 readline 或 prompt 依赖实现真正的交互式补全。
	//
	// 使用建议：
	//   - 如果需要严格的选项选择，请使用 Choice 或 ChoiceWithOptions。
	//   - 如果仅需文本输入，直接使用 Ask。
	//   - Anticipate 适合"提示性输入"场景，用户可输入任意值但期望特定格式。
	Anticipate(question string, choices []string, defaultValue ...string) (string, error)

	// NewLine 输出空行，count 省略时输出 1 行。
	NewLine(count ...int)

	// Secret 询问用户输入敏感信息（不回显输入内容）。
	Secret(question string) (string, error)

	// Table 渲染表格到终端。
	//
	// 参数 headers 是表头列表，rows 是数据行列表。
	Table(headers []string, rows [][]string) error

	// Progress 创建一个进度条。
	//
	// 参数 total 是总步数，为 0 时显示不确定进度。
	Progress(total int) Progress
}

// ChoiceOptions 配置 ChoiceWithOptions 的选择行为。
type ChoiceOptions struct {
	Multiple bool
	Defaults []string
	Attempts int
}

// Progress 是进度条的契约。
//
// 使用方式：
//
//	bar := ctx.IO().Progress(100)
//	for i := 0; i < 100; i++ {
//	    doStep()
//	    bar.Advance(1)
//	}
//	bar.Finish()
type Progress interface {
	// Advance 推进指定步数。
	Advance(step int)

	// Finish 完成进度条并输出最终状态。
	Finish()
}
