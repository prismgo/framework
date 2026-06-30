// Package kernel 提供 Laravel 风格的 CLI Kernel，用于管理命令注册、定时调度与 CLI 分派。
// 仅依赖 prismgo/console 与 prismgo/timer，零业务耦合，可直接在任意项目中复用。
package kernel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/event"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/timer"
	"github.com/prismgo/framework/version"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const rawInputAnnotation = "prismgo.raw_input"
const promptedArgsAnnotation = "prismgo.prompted_args"

// ClosureHandler 描述闭包命令的执行函数。
type ClosureHandler func(console.CommandContext) error

// Kernel 管理所有命令的注册、定时调度与 CLI 调度，作为应用的核心引擎。
//
// 用途：统一持有根命令、命令注册表与调度器，并在注册阶段完成 Definition 校验、
// Cobra 绑定、help/list 展示增强、命令互调与运行时上下文注入。
// 设计原因：所有命令都使用同一套 Definition + Run 模型后，内置命令注册流程会更简洁，
// 新增能力时也不需要再散落修改业务装配代码。
//
// 线程安全说明：Kernel 的注册方法（Register、RegisterLazy、RegisterClosure、ResolveCommand）
// 和读取方法（Commands、All）使用 RWMutex 保护，支持并发访问。RunContext、Call、CallSilently
// 等执行方法使用 execMu 保护，确保同一时刻只有一个 goroutine 执行命令。
type Kernel struct {
	rootCmd                 *cobra.Command
	schedule                *timer.Schedule
	application             ApplicationRegistrySource
	mu                      sync.RWMutex // 保护 commands 和 commandList
	commands                map[string]registeredCommand
	commandList             []registeredCommand
	execMu                  sync.RWMutex // 保护命令执行：RunContext 用 Lock，Call/CallSilently 用 RLock
	startingMu              sync.Mutex
	starting                []StartingCallback
	startingState           startingState
	startingWait            chan struct{}
	startingEventDispatched bool
}

// Option 用于定制 Kernel 初始化行为。
type Option func(*Kernel)

// startingState 描述当前 Kernel 上 starting callbacks 的生命周期状态。
//
// 设计思路：相比单个布尔值，这里显式区分“待执行”“执行中”“已成功完成”，让失败重试与并发等待的
// 语义更清晰，避免出现半初始化状态。
type startingState uint8

const (
	// startingStatePending 表示 starting callbacks 尚未成功完成，后续调用仍可继续尝试。
	startingStatePending startingState = iota
	// startingStateRunning 表示当前已有调用正在执行 starting callbacks，其他调用方需要等待本轮完成。
	startingStateRunning
	// startingStateSucceeded 表示 starting callbacks 已全部成功完成，后续调用不再重复执行。
	startingStateSucceeded
)

type registeredCommand struct {
	definition console.Definition
	command    console.Command
}

type closureCommand struct {
	definition console.Definition
	handler    ClosureHandler
}

// New 创建根命令，name 用于帮助信息输出。
func New(name string, options ...Option) *Kernel {
	k := &Kernel{
		schedule: timer.NewSchedule(),
		commands: make(map[string]registeredCommand),
	}
	k.boot(name, options...)
	return k
}

func (k *Kernel) boot(name string, options ...Option) {
	k.rootCmd = newRootCobraCommand(name)
	k.rootCmd.SetOut(os.Stdout)
	k.rootCmd.SetErr(os.Stderr)
	k.schedule.SetResolver(k.resolveScheduledCommand)
	k.installHelp()
	k.registerCoreCommands()
	k.applyOptions(options...)
}

func (k *Kernel) applyOptions(options ...Option) {
	for _, option := range options {
		if option != nil {
			option(k)
		}
	}
}

func newRootCobraCommand(name string) *cobra.Command {
	definition, err := console.NormalizeDefinition(rootDefinition(name))
	if err != nil {
		panic(err)
	}
	cmd := &cobra.Command{
		Use:   definition.Name,
		Short: definition.Description,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if versionRequested(cmd) {
				return renderVersion(cmd)
			}
			return console.RenderCommandList(cmd.OutOrStdout(), collectDefinitions(cmd), console.CommandListOptions{
				AppName:     cmd.CommandPath(),
				Description: cmd.Short,
				Output:      outputOptionsFromCobra(cmd),
			})
		},
	}
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetVersionTemplate(version.Banner() + "\n")
	flags := cmd.PersistentFlags()
	flags.Bool("ansi", false, "Force ANSI output")
	flags.Bool("no-ansi", false, "Disable ANSI output")
	flags.Bool("no-interaction", false, "Do not ask any interactive question")
	flags.BoolP("quiet", "q", false, "Do not output any message")
	flags.Bool("silent", false, "Do not output any message")
	flags.BoolP("version", "V", false, "Display this application version")
	flags.CountP("verbose", "v", "Increase the verbosity of messages")
	return cmd
}

func (k *Kernel) rootCommandName() string {
	if k == nil || k.rootCmd == nil {
		return ""
	}
	return k.rootCmd.Use
}

func rootDefinition(name string) console.Definition {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "app"
	}
	return console.Definition{
		Name:        name,
		Description: version.Banner(),
	}
}

// Register 批量注册命令实现，并统一完成 Definition 编译与 Cobra 绑定。
//
// 错误处理：遇到重复命令名、无效定义等错误时会 panic。这是有意为之的设计：
// Register 通常在应用启动阶段调用，此时错误代表编程错误（如命令名冲突），
// 应该立即失败以便开发者修复，而不是在运行时静默忽略。
// 如果需要在运行时动态注册命令并处理错误，请使用 ResolveCommand。
func (k *Kernel) Register(cmds ...console.Command) {
	k.registerCommands(cmds...)
}

// RegisterLazy 追加一组命令构造函数，供业务装配层把内置命令定义与注册逻辑保持整洁。
//
// 错误处理：与 Register 相同，遇到错误时会 panic。参见 Register 的设计说明。
func (k *Kernel) RegisterLazy(factories ...console.CommandFactory) {
	for _, factory := range factories {
		if factory == nil {
			continue
		}
		k.registerCommands(factory())
	}
}

// ResolveCommand 将显式命令实例或命令工厂解析并注册到当前 Kernel。
//
// 用途：提供 Laravel resolveCommands 风格的延迟命令挂载能力，供 console starting
// callback、可选模块或测试在当前 Kernel 上注册命令。
// 参数说明：value 支持 console.Command 或 console.CommandFactory；不支持字符串类名、
// Go 文件扫描或路径 discovery。
// 逻辑说明：该入口复用现有 Definition 校验和重复名称/alias 校验，但把 panic 转换为 error，
// 让生命周期主路径可以返回明确失败原因，而不是静默跳过或直接中断进程。
func (k *Kernel) ResolveCommand(value any) (err error) {
	if k == nil {
		return fmt.Errorf("kernel resolve command: kernel is nil")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("kernel resolve command: %v", recovered)
		}
	}()

	switch command := value.(type) {
	case nil:
		return fmt.Errorf("kernel resolve command: command is nil")
	case console.Command:
		k.mustRegister(command)
	case console.CommandFactory:
		if command == nil {
			return fmt.Errorf("kernel resolve command: factory is nil")
		}
		resolved := command()
		if resolved == nil {
			return fmt.Errorf("kernel resolve command: factory returned nil command")
		}
		k.mustRegister(resolved)
	default:
		return fmt.Errorf("kernel resolve command: unsupported type %T", value)
	}
	return nil
}

// ResolveCommands 批量解析并注册显式命令实例或命令工厂。
//
// 设计思路：保持小而显式的 API 表面，调用方可以混合传入 console.Command 与
// console.CommandFactory；任一项失败时立即返回错误，避免目标 CLI 命令在注册不完整时继续执行。
func (k *Kernel) ResolveCommands(values ...any) error {
	for _, value := range values {
		if err := k.ResolveCommand(value); err != nil {
			return err
		}
	}
	return nil
}

// RegisterClosure 注册一个闭包命令。
func (k *Kernel) RegisterClosure(definition console.Definition, handler ClosureHandler) {
	if handler == nil {
		panic("kernel register closure: handler is nil")
	}
	k.mustRegister(&closureCommand{definition: console.CloneDefinition(definition), handler: handler})
}

// Schedule 返回调度器实例，用于注册定时任务。
func (k *Kernel) Schedule() *timer.Schedule {
	return k.schedule
}

// Start 启动所有已注册的定时任务。
func (k *Kernel) Start(ctx context.Context) {
	k.schedule.Start(ctx)
}

// Stop 停止所有定时任务并等待退出。
func (k *Kernel) Stop() {
	k.schedule.Stop()
}

// Run 解析命令行参数并执行匹配的子命令。
func (k *Kernel) Run() error {
	return k.RunContext(context.Background())
}

// RunContext 使用指定 context 解析命令行参数并执行匹配的子命令。
//
// 线程安全：使用 execMu 保护，确保同一时刻只有一个 goroutine 执行命令。
func (k *Kernel) RunContext(ctx context.Context) error {
	return k.executeWithContext(ctx, nil)
}

// RunContextArgv 使用完整 argv（包含程序名）解析并执行命令。
//
// 参数说明：argv 应保持与 os.Args 相同的结构，首项是程序名，后续项才是命令名和参数。
// 线程安全：使用 execMu 保护，确保同一时刻只有一个 goroutine 执行命令。
func (k *Kernel) RunContextArgv(ctx context.Context, argv []string) error {
	if k == nil || k.rootCmd == nil {
		return fmt.Errorf("kernel run: kernel is not initialized")
	}
	return k.executeWithContext(ctx, argv)
}

// executeWithContext 是 RunContext 和 RunContextArgv 的公共实现。
// 如果 argv 为 nil，使用 rootCmd 当前的 args；否则设置新的 args。
func (k *Kernel) executeWithContext(ctx context.Context, argv []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := k.runStartingCallbacks(ctx); err != nil {
		return err
	}
	k.execMu.Lock()
	defer k.execMu.Unlock()
	if argv != nil {
		k.rootCmd.SetArgs(normalizeCommandArgs(argv, true))
	}
	resetCommandFlags(k.rootCmd)
	return k.rootCmd.ExecuteContext(ctx)
}

// Call 执行一个已注册命令，并将输出透传到当前 stdout/stderr。
func (k *Kernel) Call(ctx context.Context, signature string, input ...console.CallInput) error {
	return k.executeCall(ctx, signature, false, input...)
}

// CallSilently 执行一个已注册命令，但屏蔽其标准输出与错误输出。
func (k *Kernel) CallSilently(ctx context.Context, signature string, input ...console.CallInput) error {
	return k.executeCall(ctx, signature, true, input...)
}

// Commands 返回已注册命令的定义快照，便于 list/help/测试复用。
func (k *Kernel) Commands() []console.Definition {
	k.mu.RLock()
	defer k.mu.RUnlock()
	definitions := make([]console.Definition, 0, len(k.commandList))
	for _, registered := range k.commandList {
		definitions = append(definitions, console.CloneDefinition(registered.definition))
	}
	return definitions
}

// All 启动 Console application 并返回所有已注册命令的定义快照。
//
// 用途：对齐 Laravel Artisan::all() / Console Kernel::all() 的枚举语义，调用前先运行
// starting callbacks，让延迟注册的命令也出现在结果中。返回值包含 hidden commands，
// 调用方需要展示过滤时应自行按 Definition.Hidden 处理。
func (k *Kernel) All() ([]console.Definition, error) {
	if err := k.runStartingCallbacks(context.Background()); err != nil {
		return nil, err
	}
	return k.Commands(), nil
}

func (k *Kernel) registerCommands(cmds ...console.Command) {
	for _, cmd := range cmds {
		k.mustRegister(cmd)
	}
}

func (k *Kernel) mustRegister(cmd console.Command) {
	if cmd == nil {
		panic("kernel register: command is nil")
	}
	rawDefinition := cmd.Definition()
	if rawDefinition == nil {
		panic("kernel register: command definition is nil")
	}
	definition, err := console.NormalizeDefinition(*rawDefinition)
	if err != nil {
		panic(err)
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	if _, exists := k.commands[definition.Name]; exists {
		panic(fmt.Sprintf("kernel register: command %q already registered", definition.Name))
	}
	for _, alias := range definition.Aliases {
		if _, exists := k.commands[alias]; exists {
			panic(fmt.Sprintf("kernel register: alias %q already registered", alias))
		}
	}

	registered := registeredCommand{definition: definition, command: cmd}
	cobraCmd := k.buildCobraCommand(registered)
	k.commands[definition.Name] = registered
	for _, alias := range definition.Aliases {
		k.commands[alias] = registered
	}
	k.commandList = append(k.commandList, registered)
	sort.SliceStable(k.commandList, func(i, j int) bool {
		return k.commandList[i].definition.Name < k.commandList[j].definition.Name
	})
	// 线程安全说明：rootCmd.AddCommand 不需要额外锁保护。
	// Kernel 是单例模式，注册阶段（Register/RegisterLazy/RegisterClosure）在应用启动时串行执行，
	// 此时不会有命令在并发执行。命令执行阶段（RunContext/Call）通过 execMu 保护，
	// 但注册阶段已完成，rootCmd 的命令树已固定。
	k.rootCmd.AddCommand(cobraCmd)
}

func (k *Kernel) buildCobraCommand(registered registeredCommand) *cobra.Command {
	definition := console.CloneDefinition(registered.definition)
	cobraCmd := &cobra.Command{
		Use:     console.DefinitionUsage(definition),
		Short:   definition.Description,
		Long:    definition.Help,
		Aliases: append([]string(nil), definition.Aliases...),
		Hidden:  definition.Hidden,
		Example: strings.Join(definition.Examples, "\n"),
		Args: func(cmd *cobra.Command, args []string) error {
			if versionRequested(cmd) {
				return nil
			}
			resolvedArgs, err := k.promptMissingArguments(cmd.Context(), registered, cmd, args)
			if err != nil {
				return err
			}
			if len(resolvedArgs) != len(args) {
				if cmd.Annotations == nil {
					cmd.Annotations = map[string]string{}
				}
				cmd.Annotations[promptedArgsAnnotation] = strings.Join(resolvedArgs, "\x00")
			}
			return k.argumentValidator(definition)(cmd, resolvedArgs)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if versionRequested(cmd) {
				return renderVersion(cmd)
			}
			if cmd.Annotations != nil && cmd.Annotations[promptedArgsAnnotation] != "" {
				args = strings.Split(cmd.Annotations[promptedArgsAnnotation], "\x00")
			}
			return k.executeRegisteredCommand(cmd.Context(), registered, cmd, args)
		},
	}
	// 设计思路：Cobra 命令本身不保存 Prismgo Definition 的参数描述，
	// 因此在注册阶段把已规范化的 arguments 闭包给 help renderer。
	cobraCmd.SetHelpFunc(func(cmd *cobra.Command, _ []string) {
		renderCommandHelpWithArguments(cmd, definition.Arguments)
	})
	if len(definition.Options) > 0 {
		if err := console.BindDefinitionFlags(cobraCmd, definition); err != nil {
			panic(err)
		}
	}
	if _, ok := registered.command.(console.Isolatable); ok {
		cobraCmd.Flags().String("isolated", "", "Do not run if another instance of the command is already running")
		_ = cobraCmd.Flags().Lookup("isolated")
		if flag := cobraCmd.Flags().Lookup("isolated"); flag != nil {
			flag.NoOptDefVal = "0"
		}
	}
	registerCompletionHooks(cobraCmd, definition)
	return cobraCmd
}

func (k *Kernel) argumentValidator(definition console.Definition) func(*cobra.Command, []string) error {
	return func(_ *cobra.Command, args []string) error {
		minimum := 0
		maximum := len(definition.Arguments)
		hasArray := false
		for _, argument := range definition.Arguments {
			if argument.Required {
				minimum++
			}
			if argument.IsArray {
				hasArray = true
			}
		}
		if len(definition.Arguments) == 0 && len(args) == 0 {
			return nil
		}
		if hasArray {
			if len(args) < minimum {
				return fmt.Errorf("accepts at least %d arg(s), received %d", minimum, len(args))
			}
			return nil
		}
		if len(args) < minimum || len(args) > maximum {
			return fmt.Errorf("accepts between %d and %d arg(s), received %d", minimum, maximum, len(args))
		}
		return nil
	}
}

func (k *Kernel) executeRegisteredCommand(ctx context.Context, registered registeredCommand, cobraCmd *cobra.Command, args []string) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	commandCtx, cmdIO, rawInput, start := k.prepareCommandExecution(ctx, registered, cobraCmd, args)
	defer console.ReleaseTraps(commandCtx)

	event.Dispatch(commandCtx.Context(), event.CommandStarting{Command: registered.definition.Name, Input: rawInput})
	defer k.dispatchFinishedEvent(commandCtx, registered, rawInput, start, &err)

	if isolatable, ok := registered.command.(console.Isolatable); ok && isolatedOptionRequested(cobraCmd) {
		var release func()
		release, err = acquireIsolationLock(isolatable, commandCtx)
		if err != nil {
			cmdIO.Error(err.Error())
			return err
		}
		defer release()
	}

	err = registered.command.Handle(commandCtx)
	if err != nil {
		k.handleCommandError(commandCtx, cobraCmd, cmdIO, registered, rawInput, start, err)
	}
	return err
}

func (k *Kernel) prepareCommandExecution(ctx context.Context, registered registeredCommand, cobraCmd *cobra.Command, args []string) (console.CommandContext, console.IO, []string, time.Time) {
	input := console.NewInput(registered.definition, cobraCmd, args)
	out := cobraCmd.OutOrStdout()
	errOut := cobraCmd.ErrOrStderr()
	outputOptions := outputOptionsFromCobra(cobraCmd)
	if outputOptions.Quiet || outputOptions.Silent {
		out = io.Discard
		errOut = io.Discard
	}
	cmdIO := console.NewIOWithOutputOptions(cobraCmd.InOrStdin(), out, errOut, outputOptions)
	commandCtx := console.NewCommandContext(ctx, registered.command, registered.definition, input, cmdIO, k, cobraCmd)
	cobraCmd.SetContext(commandCtx.Context())
	rawInput := commandInputSnapshot(cobraCmd, registered.definition.Name, args)
	start := time.Now()
	return commandCtx, cmdIO, rawInput, start
}

func (k *Kernel) dispatchFinishedEvent(commandCtx console.CommandContext, registered registeredCommand, rawInput []string, start time.Time, err *error) {
	if rec := recover(); rec != nil {
		*err = fmt.Errorf("panic recovered: %v", rec)
		goexception.Report(commandCtx.Context(), *err, map[string]any{
			"command":     registered.definition.Name,
			"input":       strings.Join(rawInput, " "),
			"duration_ms": time.Since(start).Milliseconds(),
			"status":      500,
			"message":     "command panic",
			"component":   "cli",
		})
	}

	// 保护事件派发，防止 panic 覆盖原始错误
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				// 记录事件派发的 panic，但不影响主流程
				goexception.Report(commandCtx.Context(), fmt.Errorf("event dispatch panic: %v", rec), map[string]any{
					"command":   registered.definition.Name,
					"component": "cli",
					"event":     "CommandFinished",
				})
			}
		}()
		event.Dispatch(commandCtx.Context(), event.CommandFinished{
			Command:   registered.definition.Name,
			Succeeded: *err == nil,
			Error:     errorSummary(*err),
			Duration:  time.Since(start),
		})
	}()
}

func (k *Kernel) handleCommandError(commandCtx console.CommandContext, cobraCmd *cobra.Command, cmdIO console.IO, registered registeredCommand, rawInput []string, start time.Time, err error) {
	if failed, ok := console.IsManualFailure(err); ok {
		cmdIO.Error(failed.Error())
		return
	}
	if errors.Is(err, context.Canceled) {
		cobraCmd.SilenceUsage = true
		cobraCmd.SilenceErrors = true
		return
	}
	goexception.Report(commandCtx.Context(), err, map[string]any{
		"command":     registered.definition.Name,
		"input":       strings.Join(rawInput, " "),
		"duration_ms": time.Since(start).Milliseconds(),
		"status":      500,
		"component":   "cli",
	})
}

// resolveScheduledCommand 为定时任务注册阶段提供命令解析语义。
//
// 设计思路：调度器在注册任务时就需要拿到可执行闭包，但 starting 动态命令只有在首次运行前才会挂载。
// 因此这里对“尚未成功完成 starting 的命令”保留一次延迟解析机会，其余未知命令仍保持启动期尽早失败。
func (k *Kernel) resolveScheduledCommand(name string, args []string) (timer.ResolvedCommand, error) {
	resolved, err := k.resolveRegisteredCommand(name, args)
	if err == nil {
		return resolved, nil
	}
	if !k.hasPendingStartingCallbacks() {
		return timer.ResolvedCommand{}, err
	}
	return timer.ResolvedCommand{
		Description: name,
		Fn: func(ctx context.Context) error {
			if startErr := k.runStartingCallbacks(ctx); startErr != nil {
				return startErr
			}
			resolvedCommand, resolveErr := k.resolveRegisteredCommand(name, args)
			if resolveErr != nil {
				return resolveErr
			}
			return resolvedCommand.Fn(ctx)
		},
	}, nil
}

// resolveRegisteredCommand 只针对当前已经完成注册的命令构造调度器执行闭包。
//
// 用途：把“是否允许触发 starting”与“如何执行已知命令”两个职责拆开，避免不同调用路径混用同一套
// 生命周期判断逻辑。
func (k *Kernel) resolveRegisteredCommand(name string, args []string) (timer.ResolvedCommand, error) {
	k.mu.RLock()
	registered, ok := k.commands[name]
	k.mu.RUnlock()
	if !ok {
		return timer.ResolvedCommand{}, fmt.Errorf("command %q not registered", name)
	}
	fn := func(ctx context.Context) error {
		cobraCmd := k.buildCobraCommand(registered)
		cobraCmd.SetArgs(args)
		return cobraCmd.ExecuteContext(ctx)
	}
	return timer.ResolvedCommand{Fn: fn, Description: registered.definition.Description}, nil
}

func (k *Kernel) executeCall(ctx context.Context, signature string, silent bool, input ...console.CallInput) error {
	callInput, ok, err := optionalCallInput(input)
	if err != nil {
		return err
	}
	if ok {
		return k.executeCallInput(ctx, signature, callInput, silent)
	}
	return k.executeSignature(ctx, signature, silent)
}

func optionalCallInput(input []console.CallInput) (console.CallInput, bool, error) {
	switch len(input) {
	case 0:
		return console.CallInput{}, false, nil
	case 1:
		return input[0], true, nil
	default:
		return console.CallInput{}, false, fmt.Errorf("kernel call: expected at most one CallInput, got %d", len(input))
	}
}

func (k *Kernel) executeSignature(ctx context.Context, signature string, silent bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := k.runStartingCallbacks(ctx); err != nil {
		return err
	}
	parts, err := splitSignature(signature)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return fmt.Errorf("kernel call: empty signature")
	}
	k.mu.RLock()
	registered, ok := k.commands[parts[0]]
	k.mu.RUnlock()
	if !ok {
		return fmt.Errorf("command %q not registered", parts[0])
	}
	// 不需要 execMu：buildCobraCommand 创建新对象，configureProgrammaticIO 只读取 rootCmd 的 IO 方法，
	// 这些方法在命令执行期间不会改变，mustRegister 在注册阶段调用，此时不会有命令在执行
	cobraCmd := k.buildCobraCommand(registered)
	cobraCmd.Annotations = map[string]string{rawInputAnnotation: strings.Join(parts, "\x00")}
	cobraCmd.SetArgs(parts[1:])
	k.configureProgrammaticIO(ctx, cobraCmd, silent)
	return cobraCmd.ExecuteContext(ctx)
}

func (k *Kernel) executeCallInput(ctx context.Context, name string, input console.CallInput, silent bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := k.runStartingCallbacks(ctx); err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("kernel call: empty command name")
	}
	k.mu.RLock()
	registered, ok := k.commands[name]
	k.mu.RUnlock()
	if !ok {
		return fmt.Errorf("command %q not registered", name)
	}
	args, err := encodeCallInput(registered.definition, input)
	if err != nil {
		return err
	}
	// 不需要 execMu：buildCobraCommand 创建新对象，configureProgrammaticIO 只读取 rootCmd 的 IO 方法，
	// 这些方法在命令执行期间不会改变，mustRegister 在注册阶段调用，此时不会有命令在执行
	cobraCmd := k.buildCobraCommand(registered)
	raw := append([]string{name}, args...)
	cobraCmd.Annotations = map[string]string{rawInputAnnotation: strings.Join(raw, "\x00")}
	cobraCmd.SetArgs(args)
	k.configureProgrammaticIO(ctx, cobraCmd, silent)
	return cobraCmd.ExecuteContext(ctx)
}

func (k *Kernel) configureProgrammaticIO(ctx context.Context, cobraCmd *cobra.Command, silent bool) {
	if parent, ok := console.FromContext(ctx); ok && console.CobraCommand(parent) != nil {
		parentCobra := console.CobraCommand(parent)
		cobraCmd.SetIn(parentCobra.InOrStdin())
		cobraCmd.SetOut(parentCobra.OutOrStdout())
		cobraCmd.SetErr(parentCobra.ErrOrStderr())
	} else if k.rootCmd != nil {
		cobraCmd.SetIn(k.rootCmd.InOrStdin())
		cobraCmd.SetOut(k.rootCmd.OutOrStdout())
		cobraCmd.SetErr(k.rootCmd.ErrOrStderr())
	}
	if silent {
		cobraCmd.SetOut(io.Discard)
		cobraCmd.SetErr(io.Discard)
	}
}

func isolatedOptionRequested(cmd *cobra.Command) bool {
	if cmd == nil {
		return false
	}
	flag := cmd.Flags().Lookup("isolated")
	return flag != nil && flag.Changed
}

func commandInputSnapshot(cmd *cobra.Command, commandName string, args []string) []string {
	if cmd != nil && cmd.Annotations != nil && cmd.Annotations[rawInputAnnotation] != "" {
		return strings.Split(cmd.Annotations[rawInputAnnotation], "\x00")
	}
	// 预分配容量：commandName + args + 预估 flags 数量（常见 flags 约 5~10 个）
	const estimatedFlagCount = 8
	input := make([]string, 0, 1+len(args)+estimatedFlagCount)
	input = append(input, commandName)
	input = append(input, args...)
	if cmd == nil {
		return input
	}
	cmd.Flags().Visit(func(flag *pflag.Flag) {
		if flag.Value.Type() == "bool" && flag.Value.String() == "true" {
			input = append(input, "--"+flag.Name)
			return
		}
		input = append(input, "--"+flag.Name+"="+flag.Value.String())
	})
	return input
}

func errorSummary(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// splitSignature 解析命令签名字符串，支持引号和转义字符。
//
// 转义规则说明：
// - 反斜杠 (\) 用于转义下一个字符，使其失去特殊含义（如引号、空格等）
// - 如果反斜杠出现在字符串末尾（孤立反斜杠），会将其作为普通字符保留
// - 这种宽松处理是为了兼容用户输入的不完整转义序列，避免解析失败
func splitSignature(signature string) ([]string, error) {
	var parts []string
	var current strings.Builder
	var quote rune
	escaped := false

	for _, r := range strings.TrimSpace(signature) {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
				continue
			}
			current.WriteRune(r)
			continue
		}
		if r == '\'' || r == '"' {
			quote = r
			continue
		}
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteRune(r)
	}
	// 孤立反斜杠处理：如果字符串以反斜杠结尾，将其作为普通字符保留
	if escaped {
		current.WriteRune('\\')
	}
	if quote != 0 {
		return nil, fmt.Errorf("kernel call: unterminated quoted argument")
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts, nil
}

// normalizeCommandArgs 统一整理 Kernel 两类公开入口的参数格式。
//
// 参数说明：includeProgramName=true 表示输入遵循完整 argv 结构，需要去掉首项程序名；false 表示
// 输入本身就是命令参数列表，应原样复制，避免调用方传入切片后再被内部修改。
func normalizeCommandArgs(args []string, includeProgramName bool) []string {
	if len(args) == 0 {
		return nil
	}
	if includeProgramName {
		if len(args) == 1 {
			return nil
		}
		return append([]string(nil), args[1:]...)
	}
	return append([]string(nil), args...)
}

// resetCommandFlags 在每次执行前重置命令树上的 flag 状态。
// 需求背景：测试与程序化调用会复用同一个 Kernel/Command 实例；
// 如果不在执行前清空 Changed 与当前值，上一次运行的 --ansi/--no-ansi 等显式状态会泄漏到下一次执行结果。
// 递归重置：需要重置所有子命令的 flag，因为 persistent flags 的 Changed 状态不会被 Cobra 自动重置。
func resetCommandFlags(cmd *cobra.Command) {
	if cmd == nil {
		return
	}
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flag.Changed = false
		_ = flag.Value.Set(flag.DefValue)
	})
	for _, child := range cmd.Commands() {
		resetCommandFlags(child)
	}
}

func (c *closureCommand) Definition() *console.Definition {
	definition := console.CloneDefinition(c.definition)
	return &definition
}

func (c *closureCommand) Handle(ctx console.CommandContext) error {
	return c.handler(ctx)
}
