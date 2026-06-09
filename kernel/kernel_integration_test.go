package kernel

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/event"
	"github.com/spf13/cobra"
)

func TestKernelRegistersDefinitionAndRunsCommand(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	cmd := &integrationCommand{}
	k.Register(cmd)
	k.rootCmd.SetArgs([]string{"report:send", "tenant-a", "--queue=high", "--force"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext returned error: %v", err)
	}
	if cmd.tenant != "tenant-a" {
		t.Fatalf("cmd.tenant = %q, want tenant-a", cmd.tenant)
	}
	if cmd.queue != "high" {
		t.Fatalf("cmd.queue = %q, want high", cmd.queue)
	}
	if !cmd.force {
		t.Fatal("expected force flag to be true")
	}
}

func TestKernelVersionFlagIsGlobalAndDoesNotRunCommand(t *testing.T) {
	useKernelTestContainer(t)
	k := New("artisan")
	cmd := &versionProbeCommand{}
	k.Register(cmd)
	stdout := &bytes.Buffer{}
	k.rootCmd.SetOut(stdout)
	k.rootCmd.SetErr(&bytes.Buffer{})

	k.rootCmd.SetArgs([]string{"-V"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext -V returned error: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "PrismGo Framework 0.1.0" {
		t.Fatalf("-V output = %q", got)
	}
	if cmd.ran {
		t.Fatal("-V should not run registered command")
	}

	stdout.Reset()
	k.rootCmd.SetArgs([]string{"version:probe", "--version"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext subcommand --version returned error: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "PrismGo Framework 0.1.0" {
		t.Fatalf("subcommand --version output = %q", got)
	}
	if cmd.ran {
		t.Fatal("subcommand --version should not run registered command")
	}
}

func TestKernelVersionLongFlagAndOutputSuppression(t *testing.T) {
	useKernelTestContainer(t)
	for _, args := range [][]string{
		{"--version"},
		{"--version", "--quiet"},
		{"--version", "--silent"},
	} {
		k := New("artisan")
		stdout := &bytes.Buffer{}
		k.rootCmd.SetOut(stdout)
		k.rootCmd.SetErr(&bytes.Buffer{})
		k.rootCmd.SetArgs(args)

		if err := k.RunContext(context.Background()); err != nil {
			t.Fatalf("RunContext %v returned error: %v", args, err)
		}

		got := strings.TrimSpace(stdout.String())
		if len(args) == 1 && got != "PrismGo Framework 0.1.0" {
			t.Fatalf("%v output = %q", args, got)
		}
		if len(args) > 1 && got != "" {
			t.Fatalf("%v output = %q, want empty", args, got)
		}
	}
}

func TestKernelCallSilentlySuppressesOutput(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	k.Register(&silentCommand{})
	if err := k.CallSilently(context.Background(), "silent:run"); err != nil {
		t.Fatalf("CallSilently returned error: %v", err)
	}
}

func TestKernelCallParsesSignatureAndReturnsCommandErrors(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	cmd := &programmaticCommand{}
	k.Register(cmd)

	err := k.Call(context.Background(), `programmatic:run tenant-a --queue=default --tag=red --tag=blue --force`)
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if cmd.tenant != "tenant-a" || cmd.queue != "default" || !cmd.force {
		t.Fatalf("command input = tenant:%q queue:%q force:%v", cmd.tenant, cmd.queue, cmd.force)
	}
	if got := strings.Join(cmd.tags, ","); got != "red,blue" {
		t.Fatalf("tags = %q, want red,blue", got)
	}

	if err := k.Call(context.Background(), "programmatic:run"); err == nil || !strings.Contains(err.Error(), "arg") {
		t.Fatalf("missing argument error = %v, want argument validation error", err)
	}
	if err := k.Call(context.Background(), "missing:run"); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("missing command error = %v, want not registered", err)
	}

	runErr := errors.New("command failed")
	cmd.err = runErr
	if err := k.Call(context.Background(), "programmatic:run tenant-a"); !errors.Is(err, runErr) {
		t.Fatalf("command error = %v, want %v", err, runErr)
	}
}

func TestKernelCallEncodesStructuredInput(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	cmd := &programmaticCommand{}
	k.Register(cmd)

	err := k.Call(context.Background(), "programmatic:run", console.CallInput{
		Arguments: map[string]any{"tenant": "tenant-a"},
		Options: map[string]any{
			"queue": "default",
			"tag":   []string{"red", "blue"},
			"force": true,
		},
	})
	if err != nil {
		t.Fatalf("Call with input returned error: %v", err)
	}
	if cmd.tenant != "tenant-a" || cmd.queue != "default" || !cmd.force {
		t.Fatalf("command input = tenant:%q queue:%q force:%v", cmd.tenant, cmd.queue, cmd.force)
	}
	if got := strings.Join(cmd.tags, ","); got != "red,blue" {
		t.Fatalf("tags = %q, want red,blue", got)
	}

	cmd.force = true
	err = k.Call(context.Background(), "programmatic:run", console.CallInput{
		Arguments: map[string]any{"tenant": "tenant-b"},
		Options:   map[string]any{"force": false},
	})
	if err != nil {
		t.Fatalf("Call with input bool false returned error: %v", err)
	}
	if cmd.force {
		t.Fatal("bool false option should not inject --force")
	}

	err = k.Call(context.Background(), "programmatic:run", console.CallInput{}, console.CallInput{})
	if err == nil || !strings.Contains(err.Error(), "expected at most one CallInput") {
		t.Fatalf("multiple CallInput error = %v, want explicit validation error", err)
	}
}

func TestKernelCallSilentlyInputAndCallInputValueTypes(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	cmd := &programmaticCommand{}
	k.Register(cmd)

	err := k.CallSilently(context.Background(), "programmatic:run", console.CallInput{
		Arguments: map[string]any{"tenant": []string{"tenant-c"}},
		Options: map[string]any{
			"queue": 42,
			"tag":   []int{1, 2},
		},
	})
	if err != nil {
		t.Fatalf("CallSilently with input returned error: %v", err)
	}
	if cmd.tenant != "tenant-c" || cmd.queue != "42" || strings.Join(cmd.tags, ",") != "1,2" {
		t.Fatalf("structured values tenant=%q queue=%q tags=%v", cmd.tenant, cmd.queue, cmd.tags)
	}

	encoded, err := encodeCallInput(*console.MustDefinition("encode:run {items*} {--tag=*} {--force}", "encode"), console.CallInput{
		Arguments: map[string]any{"items": [2]string{"a", "b"}},
		Options:   map[string]any{"tag": []any{"x", uint(7), uint64(8), int64(9), 1.5}, "force": "true"},
	})
	if err != nil {
		t.Fatalf("encodeCallInput returned error: %v", err)
	}
	got := strings.Join(encoded, " ")
	for _, want := range []string{"a b", "--force", "--tag=x", "--tag=7", "--tag=8", "--tag=9", "--tag=1.5"} {
		if !strings.Contains(got, want) {
			t.Fatalf("encoded = %q, want %q", got, want)
		}
	}
}

func TestKernelPromptsForMissingRequiredArguments(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	cmd := &promptedCommand{}
	k.Register(cmd)
	output := &strings.Builder{}
	k.rootCmd.SetIn(strings.NewReader("tenant-a\n"))
	k.rootCmd.SetOut(output)
	k.rootCmd.SetErr(output)
	k.rootCmd.SetArgs([]string{"prompted:run"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext returned error: %v", err)
	}
	if cmd.tenant != "tenant-a" || !cmd.after {
		t.Fatalf("prompted command tenant=%q after=%v", cmd.tenant, cmd.after)
	}
	assertContains(t, output.String(), "Tenant slug?")

	k.rootCmd.SetArgs([]string{"prompted:run", "--no-interaction"})
	if err := k.RunContext(context.Background()); err == nil || !strings.Contains(err.Error(), "arg") {
		t.Fatalf("--no-interaction error = %v, want missing arg error", err)
	}
}

func TestKernelManualFailWritesError(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	k.Register(&manualFailCommand{})
	output := &strings.Builder{}
	k.rootCmd.SetOut(output)
	k.rootCmd.SetErr(output)

	err := k.Call(context.Background(), "manual:fail")
	if err == nil {
		t.Fatal("Call returned nil, want manual failure")
	}
	if _, ok := console.IsManualFailure(err); !ok {
		t.Fatalf("error = %T, want manual failure", err)
	}
	assertContains(t, output.String(), "stop now")
}

func TestKernelCompletionCommandOutputsScript(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	output := &strings.Builder{}
	k.rootCmd.SetOut(output)
	k.rootCmd.SetErr(output)
	k.rootCmd.SetArgs([]string{"completion", "bash"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("completion bash returned error: %v", err)
	}
	assertContains(t, output.String(), "bash completion")

	output.Reset()
	k.rootCmd.SetArgs([]string{"completion", "fish"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("completion fish returned error: %v", err)
	}
	assertContains(t, output.String(), "complete")

	output.Reset()
	k.rootCmd.SetArgs([]string{"completion", "powershell"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("completion powershell returned error: %v", err)
	}
	assertContains(t, output.String(), "Register-ArgumentCompleter")
}

func TestCommandContextCallUsesCurrentIOAndSilentSuppressesNestedOutput(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	parent := &parentCallCommand{}
	k.Register(parent, &childOutputCommand{})
	output := &strings.Builder{}
	k.rootCmd.SetOut(output)
	k.rootCmd.SetErr(output)
	k.rootCmd.SetArgs([]string{"parent:call"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext returned error: %v", err)
	}
	got := output.String()
	if !strings.Contains(got, "child output") {
		t.Fatalf("nested call output = %q, want child output", got)
	}
	if strings.Contains(got, "silent output") {
		t.Fatalf("silent nested output leaked: %q", got)
	}
}

func TestCommandContextCallInheritsCancellationAndIsolation(t *testing.T) {
	bindKernelCacheManagerForTest(t)

	k := New("test")
	k.Register(&cancelParentCommand{}, &cancelAwareCommand{}, &isolationParentCommand{}, &isolatedNestedCommand{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := k.Call(ctx, "parent:cancel"); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("cancelled nested call error = %v, want context canceled", err)
	}

	if err := k.Call(context.Background(), "parent:isolation --isolated"); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("nested isolation error = %v, want already running", err)
	}
}

func TestIsolatableCommandUsesLaravelStyleExplicitIsolatedOption(t *testing.T) {
	bindKernelCacheManagerForTest(t)

	k := New("test")
	cmd := &isolatedOptionProbeCommand{key: "isolated-option-" + t.Name()}
	k.Register(cmd)

	if err := k.Call(context.Background(), "probe:isolated-option"); err != nil {
		t.Fatalf("Call without --isolated returned error: %v", err)
	}
	if !cmd.hasIsolated || cmd.isolated != "" {
		t.Fatalf("without --isolated has=%v value=%q, want true empty because option is defined", cmd.hasIsolated, cmd.isolated)
	}

	if err := k.Call(context.Background(), "probe:isolated-option --isolated"); err != nil {
		t.Fatalf("Call with --isolated returned error: %v", err)
	}
	if !cmd.hasIsolated || cmd.isolated != "0" {
		t.Fatalf("--isolated has=%v value=%q, want true 0", cmd.hasIsolated, cmd.isolated)
	}

	if err := k.Call(context.Background(), "probe:isolated-option --isolated=12"); err != nil {
		t.Fatalf("Call with --isolated=12 returned error: %v", err)
	}
	if !cmd.hasIsolated || cmd.isolated != "12" {
		t.Fatalf("--isolated=12 has=%v value=%q, want true 12", cmd.hasIsolated, cmd.isolated)
	}
}

func TestKernelRunContextSuppressesUsageForContextCancellation(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	k.Register(&alwaysCanceledCommand{})
	output := &strings.Builder{}
	k.rootCmd.SetOut(output)
	k.rootCmd.SetErr(output)
	k.rootCmd.SetArgs([]string{"always:cancel"})

	err := k.RunContext(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunContext error = %v, want context.Canceled", err)
	}
	if got := output.String(); strings.Contains(got, "Usage:") || strings.Contains(got, "Error:") {
		t.Fatalf("context cancellation should not print cobra usage/error, got %q", got)
	}
}

func TestKernelDispatchesConsoleLifecycleEvents(t *testing.T) {
	bus := event.New()
	registry := useKernelTestContainer(t)
	// 测试装配说明：Console lifecycle 事件通过 event facade 从当前 Application 容器解析；
	// 这里直接绑定 dispatcher，避免回到旧的 facade Use fallback 路径。
	if err := registry.Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}

	var got []string
	bus.Listen("*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		got = append(got, ev.Name())
		switch payload := ev.(type) {
		case event.ConsoleApplicationStarting:
			if payload.KernelName != "test" {
				t.Fatalf("ConsoleApplicationStarting kernel = %q, want test", payload.KernelName)
			}
		case event.CommandStarting:
			if payload.Command != "programmatic:run" || strings.Join(payload.Input, " ") != "programmatic:run tenant-a --queue=default" {
				t.Fatalf("CommandStarting payload = %+v", payload)
			}
		case event.CommandFinished:
			if payload.Command != "programmatic:run" || !payload.Succeeded || payload.Duration < 0 || payload.Error != "" {
				t.Fatalf("CommandFinished payload = %+v", payload)
			}
		}
		return nil
	}))

	k := New("test")
	k.Register(&programmaticCommand{})
	if err := k.Call(context.Background(), "programmatic:run tenant-a --queue=default"); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if err := k.CallSilently(context.Background(), "programmatic:run tenant-a --queue=default"); err != nil {
		t.Fatalf("CallSilently returned error: %v", err)
	}

	wantPrefix := []string{
		event.EventConsoleApplicationStarting,
		event.EventCommandStarting,
		event.EventCommandFinished,
		event.EventCommandStarting,
		event.EventCommandFinished,
	}
	if len(got) != len(wantPrefix) {
		t.Fatalf("events = %v, want %v", got, wantPrefix)
	}
	for i := range wantPrefix {
		if got[i] != wantPrefix[i] {
			t.Fatalf("event[%d] = %q, want %q", i, got[i], wantPrefix[i])
		}
	}
}

func TestKernelListCommandShowsVisibleCommands(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	k.Register(&listVisibleCommand{}, &listHiddenCommand{})
	buffer := &strings.Builder{}
	k.rootCmd.SetOut(buffer)
	k.rootCmd.SetErr(buffer)
	k.rootCmd.SetArgs([]string{"list"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext returned error: %v", err)
	}
	output := buffer.String()
	if !strings.Contains(output, "visible:run") {
		t.Fatalf("expected list output to include visible command, got %q", output)
	}
	if strings.Contains(output, "hidden:run") {
		t.Fatalf("expected list output to hide hidden command, got %q", output)
	}
}

func TestKernelRootHelpGroupsNamespacedCommandsAlphabetically(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	k.Register(
		&helpAlphaCommand{},
		&helpZetaCommand{},
		&helpAreaSyncCommand{},
		&helpMigrateCommand{},
		&helpMigrateRefreshCommand{},
		&helpMigrateInstallCommand{},
	)
	buffer := &strings.Builder{}
	k.rootCmd.SetOut(buffer)
	k.rootCmd.SetErr(buffer)
	k.rootCmd.SetArgs([]string{"--help"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext returned error: %v", err)
	}

	output := buffer.String()
	assertContains(t, output, "Available Commands:")
	assertContains(t, output, "  alpha")
	assertContains(t, output, "  zeta")
	assertContains(t, output, "  area")
	assertContains(t, output, "    area:sync")
	assertContains(t, output, "  migrate")
	assertContains(t, output, "run migrations")
	assertContains(t, output, "\n migrate\n    migrate:install")
	assertContains(t, output, "    migrate:install")
	assertContains(t, output, "    migrate:refresh")

	assertBefore(t, output, "  alpha", "  list")
	assertBefore(t, output, "  list", "  zeta")
	assertBefore(t, output, "  list", "  migrate")
	assertBefore(t, output, "  migrate", "  zeta")
	assertBefore(t, output, "  zeta", " area")
	assertBefore(t, output, " area\n", "\n migrate\n    migrate:install")
	assertBefore(t, output, "    migrate:install", "    migrate:refresh")
}

func TestKernelCommandHelpShowsAliasesExamplesSubcommandsAndFlags(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	k.Register(&helpParentCommand{}, &helpParentChildCommand{})
	buffer := &strings.Builder{}
	k.rootCmd.SetOut(buffer)
	k.rootCmd.SetErr(buffer)
	k.rootCmd.SetArgs([]string{"parent", "--help"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext help returned error: %v", err)
	}

	output := buffer.String()
	assertContains(t, output, "parent command")
	assertContains(t, output, "Long parent help")
	assertContains(t, output, "Usage:")
	assertContains(t, output, "Aliases:")
	assertContains(t, output, "p")
	assertContains(t, output, "Examples:")
	assertContains(t, output, "go run ./ parent --name=demo")
	assertContains(t, output, "Options:")
	assertContains(t, output, "--name")
}

func TestKernelCommandHelpShowsDefinitionArgumentDescriptions(t *testing.T) {
	useKernelTestContainer(t)
	// 需求背景：手写 Definition 的 Argument.Description 必须进入 --help，
	// 否则命令作者已经声明的参数说明对终端用户不可见。
	k := New("test")
	k.RegisterClosure(console.Definition{
		Name:        "users:show",
		Description: "show users",
		Arguments: []console.Argument{
			{Name: "user", Description: "The ID of the user", Required: true},
			{Name: "team", Description: "The team slug"},
			{Name: "tags", Description: "Filter tags", IsArray: true},
		},
	}, func(console.CommandContext) error { return nil })
	buffer := &strings.Builder{}
	k.rootCmd.SetOut(buffer)
	k.rootCmd.SetErr(buffer)
	k.rootCmd.SetArgs([]string{"users:show", "--help"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext help returned error: %v", err)
	}

	output := buffer.String()
	assertContains(t, output, "Arguments:")
	assertContains(t, output, "user")
	assertContains(t, output, "The ID of the user")
	assertContains(t, output, "team")
	assertContains(t, output, "The team slug")
	assertContains(t, output, "tags...")
	assertContains(t, output, "Filter tags")
}

func TestKernelCommandHelpHonorsOutputOptions(t *testing.T) {
	useKernelTestContainer(t)
	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")
	k := New("artisan")
	k.Register(&helpParentCommand{})
	buffer := &strings.Builder{}
	k.rootCmd.SetOut(buffer)
	k.rootCmd.SetErr(buffer)
	k.rootCmd.SetArgs([]string{"parent", "--help", "--ansi"})

	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext ANSI help returned error: %v", err)
	}
	if !strings.Contains(buffer.String(), "\x1b[33mUsage:\x1b[0m") || !strings.Contains(buffer.String(), "\x1b[33mOptions:\x1b[0m") {
		t.Fatalf("help should render ANSI section headings: %q", buffer.String())
	}

	buffer.Reset()
	k.rootCmd.SetArgs([]string{"parent", "--help", "--no-ansi"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext no-ansi help returned error: %v", err)
	}
	if strings.Contains(buffer.String(), "\x1b[") {
		t.Fatalf("--no-ansi help included ANSI: %q", buffer.String())
	}

	buffer.Reset()
	k.rootCmd.SetArgs([]string{"parent", "--help", "--quiet"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext quiet help returned error: %v", err)
	}
	if buffer.String() != "" {
		t.Fatalf("--quiet help output = %q, want empty", buffer.String())
	}
}

func TestRenderCommandHelpShowsAvailableSubcommands(t *testing.T) {
	useKernelTestContainer(t)
	parent := &cobra.Command{Use: "parent", Short: "parent command"}
	child := &cobra.Command{Use: "child", Short: "child command", Run: func(*cobra.Command, []string) {}}
	parent.AddCommand(child)
	buffer := &strings.Builder{}
	parent.SetOut(buffer)

	renderCommandHelp(parent)

	output := buffer.String()
	assertContains(t, output, "Available Commands:")
	assertContains(t, output, "child")
	assertContains(t, output, "child command")
}

func TestKernelCallParsesQuotedAndEscapedSignatureParts(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	cmd := &programmaticCommand{}
	k.Register(cmd)

	if err := k.Call(context.Background(), `programmatic:run "tenant a" --queue=high\ priority`); err != nil {
		t.Fatalf("quoted Call returned error: %v", err)
	}
	if cmd.tenant != "tenant a" || cmd.queue != "high priority" {
		t.Fatalf("quoted input tenant=%q queue=%q", cmd.tenant, cmd.queue)
	}
	if err := k.Call(context.Background(), `programmatic:run "tenant a`); err == nil || !strings.Contains(err.Error(), "unterminated") {
		t.Fatalf("unterminated quote error = %v, want unterminated", err)
	}
}

type integrationCommand struct {
	tenant string
	queue  string
	force  bool
}

type versionProbeCommand struct {
	ran bool
}

func (c *versionProbeCommand) Definition() *console.Definition {
	return console.MustDefinition("version:probe", "version probe")
}

func (c *versionProbeCommand) Handle(_ console.CommandContext) error {
	c.ran = true
	return nil
}

func (c *integrationCommand) Definition() *console.Definition {
	return console.MustDefinition("report:send {tenant} {--queue=} {--force}", "report send")
}

func (c *integrationCommand) Handle(ctx console.CommandContext) error {
	c.tenant = ctx.Input().Argument("tenant")
	c.queue = ctx.Input().Option("queue")
	c.force = ctx.Input().OptionBool("force")
	return nil
}

type programmaticCommand struct {
	tenant string
	queue  string
	tags   []string
	force  bool
	err    error
}

func (c *programmaticCommand) Definition() *console.Definition {
	return console.MustDefinition("programmatic:run {tenant} {--queue=} {--tag=*} {--force}", "programmatic")
}

func (c *programmaticCommand) Handle(ctx console.CommandContext) error {
	c.tenant = ctx.Input().Argument("tenant")
	c.queue = ctx.Input().Option("queue")
	c.tags = ctx.Input().OptionStrings("tag")
	c.force = ctx.Input().OptionBool("force")
	return c.err
}

type promptedCommand struct {
	tenant string
	after  bool
}

func (c *promptedCommand) Definition() *console.Definition {
	definition := console.MustDefinition("prompted:run {tenant : Tenant slug}", "prompted")
	return definition
}

func (c *promptedCommand) PromptForMissingArgumentsUsing() map[string]console.MissingArgumentPrompt {
	return map[string]console.MissingArgumentPrompt{
		"tenant": {Question: "Tenant slug?"},
	}
}

func (c *promptedCommand) AfterPromptingForMissingArguments(ctx console.CommandContext) error {
	c.after = ctx.Argument("tenant") != ""
	return nil
}

func (c *promptedCommand) Handle(ctx console.CommandContext) error {
	c.tenant = ctx.Argument("tenant")
	return nil
}

type manualFailCommand struct{}

func (c *manualFailCommand) Definition() *console.Definition {
	return console.MustDefinition("manual:fail", "manual fail")
}

func (c *manualFailCommand) Handle(ctx console.CommandContext) error {
	return ctx.Fail("stop now")
}

type parentCallCommand struct{}

func (c *parentCallCommand) Definition() *console.Definition {
	return console.MustDefinition("parent:call", "parent call")
}

func (c *parentCallCommand) Handle(ctx console.CommandContext) error {
	if err := ctx.Call("child:output child"); err != nil {
		return err
	}
	return ctx.CallSilently("child:output silent")
}

type childOutputCommand struct{}

func (c *childOutputCommand) Definition() *console.Definition {
	return console.MustDefinition("child:output {label}", "child output")
}

func (c *childOutputCommand) Handle(ctx console.CommandContext) error {
	ctx.IO().Success(ctx.Input().Argument("label") + " output")
	return nil
}

type cancelParentCommand struct{}

func (c *cancelParentCommand) Definition() *console.Definition {
	return console.MustDefinition("parent:cancel", "parent cancel")
}

func (c *cancelParentCommand) Handle(ctx console.CommandContext) error {
	return ctx.Call("child:cancel")
}

type cancelAwareCommand struct{}

func (c *cancelAwareCommand) Definition() *console.Definition {
	return console.MustDefinition("child:cancel", "child cancel")
}

func (c *cancelAwareCommand) Handle(ctx console.CommandContext) error {
	select {
	case <-ctx.Context().Done():
		return ctx.Context().Err()
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

type alwaysCanceledCommand struct{}

func (c *alwaysCanceledCommand) Definition() *console.Definition {
	return console.MustDefinition("always:cancel", "always cancel")
}

func (c *alwaysCanceledCommand) Handle(console.CommandContext) error {
	return context.Canceled
}

type isolationParentCommand struct{}

func (c *isolationParentCommand) Definition() *console.Definition {
	return console.MustDefinition("parent:isolation", "parent isolation")
}

func (c *isolationParentCommand) IsolationKey(console.IsolationContext) string { return "shared" }

func (c *isolationParentCommand) Handle(ctx console.CommandContext) error {
	return ctx.Call("child:isolation --isolated")
}

type isolatedNestedCommand struct{}

func (c *isolatedNestedCommand) Definition() *console.Definition {
	return console.MustDefinition("child:isolation", "child isolation")
}

func (c *isolatedNestedCommand) IsolationKey(console.IsolationContext) string { return "shared" }

func (c *isolatedNestedCommand) Handle(console.CommandContext) error { return nil }

type isolatedOptionProbeCommand struct {
	key         string
	hasIsolated bool
	isolated    string
}

func (c *isolatedOptionProbeCommand) Definition() *console.Definition {
	return console.MustDefinition("probe:isolated-option", "probe isolated option")
}

func (c *isolatedOptionProbeCommand) IsolationKey(console.IsolationContext) string { return c.key }

func (c *isolatedOptionProbeCommand) Handle(ctx console.CommandContext) error {
	c.hasIsolated = ctx.HasOption("isolated")
	c.isolated = ctx.Option("isolated")
	return nil
}

type silentCommand struct{}

func (c *silentCommand) Definition() *console.Definition {
	return console.MustDefinition("silent:run", "silent")
}

func (c *silentCommand) Handle(ctx console.CommandContext) error {
	ctx.IO().Success("should not surface")
	return nil
}

type listVisibleCommand struct{}

func (c *listVisibleCommand) Definition() *console.Definition {
	return console.MustDefinition("visible:run", "visible")
}

func (c *listVisibleCommand) Handle(_ console.CommandContext) error { return nil }

type listHiddenCommand struct{}

func (c *listHiddenCommand) Definition() *console.Definition {
	definition := console.MustDefinition("hidden:run", "hidden")
	definition.Hidden = true
	return definition
}

func (c *listHiddenCommand) Handle(_ console.CommandContext) error { return nil }

type helpAlphaCommand struct{}

func (c *helpAlphaCommand) Definition() *console.Definition {
	return console.MustDefinition("alpha", "alpha command")
}

func (c *helpAlphaCommand) Handle(_ console.CommandContext) error { return nil }

type helpZetaCommand struct{}

func (c *helpZetaCommand) Definition() *console.Definition {
	return console.MustDefinition("zeta", "zeta command")
}

func (c *helpZetaCommand) Handle(_ console.CommandContext) error { return nil }

type helpAreaSyncCommand struct{}

func (c *helpAreaSyncCommand) Definition() *console.Definition {
	return console.MustDefinition("area:sync", "sync area")
}

func (c *helpAreaSyncCommand) Handle(_ console.CommandContext) error { return nil }

type helpMigrateCommand struct{}

func (c *helpMigrateCommand) Definition() *console.Definition {
	return console.MustDefinition("migrate", "run migrations")
}

func (c *helpMigrateCommand) Handle(_ console.CommandContext) error { return nil }

type helpMigrateInstallCommand struct{}

func (c *helpMigrateInstallCommand) Definition() *console.Definition {
	return console.MustDefinition("migrate:install", "Create the migration repository")
}

func (c *helpMigrateInstallCommand) Handle(_ console.CommandContext) error { return nil }

type helpMigrateRefreshCommand struct{}

func (c *helpMigrateRefreshCommand) Definition() *console.Definition {
	return console.MustDefinition("migrate:refresh", "Reset and re-run all migrations")
}

func (c *helpMigrateRefreshCommand) Handle(_ console.CommandContext) error { return nil }

type helpParentCommand struct{}

func (c *helpParentCommand) Definition() *console.Definition {
	definition := console.MustDefinition("parent {--name=} {--force}", "parent command")
	definition.Aliases = []string{"p"}
	definition.Examples = []string{"go run ./ parent --name=demo"}
	definition.Help = "Long parent help"
	return definition
}

func (c *helpParentCommand) Handle(_ console.CommandContext) error { return nil }

type helpParentChildCommand struct{}

func (c *helpParentChildCommand) Definition() *console.Definition {
	return console.MustDefinition("parent:child", "child command")
}

func (c *helpParentChildCommand) Handle(_ console.CommandContext) error { return nil }

func assertContains(t *testing.T, value string, substring string) {
	t.Helper()
	if !strings.Contains(value, substring) {
		t.Fatalf("expected output to contain %q, got %q", substring, value)
	}
}

func assertBefore(t *testing.T, value string, first string, second string) {
	t.Helper()
	firstIndex := strings.Index(value, first)
	secondIndex := strings.Index(value, second)
	if firstIndex < 0 || secondIndex < 0 {
		t.Fatalf("expected both %q and %q in output, got %q", first, second, value)
	}
	if firstIndex >= secondIndex {
		t.Fatalf("expected %q before %q, got %q", first, second, value)
	}
}
