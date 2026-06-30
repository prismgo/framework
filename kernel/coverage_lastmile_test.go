package kernel

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/prismgo/framework/console"
	"github.com/spf13/cobra"
)

func TestKernelCompletionVersionAndHelperBranches(t *testing.T) {
	// 测试意图：用真实 Kernel 路径覆盖 completion、version、argv 归一化和补全 hook 分支。
	useKernelTestContainer(t)
	k := New("")
	if k.rootCommandName() != "app" {
		t.Fatalf("empty kernel name should fall back to app, got %q", k.rootCommandName())
	}
	if rootDefinition("   ").Name != "app" {
		t.Fatal("rootDefinition should normalize blank names to app")
	}

	k.Register(&completionProbeCommand{})
	if err := k.CallSilently(context.Background(), "completion fish"); err != nil {
		t.Fatalf("completion fish failed: %v", err)
	}
	if err := k.CallSilently(context.Background(), "completion unknown"); err == nil || !strings.Contains(err.Error(), "unsupported shell") {
		t.Fatalf("completion unknown error = %v, want unsupported shell", err)
	}
	if err := (&completionCommand{}).Handle(nil); err == nil {
		t.Fatal("nil completion command should report an uninitialized kernel")
	}

	cmd, _, err := k.rootCmd.Find([]string{"probe:complete"})
	if err != nil {
		t.Fatalf("find probe command: %v", err)
	}
	argSuggestions, directive := cmd.ValidArgsFunction(cmd, nil, "")
	if directive != cobra.ShellCompDirectiveNoFileComp || strings.Join(argSuggestions, ",") != "dev,prod" {
		t.Fatalf("argument completion = %v / %v", argSuggestions, directive)
	}
	if suggestions, _ := cmd.ValidArgsFunction(cmd, []string{"dev"}, ""); suggestions != nil {
		t.Fatalf("completed argument list should not suggest more values: %v", suggestions)
	}
	flagCompletion, ok := cmd.GetFlagCompletionFunc("queue")
	if !ok {
		t.Fatal("expected queue flag completion function")
	}
	flagSuggestions, directive := flagCompletion(cmd, nil, "")
	if directive != cobra.ShellCompDirectiveNoFileComp || strings.Join(flagSuggestions, ",") != "high,low" {
		t.Fatalf("flag completion = %v / %v", flagSuggestions, directive)
	}

	var out bytes.Buffer
	k.rootCmd.SetOut(&out)
	if err := k.RunContextArgv(context.Background(), []string{"artisan", "--version"}); err != nil {
		t.Fatalf("run version argv: %v", err)
	}
	if strings.TrimSpace(out.String()) == "" {
		t.Fatal("version output should not be empty")
	}
	if err := ((*Kernel)(nil)).RunContextArgv(context.Background(), []string{"artisan"}); err == nil {
		t.Fatal("nil kernel RunContextArgv should fail")
	}

	resetCommandFlags(nil)
	if got := normalizeCommandArgs([]string{"artisan"}, true); got != nil {
		t.Fatalf("single argv should normalize to nil, got %v", got)
	}
	if got := normalizeCommandArgs([]string{"probe:complete"}, false); len(got) != 1 || got[0] != "probe:complete" {
		t.Fatalf("command args should be copied unchanged, got %v", got)
	}
}

func TestKernelHelpGroupingAndOutputHelpers(t *testing.T) {
	// 测试意图：直接构造可运行 Cobra 命令树，覆盖 help 分组和空输出 helper 的边界。
	root := &cobra.Command{Use: "app"}
	run := func(*cobra.Command, []string) {}
	root.AddCommand(
		&cobra.Command{Use: "cache", Short: "cache parent", Run: run},
		&cobra.Command{Use: "queue:work", Short: "work queues", Run: run},
		&cobra.Command{Use: "queue:retry", Short: "retry jobs", Run: run},
		&cobra.Command{Use: "secret", Hidden: true, Run: run},
	)

	groups := groupedHelpCommands(root)
	if len(groups) != 2 {
		t.Fatalf("group count = %d, want 2: %#v", len(groups), groups)
	}
	if namespaceGroupKey("queue") != "namespace:queue" {
		t.Fatal("namespace group key should be stable")
	}
	if ns, ok := commandNamespace("queue:work"); !ok || ns != "queue" {
		t.Fatalf("commandNamespace = %q / %v, want queue true", ns, ok)
	}
	if ns, ok := commandNamespace("cache"); ok || ns != "cache" {
		t.Fatalf("commandNamespace without namespace = %q / %v", ns, ok)
	}

	var out bytes.Buffer
	writeDescription(&out, "")
	writeAvailableCommands(&out, nil, console.OutputOptions{})
	if out.Len() != 0 {
		t.Fatalf("empty description and groups should not write output: %q", out.String())
	}
	writeAvailableCommands(&out, groups, console.OutputOptions{})
	if !strings.Contains(out.String(), "queue:retry") || !strings.Contains(out.String(), "cache") {
		t.Fatalf("available commands output missing expected commands: %q", out.String())
	}

	if boolValue, changed := boolFlagValue(nil, "ansi"); boolValue || changed {
		t.Fatalf("nil bool flag = %v / %v, want false false", boolValue, changed)
	}
	if outputOptionsFromCobra(nil).Quiet {
		t.Fatal("nil cobra output options should not be quiet")
	}
	if versionRequested(nil) {
		t.Fatal("nil cobra command should not request version")
	}
	if err := renderVersion(nil); err != nil {
		t.Fatalf("nil renderVersion should not fail: %v", err)
	}
}

func TestKernelCallValuesAdditionalTypes(t *testing.T) {
	// 测试意图：补齐 programmatic call input 对 Stringer、整数和混合 slice 的转换路径。
	values, err := callValues(fmt.Stringer(lastmileStringer("named")), 0)
	if err != nil || len(values) != 1 || values[0] != "named" {
		t.Fatalf("Stringer callValues = %v / %v", values, err)
	}
	values, err = callValues([]int{1, 2}, 0)
	if err != nil || strings.Join(values, ",") != "1,2" {
		t.Fatalf("[]int callValues = %v / %v", values, err)
	}
	values, err = callValues([]any{uint(3), uint64(4), float64(5.5), ""}, 0)
	if err != nil || strings.Join(values, ",") != "3,4,5.5" {
		t.Fatalf("mixed slice callValues = %v / %v", values, err)
	}
}

type completionProbeCommand struct{}

func (c *completionProbeCommand) Definition() *console.Definition {
	return &console.Definition{
		Name:        "probe:complete",
		Description: "probe completion",
		Arguments: []console.Argument{{
			Name:        "env",
			Required:    true,
			Suggestions: []string{"dev", "prod"},
		}},
		Options: []console.Option{{
			Name:        "queue",
			ValueMode:   console.OptionValueRequired,
			Suggestions: []string{"high", "low"},
		}},
	}
}

func (c *completionProbeCommand) Handle(console.CommandContext) error { return nil }

type lastmileStringer string

func (s lastmileStringer) String() string { return string(s) }
