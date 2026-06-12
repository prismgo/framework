package kernel

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/version"
)

func TestKernelRunContextPassesContextToCommand(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")
	ctx := context.WithValue(context.Background(), testContextKey{}, "root")
	cmd := &testCommand{}
	k.Register(cmd)
	k.rootCmd.SetArgs([]string{"sample"})

	if err := k.RunContext(ctx); err != nil {
		t.Fatalf("RunContext failed: %v", err)
	}
	if cmd.ctx == nil {
		t.Fatal("expected command context to be captured")
	}
	if got := cmd.ctx.Value(testContextKey{}); got != "root" {
		t.Fatalf("command context value = %v, want root", got)
	}
}

func TestKernelRootRendersLaravelStyleCommandList(t *testing.T) {
	useKernelTestContainer(t)

	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")
	k := New("artisan")
	k.Register(&testCommand{})
	stdout := &bytes.Buffer{}
	k.rootCmd.SetOut(stdout)
	k.rootCmd.SetErr(&bytes.Buffer{})

	if err := k.RunContextArgv(context.Background(), []string{"artisan"}); err != nil {
		t.Fatalf("RunContextArgv failed: %v", err)
	}
	output := stdout.String()
	firstLine, _, _ := strings.Cut(output, "\n")
	// Root output should reflect the shared version banner instead of duplicating a release number.
	if firstLine != version.Banner() {
		t.Fatalf("root output first line = %q, want %s\n%s", firstLine, version.Banner(), output)
	}
	for _, want := range []string{"Usage:", "Options:", "Available Commands:", "sample"} {
		if !strings.Contains(output, want) {
			t.Fatalf("root output missing %q:\n%s", want, output)
		}
	}
	if !strings.Contains(output, "\x1b[33mUsage:\x1b[0m") || !strings.Contains(output, "\x1b[32msample\x1b[0m") {
		t.Fatalf("root output should auto-enable ANSI with FORCE_COLOR:\n%q", output)
	}
}

func TestKernelRootHonorsQuietAndNoANSI(t *testing.T) {
	useKernelTestContainer(t)

	t.Setenv("FORCE_COLOR", "1")
	k := New("artisan")
	k.Register(&testCommand{})
	stdout := &bytes.Buffer{}
	k.rootCmd.SetOut(stdout)

	if err := k.RunContextArgv(context.Background(), []string{"artisan", "--no-ansi"}); err != nil {
		t.Fatalf("RunContextArgv failed: %v", err)
	}
	if strings.Contains(stdout.String(), "\x1b[") {
		t.Fatalf("no-ansi root output included ANSI: %q", stdout.String())
	}

	stdout.Reset()
	if err := k.RunContextArgv(context.Background(), []string{"artisan", "--quiet"}); err != nil {
		t.Fatalf("RunContextArgv failed: %v", err)
	}
	if stdout.String() != "" {
		t.Fatalf("quiet root output = %q, want empty", stdout.String())
	}
}

func TestKernelCommandsUseAutoDetectedConsoleColors(t *testing.T) {
	useKernelTestContainer(t)

	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")
	k := New("artisan")
	k.Register(&outputCommand{})
	stdout := &bytes.Buffer{}
	k.rootCmd.SetOut(stdout)

	if err := k.RunContextArgv(context.Background(), []string{"artisan", "output:test"}); err != nil {
		t.Fatalf("RunContextArgv failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "\x1b[0;32mcommand output\x1b[0m") {
		t.Fatalf("command output should auto-enable ANSI with FORCE_COLOR: %q", stdout.String())
	}

	stdout.Reset()
	if err := k.RunContextArgv(context.Background(), []string{"artisan", "--no-ansi", "output:test"}); err != nil {
		t.Fatalf("RunContextArgv with no-ansi failed: %v", err)
	}
	if got := stdout.String(); got != "command output\n" {
		t.Fatalf("command output with --no-ansi = %q", got)
	}
}

func TestKernelExplicitANSIConflictFollowsSymfonyStyleOrder(t *testing.T) {
	useKernelTestContainer(t)

	t.Setenv("FORCE_COLOR", "")
	t.Setenv("NO_COLOR", "")
	k := New("artisan")
	k.Register(&outputCommand{})
	stdout := &bytes.Buffer{}
	k.rootCmd.SetOut(stdout)

	if err := k.RunContextArgv(context.Background(), []string{"artisan", "--ansi", "--no-ansi", "output:test"}); err != nil {
		t.Fatalf("RunContextArgv failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "\x1b[") {
		t.Fatalf("expected --ansi before --no-ansi to preserve ANSI output, got %q", stdout.String())
	}
}

func TestKernelRunContextArgvExposesExplicitFullArgvBoundary(t *testing.T) {
	useKernelTestContainer(t)

	k := New("artisan")
	k.Register(&outputCommand{})
	stdout := &bytes.Buffer{}
	k.rootCmd.SetOut(stdout)

	if err := k.RunContextArgv(context.Background(), []string{"artisan", "output:test"}); err != nil {
		t.Fatalf("RunContextArgv failed: %v", err)
	}
	if got := stdout.String(); got != "command output\n" {
		t.Fatalf("RunContextArgv output = %q, want command output", got)
	}
}

type testContextKey struct{}

type testCommand struct {
	ctx context.Context
}

func (c *testCommand) Definition() *console.Definition {
	return console.MustDefinition("sample", "sample command")
}

func (c *testCommand) Handle(ctx console.CommandContext) error {
	c.ctx = ctx.Context()
	return nil
}

type outputCommand struct{}

func (c *outputCommand) Definition() *console.Definition {
	return console.MustDefinition("output:test", "output test")
}

func (c *outputCommand) Handle(ctx console.CommandContext) error {
	ctx.IO().Info("command output")
	return nil
}
