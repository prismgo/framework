package console

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/mgutz/ansi"
	"github.com/prismgo/framework/container"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
	"github.com/spf13/cobra"
)

type capturedException struct {
	err    error
	fields map[string]any
}

func captureConsoleReports(t *testing.T) <-chan capturedException {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	reports := make(chan capturedException, 4)
	handler := goexception.New(goexception.WithPanicStack(false))
	handler.Reporters = append(handler.Reporters, func(_ any, err error, fields map[string]any) {
		var copied map[string]any
		if fields != nil {
			copied = make(map[string]any, len(fields))
			for key, value := range fields {
				copied[key] = value
			}
		}
		reports <- capturedException{err: err, fields: copied}
	})
	if err := registry.Instance("exception.handler", handler, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	return reports
}

func waitConsoleReport(t *testing.T, reports <-chan capturedException) capturedException {
	t.Helper()
	select {
	case report := <-reports:
		return report
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exception report")
		return capturedException{}
	}
}

func waitTrapCallback(t *testing.T, callbacks <-chan os.Signal) os.Signal {
	t.Helper()
	select {
	case sig := <-callbacks:
		return sig
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for trap callback")
		return nil
	}
}

func TestConsoleFailErrorBranches(t *testing.T) {
	base := io.ErrUnexpectedEOF
	err := Fail("wrapped", base)
	failed, ok := IsManualFailure(err)
	if !ok || failed.Error() != "wrapped unexpected EOF" || failed.Unwrap() == nil {
		t.Fatalf("manual failure = %#v ok=%v", failed, ok)
	}
	if Fail().Error() != "command failed" {
		t.Fatalf("empty Fail = %q", Fail().Error())
	}
	if (&ManuallyFailedError{Err: base}).Error() != base.Error() {
		t.Fatalf("wrapped-only manual failure mismatch")
	}
}

func TestFailWithOnlyNil(t *testing.T) {
	err := Fail(nil)
	if err == nil || err.Error() != "command failed" {
		t.Fatalf("Fail(nil) = %v, want 'command failed'", err)
	}
	failed, ok := IsManualFailure(err)
	if !ok {
		t.Fatal("IsManualFailure returned false for Fail(nil)")
	}
	if failed.Message != "" {
		t.Fatalf("Fail(nil) message = %q, want empty", failed.Message)
	}
	if failed.Unwrap() != nil {
		t.Fatal("Fail(nil) should not wrap any error")
	}
}

func TestDefinitionUsageAndNormalizeAndGlobalConsoleOutput(t *testing.T) {
	defaultValue := "default"
	definition, err := NormalizeDefinition(Definition{
		Name:      "report:send",
		Arguments: []Argument{{Name: "tenant", Required: true}, {Name: "user", DefaultValue: &defaultValue}, {Name: "tags", IsArray: true}},
		Options:   []Option{{Name: "queue", ValueMode: OptionValueRequired}, {Name: "force", ValueMode: OptionValueNone}, {Name: "tag", ValueMode: OptionValueRequired, IsArray: true}},
		Aliases:   []string{"report", "report"},
		Examples:  []string{"go run ./ report:send"},
	})
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	if usage := DefinitionUsage(definition); usage != "report:send <tenant> [user] [tags...]" {
		t.Fatalf("Usage = %q, want report:send <tenant> [user] [tags...]", usage)
	}

	buffer := captureConsoleOutput(t, func() {
		Line("plain")
		Info("info")
		Comment("comment")
		Question("question")
		Success("ok")
		Error("bad")
		Warn("warn")
		Alert("alert")
		ExitIf(nil)
	})
	if !strings.Contains(buffer, "plain") || !strings.Contains(buffer, "info") || !strings.Contains(buffer, "comment") || !strings.Contains(buffer, "question") || !strings.Contains(buffer, "ok") || !strings.Contains(buffer, "bad") || !strings.Contains(buffer, "warn") || !strings.Contains(buffer, "alert") {
		t.Fatalf("expected console output to contain messages, got %q", buffer)
	}
}

func TestParseSignatureOptionalArrayAndOptionInt(t *testing.T) {
	definition, err := ParseSignature("sample:run {users?*} {--take=10}")
	if err != nil {
		t.Fatalf("ParseSignature returned error: %v", err)
	}
	if !definition.Arguments[0].IsArray || definition.Arguments[0].Required {
		t.Fatalf("unexpected optional array argument: %+v", definition.Arguments[0])
	}
	cmd := &cobra.Command{Use: "sample:run"}
	if err := BindDefinitionFlags(cmd, definition); err != nil {
		t.Fatalf("BindDefinitionFlags returned error: %v", err)
	}
	input := NewInput(definition, cmd, nil)
	if got, err := input.OptionInt("take"); err != nil || got != 10 {
		t.Fatalf("OptionInt(take) = %d, err=%v, want 10, nil", got, err)
	}
}

func TestGlobalConsoleOutputStylesMatchIO(t *testing.T) {
	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")
	buffer := captureConsoleOutput(t, func() {
		Line("plain")
		Line("unknown", "missing")
		Line("warning", "warning")
		Info("info")
		Comment("comment")
		Question("question")
		Success("success")
		Warn("warn")
		Error("error")
	})

	want := "plain\n" +
		"unknown\n" +
		ansi.Color("warning", "yellow") + "\n" +
		ansi.Color("info", "green") + "\n" +
		ansi.Color("comment", "yellow") + "\n" +
		ansi.Color("question", "black:cyan") + "\n" +
		ansi.Color("success", "white:green") + "\n" +
		ansi.Color("warn", "yellow") + "\n" +
		ansi.Color("error", "white:red") + "\n"
	if buffer != want {
		t.Fatalf("console output = %q, want %q", buffer, want)
	}
}

func TestExitWritesErrorStyleBeforeExiting(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestConsoleExitHelperProcess", "--")
	cmd.Env = append(os.Environ(), "PRISMGO_TEST_CONSOLE_EXIT=1", "FORCE_COLOR=1", "NO_COLOR=")

	output, err := cmd.Output()
	if err == nil {
		t.Fatal("expected helper process to exit non-zero")
	}
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 1 {
		t.Fatalf("Exit returned %T %v, want exit code 1", err, err)
	}
	want := ansi.Color("fatal", "white:red") + "\n"
	if string(output) != want {
		t.Fatalf("Exit output = %q, want %q", string(output), want)
	}
}

func TestConsoleExitHelperProcess(t *testing.T) {
	if os.Getenv("PRISMGO_TEST_CONSOLE_EXIT") != "1" {
		return
	}
	Exit("fatal")
}

func captureConsoleOutput(t *testing.T, run func()) string {
	t.Helper()
	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = originalStdout
	}()

	outputCh := make(chan string, 1)
	go func() {
		buffer := &bytes.Buffer{}
		_, _ = io.Copy(buffer, reader)
		outputCh <- buffer.String()
	}()

	run()
	_ = writer.Close()
	return <-outputCh
}
