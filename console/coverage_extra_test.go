package console

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestTerminalIOWritesMessagesToConfiguredStreams(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	io := NewIO(strings.NewReader(""), stdout, stderr)

	io.Line("line")
	io.Info("info")
	io.Comment("comment")
	io.Question("question")
	io.Success("success")
	io.Warn("warn")
	io.Error("error")
	io.Alert("alert")

	if got := stdout.String(); !strings.Contains(got, "line") || !strings.Contains(got, "info") || !strings.Contains(got, "comment") || !strings.Contains(got, "question") || !strings.Contains(got, "success") || !strings.Contains(got, "alert") {
		t.Fatalf("stdout = %q, want line, info, comment, question, success, and alert", got)
	}
	if got := stderr.String(); !strings.Contains(got, "warn") || !strings.Contains(got, "error") {
		t.Fatalf("stderr = %q, want warn and error", got)
	}
}

func TestCommandContextHandlesMissingCallerAndCommandContext(t *testing.T) {
	cmd := &cobra.Command{Use: "sample"}
	if _, ok := FromCommand(cmd); ok {
		t.Fatal("expected FromCommand to fail without context")
	}
	if _, err := MustFromCommand(cmd); err == nil {
		t.Fatal("expected MustFromCommand to fail without context")
	}

	commandCtx := NewCommandContext(context.TODO(), nil, Definition{Name: "sample"}, nil, nil, nil, cmd)
	if commandCtx.Context() == nil {
		t.Fatal("expected NewCommandContext to initialize context")
	}
	if err := commandCtx.Call("child:run"); err == nil {
		t.Fatal("expected Call to fail without caller")
	}
	if err := commandCtx.CallSilently("child:run"); err == nil {
		t.Fatal("expected CallSilently to fail without caller")
	}
	withCtx := WithContext(context.TODO(), commandCtx)
	if extracted, ok := FromContext(withCtx); !ok || extracted.CommandName() != "sample" {
		t.Fatalf("FromContext returned %+v, %v", extracted, ok)
	}
	if got := CobraCommand(commandCtx); got != cmd {
		t.Fatalf("CobraCommand(commandCtx) = %+v, want bound command", got)
	}
	if got := CobraCommand(nil); got != nil {
		t.Fatalf("CobraCommand(nil) = %+v, want nil", got)
	}
}

func TestCommandContextExposesReadOnlyInputContract(t *testing.T) {
	definition := MustDefinition("sample:run {tenant} {tags*} {--name=} {--enabled} {--take=} {--label=*}", "sample")
	cmd := &cobra.Command{Use: "sample:run"}
	if err := BindDefinitionFlags(cmd, *definition); err != nil {
		t.Fatalf("BindDefinitionFlags returned error: %v", err)
	}
	if err := cmd.Flags().Parse([]string{"--name=export", "--enabled", "--take=25", "--label=first", "--label=second"}); err != nil {
		t.Fatalf("ParseFlags returned error: %v", err)
	}
	input := NewInput(*definition, cmd, []string{"tenant-a", "tag-a", "tag-b"})
	commandCtx := NewCommandContext(context.Background(), nil, *definition, input, nil, nil, cmd)

	if got := commandCtx.CommandName(); got != "sample:run" {
		t.Fatalf("CommandName() = %q, want sample:run", got)
	}
	if got := commandCtx.Argument("tenant"); got != "tenant-a" {
		t.Fatalf("Argument(tenant) = %q, want tenant-a", got)
	}
	if got := commandCtx.Arguments("tags"); len(got) != 2 || got[0] != "tag-a" || got[1] != "tag-b" {
		t.Fatalf("Arguments(tags) = %#v, want [tag-a tag-b]", got)
	}
	if got := commandCtx.Option("name"); got != "export" {
		t.Fatalf("Option(name) = %q, want export", got)
	}
	if got := commandCtx.OptionStrings("label"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("OptionStrings(label) = %#v, want [first second]", got)
	}
	if !commandCtx.OptionBool("enabled") {
		t.Fatal("expected OptionBool(enabled) = true")
	}
	if got := commandCtx.OptionInt("take"); got != 25 {
		t.Fatalf("OptionInt(take) = %d, want 25", got)
	}
	if !commandCtx.HasOption("name") {
		t.Fatal("expected HasOption(name) = true")
	}
}

func TestCommandContextPreservesParentContextValuesAndCancellation(t *testing.T) {
	type contextKey string

	parent, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey("trace"), "trace-1"))
	commandCtx := NewCommandContext(parent, nil, Definition{Name: "sample"}, nil, nil, nil, nil)

	if got := commandCtx.Context().Value(contextKey("trace")); got != "trace-1" {
		t.Fatalf("Context value = %v, want trace-1", got)
	}
	if extracted, ok := FromContext(commandCtx.Context()); !ok || extracted != commandCtx {
		t.Fatalf("FromContext(Context()) = %+v, %v; want original command context", extracted, ok)
	}

	cancel()
	select {
	case <-commandCtx.Context().Done():
	default:
		t.Fatal("expected CommandContext.Context() to observe parent cancellation")
	}
}

func TestBindDefinitionFlagsSupportsDefaultValuesAndBoolInput(t *testing.T) {
	defaultTake := "10"
	defaultForce := "true"
	definition := Definition{
		Name: "sample",
		Options: []Option{
			{Name: "take", ValueMode: OptionValueRequired, DefaultValue: &defaultTake},
			{Name: "force", ValueMode: OptionValueNone, DefaultValue: &defaultForce},
		},
	}
	cmd := &cobra.Command{Use: "sample"}
	if err := BindDefinitionFlags(cmd, definition); err != nil {
		t.Fatalf("BindDefinitionFlags returned error: %v", err)
	}
	input := NewInput(definition, cmd, nil)
	if got := input.OptionInt("take"); got != 10 {
		t.Fatalf("OptionInt(take) = %d, want 10", got)
	}
	if !input.OptionBool("force") {
		t.Fatal("expected OptionBool(force) = true")
	}
	if input.HasOption("missing") {
		t.Fatal("expected HasOption(missing) = false")
	}
}

func TestNormalizeRejectsInvalidArrayOptionAndDuplicateArgument(t *testing.T) {
	_, err := NormalizeDefinition(Definition{
		Name:    "broken",
		Options: []Option{{Name: "tag", IsArray: true, ValueMode: OptionValueNone}},
	})
	if err == nil {
		t.Fatal("expected Normalize to reject array option without value")
	}

	_, err = NormalizeDefinition(Definition{
		Name:      "broken",
		Arguments: []Argument{{Name: "tenant"}, {Name: "tenant"}},
	})
	if err == nil {
		t.Fatal("expected Normalize to reject duplicated arguments")
	}
}

func TestParseSignatureHandlesDescriptionsAndDefaults(t *testing.T) {
	definition, err := ParseSignature("sample:run {tenant=acme : tenant name} {--take=10 : batch size}")
	if err != nil {
		t.Fatalf("ParseSignature returned error: %v", err)
	}
	if definition.Arguments[0].DefaultValue == nil || *definition.Arguments[0].DefaultValue != "acme" {
		t.Fatalf("unexpected argument default value: %+v", definition.Arguments[0])
	}
	if definition.Options[0].DefaultValue == nil || *definition.Options[0].DefaultValue != "10" {
		t.Fatalf("unexpected option default value: %+v", definition.Options[0])
	}
	if definition.Options[0].Description != "batch size" {
		t.Fatalf("unexpected option description: %+v", definition.Options[0])
	}
}

func TestCommandContextFromContextNil(t *testing.T) {
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("expected FromContext on plain background context to fail")
	}
}

func TestNilRuntimeCommandContextReturnsDefaults(t *testing.T) {
	var commandCtx *runtimeCommandContext

	if commandCtx.Context() == nil {
		t.Fatal("expected nil runtime context to return background context")
	}
	if commandCtx.CommandName() != "" {
		t.Fatal("expected nil runtime context command name to be empty")
	}
	if commandCtx.Definition() != nil {
		t.Fatal("expected nil runtime context definition to be nil")
	}
	if commandCtx.Input() != nil {
		t.Fatal("expected nil runtime context input to be nil")
	}
	if commandCtx.IO() != nil {
		t.Fatal("expected nil runtime context IO to be nil")
	}
	if commandCtx.CobraCommand() != nil {
		t.Fatal("expected nil runtime context cobra command to be nil")
	}
	if commandCtx.Argument("name") != "" {
		t.Fatal("expected nil runtime context argument to be empty")
	}
	if commandCtx.Arguments("name") != nil {
		t.Fatal("expected nil runtime context arguments to be nil")
	}
	if commandCtx.Option("name") != "" {
		t.Fatal("expected nil runtime context option to be empty")
	}
	if commandCtx.OptionStrings("name") != nil {
		t.Fatal("expected nil runtime context option strings to be nil")
	}
	if commandCtx.OptionBool("name") {
		t.Fatal("expected nil runtime context bool option to be false")
	}
	if commandCtx.OptionInt("name") != 0 {
		t.Fatal("expected nil runtime context int option to be zero")
	}
	if commandCtx.HasOption("name") {
		t.Fatal("expected nil runtime context HasOption to be false")
	}
	if err := commandCtx.Call("child", CallInput{}); err == nil {
		t.Fatal("expected nil runtime context Call with input to fail")
	}
	if err := commandCtx.CallSilently("child", CallInput{}); err == nil {
		t.Fatal("expected nil runtime context CallSilently with input to fail")
	}
}

func TestRenderArgumentListUsesDescriptors(t *testing.T) {
	arguments := []Argument{
		{Name: " tenant ", Description: " primary tenant "},
		{Name: "tags", Description: "labels", IsArray: true},
		{Name: " ", Description: "skipped"},
	}

	descriptors := ArgumentDescriptors(arguments)
	if len(descriptors) != 2 {
		t.Fatalf("ArgumentDescriptors len = %d, want 2", len(descriptors))
	}
	if descriptors[0].Synopsis != "tenant" || descriptors[0].Description != "primary tenant" {
		t.Fatalf("first descriptor = %+v, want trimmed tenant", descriptors[0])
	}
	if descriptors[1].Synopsis != "tags..." || descriptors[1].Description != "labels" {
		t.Fatalf("second descriptor = %+v, want array tags", descriptors[1])
	}

	var out bytes.Buffer
	RenderArgumentList(&out, arguments, OutputOptions{})
	rendered := out.String()
	if !strings.Contains(rendered, "tenant") || !strings.Contains(rendered, "primary tenant") || !strings.Contains(rendered, "tags...") || !strings.Contains(rendered, "labels") {
		t.Fatalf("rendered arguments = %q, want tenant and tags", rendered)
	}
	if strings.Contains(rendered, "skipped") {
		t.Fatalf("rendered arguments = %q, want unnamed argument skipped", rendered)
	}
}
