package console

import (
	"reflect"
	"testing"

	"github.com/spf13/cobra"
)

func TestNewInputReadsArgumentsAndOptions(t *testing.T) {
	definition := Definition{
		Name: "sample:run",
		Arguments: []Argument{
			{Name: "tenant", Required: true},
			{Name: "tags", IsArray: true},
		},
		Options: []Option{
			{Name: "queue", ValueMode: OptionValueRequired},
			{Name: "force", ValueMode: OptionValueNone},
			{Name: "tag", ValueMode: OptionValueRequired, IsArray: true},
		},
	}
	cmd := &cobra.Command{Use: "sample:run"}
	if err := BindDefinitionFlags(cmd, definition); err != nil {
		t.Fatalf("BindDefinitionFlags returned error: %v", err)
	}
	if err := cmd.Flags().Set("queue", "high"); err != nil {
		t.Fatalf("set queue flag failed: %v", err)
	}
	if err := cmd.Flags().Set("force", "true"); err != nil {
		t.Fatalf("set force flag failed: %v", err)
	}
	if err := cmd.Flags().Set("tag", "a"); err != nil {
		t.Fatalf("set first tag failed: %v", err)
	}
	if err := cmd.Flags().Set("tag", "b"); err != nil {
		t.Fatalf("set second tag failed: %v", err)
	}

	input := NewInput(definition, cmd, []string{"tenant-a", "x", "y"})
	if got := input.Argument("tenant"); got != "tenant-a" {
		t.Fatalf("input.Argument(tenant) = %q, want tenant-a", got)
	}
	if got := input.Arguments("tags"); !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("input.Arguments(tags) = %#v, want [x y]", got)
	}
	if got := input.Option("queue"); got != "high" {
		t.Fatalf("input.Option(queue) = %q, want high", got)
	}
	if !input.OptionBool("force") {
		t.Fatal("expected input.OptionBool(force) = true")
	}
	if got := input.OptionStrings("tag"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("input.OptionStrings(tag) = %#v, want [a b]", got)
	}
	if !input.HasOption("queue") {
		t.Fatal("expected HasOption(queue) = true for defined option")
	}
	if !input.HasOption("force") {
		t.Fatal("expected HasOption(force) = true for defined bool option")
	}
	if !input.HasOption("tag") {
		t.Fatal("expected HasOption(tag) = true for defined array option")
	}
	if input.HasOption("missing") {
		t.Fatal("expected HasOption(missing) = false")
	}
}

func TestNewInputReadsInheritedOptionDefinitionsAndValues(t *testing.T) {
	root := &cobra.Command{Use: "root"}
	root.PersistentFlags().Bool("verbose", false, "")
	root.PersistentFlags().StringArray("label", nil, "")
	child := &cobra.Command{Use: "child"}
	root.AddCommand(child)
	if err := child.ParseFlags([]string{"--verbose", "--label=first", "--label=second"}); err != nil {
		t.Fatalf("ParseFlags returned error: %v", err)
	}

	input := NewInput(Definition{Name: "child"}, child, nil)
	if !input.HasOption("verbose") {
		t.Fatal("expected HasOption(verbose) = true for inherited option")
	}
	if !input.HasOption("label") {
		t.Fatal("expected HasOption(label) = true for inherited array option")
	}
	if !input.OptionBool("verbose") {
		t.Fatal("expected OptionBool(verbose) = true from inherited flag")
	}
	if got := input.OptionStrings("label"); !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("input.OptionStrings(label) = %#v, want [first second]", got)
	}
}

func TestBindArgumentsUsesDefaultValue(t *testing.T) {
	defaultTenant := "tenant-default"
	definition := Definition{
		Name:      "sample:run",
		Arguments: []Argument{{Name: "tenant", DefaultValue: &defaultTenant}},
	}
	input := NewInput(definition, &cobra.Command{Use: "sample:run"}, nil)
	if got := input.Argument("tenant"); got != defaultTenant {
		t.Fatalf("input.Argument(tenant) = %q, want %q", got, defaultTenant)
	}
}

func TestBindDefinitionFlagsSupportsOptionalValueOption(t *testing.T) {
	defaultQueue := "redis"
	definition := Definition{
		Name:    "sample:run",
		Options: []Option{{Name: "queue", ValueMode: OptionValueOptional, DefaultValue: &defaultQueue}},
	}
	cmd := &cobra.Command{Use: "sample:run"}
	if err := BindDefinitionFlags(cmd, definition); err != nil {
		t.Fatalf("BindDefinitionFlags returned error: %v", err)
	}
	if err := cmd.Flags().Parse([]string{"--queue"}); err != nil {
		t.Fatalf("ParseFlags returned error: %v", err)
	}
	input := NewInput(definition, cmd, nil)
	if got := input.Option("queue"); got != "" {
		t.Fatalf("input.Option(queue) = %q, want empty string for bare optional option", got)
	}
}
