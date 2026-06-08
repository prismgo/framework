package console

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestBindDefinitionFlagsAndInputExtraBranches(t *testing.T) {
	defaultArray := "seed"
	defaultQueue := "high"
	definition := Definition{
		Name: "sample",
		Options: []Option{
			{Name: "tag", Shortcut: "t", ValueMode: OptionValueRequired, IsArray: true, DefaultValue: &defaultArray},
			{Name: "queue", Shortcut: "q", ValueMode: OptionValueRequired, DefaultValue: &defaultQueue},
			{Name: "enabled", ValueMode: OptionValueNone},
		},
	}
	cmd := &cobra.Command{Use: "sample"}
	if err := BindDefinitionFlags(cmd, definition); err != nil {
		t.Fatalf("BindDefinitionFlags returned error: %v", err)
	}
	if err := cmd.Flags().Set("tag", "a"); err != nil {
		t.Fatalf("set first tag failed: %v", err)
	}
	if err := cmd.Flags().Set("tag", "b"); err != nil {
		t.Fatalf("set second tag failed: %v", err)
	}
	if err := cmd.Flags().Set("enabled", "false"); err != nil {
		t.Fatalf("set enabled failed: %v", err)
	}
	input := NewInput(definition, cmd, nil)
	if got := input.OptionStrings("tag"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("OptionStrings(tag) = %#v, want [a b]", got)
	}
	if got := input.Option("queue"); got != "high" {
		t.Fatalf("Option(queue) = %q, want high", got)
	}
	if input.OptionBool("enabled") {
		t.Fatal("expected OptionBool(enabled) = false")
	}
	if !input.HasOption("enabled") {
		t.Fatal("expected HasOption(enabled) = true after setting flag")
	}
}

func TestAskUsesDefaultOnEOFAndChoiceSupportsNumericSelection(t *testing.T) {
	io := NewIO(strings.NewReader("1\n"), &strings.Builder{}, &strings.Builder{})
	choice, err := io.Choice("pick", []string{"first", "second"})
	if err != nil {
		t.Fatalf("Choice returned error: %v", err)
	}
	if choice != "first" {
		t.Fatalf("Choice = %q, want first", choice)
	}

	eofIO := NewIO(strings.NewReader(""), &strings.Builder{}, &strings.Builder{})
	answer, err := eofIO.Ask("name", "fallback")
	if err != nil {
		t.Fatalf("Ask returned error on EOF: %v", err)
	}
	if answer != "fallback" {
		t.Fatalf("Ask fallback = %q, want fallback", answer)
	}
}
