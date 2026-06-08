package console

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestConfirmChoiceAndOptionReadersExtraBranches(t *testing.T) {
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	io := NewIO(strings.NewReader("maybe\nfirst\n"), stdout, stderr)

	confirmed, err := io.Confirm("continue", true)
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if confirmed {
		t.Fatal("expected ambiguous answer to resolve to false")
	}

	choice, err := io.Choice("pick", []string{"first", "second"})
	if err != nil {
		t.Fatalf("Choice returned error: %v", err)
	}
	if choice != "first" {
		t.Fatalf("Choice = %q, want first", choice)
	}

	definition := Definition{
		Name: "sample",
		Options: []Option{
			{Name: "names", ValueMode: OptionValueRequired, IsArray: true},
			{Name: "enabled", ValueMode: OptionValueNone},
		},
	}
	cmd := &cobra.Command{Use: "sample"}
	if err := BindDefinitionFlags(cmd, definition); err != nil {
		t.Fatalf("BindDefinitionFlags returned error: %v", err)
	}
	if err := cmd.Flags().Set("enabled", "true"); err != nil {
		t.Fatalf("set enabled failed: %v", err)
	}
	input := NewInput(definition, cmd, nil)
	if got := input.Option("names"); got != "" {
		t.Fatalf("Option(names) = %q, want empty", got)
	}
	if got := input.Argument("missing"); got != "" {
		t.Fatalf("Argument(missing) = %q, want empty", got)
	}
	if !input.OptionBool("enabled") {
		t.Fatal("expected OptionBool(enabled) = true")
	}
}
