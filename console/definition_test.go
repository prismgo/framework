package console

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestDefinitionCloneUsageAndNormalize(t *testing.T) {
	defaultValue := " user "
	definition := Definition{
		Name:        " report:send ",
		Description: " Send reports ",
		Arguments: []Argument{
			{Name: " tenant ", Description: " Tenant ", Required: true},
			{Name: " user ", Required: true, DefaultValue: &defaultValue},
			{Name: " tags ", IsArray: true},
		},
		Options: []Option{
			{Name: " queue ", Shortcut: " q ", Description: " Queue ", ValueMode: OptionValueRequired},
			{Name: " force ", ValueMode: OptionValueNone},
			{Name: " tag ", ValueMode: OptionValueRequired, IsArray: true, DefaultValue: &defaultValue},
		},
		Aliases:  []string{" report ", "report", ""},
		Examples: []string{" go run ./ report:send ", "go run ./ report:send"},
	}

	normalized, err := NormalizeDefinition(definition)
	if err != nil {
		t.Fatalf("NormalizeDefinition returned error: %v", err)
	}
	if normalized.Name != "report:send" || normalized.Description != "Send reports" {
		t.Fatalf("normalized definition = %+v, want trimmed name and description", normalized)
	}
	if got := DefinitionUsage(normalized); got != "report:send <tenant> [user] [tags...]" {
		t.Fatalf("DefinitionUsage = %q", got)
	}
	if normalized.Arguments[1].Required {
		t.Fatal("argument with default should not remain required")
	}
	if got := *normalized.Arguments[1].DefaultValue; got != "user" {
		t.Fatalf("argument default = %q", got)
	}
	if got := *normalized.Options[2].DefaultValue; got != "user" {
		t.Fatalf("option default = %q", got)
	}
	if !reflect.DeepEqual(normalized.Aliases, []string{"report"}) {
		t.Fatalf("aliases = %#v", normalized.Aliases)
	}
	if !reflect.DeepEqual(normalized.Examples, []string{"go run ./ report:send"}) {
		t.Fatalf("examples = %#v", normalized.Examples)
	}

	clone := CloneDefinition(normalized)
	clone.Arguments[0].Name = "changed"
	clone.Options[0].Name = "changed"
	clone.Aliases[0] = "changed"
	clone.Examples[0] = "changed"
	if normalized.Arguments[0].Name != "tenant" || normalized.Options[0].Name != "queue" || normalized.Aliases[0] != "report" || normalized.Examples[0] != "go run ./ report:send" {
		t.Fatal("CloneDefinition did not isolate slices")
	}
}

func TestDefinitionNormalizeRejectsInvalidShapes(t *testing.T) {
	ptr := func(value string) *string { return &value }
	cases := []struct {
		name       string
		definition Definition
		want       string
	}{
		{name: "missing command", definition: Definition{}, want: "command name is required"},
		{name: "missing argument", definition: Definition{Name: "x", Arguments: []Argument{{}}}, want: "argument name is required"},
		{name: "duplicate argument", definition: Definition{Name: "x", Arguments: []Argument{{Name: "id"}, {Name: " id "}}}, want: "duplicated argument"},
		{name: "optional before required", definition: Definition{Name: "x", Arguments: []Argument{{Name: "first"}, {Name: "second", Required: true}}}, want: "required argument"},
		{name: "default before required", definition: Definition{Name: "x", Arguments: []Argument{{Name: "first", DefaultValue: ptr("default")}, {Name: "second", Required: true}}}, want: "required argument"},
		{name: "array before argument", definition: Definition{Name: "x", Arguments: []Argument{{Name: "items", IsArray: true}, {Name: "tail"}}}, want: "array argument"},
		{name: "missing option", definition: Definition{Name: "x", Options: []Option{{}}}, want: "option name is required"},
		{name: "duplicate option", definition: Definition{Name: "x", Options: []Option{{Name: "force"}, {Name: " force "}}}, want: "duplicated option"},
		{name: "duplicate shortcut", definition: Definition{Name: "x", Options: []Option{{Name: "force", Shortcut: "f"}, {Name: "fresh", Shortcut: "f"}}}, want: "duplicated option shortcut"},
		{name: "invalid shortcut length", definition: Definition{Name: "x", Options: []Option{{Name: "force", Shortcut: "ff"}}}, want: "option shortcut"},
		{name: "array option without value", definition: Definition{Name: "x", Options: []Option{{Name: "tag", IsArray: true, ValueMode: OptionValueNone}}}, want: "array option"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeDefinition(tc.definition)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NormalizeDefinition error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestDefinitionNormalizeAllowsFinalOptionalArrayArgument(t *testing.T) {
	definition, err := NormalizeDefinition(Definition{
		Name:      "x",
		Arguments: []Argument{{Name: "items", IsArray: true}},
	})
	if err != nil {
		t.Fatalf("NormalizeDefinition returned error: %v", err)
	}
	if len(definition.Arguments) != 1 || !definition.Arguments[0].IsArray || definition.Arguments[0].Required {
		t.Fatalf("unexpected normalized arguments: %+v", definition.Arguments)
	}
}

func TestCommandContractsCompile(t *testing.T) {
	var _ Command = contractCommand{}
	var _ CommandFactory = func() Command { return contractCommand{} }
	var _ CommandContext = contractContext{}
	var _ CommandCaller = contractCaller{}
	var _ Isolatable = contractIsolatable{}
}

type contractCommand struct{}

func (contractCommand) Definition() *Definition { return &Definition{Name: "contract:run"} }
func (contractCommand) Handle(CommandContext) error {
	return nil
}

type contractContext struct{}

func (contractContext) Context() context.Context                { return context.Background() }
func (contractContext) CommandName() string                     { return "contract:run" }
func (contractContext) Definition() *Definition                 { return &Definition{Name: "contract:run"} }
func (contractContext) Input() Input                            { return nil }
func (contractContext) IO() IO                                  { return nil }
func (contractContext) Call(string, ...CallInput) error         { return nil }
func (contractContext) CallSilently(string, ...CallInput) error { return nil }
func (contractContext) Fail(...any) error                       { return nil }
func (contractContext) Trap([]os.Signal, func(os.Signal)) (func(), error) {
	return func() {}, nil
}
func (contractContext) Argument(string) string        { return "" }
func (contractContext) Arguments(string) []string     { return nil }
func (contractContext) Option(string) string          { return "" }
func (contractContext) OptionStrings(string) []string { return nil }
func (contractContext) OptionBool(string) bool        { return false }
func (contractContext) OptionInt(string) int          { return 0 }
func (contractContext) HasOption(string) bool         { return false }

type contractCaller struct{}

func (contractCaller) Call(context.Context, string, ...CallInput) error         { return nil }
func (contractCaller) CallSilently(context.Context, string, ...CallInput) error { return nil }

type contractIsolatable struct{}

func (contractIsolatable) IsolationKey(CommandContext) string { return "contract" }
