package console

import (
	"testing"

	consolecontract "github.com/prismgo/framework/contracts/console"
)

var (
	_ Command                        = consolecontract.Command(nil)
	_ consolecontract.Command        = Command(nil)
	_ CommandFactory                 = consolecontract.CommandFactory(nil)
	_ consolecontract.CommandFactory = CommandFactory(nil)
	_ Input                          = consolecontract.Input(nil)
	_ consolecontract.Input          = Input(nil)
	_ IO                             = consolecontract.IO(nil)
	_ consolecontract.IO             = IO(nil)
	_ Progress                       = consolecontract.Progress(nil)
	_ consolecontract.Progress       = Progress(nil)
	_ CommandCaller                  = consolecontract.CommandCaller(nil)
	_ consolecontract.CommandCaller  = CommandCaller(nil)
	_ consolecontract.CommandContext = (CommandContext)(nil)
	_ Isolatable                     = consolecontract.Isolatable(nil)
	_ consolecontract.Isolatable     = Isolatable(nil)
	_ IsolationContext               = consolecontract.CommandContext(nil)
	_ consolecontract.CommandContext = IsolationContext(nil)
)

func TestDefinitionAliasesContractType(t *testing.T) {
	definition := Definition{Name: "sample"}
	contractDefinition := consolecontract.Definition(definition)
	consoleDefinition := Definition(contractDefinition)
	if consoleDefinition.Name != "sample" {
		t.Fatalf("Definition alias name = %q", consoleDefinition.Name)
	}

	argument := Argument{Name: "id"}
	contractArgument := consolecontract.Argument(argument)
	if contractArgument.Name != "id" {
		t.Fatalf("Argument alias name = %q", contractArgument.Name)
	}

	option := Option{Name: "force", ValueMode: OptionValueNone}
	contractOption := consolecontract.Option(option)
	if contractOption.ValueMode != consolecontract.OptionValueNone {
		t.Fatalf("Option alias value mode = %v", contractOption.ValueMode)
	}
	mode := consolecontract.OptionValueMode(OptionValueRequired)
	if mode != consolecontract.OptionValueRequired {
		t.Fatalf("OptionValueMode alias = %v", mode)
	}
	optionalMode := consolecontract.OptionValueMode(OptionValueOptional)
	if optionalMode != consolecontract.OptionValueOptional {
		t.Fatalf("OptionValueOptional alias = %v", optionalMode)
	}
}
