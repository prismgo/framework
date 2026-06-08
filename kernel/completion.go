package kernel

import (
	"fmt"

	"github.com/prismgo/framework/console"
	"github.com/spf13/cobra"
)

type completionCommand struct {
	kernel *Kernel
}

func newCompletionCommand(k *Kernel) console.Command {
	return &completionCommand{kernel: k}
}

func (c *completionCommand) Definition() *console.Definition {
	return &console.Definition{
		Name:        "completion",
		Description: "Generate the autocompletion script for the specified shell",
		Arguments: []console.Argument{
			{Name: "shell", Description: "The shell type", Required: true, Suggestions: []string{"bash", "zsh", "fish", "powershell"}},
		},
	}
}

func (c *completionCommand) Handle(ctx console.CommandContext) error {
	if c == nil || c.kernel == nil || c.kernel.rootCmd == nil {
		return fmt.Errorf("completion: kernel is not initialized")
	}
	out := console.OutputWriter(ctx.IO())
	switch ctx.Argument("shell") {
	case "bash":
		return c.kernel.rootCmd.GenBashCompletion(out)
	case "zsh":
		return c.kernel.rootCmd.GenZshCompletion(out)
	case "fish":
		return c.kernel.rootCmd.GenFishCompletion(out, true)
	case "powershell":
		return c.kernel.rootCmd.GenPowerShellCompletion(out)
	default:
		return fmt.Errorf("unsupported shell %q", ctx.Argument("shell"))
	}
}

func registerCompletionHooks(cmd *cobra.Command, definition console.Definition) {
	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
		if len(args) >= len(definition.Arguments) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return definition.Arguments[len(args)].Suggestions, cobra.ShellCompDirectiveNoFileComp
	}
	for _, option := range definition.Options {
		if len(option.Suggestions) == 0 {
			continue
		}
		values := append([]string(nil), option.Suggestions...)
		_ = cmd.RegisterFlagCompletionFunc(option.Name, func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
			return values, cobra.ShellCompDirectiveNoFileComp
		})
	}
}
