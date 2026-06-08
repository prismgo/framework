package kernel

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/prismgo/framework/console"
	"github.com/spf13/cobra"
)

func (k *Kernel) promptMissingArguments(ctx context.Context, registered registeredCommand, cobraCmd *cobra.Command, args []string) ([]string, error) {
	if cobraCmd == nil || noInteraction(cobraCmd) {
		return args, nil
	}
	required := requiredArgumentCount(registered.definition)
	if len(args) >= required {
		return args, nil
	}
	input := console.NewInput(registered.definition, cobraCmd, args)
	out := cobraCmd.OutOrStdout()
	errOut := cobraCmd.ErrOrStderr()
	outputOptions := outputOptionsFromCobra(cobraCmd)
	if outputOptions.Quiet || outputOptions.Silent {
		out = io.Discard
		errOut = io.Discard
	}
	ioo := console.NewIOWithOutputOptions(cobraCmd.InOrStdin(), out, errOut, outputOptions)
	commandCtx := console.NewCommandContext(ctx, registered.command, registered.definition, input, ioo, k, cobraCmd)
	prompted := append([]string(nil), args...)

	custom, _ := registered.command.(console.PromptsForMissingInput)
	customPrompts := map[string]console.MissingArgumentPrompt{}
	if custom != nil {
		customPrompts = custom.PromptForMissingArgumentsUsing()
	}
	for _, argument := range registered.definition.Arguments {
		if !argument.Required || len(prompted) >= requiredArgumentPosition(registered.definition, argument.Name)+1 {
			continue
		}
		prompt := customPrompts[argument.Name]
		var (
			answer string
			err    error
		)
		if prompt.Ask != nil {
			answer, err = prompt.Ask(commandCtx, argument)
		} else {
			question := strings.TrimSpace(prompt.Question)
			if question == "" {
				label := strings.TrimSpace(argument.Description)
				if label == "" {
					label = argument.Name
				}
				question = fmt.Sprintf("What is %s?", label)
			}
			if strings.TrimSpace(prompt.Default) != "" {
				answer, err = ioo.Ask(question, prompt.Default)
			} else {
				answer, err = ioo.Ask(question)
			}
		}
		if err != nil {
			return args, err
		}
		if strings.TrimSpace(answer) == "" {
			return prompted, nil
		}
		prompted = append(prompted, answer)
	}
	input = console.NewInput(registered.definition, cobraCmd, prompted)
	commandCtx = console.NewCommandContext(ctx, registered.command, registered.definition, input, ioo, k, cobraCmd)
	if custom != nil {
		if err := custom.AfterPromptingForMissingArguments(commandCtx); err != nil {
			return args, err
		}
	}
	return prompted, nil
}

func requiredArgumentCount(definition console.Definition) int {
	count := 0
	for _, argument := range definition.Arguments {
		if argument.Required {
			count++
		}
	}
	return count
}

func requiredArgumentPosition(definition console.Definition, name string) int {
	position := 0
	for _, argument := range definition.Arguments {
		if argument.Name == name {
			return position
		}
		if argument.Required {
			position++
		}
	}
	return position
}

func noInteraction(cmd *cobra.Command) bool {
	value, err := cmd.Flags().GetBool("no-interaction")
	if err == nil && value {
		return true
	}
	value, err = cmd.InheritedFlags().GetBool("no-interaction")
	return err == nil && value
}
