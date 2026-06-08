package cmd

import (
	"strings"

	"github.com/prismgo/framework/console"
)

// ListCommand 展示当前 Console Kernel 中已注册的可见命令。
type ListCommand struct {
	definitions func() []console.Definition
}

// NewListCommand 创建命令列表展示命令。
func NewListCommand(definitions func() []console.Definition) *ListCommand {
	return &ListCommand{definitions: definitions}
}

func (c *ListCommand) Definition() *console.Definition {
	definition := console.MustDefinition("list {namespace? : The namespace name} {--raw : To output raw command list} {--format=txt : The output format (txt, json, md)} {--short : To skip describing commands' arguments}", "列出所有可用命令")
	definition.Aliases = []string{"ls"}
	definition.Examples = []string{
		"go run ./ list",
		"go run ./ list | findstr serve",
	}
	return definition
}

func (c *ListCommand) Handle(ctx console.CommandContext) error {
	out := console.OutputWriter(ctx.IO())
	opts := console.CommandListOptions{
		AppName:   "PrismGo",
		Namespace: strings.TrimSpace(ctx.Input().Argument("namespace")),
		Raw:       ctx.Input().OptionBool("raw"),
		Format:    strings.TrimSpace(ctx.Input().Option("format")),
		Short:     ctx.Input().OptionBool("short"),
		Output:    console.OutputOptionsForIO(ctx.IO()),
	}
	return console.RenderCommandList(out, c.commandDefinitions(), opts)
}

func (c *ListCommand) commandDefinitions() []console.Definition {
	if c.definitions == nil {
		return nil
	}
	return c.definitions()
}
