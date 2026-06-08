package console

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
)

// OutputOptions carries user-facing console rendering switches shared by
// kernel help, list, and framework commands.
type OutputOptions struct {
	ANSI   bool
	Quiet  bool
	Silent bool
}

// CommandListOptions controls Symfony/Laravel-style command list rendering.
type CommandListOptions struct {
	AppName     string
	Description string
	Namespace   string
	Format      string
	Raw         bool
	Short       bool
	Output      OutputOptions
}

type CommandDescriptor struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Aliases     []string `json:"aliases,omitempty"`
	Hidden      bool     `json:"-"`
}

// ArgumentDescriptor 描述命令 help 中一行位置参数说明。
//
// 需求背景：Laravel/Symfony help 会把 input argument 的名称和描述渲染到
// Arguments 区块。Prismgo 的 Definition 已有 Argument.Description 字段，
// 这里用一个轻量展示结构承接“声明数据 -> help 行”的转换结果。
type ArgumentDescriptor struct {
	// Synopsis 是展示给用户看的参数概要，例如 user 或 tags...。
	Synopsis string
	// Description 是参数说明文本，来源于 Definition.Arguments。
	Description string
}

type commandGroup struct {
	Name     string
	Commands []CommandDescriptor
}

const (
	StyleInfo      = "info"
	StyleComment   = "comment"
	StyleError     = "error"
	StyleMuted     = "muted"
	StyleWhiteBold = "white_bold"
	StyleBlueBold  = "blue_bold"
	StyleYellow    = "yellow"
)

var styleCodes = map[string]string{
	StyleInfo:      "32",
	StyleComment:   "33",
	StyleError:     "37;41",
	StyleMuted:     "90",
	StyleWhiteBold: "1;37",
	StyleBlueBold:  "1;34",
	StyleYellow:    "33",
}

// Styled applies a small Symfony/Laravel-aligned ANSI style name.
func Styled(value, style string, opts OutputOptions) string {
	if !opts.ANSI || value == "" {
		return value
	}
	code, ok := styleCodes[style]
	if !ok {
		return value
	}
	return "\x1b[" + code + "m" + value + "\x1b[0m"
}

// RenderCommandList writes command descriptors using Symfony TextDescriptor
// section names and Laravel-friendly grouping.
func RenderCommandList(out io.Writer, definitions []Definition, opts CommandListOptions) error {
	if opts.Output.Quiet || opts.Output.Silent {
		return nil
	}
	commands := visibleCommandDescriptors(definitions, opts.Namespace)
	sort.SliceStable(commands, func(i, j int) bool { return commands[i].Name < commands[j].Name })

	format := strings.ToLower(strings.TrimSpace(opts.Format))
	if format == "" {
		format = "txt"
	}
	switch format {
	case "json":
		payload := map[string]any{"commands": commands}
		if opts.Namespace != "" {
			payload["namespace"] = opts.Namespace
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	case "md", "markdown":
		return renderCommandListMarkdown(out, commands, opts)
	case "txt":
	default:
		return fmt.Errorf("unsupported list format %q", opts.Format)
	}

	if opts.Raw {
		for _, command := range commands {
			fmt.Fprintf(out, "%s %s\n", command.Name, command.Description)
		}
		return nil
	}
	if opts.Short {
		for _, command := range commands {
			fmt.Fprintln(out, command.Name)
		}
		return nil
	}

	if strings.TrimSpace(opts.Description) != "" {
		fmt.Fprintln(out, opts.Description)
		fmt.Fprintln(out)
	}
	fmt.Fprintln(out, Styled("Usage:", StyleComment, opts.Output))
	fmt.Fprintln(out, "  command [options] [arguments]")
	fmt.Fprintln(out)
	fmt.Fprintln(out, Styled("Options:", StyleComment, opts.Output))
	writeOptionLine(out, "-h, --help", "Display help for the given command", opts.Output)
	writeOptionLine(out, "    --silent", "Do not output any message", opts.Output)
	writeOptionLine(out, "-q, --quiet", "Only errors are displayed. All other output is suppressed", opts.Output)
	writeOptionLine(out, "-V, --version", "Display this application version", opts.Output)
	writeOptionLine(out, "    --ansi|--no-ansi", "Force or disable ANSI output", opts.Output)
	writeOptionLine(out, "-n, --no-interaction", "Do not ask any interactive question", opts.Output)
	writeOptionLine(out, "-v|vv|vvv, --verbose", "Increase the verbosity of messages", opts.Output)
	fmt.Fprintln(out)
	fmt.Fprintln(out, Styled("Available Commands:", StyleComment, opts.Output))
	renderCommandGroups(out, groupCommands(commands), opts.Output)
	return nil
}

func renderCommandListMarkdown(out io.Writer, commands []CommandDescriptor, opts CommandListOptions) error {
	title := strings.TrimSpace(opts.AppName)
	if title == "" {
		title = "Commands"
	}
	fmt.Fprintf(out, "# %s\n\n", title)
	for _, command := range commands {
		fmt.Fprintf(out, "- `%s`", command.Name)
		if command.Description != "" {
			fmt.Fprintf(out, " - %s", command.Description)
		}
		if len(command.Aliases) > 0 {
			fmt.Fprintf(out, " _(aliases: %s)_", strings.Join(command.Aliases, ", "))
		}
		fmt.Fprintln(out)
	}
	return nil
}

func renderCommandGroups(out io.Writer, groups []commandGroup, opts OutputOptions) {
	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, group := range groups {
		if group.Name != "" {
			fmt.Fprintf(writer, " %s\n", Styled(group.Name, StyleComment, opts))
		}
		for _, command := range group.Commands {
			name := command.Name
			indent := "  "
			if group.Name != "" {
				indent = "    "
			}
			fmt.Fprintf(writer, "%s%s\t%s\n", indent, Styled(name, StyleInfo, opts), commandDescription(command))
		}
	}
	_ = writer.Flush()
}

func writeOptionLine(out io.Writer, synopsis string, description string, opts OutputOptions) {
	fmt.Fprintf(out, "  %s  %s%s\n", Styled(synopsis, StyleInfo, opts), strings.Repeat(" ", max(0, 22-len(synopsis))), description)
}

// ArgumentDescriptors 将结构化参数定义转换为 help 可渲染的参数行。
//
// 设计思路：该函数只负责展示层转换，不做 Definition 校验；校验仍由
// Definition.Normalize 负责。数组参数使用 tags... 形式展示，和 Usage 中
// 的数组语义保持一致，方便用户理解该参数可接收多个值。
func ArgumentDescriptors(arguments []Argument) []ArgumentDescriptor {
	descriptors := make([]ArgumentDescriptor, 0, len(arguments))
	for _, argument := range arguments {
		name := strings.TrimSpace(argument.Name)
		if name == "" {
			continue
		}
		if argument.IsArray {
			name += "..."
		}
		descriptors = append(descriptors, ArgumentDescriptor{
			Synopsis:    name,
			Description: strings.TrimSpace(argument.Description),
		})
	}
	return descriptors
}

// RenderArgumentList 输出命令 help 的 Arguments 明细行。
//
// 参数说明：
// - out：help 内容写入目标，通常来自 Cobra command 的 stdout。
// - arguments：命令 Definition 中的位置参数声明。
// - opts：当前输出选项，用于决定是否渲染 ANSI 样式。
//
// 逻辑说明：复用 option 行的对齐方式，让 Arguments 与 Options 在终端中
// 采用一致的 synopsis + description 布局。
func RenderArgumentList(out io.Writer, arguments []Argument, opts OutputOptions) {
	for _, argument := range ArgumentDescriptors(arguments) {
		writeOptionLine(out, argument.Synopsis, argument.Description, opts)
	}
}

func commandDescription(command CommandDescriptor) string {
	description := command.Description
	if len(command.Aliases) > 0 {
		description = "[" + strings.Join(command.Aliases, "|") + "] " + description
	}
	return description
}

func visibleCommandDescriptors(definitions []Definition, namespace string) []CommandDescriptor {
	namespace = strings.TrimSpace(namespace)
	commands := make([]CommandDescriptor, 0, len(definitions))
	for _, definition := range definitions {
		if definition.Hidden {
			continue
		}
		if namespace != "" && commandNamespace(definition.Name) != namespace {
			continue
		}
		commands = append(commands, CommandDescriptor{
			Name:        definition.Name,
			Description: definition.Description,
			Aliases:     append([]string(nil), definition.Aliases...),
		})
	}
	return commands
}

func groupCommands(commands []CommandDescriptor) []commandGroup {
	byName := map[string]*commandGroup{}
	for _, command := range commands {
		groupName := commandNamespace(command.Name)
		group := byName[groupName]
		if group == nil {
			group = &commandGroup{Name: groupName}
			byName[groupName] = group
		}
		group.Commands = append(group.Commands, command)
	}
	groups := make([]commandGroup, 0, len(byName))
	for _, group := range byName {
		sort.SliceStable(group.Commands, func(i, j int) bool {
			return group.Commands[i].Name < group.Commands[j].Name
		})
		groups = append(groups, *group)
	}
	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].Name == "" && groups[j].Name != "" {
			return true
		}
		if groups[i].Name != "" && groups[j].Name == "" {
			return false
		}
		return groups[i].Name < groups[j].Name
	})
	return groups
}

func commandNamespace(name string) string {
	namespace, _, ok := strings.Cut(name, ":")
	if !ok {
		return ""
	}
	return strings.TrimSpace(namespace)
}
