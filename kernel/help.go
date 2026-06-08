package kernel

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/version"
	"github.com/spf13/cobra"
)

type helpCommand struct {
	name        string
	description string
}

type helpGroup struct {
	name       string
	namespaced bool
	parent     *helpCommand
	children   []helpCommand
}

func (k *Kernel) installHelp() {
	k.rootCmd.SetHelpFunc(func(cmd *cobra.Command, _ []string) {
		if cmd == k.rootCmd {
			renderRootHelp(cmd)
			return
		}
		renderCommandHelp(cmd)
	})
}

func renderRootHelp(cmd *cobra.Command) {
	out := cmd.OutOrStdout()
	_ = console.RenderCommandList(out, collectDefinitions(cmd), console.CommandListOptions{
		AppName:     cmd.CommandPath(),
		Description: cmd.Short,
		Output:      outputOptionsFromCobra(cmd),
	})
}

func renderCommandHelp(cmd *cobra.Command) {
	renderCommandHelpWithArguments(cmd, nil)
}

// renderCommandHelpWithArguments 输出单个命令的 help 内容，并可附带
// Definition 中声明的位置参数说明。
//
// 需求背景：Cobra 的 UseLine 只能展示参数概要，无法展示 Prismgo
// Definition.Argument.Description。这里由 Kernel 在注册命令时传入已规范化
// 的参数定义，使 `command --help` 能对齐 Laravel 13 的 Arguments 区块。
func renderCommandHelpWithArguments(cmd *cobra.Command, arguments []console.Argument) {
	out := cmd.OutOrStdout()
	outputOptions := outputOptionsFromCobra(cmd)
	if outputOptions.Quiet || outputOptions.Silent {
		return
	}
	description := cmd.Short
	if strings.TrimSpace(cmd.Long) != "" {
		if strings.TrimSpace(description) == "" {
			description = cmd.Long
		} else {
			description = description + "\n" + cmd.Long
		}
	}
	writeDescription(out, description)
	writeSection(out, "Usage:", outputOptions)
	fmt.Fprintf(out, "  %s\n", cmd.UseLine())
	if len(cmd.Aliases) > 0 {
		fmt.Fprintln(out)
		writeSection(out, "Aliases:", outputOptions)
		fmt.Fprintf(out, "  %s\n", strings.Join(cmd.Aliases, ", "))
	}
	if len(console.ArgumentDescriptors(arguments)) > 0 {
		fmt.Fprintln(out)
		writeSection(out, "Arguments:", outputOptions)
		console.RenderArgumentList(out, arguments, outputOptions)
	}
	if cmd.HasAvailableSubCommands() {
		fmt.Fprintln(out)
		writeAvailableCommands(out, groupedHelpCommands(cmd), outputOptions)
	}
	if strings.TrimSpace(cmd.Example) != "" {
		fmt.Fprintln(out)
		writeSection(out, "Examples:", outputOptions)
		fmt.Fprintln(out, cmd.Example)
	}
	writeFlags(out, "Options", cmd.LocalFlags().FlagUsages(), outputOptions)
	writeFlags(out, "Global Options", cmd.InheritedFlags().FlagUsages(), outputOptions)
}

func writeDescription(out io.Writer, description string) {
	if strings.TrimSpace(description) == "" {
		return
	}
	fmt.Fprintln(out, description)
	fmt.Fprintln(out)
}

func writeAvailableCommands(out io.Writer, groups []helpGroup, opts console.OutputOptions) {
	if len(groups) == 0 {
		return
	}
	writeSection(out, "Available Commands:", opts)
	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, group := range groups {
		if group.parent != nil {
			fmt.Fprintf(writer, "  %s\t%s\n", console.Styled(group.parent.name, console.StyleInfo, opts), group.parent.description)
		} else {
			fmt.Fprintf(writer, "  %s\n", console.Styled(group.name, console.StyleComment, opts))
		}
		for _, child := range group.children {
			fmt.Fprintf(writer, "    %s\t%s\n", console.Styled(child.name, console.StyleInfo, opts), child.description)
		}
	}
	_ = writer.Flush()
	fmt.Fprintln(out)
}

func writeFlags(out io.Writer, title string, usages string, opts console.OutputOptions) {
	usages = strings.TrimRight(usages, "\n")
	if strings.TrimSpace(usages) == "" {
		return
	}
	fmt.Fprintln(out)
	writeSection(out, title+":", opts)
	fmt.Fprintln(out, usages)
}

func writeSection(out io.Writer, title string, opts console.OutputOptions) {
	fmt.Fprintln(out, console.Styled(title, console.StyleComment, opts))
}

func groupedHelpCommands(cmd *cobra.Command) []helpGroup {
	groupByName := make(map[string]*helpGroup)
	for _, child := range cmd.Commands() {
		if !child.IsAvailableCommand() && child.Name() != "help" {
			continue
		}
		entry := helpCommand{name: child.Name(), description: child.Short}
		namespace, namespaced := commandNamespace(entry.name)
		if !namespaced {
			group := ensureHelpGroup(groupByName, entry.name)
			group.parent = &entry
			continue
		}
		group := ensureHelpGroup(groupByName, namespaceGroupKey(namespace))
		group.name = namespace
		group.namespaced = true
		group.children = append(group.children, entry)
	}

	groups := make([]helpGroup, 0, len(groupByName))
	for _, group := range groupByName {
		sort.SliceStable(group.children, func(i, j int) bool {
			return group.children[i].name < group.children[j].name
		})
		groups = append(groups, *group)
	}
	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].namespaced != groups[j].namespaced {
			return !groups[i].namespaced
		}
		return groups[i].name < groups[j].name
	})
	return groups
}

func ensureHelpGroup(groups map[string]*helpGroup, name string) *helpGroup {
	if group, ok := groups[name]; ok {
		return group
	}
	group := &helpGroup{name: name}
	groups[name] = group
	return group
}

func commandNamespace(name string) (string, bool) {
	namespace, _, ok := strings.Cut(name, ":")
	namespace = strings.TrimSpace(namespace)
	return namespace, ok && namespace != ""
}

func namespaceGroupKey(namespace string) string {
	return "namespace:" + namespace
}

func collectDefinitions(cmd *cobra.Command) []console.Definition {
	if cmd == nil {
		return nil
	}
	root := cmd.Root()
	definitions := make([]console.Definition, 0, len(root.Commands()))
	for _, child := range root.Commands() {
		if child.Name() == "help" || child.Hidden {
			continue
		}
		definitions = append(definitions, console.Definition{
			Name:        child.Name(),
			Description: child.Short,
			Aliases:     append([]string(nil), child.Aliases...),
			Hidden:      child.Hidden,
		})
	}
	return definitions
}

func outputOptionsFromCobra(cmd *cobra.Command) console.OutputOptions {
	if cmd == nil {
		return console.OutputOptions{}
	}
	ansiSet, noANSISet := explicitANSIOverride(cmd)
	quietValue, _ := cmd.Flags().GetBool("quiet")
	silentValue, _ := cmd.Flags().GetBool("silent")
	if !quietValue {
		quietValue, _ = cmd.InheritedFlags().GetBool("quiet")
	}
	if !silentValue {
		silentValue, _ = cmd.InheritedFlags().GetBool("silent")
	}
	return console.ResolveOutputOptions(cmd.OutOrStdout(), ansiSet, noANSISet, quietValue, silentValue)
}

// explicitANSIOverride 提取当前命令解析结果中的显式 ANSI 覆盖状态。
// 设计原因：显式 --ansi/--no-ansi 与环境变量自动探测属于两层职责；
// Kernel 先把显式覆盖收口，再交给 console 输出层决定自动探测分支，避免规则混杂。
func explicitANSIOverride(cmd *cobra.Command) (bool, bool) {
	ansiValue, ansiChanged := boolFlagValue(cmd, "ansi")
	noANSIValue, noANSIChanged := boolFlagValue(cmd, "no-ansi")
	if ansiChanged && ansiValue {
		return true, false
	}
	if noANSIChanged && noANSIValue {
		return false, true
	}
	return false, false
}

func boolFlagValue(cmd *cobra.Command, name string) (bool, bool) {
	if cmd == nil {
		return false, false
	}
	if flag := cmd.Flags().Lookup(name); flag != nil {
		value, _ := cmd.Flags().GetBool(name)
		return value, flag.Changed
	}
	if flag := cmd.InheritedFlags().Lookup(name); flag != nil {
		value, _ := cmd.InheritedFlags().GetBool(name)
		return value, flag.Changed
	}
	return false, false
}

func versionRequested(cmd *cobra.Command) bool {
	if cmd == nil {
		return false
	}
	value, _ := cmd.Flags().GetBool("version")
	if value {
		return true
	}
	value, _ = cmd.InheritedFlags().GetBool("version")
	return value
}

func renderVersion(cmd *cobra.Command) error {
	if cmd == nil {
		return nil
	}
	if outputOptionsFromCobra(cmd).Quiet || outputOptionsFromCobra(cmd).Silent {
		return nil
	}
	_, err := fmt.Fprintln(cmd.OutOrStdout(), version.Banner())
	return err
}
