package console

import (
	"fmt"
	"strings"

	consolecontract "github.com/prismgo/framework/contracts/console"
)

type OptionValueMode = consolecontract.OptionValueMode

const (
	OptionValueNone     = consolecontract.OptionValueNone
	OptionValueRequired = consolecontract.OptionValueRequired
	OptionValueOptional = consolecontract.OptionValueOptional
)

type Argument = consolecontract.Argument
type Option = consolecontract.Option
type Definition = consolecontract.Definition
type CallInput = consolecontract.CallInput
type MissingArgumentPrompt = consolecontract.MissingArgumentPrompt
type PromptsForMissingInput = consolecontract.PromptsForMissingInput
type ChoiceOptions = consolecontract.ChoiceOptions

// CloneDefinition 复制 Definition，避免注册时修改调用方传入的数据。
func CloneDefinition(definition Definition) Definition {
	cloned := definition
	cloned.Arguments = append([]Argument(nil), definition.Arguments...)
	cloned.Options = append([]Option(nil), definition.Options...)
	cloned.Aliases = append([]string(nil), definition.Aliases...)
	cloned.Examples = append([]string(nil), definition.Examples...)
	for i := range cloned.Arguments {
		cloned.Arguments[i].Suggestions = append([]string(nil), definition.Arguments[i].Suggestions...)
	}
	for i := range cloned.Options {
		cloned.Options[i].Suggestions = append([]string(nil), definition.Options[i].Suggestions...)
	}
	return cloned
}

// DefinitionUsage 返回适合 Cobra Use 字段的参数用法字符串。
func DefinitionUsage(definition Definition) string {
	if strings.TrimSpace(definition.UsageText) != "" {
		return strings.TrimSpace(definition.UsageText)
	}
	parts := []string{strings.TrimSpace(definition.Name)}
	for _, arg := range definition.Arguments {
		parts = append(parts, formatArgumentUsage(arg))
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

// NormalizeDefinition 校验并整理 Definition，返回适合注册到 Kernel 的规范化定义。
func NormalizeDefinition(definition Definition) (Definition, error) {
	cloned := CloneDefinition(definition)
	cloned.Name = strings.TrimSpace(cloned.Name)
	cloned.Description = strings.TrimSpace(cloned.Description)
	if cloned.Name == "" {
		return Definition{}, fmt.Errorf("console definition: command name is required")
	}

	argumentNames := make(map[string]struct{}, len(cloned.Arguments))
	optionalArgumentSeen := false
	arrayArgumentSeen := false
	for i := range cloned.Arguments {
		arg := &cloned.Arguments[i]
		arg.Name = strings.TrimSpace(arg.Name)
		arg.Description = strings.TrimSpace(arg.Description)
		arg.Suggestions = normalizeStrings(arg.Suggestions)
		if arg.Name == "" {
			return Definition{}, fmt.Errorf("console definition: argument name is required")
		}
		if _, exists := argumentNames[arg.Name]; exists {
			return Definition{}, fmt.Errorf("console definition: duplicated argument %q", arg.Name)
		}
		argumentNames[arg.Name] = struct{}{}
		if arg.DefaultValue != nil {
			value := strings.TrimSpace(*arg.DefaultValue)
			arg.DefaultValue = &value
			arg.Required = false
		}
		if arrayArgumentSeen {
			return Definition{}, fmt.Errorf("console definition: argument %q cannot follow array argument", arg.Name)
		}
		if arg.Required && optionalArgumentSeen {
			return Definition{}, fmt.Errorf("console definition: required argument %q cannot follow optional argument", arg.Name)
		}
		if !arg.Required {
			optionalArgumentSeen = true
		}
		if arg.IsArray {
			arrayArgumentSeen = true
		}
	}

	optionNames := make(map[string]struct{}, len(cloned.Options))
	shortcutNames := make(map[string]struct{}, len(cloned.Options))
	for i := range cloned.Options {
		opt := &cloned.Options[i]
		opt.Name = strings.TrimSpace(opt.Name)
		opt.Shortcut = strings.TrimSpace(opt.Shortcut)
		opt.Description = strings.TrimSpace(opt.Description)
		opt.Suggestions = normalizeStrings(opt.Suggestions)
		if opt.Name == "" {
			return Definition{}, fmt.Errorf("console definition: option name is required")
		}
		if _, exists := optionNames[opt.Name]; exists {
			return Definition{}, fmt.Errorf("console definition: duplicated option %q", opt.Name)
		}
		optionNames[opt.Name] = struct{}{}
		if opt.Shortcut != "" {
			if len([]rune(opt.Shortcut)) != 1 || opt.Shortcut == "-" {
				return Definition{}, fmt.Errorf("console definition: option shortcut %q must be a single character", opt.Shortcut)
			}
			if _, exists := shortcutNames[opt.Shortcut]; exists {
				return Definition{}, fmt.Errorf("console definition: duplicated option shortcut %q", opt.Shortcut)
			}
			shortcutNames[opt.Shortcut] = struct{}{}
		}
		if opt.DefaultValue != nil {
			value := strings.TrimSpace(*opt.DefaultValue)
			opt.DefaultValue = &value
		}
		if opt.IsArray && opt.ValueMode == OptionValueNone {
			return Definition{}, fmt.Errorf("console definition: array option %q must accept values", opt.Name)
		}
	}

	cloned.Aliases = normalizeStrings(cloned.Aliases)
	cloned.Examples = normalizeStrings(cloned.Examples)
	return cloned, nil
}

// MustDefinition 通过 signature DSL 构造 Definition，解析失败时直接 panic。
//
// 用途：供命令以简洁写法声明自己的 Definition，避免每个文件重复处理解析错误。
func MustDefinition(signature string, description string) *Definition {
	definition, err := ParseSignature(signature)
	if err != nil {
		panic(err)
	}
	definition.Description = strings.TrimSpace(description)
	return &definition
}

func formatArgumentUsage(arg Argument) string {
	name := arg.Name
	if arg.IsArray {
		name += "..."
	}
	if arg.Required {
		return "<" + name + ">"
	}
	return "[" + name + "]"
}

func normalizeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}
