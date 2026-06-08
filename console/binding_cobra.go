package console

import (
	"fmt"

	"github.com/spf13/cobra"
)

// optionalOptionNoValueSentinel 用于承接“显式传入可选值 option 但未提供值”的临时绑定值。
// 设计原因：pflag 的 String/StringArray 需要通过 NoOptDefVal 才能接受裸 --flag 形式；
// 这里先写入私有哨兵值，再由 Input 读取层统一还原成空字符串，避免把绑定层细节泄漏给命令实现。
const optionalOptionNoValueSentinel = "__prismgo_optional_no_value__"

// BindDefinitionFlags 根据结构化 Definition 为 Cobra 命令注册 flags。
//
// 用途：把 signature DSL 解析得到的 Option 定义统一绑定成 Cobra flags，避免每个命令重复书写
// 样板参数注册逻辑。
// 设计原因：将 flag 绑定逻辑集中后，Kernel 注册流程可以只关心“编译命令定义”和“执行命令”，
// 代码更简洁，也更容易扩展新的 option 语义。
func BindDefinitionFlags(cmd *cobra.Command, definition Definition) error {
	for _, option := range definition.Options {
		description := option.Description
		switch {
		case option.IsArray:
			defaultValue := []string(nil)
			if option.DefaultValue != nil && *option.DefaultValue != "" {
				defaultValue = []string{*option.DefaultValue}
			}
			if option.Shortcut != "" {
				cmd.Flags().StringArrayP(option.Name, option.Shortcut, defaultValue, description)
			} else {
				cmd.Flags().StringArray(option.Name, defaultValue, description)
			}
			if option.ValueMode == OptionValueOptional {
				if flag := cmd.Flags().Lookup(option.Name); flag != nil {
					flag.NoOptDefVal = optionalOptionNoValueSentinel
				}
			}
		case option.ValueMode == OptionValueRequired || option.ValueMode == OptionValueOptional:
			defaultValue := ""
			if option.DefaultValue != nil {
				defaultValue = *option.DefaultValue
			}
			if option.Shortcut != "" {
				cmd.Flags().StringP(option.Name, option.Shortcut, defaultValue, description)
			} else {
				cmd.Flags().String(option.Name, defaultValue, description)
			}
			if option.ValueMode == OptionValueOptional {
				if flag := cmd.Flags().Lookup(option.Name); flag != nil {
					flag.NoOptDefVal = optionalOptionNoValueSentinel
				}
			}
		case option.ValueMode == OptionValueNone:
			defaultValue := false
			if option.DefaultValue != nil {
				defaultValue = *option.DefaultValue == "true"
			}
			if option.Shortcut != "" {
				cmd.Flags().BoolP(option.Name, option.Shortcut, defaultValue, description)
			} else {
				cmd.Flags().Bool(option.Name, defaultValue, description)
			}
		default:
			return fmt.Errorf("console bind flags: unsupported option mode for %q", option.Name)
		}
	}
	return nil
}
