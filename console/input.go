package console

import (
	"strconv"
	"strings"

	consolecontract "github.com/prismgo/framework/contracts/console"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Input 提供统一的命令输入读取接口。
//
// 用途：屏蔽 Cobra flag/arg 的底层细节，向命令暴露稳定的参数访问方法。
// 设计原因：命令作者应该更多关注“要读什么参数”，而不是每次都手写 flag 解析与默认值回退逻辑。
type Input = consolecontract.Input

type cobraInput struct {
	cmd        *cobra.Command
	args       map[string][]string
	definition Definition
}

// NewInput 根据命令定义、Cobra 命令和原始位置参数构造统一输入对象。
func NewInput(definition Definition, cmd *cobra.Command, args []string) Input {
	return &cobraInput{
		cmd:        cmd,
		args:       bindArguments(definition.Arguments, args),
		definition: definition,
	}
}

func (i *cobraInput) Argument(name string) string {
	values := i.Arguments(name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (i *cobraInput) Arguments(name string) []string {
	values := i.args[name]
	return append([]string(nil), values...)
}

func (i *cobraInput) Option(name string) string {
	values := i.OptionStrings(name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (i *cobraInput) OptionStrings(name string) []string {
	flag, getter := i.lookupFlag(name)
	if flag == nil || getter == nil {
		return nil
	}

	switch flag.Value.Type() {
	case "stringArray":
		values, err := getter.GetStringArray(name)
		if err == nil {
			return normalizeOptionStringValues(values)
		}
	case "stringSlice":
		values, err := getter.GetStringSlice(name)
		if err == nil {
			return normalizeOptionStringValues(values)
		}
	}

	value := strings.TrimSpace(flag.Value.String())
	if value == "" || value == "[]" {
		return nil
	}
	if value == optionalOptionNoValueSentinel {
		return []string{""}
	}
	return []string{value}
}

func (i *cobraInput) OptionBool(name string) bool {
	flag, getter := i.lookupFlag(name)
	if flag == nil {
		return false
	}
	if flag.Value.Type() == "bool" && getter != nil {
		value, err := getter.GetBool(name)
		if err == nil {
			return value
		}
	}
	value, _ := strconv.ParseBool(strings.TrimSpace(flag.Value.String()))
	return value
}

func (i *cobraInput) OptionInt(name string) (int, error) {
	value := i.Option(name)
	if value == "" {
		return 0, nil
	}
	return strconv.Atoi(value)
}

func (i *cobraInput) HasOption(name string) bool {
	flag, _ := i.lookupFlag(name)
	return flag != nil
}

// normalizeOptionStringValues 统一清洗字符串型 option 的读取结果。
// 逻辑说明：可选值 option 在绑定层会把裸 --flag 先映射成内部哨兵值；
// 这里负责把该哨兵值还原成命令侧可消费的空字符串语义，并保持数组返回值的只读副本特性。
func normalizeOptionStringValues(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	if len(values) == 1 && values[0] == optionalOptionNoValueSentinel {
		return nil
	}
	result := append([]string(nil), values...)
	for i := range result {
		if result[i] == optionalOptionNoValueSentinel {
			result[i] = ""
		}
	}
	return result
}

// lookupFlag 返回 option 实际所属的 flag 与其 getter。
// 设计思路：命令既可能读取本地 flags，也可能读取继承自父命令的全局 flags；
// 这里把定位逻辑集中起来，避免 lookup 走 inherited、取值却仍从 local flag set 读取造成行为分叉。
func (i *cobraInput) lookupFlag(name string) (*pflag.Flag, interface {
	GetBool(string) (bool, error)
	GetStringArray(string) ([]string, error)
	GetStringSlice(string) ([]string, error)
}) {
	if i.cmd == nil {
		return nil, nil
	}
	if flag := i.cmd.Flags().Lookup(name); flag != nil {
		return flag, i.cmd.Flags()
	}
	if flag := i.cmd.InheritedFlags().Lookup(name); flag != nil {
		return flag, i.cmd.InheritedFlags()
	}
	return nil, nil
}

func bindArguments(definitions []Argument, values []string) map[string][]string {
	result := make(map[string][]string, len(definitions))
	remaining := append([]string(nil), values...)
	for _, definition := range definitions {
		if definition.IsArray {
			result[definition.Name] = append([]string(nil), remaining...)
			remaining = nil
			continue
		}
		if len(remaining) > 0 {
			result[definition.Name] = []string{remaining[0]}
			remaining = remaining[1:]
			continue
		}
		if definition.DefaultValue != nil {
			result[definition.Name] = []string{*definition.DefaultValue}
		}
	}
	return result
}
