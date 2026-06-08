package cmd

import (
	"strconv"
	"strings"

	"github.com/prismgo/framework/console"
)

// intCommandOption 读取整数命令选项。
//
// 设计思路：CLI 层只做稳定绑定，非法数值回退到调用方指定默认值，避免把参数解析错误与 Runtime
// 业务错误混在一起；真正需要 fail fast 的配置校验仍由 Horizon 配置解析负责。
func intCommandOption(ctx console.CommandContext, name string, fallback int) int {
	value := strings.TrimSpace(ctx.Input().Option(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

// boolCommandOption 兼容 --flag 和 --flag=true 两类输入。
//
// 参数说明：name 是 signature 中定义的选项名；测试 fake input 通常通过字符串值表达布尔选项，
// 因此这里同时读取 OptionBool 和常见字符串布尔值。
func boolCommandOption(ctx console.CommandContext, name string) bool {
	if ctx.Input().OptionBool(name) {
		return true
	}
	switch strings.TrimSpace(strings.ToLower(ctx.Input().Option(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// firstCommandOption 返回第一个非空选项值。
//
// 需求背景：`horizon:supervisor` 需要保留 Prismgo 内部 `master-id` 兼容参数，同时支持 Laravel
// 对齐的 `parent-id` 名称；该 helper 让兼容策略集中在命令绑定层。
func firstCommandOption(ctx console.CommandContext, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(ctx.Input().Option(name)); value != "" {
			return value
		}
	}
	return ""
}
