// Package translation 提供了完整的国际化翻译能力，对齐 Laravel 13 的 lang 子系统。
//
// 主要功能：
//   - group 翻译文件（lang/{locale}/{group}.json）
//   - root JSON 翻译文件（lang/{locale}.json）
//   - vendor namespace 翻译文件
//   - placeholder 替换与大小写规则
//   - pluralization / choice 规则
//   - 运行时 locale 切换
//   - 缺失 key 回退与 missing key hook
//
// 使用方式：
//
//	import translation "github.com/prismgo/framework/translation"
//
//	msg := translation.Get("messages.welcome", map[string]any{"name": "dayle"})
//	plural := translation.Choice("messages.apples", 5, nil)
package translation