// Package translation 定义翻译系统的公共契约。
package translation

import "context"

// ParsedKey 表示解析后的翻译 key。
//
// Namespace 表示命名空间，如 "acme"。
// Group 表示分组，如 "messages"。
// Item 表示具体的键名，如 "welcome"。
// IsJSON 表示是否为 JSON key（根级翻译）。
type ParsedKey struct {
	Namespace string
	Group     string
	Item      string
	IsJSON    bool
}

// MissingKeyHandler 是缺失 key 的处理函数类型。
//
// 返回 (value, true) 表示使用该值；返回 ("", false) 表示继续默认逻辑。
type MissingKeyHandler func(ctx context.Context, key, locale string) (string, bool)

// LocaleResolver 是 locale 解析函数类型。
//
// 用于自定义 locale 解析链，返回要尝试的 locale 列表。
type LocaleResolver func(key string, requested string) []string
