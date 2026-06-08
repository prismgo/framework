// Package translation 定义翻译系统的公共契约。
package translation

import "context"

// Translator 是翻译系统的核心契约。
//
// 用途：提供完整的翻译能力，包括 key 查找、placeholder 替换、pluralization 和 locale 管理。
// 实现类负责调用 Loader、解析 key、合并 fallback、处理 missing key 和选择翻译文本。
type Translator interface {
	Has(key string, locale ...string) bool
	HasForLocale(key, locale string) bool

	Get(key string, replace map[string]any, locale ...string) string
	Choice(key string, number any, replace map[string]any, locale ...string) string
	AddLines(lines map[string]any, locale string, namespace ...string)

	Locale() string
	CurrentLocale() string
	SetLocale(locale string) error
	IsLocale(locale string) bool

	GetFallback() string
	SetFallback(locale string) error

	AddNamespace(namespace, hint string)
	AddPath(path string)
	AddJSONPath(path string)

	Stringable(sample any, formatter func(any) string)
	HandleMissingKeysUsing(handler func(context.Context, string, string) (string, bool))
	DetermineLocalesUsing(resolver func(key string, requested string) []string)

	GetMap(key string, locale ...string) map[string]any
}
