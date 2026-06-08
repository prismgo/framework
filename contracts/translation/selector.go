// Package translation 定义翻译系统的公共契约。
package translation

// Selector 是消息选择器的基础契约。
//
// 用途：负责根据数量选择合适的翻译消息变体（pluralization）。
// 支持 Laravel 风格的区间语法，如 {0}, {1}, [2,*] 等。
type Selector interface {
	Select(message string, number any, locale string) string
}
