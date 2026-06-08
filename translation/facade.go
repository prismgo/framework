package translation

import (
	"context"

	transcontract "github.com/prismgo/framework/contracts/translation"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "translator"

// Resolve 从当前 Application 容器解析翻译器。
func Resolve() transcontract.Translator {
	return facade.Resolve[transcontract.Translator](serviceKey)
}

func Get(key string, replace map[string]any, locale ...string) string {
	return Resolve().Get(key, replace, locale...)
}

func Choice(key string, number any, replace map[string]any, locale ...string) string {
	return Resolve().Choice(key, number, replace, locale...)
}

func Has(key string, locale ...string) bool {
	return Resolve().Has(key, locale...)
}

func HasForLocale(key string, locale string) bool {
	return Resolve().HasForLocale(key, locale)
}

func Locale() string {
	return Resolve().Locale()
}

func CurrentLocale() string {
	return Resolve().CurrentLocale()
}

func SetLocale(locale string) error {
	return Resolve().SetLocale(locale)
}

func IsLocale(locale string) bool {
	return Resolve().IsLocale(locale)
}

func GetFallback() string {
	return Resolve().GetFallback()
}

func SetFallback(locale string) error {
	return Resolve().SetFallback(locale)
}

func AddNamespace(namespace, hint string) {
	Resolve().AddNamespace(namespace, hint)
}

func AddPath(path string) {
	Resolve().AddPath(path)
}

func AddJSONPath(path string) {
	Resolve().AddJSONPath(path)
}

func AddLines(lines map[string]any, locale string, namespace ...string) {
	Resolve().AddLines(lines, locale, namespace...)
}

func Stringable(sample any, formatter func(any) string) {
	Resolve().Stringable(sample, formatter)
}

func HandleMissingKeysUsing(handler func(context.Context, string, string) (string, bool)) {
	Resolve().HandleMissingKeysUsing(handler)
}

func DetermineLocalesUsing(resolver func(key string, requested string) []string) {
	Resolve().DetermineLocalesUsing(resolver)
}

func GetMap(key string, locale ...string) map[string]any {
	return Resolve().GetMap(key, locale...)
}

func Loader() transcontract.Loader {
	if t, ok := Resolve().(*Translator); ok {
		return t.Loader()
	}
	return nil
}

func Reset() {
	// Reset 清空翻译器状态，供测试使用。
}
