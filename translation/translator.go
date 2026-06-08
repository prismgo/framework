package translation

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"

	transcontract "github.com/prismgo/framework/contracts/translation"
)

type Translator struct {
	loader   transcontract.Loader
	resolver *NamespacedItemResolver
	selector *MessageSelector
	replacer *Replacer

	locale   string
	fallback string

	lines      map[string]map[string]map[string]map[string]any
	linesMutex sync.RWMutex

	stringables map[reflect.Type]func(any) string

	missingHandler func(context.Context, string, string) (string, bool)
	localeResolver func(key string, requested string) []string

	mu sync.RWMutex
}

func NewTranslator(loader transcontract.Loader, locale, fallback string) *Translator {
	return &Translator{
		loader:      loader,
		resolver:    NewNamespacedItemResolver(),
		selector:    NewMessageSelector(),
		replacer:    NewReplacer(),
		locale:      locale,
		fallback:    fallback,
		lines:       make(map[string]map[string]map[string]map[string]any),
		stringables: make(map[reflect.Type]func(any) string),
	}
}

func (t *Translator) Has(key string, locale ...string) bool {
	targetLocale := t.resolveLocale(locale...)
	return t.hasForLocale(key, targetLocale)
}

func (t *Translator) HasForLocale(key, locale string) bool {
	return t.hasForLocale(key, locale)
}

func (t *Translator) hasForLocale(key, locale string) bool {
	if key == "" {
		return false
	}

	parsed := t.resolver.ParseKey(key)

	if parsed.IsJSON {
		return t.hasJSON(key, locale)
	}

	if parsed.Group == "" {
		return t.hasJSON(parsed.Item, locale)
	}

	return t.getLine(parsed.Namespace, parsed.Group, parsed.Item, locale) != "" || t.GetMap(key, locale) != nil
}

func (t *Translator) hasJSON(key, locale string) bool {
	locales := t.getLocaleChain(locale)

	for _, loc := range locales {
		t.linesMutex.RLock()
		if lines, ok := t.lines[defaultNamespace]; ok {
			if localeLines, ok := lines[loc]; ok {
				if jsonLines, ok := localeLines[""]; ok {
					if value, exists := jsonLines[key]; exists {
						_, isString := value.(string)
						_, isMap := value.(map[string]any)
						t.linesMutex.RUnlock()
						return isString || isMap
					}
				}
			}
		}
		t.linesMutex.RUnlock()

		data, err := t.loader.Load(loc, "", defaultNamespace)
		if err == nil {
			if value, exists := data[key]; exists {
				switch value.(type) {
				case string, map[string]any:
					return true
				}
			}
		}

		groupData, err := t.loader.Load(loc, key, defaultNamespace)
		if err == nil && len(groupData) > 0 {
			return true
		}
	}

	return false
}

func (t *Translator) Get(key string, replace map[string]any, locale ...string) string {
	targetLocale := t.resolveLocale(locale...)
	return t.get(key, replace, targetLocale)
}

func (t *Translator) get(key string, replace map[string]any, locale string) string {
	if key == "" {
		return key
	}

	ctx := context.Background()

	if t.missingHandler != nil {
		if value, ok := t.missingHandler(ctx, key, locale); ok {
			return t.replacer.Replace(value, replace)
		}
	}

	parsed := t.resolver.ParseKey(key)

	if parsed.IsJSON {
		return t.getJSON(key, replace, locale)
	}

	if parsed.Group == "" {
		return t.getJSON(parsed.Item, replace, locale)
	}

	line := t.getLine(parsed.Namespace, parsed.Group, parsed.Item, locale)
	if line != "" {
		return t.replacer.Replace(line, replace)
	}

	return key
}

func (t *Translator) getJSON(key string, replace map[string]any, locale string) string {
	locales := t.getLocaleChain(locale)

	for _, loc := range locales {
		t.linesMutex.RLock()
		if lines, ok := t.lines[defaultNamespace]; ok {
			if localeLines, ok := lines[loc]; ok {
				if jsonLines, ok := localeLines[""]; ok {
					if value, exists := jsonLines[key]; exists {
						if str, ok := value.(string); ok {
							t.linesMutex.RUnlock()
							return t.replacer.Replace(str, replace)
						}
					}
				}
			}
		}
		t.linesMutex.RUnlock()

		data, err := t.loader.Load(loc, "", defaultNamespace)
		if err == nil {
			if value, exists := data[key]; exists {
				if str, ok := value.(string); ok {
					return t.replacer.Replace(str, replace)
				}
			}
		}
	}

	return key
}

func (t *Translator) getLine(namespace, group, item, locale string) string {
	locales := t.getLocaleChain(locale)

	for _, loc := range locales {
		t.linesMutex.RLock()
		if nsLines, ok := t.lines[namespace]; ok {
			if localeLines, ok := nsLines[loc]; ok {
				if groupLines, ok := localeLines[group]; ok {
					if line, exists := groupLines[item]; exists {
						t.linesMutex.RUnlock()
						if str, ok := line.(string); ok {
							return str
						}
					}
				}
			}
		}
		t.linesMutex.RUnlock()

		data, err := t.loader.Load(loc, group, namespace)
		if err == nil {
			if value, exists := data[item]; exists {
				if str, ok := value.(string); ok {
					return str
				}
			}
		}
	}

	return ""
}

func (t *Translator) Choice(key string, number any, replace map[string]any, locale ...string) string {
	targetLocale := t.resolveLocale(locale...)

	message := t.get(key, nil, targetLocale)

	selected := t.selector.Select(message, number, targetLocale)

	if replace == nil {
		replace = make(map[string]any)
	}
	if _, exists := replace["count"]; !exists {
		replace["count"] = number
	}

	return t.replacer.Replace(selected, replace)
}

func (t *Translator) AddLines(lines map[string]any, locale string, namespace ...string) {
	ns := defaultNamespace
	if len(namespace) > 0 && namespace[0] != "" {
		ns = namespace[0]
	}

	t.linesMutex.Lock()
	defer t.linesMutex.Unlock()

	if t.lines[ns] == nil {
		t.lines[ns] = make(map[string]map[string]map[string]any)
	}
	if t.lines[ns][locale] == nil {
		t.lines[ns][locale] = make(map[string]map[string]any)
	}

	for k, v := range lines {
		group, item := splitKey(k)
		if t.lines[ns][locale][group] == nil {
			t.lines[ns][locale][group] = make(map[string]any)
		}
		t.lines[ns][locale][group][item] = v
	}
}

func splitKey(key string) (group, item string) {
	if idx := strings.IndexByte(key, '.'); idx >= 0 {
		return key[:idx], key[idx+1:]
	}
	return "", key
}

func (t *Translator) Locale() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.locale
}

func (t *Translator) CurrentLocale() string {
	return t.Locale()
}

func (t *Translator) SetLocale(locale string) error {
	if strings.ContainsAny(locale, "/\\") {
		return errors.New("translation: invalid characters in locale")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.locale = locale
	return nil
}

func (t *Translator) IsLocale(locale string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.locale == locale
}

func (t *Translator) GetFallback() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.fallback
}

func (t *Translator) SetFallback(locale string) error {
	if strings.ContainsAny(locale, "/\\") {
		return errors.New("translation: invalid characters in fallback locale")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fallback = locale
	return nil
}

func (t *Translator) AddNamespace(namespace, hint string) {
	if loader, ok := t.loader.(interface {
		AddNamespace(namespace, hint string)
	}); ok {
		loader.AddNamespace(namespace, hint)
	}
}

func (t *Translator) AddPath(path string) {
	if loader, ok := t.loader.(interface {
		AddPath(path string)
	}); ok {
		loader.AddPath(path)
	}
}

func (t *Translator) AddJSONPath(path string) {
	if loader, ok := t.loader.(interface {
		AddJSONPath(path string)
	}); ok {
		loader.AddJSONPath(path)
	}
}

func (t *Translator) Stringable(sample any, formatter func(any) string) {
	if sample == nil || formatter == nil {
		return
	}
	t.stringables[reflect.TypeOf(sample)] = formatter
	t.replacer.stringables[reflect.TypeOf(sample)] = formatter
}

func (t *Translator) HandleMissingKeysUsing(handler func(context.Context, string, string) (string, bool)) {
	t.missingHandler = handler
}

func (t *Translator) DetermineLocalesUsing(resolver func(key string, requested string) []string) {
	t.localeResolver = resolver
}

func (t *Translator) resolveLocale(locale ...string) string {
	if len(locale) > 0 && locale[0] != "" {
		return locale[0]
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.locale
}

func (t *Translator) getLocaleChain(locale string) []string {
	if t.localeResolver != nil {
		return t.localeResolver("", locale)
	}

	chain := []string{locale}

	t.mu.RLock()
	fallback := t.fallback
	t.mu.RUnlock()

	if fallback != "" && fallback != locale {
		chain = append(chain, fallback)
	}

	return chain
}

func (t *Translator) Loader() transcontract.Loader {
	return t.loader
}

func (t *Translator) SetLoader(loader transcontract.Loader) {
	t.loader = loader
}

func (t *Translator) GetMap(key string, locale ...string) map[string]any {
	targetLocale := t.resolveLocale(locale...)

	parsed := t.resolver.ParseKey(key)

	group := parsed.Group
	item := parsed.Item
	ns := parsed.Namespace

	if parsed.IsJSON {
		group = key
		item = ""
		ns = defaultNamespace
	}

	locales := t.getLocaleChain(targetLocale)
	for _, loc := range locales {
		data, err := t.loader.Load(loc, group, ns)
		if err != nil || len(data) == 0 {
			continue
		}

		if item == "" {
			return data
		}

		if v, ok := data[item]; ok {
			if m, ok := v.(map[string]any); ok {
				return m
			}
		}
	}

	return nil
}
