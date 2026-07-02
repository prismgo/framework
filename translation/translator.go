package translation

import (
	"context"
	"errors"
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

	missingHandler func(context.Context, string, string) (string, bool)
	localeResolver func(key string, requested string) []string

	mu sync.RWMutex
}

func NewTranslator(loader transcontract.Loader, locale, fallback string) *Translator {
	return &Translator{
		loader:   loader,
		resolver: NewNamespacedItemResolver(),
		selector: NewMessageSelector(),
		replacer: NewReplacer(),
		locale:   locale,
		fallback: fallback,
		lines:    make(map[string]map[string]map[string]map[string]any),
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

		loader := t.getLoader()
		data, err := loader.Load(loc, "", defaultNamespace)
		if err == nil {
			if value, exists := data[key]; exists {
				switch value.(type) {
				case string, map[string]any:
					return true
				}
			}
		}

		groupData, err := loader.Load(loc, key, defaultNamespace)
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

	parsed := t.resolver.ParseKey(key)

	var result string
	var found bool

	if parsed.IsJSON {
		result = t.getJSON(key, locale)
		found = result != key
	} else if parsed.Group == "" {
		result = t.getJSON(parsed.Item, locale)
		found = result != parsed.Item
	} else {
		result = t.getLine(parsed.Namespace, parsed.Group, parsed.Item, locale)
		found = result != ""
	}

	if found {
		return t.replacer.Replace(result, replace)
	}

	t.mu.RLock()
	handler := t.missingHandler
	t.mu.RUnlock()

	if handler != nil {
		ctx := context.Background()
		if value, ok := handler(ctx, key, locale); ok {
			return t.replacer.Replace(value, replace)
		}
	}

	return key
}

func (t *Translator) getJSON(key string, locale string) string {
	locales := t.getLocaleChain(locale)
	loader := t.getLoader()

	for _, loc := range locales {
		t.linesMutex.RLock()
		if lines, ok := t.lines[defaultNamespace]; ok {
			if localeLines, ok := lines[loc]; ok {
				if jsonLines, ok := localeLines[""]; ok {
					if value, exists := jsonLines[key]; exists {
						if str, ok := value.(string); ok {
							t.linesMutex.RUnlock()
							return str
						}
					}
				}
			}
		}
		t.linesMutex.RUnlock()

		data, err := loader.Load(loc, "", defaultNamespace)
		if err == nil {
			if value, exists := data[key]; exists {
				if str, ok := value.(string); ok {
					return str
				}
			}
		}
	}

	return key
}

func (t *Translator) getLine(namespace, group, item, locale string) string {
	locales := t.getLocaleChain(locale)
	loader := t.getLoader()

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

		data, err := loader.Load(loc, group, namespace)
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
	loader := t.getLoader()
	if l, ok := loader.(interface {
		AddNamespace(namespace, hint string)
	}); ok {
		l.AddNamespace(namespace, hint)
	}
}

func (t *Translator) AddPath(path string) {
	loader := t.getLoader()
	if l, ok := loader.(interface {
		AddPath(path string)
	}); ok {
		l.AddPath(path)
	}
}

func (t *Translator) AddJSONPath(path string) {
	loader := t.getLoader()
	if l, ok := loader.(interface {
		AddJSONPath(path string)
	}); ok {
		l.AddJSONPath(path)
	}
}

func (t *Translator) Stringable(sample any, formatter func(any) string) {
	if sample == nil || formatter == nil {
		return
	}
	t.replacer.AddStringable(sample, formatter)
}

func (t *Translator) HandleMissingKeysUsing(handler func(context.Context, string, string) (string, bool)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.missingHandler = handler
}

func (t *Translator) DetermineLocalesUsing(resolver func(key string, requested string) []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.mu.RLock()
	resolver := t.localeResolver
	fallback := t.fallback
	t.mu.RUnlock()

	if resolver != nil {
		return resolver("", locale)
	}

	chain := []string{locale}

	if fallback != "" && fallback != locale {
		chain = append(chain, fallback)
	}

	return chain
}

func (t *Translator) Loader() transcontract.Loader {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.loader
}

func (t *Translator) SetLoader(loader transcontract.Loader) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.loader = loader
}

// getLoader 安全地获取 loader 实例（内部使用）
func (t *Translator) getLoader() transcontract.Loader {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.loader
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
		// 检查内存缓存（仅检查，不写入）
		t.linesMutex.RLock()
		if nsLines, ok := t.lines[ns]; ok {
			if localeLines, ok := nsLines[loc]; ok {
				if groupLines, ok := localeLines[group]; ok {
					if item == "" {
						t.linesMutex.RUnlock()
						return groupLines
					}
					if v, ok := groupLines[item]; ok {
						t.linesMutex.RUnlock()
						if m, ok := v.(map[string]any); ok {
							return m
						}
						return nil
					}
				}
			}
		}
		t.linesMutex.RUnlock()

		// 缓存未命中，从 loader 加载
		data, err := t.getLoader().Load(loc, group, ns)
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
