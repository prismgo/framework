package translation

import (
	"fmt"
	"reflect"
	"strings"
)

type Replacer struct {
	stringables map[reflect.Type]func(any) string
}

func NewReplacer() *Replacer {
	return &Replacer{
		stringables: make(map[reflect.Type]func(any) string),
	}
}

func (r *Replacer) Replace(message string, replace map[string]any) string {
	if len(replace) == 0 {
		return message
	}

	for key, value := range replace {
		message = r.replacePlaceholder(message, key, value)
	}

	return message
}

func (r *Replacer) replacePlaceholder(message, key string, value any) string {
	lowerKey := strings.ToLower(key)
	upperKey := strings.ToUpper(key)
	titleKey := r.titleCase(key)

	strValue := r.toString(value)

	message = strings.ReplaceAll(message, ":"+lowerKey, r.lowerCase(strValue))
	message = strings.ReplaceAll(message, ":"+upperKey, r.upperCase(strValue))
	message = strings.ReplaceAll(message, ":"+titleKey, r.titleCase(strValue))
	message = strings.ReplaceAll(message, ":"+key, strValue)

	return message
}

func (r *Replacer) toString(value any) string {
	if s, ok := value.(fmt.Stringer); ok {
		return s.String()
	}

	if formatter, ok := r.stringables[reflect.TypeOf(value)]; ok {
		return formatter(value)
	}

	switch v := value.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("%d", v)
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%v", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func (r *Replacer) lowerCase(s string) string {
	return strings.ToLower(s)
}

func (r *Replacer) upperCase(s string) string {
	return strings.ToUpper(s)
}

func (r *Replacer) titleCase(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
}
