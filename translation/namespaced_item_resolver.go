package translation

import (
	"strings"
	"sync"

	transcontract "github.com/prismgo/framework/contracts/translation"
)

const (
	namespaceSeparator = "::"
	defaultNamespace   = "*"
)

type NamespacedItemResolver struct {
	cache sync.Map
}

func NewNamespacedItemResolver() *NamespacedItemResolver {
	return &NamespacedItemResolver{}
}

func (r *NamespacedItemResolver) ParseKey(key string) transcontract.ParsedKey {
	if key == "" {
		return transcontract.ParsedKey{}
	}

	if cached, ok := r.cache.Load(key); ok {
		return cached.(transcontract.ParsedKey)
	}

	parsed := r.doParseKey(key)
	r.cache.Store(key, parsed)
	return parsed
}

func (r *NamespacedItemResolver) doParseKey(key string) transcontract.ParsedKey {
	if strings.Contains(key, namespaceSeparator) {
		parts := strings.SplitN(key, namespaceSeparator, 2)
		namespace := parts[0]
		item := parts[1]

		if strings.Contains(item, ".") {
			itemParts := strings.SplitN(item, ".", 2)
			return transcontract.ParsedKey{
				Namespace: namespace,
				Group:     itemParts[0],
				Item:      itemParts[1],
				IsJSON:    false,
			}
		}

		return transcontract.ParsedKey{
			Namespace: namespace,
			Group:     item,
			Item:      "",
			IsJSON:    false,
		}
	}

	if strings.Contains(key, ".") {
		parts := strings.SplitN(key, ".", 2)
		return transcontract.ParsedKey{
			Namespace: defaultNamespace,
			Group:     parts[0],
			Item:      parts[1],
			IsJSON:    false,
		}
	}

	return transcontract.ParsedKey{
		Namespace: defaultNamespace,
		Group:     "",
		Item:      key,
		IsJSON:    true,
	}
}

// FlushParsedKeys 清空所有已解析的 key 缓存。
// 使用 Range+Delete 而非重新赋值 sync.Map，以避免与并发 ParseKey 调用产生数据竞争。
func (r *NamespacedItemResolver) FlushParsedKeys() {
	r.cache.Range(func(key, value any) bool {
		r.cache.Delete(key)
		return true
	})
}

func (r *NamespacedItemResolver) IsJSONKey(key string) bool {
	if key == "" {
		return false
	}

	if strings.Contains(key, namespaceSeparator) {
		return false
	}

	if strings.Contains(key, ".") {
		return false
	}

	return true
}
