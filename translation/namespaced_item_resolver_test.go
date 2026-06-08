package translation

import (
	"testing"

	transcontract "github.com/prismgo/framework/contracts/translation"
)

func TestNamespacedItemResolverParseKeyGroupKey(t *testing.T) {
	r := NewNamespacedItemResolver()

	tests := []struct {
		key      string
		expected transcontract.ParsedKey
	}{
		{
			key: "messages.welcome",
			expected: transcontract.ParsedKey{
				Namespace: defaultNamespace,
				Group:     "messages",
				Item:      "welcome",
				IsJSON:    false,
			},
		},
		{
			key: "auth.failed",
			expected: transcontract.ParsedKey{
				Namespace: defaultNamespace,
				Group:     "auth",
				Item:      "failed",
				IsJSON:    false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := r.ParseKey(tt.key)
			if result.Namespace != tt.expected.Namespace {
				t.Errorf("Namespace = %v, want %v", result.Namespace, tt.expected.Namespace)
			}
			if result.Group != tt.expected.Group {
				t.Errorf("Group = %v, want %v", result.Group, tt.expected.Group)
			}
			if result.Item != tt.expected.Item {
				t.Errorf("Item = %v, want %v", result.Item, tt.expected.Item)
			}
			if result.IsJSON != tt.expected.IsJSON {
				t.Errorf("IsJSON = %v, want %v", result.IsJSON, tt.expected.IsJSON)
			}
		})
	}
}

func TestNamespacedItemResolverParseKeyCache(t *testing.T) {
	resolver := NewNamespacedItemResolver()

	key := "messages.welcome"

	first := resolver.ParseKey(key)
	second := resolver.ParseKey(key)

	if first.Group != second.Group || first.Item != second.Item {
		t.Error("cached result should equal original")
	}
}

func TestNamespacedItemResolverFlushParsedKeys(t *testing.T) {
	resolver := NewNamespacedItemResolver()

	key := "messages.welcome"
	resolver.ParseKey(key)

	resolver.FlushParsedKeys()

	result := resolver.ParseKey(key)
	if result.Group != "messages" || result.Item != "welcome" {
		t.Errorf("after flush, ParseKey = %v, want messages.welcome", result)
	}
}

func TestNamespacedItemResolverParseKeyNamespace(t *testing.T) {
	r := NewNamespacedItemResolver()

	tests := []struct {
		key      string
		expected transcontract.ParsedKey
	}{
		{
			key: "acme::messages.welcome",
			expected: transcontract.ParsedKey{
				Namespace: "acme",
				Group:     "messages",
				Item:      "welcome",
				IsJSON:    false,
			},
		},
		{
			key: "package::validation.required",
			expected: transcontract.ParsedKey{
				Namespace: "package",
				Group:     "validation",
				Item:      "required",
				IsJSON:    false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := r.ParseKey(tt.key)
			if result.Namespace != tt.expected.Namespace {
				t.Errorf("Namespace = %v, want %v", result.Namespace, tt.expected.Namespace)
			}
			if result.Group != tt.expected.Group {
				t.Errorf("Group = %v, want %v", result.Group, tt.expected.Group)
			}
			if result.Item != tt.expected.Item {
				t.Errorf("Item = %v, want %v", result.Item, tt.expected.Item)
			}
			if result.IsJSON != tt.expected.IsJSON {
				t.Errorf("IsJSON = %v, want %v", result.IsJSON, tt.expected.IsJSON)
			}
		})
	}
}

func TestNamespacedItemResolverParseKeySimpleKey(t *testing.T) {
	r := NewNamespacedItemResolver()

	result := r.ParseKey("simple_key")
	if result.Namespace != defaultNamespace {
		t.Errorf("Namespace = %v, want %v", result.Namespace, defaultNamespace)
	}
	if result.Group != "" {
		t.Errorf("Group = %v, want empty", result.Group)
	}
	if result.Item != "simple_key" {
		t.Errorf("Item = %v, want simple_key", result.Item)
	}
	if !result.IsJSON {
		t.Error("IsJSON should be true for simple keys")
	}
}

func TestNamespacedItemResolverParseKeyEmpty(t *testing.T) {
	r := NewNamespacedItemResolver()

	result := r.ParseKey("")
	if result.Namespace != "" {
		t.Errorf("Namespace = %v, want empty", result.Namespace)
	}
	if result.Group != "" {
		t.Errorf("Group = %v, want empty", result.Group)
	}
	if result.Item != "" {
		t.Errorf("Item = %v, want empty", result.Item)
	}
	if result.IsJSON {
		t.Error("IsJSON = true, want false for empty key")
	}
}

func TestNamespacedItemResolverIsJSONKey(t *testing.T) {
	r := NewNamespacedItemResolver()

	tests := []struct {
		key      string
		expected bool
	}{
		{"simple_key", true},
		{"welcome", true},
		{"messages.welcome", false},
		{"acme::messages.welcome", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := r.IsJSONKey(tt.key)
			if result != tt.expected {
				t.Errorf("IsJSONKey(%q) = %v, want %v", tt.key, result, tt.expected)
			}
		})
	}
}
