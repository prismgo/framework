package translation

import (
	"sync"
	"testing"
)

func TestReplacerReplaceNoReplacements(t *testing.T) {
	r := NewReplacer()

	result := r.Replace("Hello, World", nil)
	if result != "Hello, World" {
		t.Errorf("Replace = %v, want Hello, World", result)
	}

	result = r.Replace("Hello, World", map[string]any{})
	if result != "Hello, World" {
		t.Errorf("Replace = %v, want Hello, World", result)
	}
}

func TestReplacerReplaceBasicPlaceholder(t *testing.T) {
	r := NewReplacer()

	tests := []struct {
		message  string
		replace  map[string]any
		expected string
	}{
		{
			message:  "Hello, :name",
			replace:  map[string]any{"name": "John"},
			expected: "Hello, john",
		},
		{
			message:  "Welcome, :Name",
			replace:  map[string]any{"name": "john"},
			expected: "Welcome, John",
		},
		{
			message:  "Hello, :NAME",
			replace:  map[string]any{"name": "john"},
			expected: "Hello, JOHN",
		},
		{
			message:  "Hello, :name. You are :age years old.",
			replace:  map[string]any{"name": "Jane", "age": 25},
			expected: "Hello, jane. You are 25 years old.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.message, func(t *testing.T) {
			result := r.Replace(tt.message, tt.replace)
			if result != tt.expected {
				t.Errorf("Replace = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestReplacerReplaceIntValue(t *testing.T) {
	r := NewReplacer()

	result := r.Replace("Count: :count", map[string]any{"count": 42})
	if result != "Count: 42" {
		t.Errorf("Replace = %v, want Count: 42", result)
	}
}

func TestReplacerReplaceMissingPlaceholder(t *testing.T) {
	r := NewReplacer()

	result := r.Replace("Hello, :name", map[string]any{"title": "Dr."})
	if result != "Hello, :name" {
		t.Errorf("Replace = %v, want Hello, :name", result)
	}
}

func TestReplacerLowerCase(t *testing.T) {
	r := NewReplacer()

	tests := []struct {
		input    string
		expected string
	}{
		{"HELLO", "hello"},
		{"Hello", "hello"},
		{"HELLO WORLD", "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := r.lowerCase(tt.input)
			if result != tt.expected {
				t.Errorf("lowerCase(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestReplacerUpperCase(t *testing.T) {
	r := NewReplacer()

	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "HELLO"},
		{"Hello", "HELLO"},
		{"hello world", "HELLO WORLD"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := r.upperCase(tt.input)
			if result != tt.expected {
				t.Errorf("upperCase(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestReplacerTitleCase(t *testing.T) {
	r := NewReplacer()

	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "Hello"},
		{"john", "John"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := r.titleCase(tt.input)
			if result != tt.expected {
				t.Errorf("titleCase(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestReplacerReplacePlaceholder(t *testing.T) {
	r := NewReplacer()

	result := r.replacePlaceholder("Hello, :Name", "Name", "john")
	if result != "Hello, John" {
		t.Errorf("replacePlaceholder = %v, want Hello, John", result)
	}

	result = r.replacePlaceholder("Hello, :NAME", "Name", "john")
	if result != "Hello, JOHN" {
		t.Errorf("replacePlaceholder = %v, want Hello, JOHN", result)
	}
}

type customStringer struct {
	val string
}

func (c customStringer) String() string {
	return c.val
}

func TestReplacerToStringStringer(t *testing.T) {
	r := NewReplacer()

	s := customStringer{val: "custom_value"}
	result := r.toString(s)
	if result != "custom_value" {
		t.Errorf("toString = %v, want custom_value", result)
	}
}

func TestReplacerToStringInteger(t *testing.T) {
	r := NewReplacer()

	result := r.toString(42)
	if result != "42" {
		t.Errorf("toString = %v, want 42", result)
	}
}

func TestReplacerToStringFloat(t *testing.T) {
	r := NewReplacer()

	result := r.toString(3.14)
	if result != "3.14" {
		t.Errorf("toString = %v, want 3.14", result)
	}
}

func TestReplacerToStringBool(t *testing.T) {
	r := NewReplacer()

	result := r.toString(true)
	if result != "true" {
		t.Errorf("toString(true) = %v, want true", result)
	}

	result = r.toString(false)
	if result != "false" {
		t.Errorf("toString(false) = %v, want false", result)
	}
}

func TestReplacerStringableConcurrentAccess(t *testing.T) {
	r := NewReplacer()

	type CustomType struct {
		value string
	}

	var wg sync.WaitGroup
	// 启动多个 goroutine 并发写入 stringables
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.AddStringable(CustomType{}, func(v any) string {
				return "custom"
			})
		}(i)
	}

	// 同时启动多个 goroutine 并发读取 stringables
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.toString(CustomType{value: "test"})
			}
		}()
	}

	wg.Wait()
}
