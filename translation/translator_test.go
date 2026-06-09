package translation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	transcontract "github.com/prismgo/framework/contracts/translation"
)

func bindFacadeTranslator(t *testing.T) {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	translator := NewTranslator(NewFileLoader(), "en", "en")
	if err := registry.Instance(serviceKey, translator); err != nil {
		t.Fatalf("bind translator: %v", err)
	}
}

func writeTranslationFile(t *testing.T, root string, locale string, group string, content string) {
	t.Helper()
	dir := filepath.Join(root, locale)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("create translation dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, group+".json"), []byte(content), 0644); err != nil {
		t.Fatalf("write translation file: %v", err)
	}
}

func TestTranslatorGetBasic(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"welcome": "Welcome",
	}, "en")

	result := translator.Get("welcome", nil)
	if result != "Welcome" {
		t.Errorf("Get = %v, want Welcome", result)
	}
}

func TestFileLoaderLoadMissingFile(t *testing.T) {
	loader := NewFileLoader()

	loader.AddPath("/nonexistent/path")

	data, err := loader.Load("en", "nonexistent", "")
	if err != nil {
		t.Errorf("Load should not error for missing file, got: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("Load should return empty map for missing file, got %v", data)
	}
}

func TestFileLoaderNamespaces(t *testing.T) {
	loader := NewFileLoader()

	loader.AddNamespace("test", "/some/path")

	namespaces := loader.Namespaces()
	if len(namespaces) != 1 {
		t.Errorf("Namespaces = %v, want 1 namespace", len(namespaces))
	}
	if namespaces["test"] != "/some/path" {
		t.Errorf("Namespaces[test] = %v, want /some/path", namespaces["test"])
	}
}

func TestFileLoaderJSONPaths(t *testing.T) {
	loader := NewFileLoader()

	loader.AddJSONPath("/some/path")

	paths := loader.JSONPaths()
	if len(paths) != 1 {
		t.Errorf("JSONPaths = %v, want 1 path", len(paths))
	}
	if paths[0] != "/some/path" {
		t.Errorf("JSONPaths[0] = %v, want /some/path", paths[0])
	}
}

func TestTranslatorAddPath(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddPath("/test/path")

	paths := loader.Paths()
	if len(paths) != 1 || paths[0] != "/test/path" {
		t.Errorf("Paths = %v, want [/test/path]", paths)
	}
}

func TestTranslatorAddJSONPath(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddJSONPath("/test/path")

	paths := loader.JSONPaths()
	if len(paths) != 1 || paths[0] != "/test/path" {
		t.Errorf("JSONPaths = %v, want [/test/path]", paths)
	}
}

func TestTranslatorLoader(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	if translator.Loader() != loader {
		t.Error("Loader should return the loader instance")
	}
}

func TestTranslatorGetWithPlaceholder(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"greeting": "Hello, :Name",
	}, "en")

	result := translator.Get("greeting", map[string]any{"name": "John"})
	if result != "Hello, John" {
		t.Errorf("Get = %v, want Hello, John", result)
	}
}

func TestTranslatorGetMissingKey(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	result := translator.Get("nonexistent", nil)
	if result != "nonexistent" {
		t.Errorf("Get = %v, want nonexistent", result)
	}
}

func TestTranslatorGetWithLocale(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"hello": "Hello",
	}, "en")

	translator.AddLines(map[string]any{
		"hello": "你好",
	}, "zh_CN")

	result := translator.Get("hello", nil, "zh_CN")
	if result != "你好" {
		t.Errorf("Get = %v, want 你好", result)
	}

	result = translator.Get("hello", nil)
	if result != "Hello" {
		t.Errorf("Get = %v, want Hello", result)
	}
}

func TestTranslatorHas(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"exists": "Exists",
	}, "en")

	tests := []struct {
		key      string
		expected bool
	}{
		{"exists", true},
		{"nonexistent", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := translator.Has(tt.key)
			if result != tt.expected {
				t.Errorf("Has(%q) = %v, want %v", tt.key, result, tt.expected)
			}
		})
	}
}

func TestTranslatorHasForLocale(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"hello": "Hello",
	}, "zh_CN")

	if translator.HasForLocale("hello", "zh_CN") != true {
		t.Error("HasForLocale should return true for existing key in locale")
	}

	if translator.HasForLocale("hello", "en") != false {
		t.Error("HasForLocale should return false for missing key in locale")
	}
}

func TestTranslatorChoice(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"apples": "No apples|One apple|Many apples",
	}, "en")

	tests := []struct {
		number   any
		expected string
	}{
		{0, "No apples"},
		{1, "One apple"},
		{5, "Many apples"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := translator.Choice("apples", tt.number, nil)
			if result != tt.expected {
				t.Errorf("Choice(%v) = %v, want %v", tt.number, result, tt.expected)
			}
		})
	}
}

func TestTranslatorChoiceWithReplace(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"apples": "There are :count apples",
	}, "en")

	result := translator.Get("apples", map[string]any{"count": 5})
	if result != "There are 5 apples" {
		t.Errorf("Get = %v, want There are 5 apples", result)
	}
}

func TestTranslatorChoiceRespectsProvidedCount(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"items": "{0} No items|{1} One item|[2,*] :count items",
	}, "en")

	replace := map[string]any{"count": 99}

	result := translator.Choice("items", 5, replace, "en")
	if result != "99 items" {
		t.Errorf("Choice = %v, want 99 items", result)
	}

	if replace["count"] != 99 {
		t.Errorf("replace map count was overwritten: got %v, want 99", replace["count"])
	}
}

func TestTranslatorLocale(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "zh_CN")

	if translator.Locale() != "en" {
		t.Errorf("Locale = %v, want en", translator.Locale())
	}

	if err := translator.SetLocale("zh_CN"); err != nil {
		t.Errorf("SetLocale(zh_CN) should not error: %v", err)
	}
	if translator.Locale() != "zh_CN" {
		t.Errorf("Locale = %v, want zh_CN", translator.Locale())
	}
}

func TestTranslatorSetLocaleInvalidChars(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	if err := translator.SetLocale("en/../malicious"); err == nil {
		t.Error("SetLocale with path traversal should return error")
	}

	if err := translator.SetLocale("en\\..\\malicious"); err == nil {
		t.Error("SetLocale with backslash should return error")
	}

	if translator.Locale() != "en" {
		t.Errorf("Locale should not have changed: got %v", translator.Locale())
	}
}

func TestTranslatorSetFallbackInvalidChars(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	if err := translator.SetFallback("en/../malicious"); err == nil {
		t.Error("SetFallback with path traversal should return error")
	}

	if translator.GetFallback() != "en" {
		t.Errorf("Fallback should not have changed: got %v", translator.GetFallback())
	}
}

func TestTranslatorCurrentLocale(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	if translator.CurrentLocale() != "en" {
		t.Errorf("CurrentLocale = %v, want en", translator.CurrentLocale())
	}
}

func TestTranslatorIsLocale(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	if !translator.IsLocale("en") {
		t.Error("IsLocale(en) should return true")
	}

	if translator.IsLocale("zh_CN") {
		t.Error("IsLocale(zh_CN) should return false")
	}
}

func TestTranslatorFallback(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "zh_CN")

	if translator.GetFallback() != "zh_CN" {
		t.Errorf("GetFallback = %v, want zh_CN", translator.GetFallback())
	}

	if err := translator.SetFallback("fr"); err != nil {
		t.Errorf("SetFallback(fr) should not error: %v", err)
	}
	if translator.GetFallback() != "fr" {
		t.Errorf("GetFallback = %v, want fr", translator.GetFallback())
	}
}

func TestTranslatorAddLines(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"key1": "Value 1",
		"key2": "Value 2",
	}, "en")

	result := translator.Get("key1", nil)
	if result != "Value 1" {
		t.Errorf("AddLines should add keys: got %v", result)
	}
}

func TestTranslatorAddLinesWithGroupKey(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"messages.welcome": "Welcome, :Name",
		"messages.goodbye": "Goodbye",
	}, "en")

	result := translator.Get("messages.welcome", nil, "en")
	if result != "Welcome, :Name" {
		t.Errorf("Get(messages.welcome) = %v, want Welcome, :Name", result)
	}

	result = translator.Get("messages.goodbye", nil, "en")
	if result != "Goodbye" {
		t.Errorf("Get(messages.goodbye) = %v, want Goodbye", result)
	}

	if !translator.Has("messages.welcome") {
		t.Error("Has(messages.welcome) should return true")
	}
}

func TestTranslatorAddLinesWithNamespaceKey(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"alerts.info": "Info from acme",
	}, "en", "acme")

	result := translator.Get("acme::alerts.info", nil, "en")
	if result != "Info from acme" {
		t.Errorf("Get(acme::alerts.info) = %v, want Info from acme", result)
	}
}

func TestTranslatorHandleMissingKeysUsing(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.HandleMissingKeysUsing(func(ctx context.Context, key, locale string) (string, bool) {
		if key == "custom.missing" {
			return "Custom Missing Value", true
		}
		return "", false
	})

	result := translator.Get("custom.missing", nil)
	if result != "Custom Missing Value" {
		t.Errorf("Get = %v, want Custom Missing Value", result)
	}
}

func TestTranslatorDetermineLocalesUsing(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.DetermineLocalesUsing(func(key string, requested string) []string {
		return []string{"zh_CN", "en", "fr"}
	})

	translator.AddLines(map[string]any{
		"hello": "你好",
	}, "zh_CN")

	result := translator.Get("hello", nil, "en")
	if result != "你好" {
		t.Errorf("Get = %v, want 你好", result)
	}
}

func TestTranslatorFallbackChain(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "fr")

	translator.AddLines(map[string]any{
		"hello": "Bonjour",
	}, "fr")

	translator.AddLines(map[string]any{
		"hello": "Hello",
	}, "en")

	translator.AddLines(map[string]any{
		"hello": "你好",
	}, "zh_CN")

	result := translator.Get("hello", nil, "zh_CN")
	if result != "你好" {
		t.Errorf("Get = %v, want 你好", result)
	}
}

func TestFileLoaderLoadFromFile(t *testing.T) {
	tmpDir := t.TempDir()

	zhDir := filepath.Join(tmpDir, "zh_CN")
	if err := os.MkdirAll(zhDir, 0755); err != nil {
		t.Fatal(err)
	}

	zhMessages := filepath.Join(zhDir, "messages.json")
	if err := os.WriteFile(zhMessages, []byte(`{"hello": "你好", "welcome": "欢迎"}`), 0644); err != nil {
		t.Fatal(err)
	}

	loader := NewFileLoader()
	loader.AddPath(tmpDir)

	translator := NewTranslator(loader, "zh_CN", "en")

	result := translator.Get("messages.hello", nil)
	if result != "你好" {
		t.Errorf("Get = %v, want 你好", result)
	}

	result = translator.Get("messages.welcome", nil)
	if result != "欢迎" {
		t.Errorf("Get = %v, want 欢迎", result)
	}
}

func TestFileLoaderJSON(t *testing.T) {
	tmpDir := t.TempDir()

	zhJSON := filepath.Join(tmpDir, "zh_CN.json")
	if err := os.WriteFile(zhJSON, []byte(`{"I love programming": "我喜欢编程"}`), 0644); err != nil {
		t.Fatal(err)
	}

	loader := NewFileLoader()
	loader.AddJSONPath(tmpDir)

	translator := NewTranslator(loader, "zh_CN", "en")

	result := translator.Get("I love programming", nil)
	if result != "我喜欢编程" {
		t.Errorf("Get = %v, want 我喜欢编程", result)
	}
}

func TestFileLoaderNamespace(t *testing.T) {
	tmpDir := t.TempDir()

	acmeDir := filepath.Join(tmpDir, "vendor", "acme", "zh_CN")
	if err := os.MkdirAll(acmeDir, 0755); err != nil {
		t.Fatal(err)
	}

	acmeMessages := filepath.Join(acmeDir, "messages.json")
	if err := os.WriteFile(acmeMessages, []byte(`{"hello": "来自 acme 的 hello"}`), 0644); err != nil {
		t.Fatal(err)
	}

	loader := NewFileLoader()
	loader.AddPath(tmpDir)

	translator := NewTranslator(loader, "zh_CN", "en")

	translator.AddNamespace("acme", filepath.Join(tmpDir, "vendor", "acme"))

	result := translator.Get("acme::messages.hello", nil)
	if result != "来自 acme 的 hello" {
		t.Errorf("Get = %v, want 来自 acme 的 hello", result)
	}
}

func TestTranslatorInterface(t *testing.T) {
	loader := NewFileLoader()
	var translator transcontract.Translator = NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{"test": "Test"}, "en")
	if translator.Get("test", nil) != "Test" {
		t.Error("Should implement Translator interface")
	}
}

func TestTranslatorLoaderInterface(t *testing.T) {
	var loader transcontract.Loader = NewFileLoader()

	loader.AddPath("/some/path")
	if len(loader.Paths()) != 1 {
		t.Error("Should implement Loader interface")
	}
}

func TestTranslatorStringableCustomFormatter(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"greeting": "Hello, :Name",
	}, "en")

	translator.Stringable("John", func(a any) string {
		return "Custom[" + a.(string) + "]"
	})

	result := translator.Get("greeting", map[string]any{"name": "John"})
	if result != "Hello, Custom[john]" {
		t.Errorf("Get greeting with name=John = %v, want Hello, Custom[john]", result)
	}
}

func TestTranslatorStringableNilParams(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"test": "Hello",
	}, "en")

	translator.Stringable(nil, func(a any) string { return "" })
	translator.Stringable("sample", nil)

	result := translator.Get("test", nil)
	if result != "Hello" {
		t.Errorf("Get = %v, want Hello", result)
	}
}

func TestTranslatorGetMapGroup(t *testing.T) {
	loader := NewFileLoader()

	langDir := t.TempDir()
	writeTranslationFile(t, langDir, "en", "messages", `{"welcome":"Welcome","exit":"Goodbye"}`)

	loader.AddPath(langDir)
	translator := NewTranslator(loader, "en", "en")

	result := translator.GetMap("messages", "en")
	if result == nil {
		t.Fatal("GetMap(messages) should return non-nil map")
	}

	if result["welcome"] != "Welcome" {
		t.Errorf("result[welcome] = %v, want Welcome", result["welcome"])
	}
	if result["exit"] != "Goodbye" {
		t.Errorf("result[exit] = %v, want Goodbye", result["exit"])
	}
}

func TestTranslatorGetMapSubItem(t *testing.T) {
	loader := NewFileLoader()

	langDir := t.TempDir()
	writeTranslationFile(t, langDir, "en", "messages", `{"auth":{"failed":"Auth failed","throttle":"Too many attempts"}}`)

	loader.AddPath(langDir)
	translator := NewTranslator(loader, "en", "en")

	result := translator.GetMap("messages.auth", "en")
	if result == nil {
		t.Fatal("GetMap(messages.auth) should return non-nil map")
	}

	if result["failed"] != "Auth failed" {
		t.Errorf("result[failed] = %v, want Auth failed", result["failed"])
	}
	if result["throttle"] != "Too many attempts" {
		t.Errorf("result[throttle] = %v, want Too many attempts", result["throttle"])
	}
}

func TestTranslatorGetMapNotFound(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	result := translator.GetMap("nonexistent", "en")
	if result != nil {
		t.Errorf("GetMap for nonexistent group should return nil, got %v", result)
	}
}

func TestTranslatorHasGroupFile(t *testing.T) {
	loader := NewFileLoader()

	langDir := t.TempDir()
	writeTranslationFile(t, langDir, "en", "messages", `{"welcome":"Welcome","exit":"Goodbye"}`)

	loader.AddPath(langDir)
	translator := NewTranslator(loader, "en", "en")

	if !translator.Has("messages") {
		t.Error("Has(messages) should return true for group file")
	}
}

func TestTranslatorHasNestedMap(t *testing.T) {
	loader := NewFileLoader()

	langDir := t.TempDir()
	writeTranslationFile(t, langDir, "en", "messages", `{"auth":{"failed":"Auth failed","throttle":"Too many"}}`)

	loader.AddPath(langDir)
	translator := NewTranslator(loader, "en", "en")

	if !translator.Has("messages.auth") {
		t.Error("Has(messages.auth) should return true for nested map")
	}

	if !translator.Has("messages") {
		t.Error("Has(messages) should return true for group file with nested maps")
	}
}

func TestTranslatorHasNoFalsePositive(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	if translator.Has("nonexistent") {
		t.Error("Has(nonexistent) should return false")
	}

	if translator.Has("nonexistent.group") {
		t.Error("Has(nonexistent.group) should return false")
	}
}

func TestFacadeResetAfterUse(t *testing.T) {
	Reset()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Get after Reset without container did not panic")
		}
		if got := fmt.Sprint(recovered); got != `container "translator": no current application container` {
			t.Fatalf("panic = %q, want translator no current container", got)
		}
	}()

	_ = Get("hello", nil)
}

func TestFacadeGet(t *testing.T) {
	bindFacadeTranslator(t)

	AddLines(map[string]any{"welcome": "Hello"}, "en")

	result := Get("welcome", nil)
	if result != "Hello" {
		t.Errorf("Facade Get = %v, want Hello", result)
	}
}

func TestFacadeHas(t *testing.T) {
	bindFacadeTranslator(t)

	AddLines(map[string]any{"welcome": "Hello"}, "en")

	if !Has("welcome") {
		t.Error("Facade Has should return true")
	}

	if Has("nonexistent") {
		t.Error("Facade Has should return false for nonexistent key")
	}
}

func TestFacadeChoice(t *testing.T) {
	bindFacadeTranslator(t)

	AddLines(map[string]any{"apples": "{0}:count|{1}:count|[2,*]:count"}, "en")

	result := Choice("apples", 3, nil, "en")
	if result != "3" {
		t.Errorf("Facade Choice = %v, want 3", result)
	}
}

func TestFacadeLocale(t *testing.T) {
	bindFacadeTranslator(t)

	if err := SetLocale("zh_CN"); err != nil {
		t.Errorf("SetLocale error: %v", err)
	}

	if Locale() != "zh_CN" {
		t.Errorf("Facade Locale = %v, want zh_CN", Locale())
	}
}

func TestFacadeSetLocale(t *testing.T) {
	bindFacadeTranslator(t)

	if err := SetLocale("fr"); err != nil {
		t.Errorf("SetLocale error: %v", err)
	}

	if !IsLocale("fr") {
		t.Error("IsLocale(fr) should return true")
	}

	if IsLocale("en") {
		t.Error("IsLocale(en) should return false after SetLocale(fr)")
	}
}

func TestFacadeFallback(t *testing.T) {
	bindFacadeTranslator(t)

	if GetFallback() != "en" {
		t.Errorf("Facade GetFallback = %v, want en", GetFallback())
	}

	if err := SetFallback("fr"); err != nil {
		t.Errorf("SetFallback error: %v", err)
	}

	if GetFallback() != "fr" {
		t.Errorf("Facade GetFallback = %v, want fr", GetFallback())
	}
}

func TestFacadeAddLines(t *testing.T) {
	bindFacadeTranslator(t)

	AddLines(map[string]any{"greeting": "Hi"}, "en")

	result := Get("greeting", nil)
	if result != "Hi" {
		t.Errorf("Facade AddLines+Get = %v, want Hi", result)
	}
}

func TestFacadeGetMap(t *testing.T) {
	bindFacadeTranslator(t)

	langDir := t.TempDir()
	writeTranslationFile(t, langDir, "en", "messages", `{"welcome":"Welcome","exit":"Goodbye"}`)

	AddPath(langDir)

	result := GetMap("messages", "en")
	if result == nil {
		t.Fatal("Facade GetMap should return non-nil")
	}
	if result["welcome"] != "Welcome" {
		t.Errorf("welcome = %v, want Welcome", result["welcome"])
	}
}

func TestFacadeStringable(t *testing.T) {
	bindFacadeTranslator(t)

	Stringable("sample", func(any) string { return "formatted" })

	AddLines(map[string]any{"test": "Hello :name"}, "en")

	result := Get("test", map[string]any{"name": "sample"})
	if result != "Hello formatted" {
		t.Errorf("Stringable Get = %v, want Hello formatted", result)
	}
}

func TestFacadeHandleMissingKeysUsing(t *testing.T) {
	bindFacadeTranslator(t)

	HandleMissingKeysUsing(func(ctx context.Context, key, locale string) (string, bool) {
		return "missing:" + key, true
	})

	result := Get("nonexistent", nil)
	if result != "missing:nonexistent" {
		t.Errorf("HandleMissingKeysUsing = %v, want missing:nonexistent", result)
	}
}

func TestFacadeHasForLocale(t *testing.T) {
	bindFacadeTranslator(t)

	AddLines(map[string]any{"welcome": "Hello"}, "en")

	if !HasForLocale("welcome", "en") {
		t.Error("HasForLocale(welcome, en) should return true")
	}
}

func TestFacadeCurrentLocale(t *testing.T) {
	bindFacadeTranslator(t)

	if CurrentLocale() != "en" {
		t.Errorf("CurrentLocale = %v, want en", CurrentLocale())
	}
}

func TestFacadeAddNamespace(t *testing.T) {
	bindFacadeTranslator(t)

	AddNamespace("test", "/test/path")

	loader := Loader()
	if loader == nil {
		t.Fatal("Loader should not be nil")
	}

	namespaces := loader.Namespaces()
	if namespaces["test"] != "/test/path" {
		t.Errorf("Namespaces[test] = %v, want /test/path", namespaces["test"])
	}
}

func TestFacadeAddJSONPath(t *testing.T) {
	bindFacadeTranslator(t)

	AddJSONPath("/test/path")

	loader := Loader()
	if loader == nil {
		t.Fatal("Loader should not be nil")
	}

	paths := loader.JSONPaths()
	if len(paths) != 1 || paths[0] != "/test/path" {
		t.Errorf("JSONPaths = %v, want [/test/path]", paths)
	}
}

func TestFacadeDetermineLocalesUsing(t *testing.T) {
	bindFacadeTranslator(t)

	DetermineLocalesUsing(func(key string, requested string) []string {
		return []string{requested}
	})

	AddLines(map[string]any{"welcome": "Hello"}, "en")
	result := Get("welcome", nil)
	if result != "Hello" {
		t.Errorf("Get = %v, want Hello", result)
	}
}

func TestTranslatorSetLoader(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	newLoader := NewFileLoader()
	translator.SetLoader(newLoader)

	if translator.Loader() != newLoader {
		t.Error("SetLoader should update loader instance")
	}
}

func TestServiceProviderName(t *testing.T) {
	sp := ServiceProvider{}
	if sp.Name() != "translation" {
		t.Errorf("Name = %v, want translation", sp.Name())
	}
}

func TestServiceProviderProvides(t *testing.T) {
	sp := ServiceProvider{}
	provides := sp.Provides()
	if len(provides) != 2 {
		t.Errorf("Provides = %v, want 2 bindings", len(provides))
	}
	if provides[0] != "translation.loader" {
		t.Errorf("Provides[0] = %v, want translation.loader", provides[0])
	}
	if provides[1] != "translator" {
		t.Errorf("Provides[1] = %v, want translator", provides[1])
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }

func TestServiceProviderRegister(t *testing.T) {
	registry := container.NewContainer()
	app := providerTestApp{registry: registry}

	if err := (ServiceProvider{}).Register(app); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if !registry.Bound("translation.loader") {
		t.Fatal("Register should bind translation.loader")
	}
	if !registry.Bound("translator") {
		t.Fatal("Register should bind translator")
	}
}

func TestServiceProviderBoot(t *testing.T) {
	registry := container.NewContainer()
	app := providerTestApp{registry: registry}

	if err := (ServiceProvider{}).Boot(app); err != nil {
		t.Errorf("Boot should not error: %v", err)
	}
}

func TestReplacerInt8Int16(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	translator.AddLines(map[string]any{
		"small": ":val",
	}, "en")

	result := translator.Get("small", map[string]any{"val": int8(8)})
	if result != "8" {
		t.Errorf("int8 = %v, want 8", result)
	}

	result = translator.Get("small", map[string]any{"val": int16(16)})
	if result != "16" {
		t.Errorf("int16 = %v, want 16", result)
	}
}

func TestReplacerStringable(t *testing.T) {
	loader := NewFileLoader()
	translator := NewTranslator(loader, "en", "en")

	type customType string
	translator.Stringable(customType(""), func(any) string { return "CUSTOM" })

	translator.AddLines(map[string]any{
		"greeting": "Hello :name",
	}, "en")

	result := translator.Get("greeting", map[string]any{"name": customType("sample")})
	if result != "Hello custom" {
		t.Errorf("Stringable = %v, want Hello custom", result)
	}
}

func TestMessageSelectorInvalidIntervals(t *testing.T) {
	s := NewMessageSelector()

	tests := []struct {
		name    string
		message string
		number  int
		want    string
	}{
		{"no_pipe", "Just text", 0, "Just text"},
		{"open_close_only", "{} Em|default", 0, "{} Em|default"},
		{"non_numeric_exact", "{abc} No|default", 0, "{abc} No|default"},
		{"unclosed_exact", "{0 No|default", 0, "{0 No|default"},
		{"unclosed_range", "[1 No|default", 0, "[1 No|default"},
		{"no_interval_match", "{99} Only|default", 1, "{99} Only|default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.Select(tt.message, tt.number, "en")
			if result != tt.want {
				t.Errorf("Select = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestFileLoaderInvalidJSON(t *testing.T) {
	loader := NewFileLoader()

	langDir := t.TempDir()
	writeTranslationFile(t, langDir, "en", "messages", `{invalid json`)

	loader.AddPath(langDir)

	data, err := loader.Load("en", "messages", "")
	if err != nil {
		t.Errorf("Load should not error for invalid JSON, got: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("Load with invalid JSON should return empty data, got %v", data)
	}
}

func TestFileLoaderDuplicatePath(t *testing.T) {
	loader := NewFileLoader()

	loader.AddPath("/some/path")
	loader.AddPath("/some/path")

	if len(loader.Paths()) != 1 {
		t.Errorf("Duplicate AddPath should be deduplicated, got %v", len(loader.Paths()))
	}

	loader.AddJSONPath("/some/path")
	loader.AddJSONPath("/some/path")

	if len(loader.JSONPaths()) != 1 {
		t.Errorf("Duplicate AddJSONPath should be deduplicated, got %v", len(loader.JSONPaths()))
	}
}

func TestFacadeLoaderFallback(t *testing.T) {
	Reset()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Loader after Reset without container did not panic")
		}
		if got := fmt.Sprint(recovered); got != `container "translator": no current application container` {
			t.Fatalf("panic = %q, want translator no current container", got)
		}
	}()

	_ = Loader()
}
