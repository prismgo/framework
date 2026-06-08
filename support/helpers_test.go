package support_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/foundation"
	"github.com/prismgo/framework/support"
)

func TestEmpty(t *testing.T) {
	var ptr *int
	var slice []string
	zero := struct {
		Name string
	}{}
	nonZero := struct {
		Name string
	}{Name: "prismgo"}

	tests := []struct {
		name string
		in   any
		want bool
	}{
		{name: "nil", in: nil, want: true},
		{name: "empty string", in: "", want: true},
		{name: "non empty string", in: "support", want: false},
		{name: "empty array", in: [0]int{}, want: true},
		{name: "non empty array", in: [1]int{}, want: false},
		{name: "nil map", in: map[string]string(nil), want: true},
		{name: "empty map", in: map[string]string{}, want: true},
		{name: "non empty map", in: map[string]string{"key": "value"}, want: false},
		{name: "nil slice", in: slice, want: true},
		{name: "empty slice", in: []string{}, want: true},
		{name: "non empty slice", in: []string{"value"}, want: false},
		{name: "false bool", in: false, want: true},
		{name: "true bool", in: true, want: false},
		{name: "zero int", in: 0, want: true},
		{name: "non zero int", in: 1, want: false},
		{name: "zero uint", in: uint(0), want: true},
		{name: "non zero uint", in: uint(1), want: false},
		{name: "zero float", in: 0.0, want: true},
		{name: "non zero float", in: 0.1, want: false},
		{name: "nil pointer", in: ptr, want: true},
		{name: "zero struct", in: zero, want: true},
		{name: "non zero struct", in: nonZero, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := support.Empty(tt.in); got != tt.want {
				t.Fatalf("Empty(%#v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseInt(t *testing.T) {
	if got := support.ParseInt("10", 1); got != 10 {
		t.Fatalf("expected 10, got %d", got)
	}
	if got := support.ParseInt("bad", 7); got != 7 {
		t.Fatalf("expected fallback 7, got %d", got)
	}
	if got := support.ParseInt("", 8); got != 8 {
		t.Fatalf("expected fallback 8, got %d", got)
	}
	if got := support.ParseInt(" 12 ", 9); got != 12 {
		t.Fatalf("expected trimmed 12, got %d", got)
	}
}

func TestURLGeneratesApplicationURLFromConfiguredBase(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	t.Setenv("APP_URL", "https://example.test/base/")
	app := foundation.Configure(t.TempDir()).Create()
	t.Cleanup(func() { _ = app.Close() })

	if got := support.URL("user/profile"); got != "https://example.test/base/user/profile" {
		t.Fatalf("URL(user/profile) = %q, want %q", got, "https://example.test/base/user/profile")
	}
	if got := support.URL("/user/profile"); got != "https://example.test/base/user/profile" {
		t.Fatalf("URL(/user/profile) = %q, want %q", got, "https://example.test/base/user/profile")
	}
	if got := support.URL(""); got != "https://example.test/base" {
		t.Fatalf("URL(empty) = %q, want %q", got, "https://example.test/base")
	}
}

func TestURLPreservesAbsoluteURL(t *testing.T) {
	if got := support.URL("https://cdn.example.test/assets/app.js"); got != "https://cdn.example.test/assets/app.js" {
		t.Fatalf("URL(absolute) = %q, want %q", got, "https://cdn.example.test/assets/app.js")
	}
}

func TestURLAppendsLaravelStyleParameters(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	t.Setenv("APP_URL", "https://example.test")
	app := foundation.Configure(t.TempDir()).Create()
	t.Cleanup(func() { _ = app.Close() })

	if got := support.URL("user/profile", []any{1, "张 三"}); got != "https://example.test/user/profile/1/%E5%BC%A0%20%E4%B8%89" {
		t.Fatalf("URL with slice parameters = %q", got)
	}
	if got := support.URL("orders", 10, "items"); got != "https://example.test/orders/10/items" {
		t.Fatalf("URL with variadic parameters = %q", got)
	}
}

func TestURLFallsBackWhenApplicationConfigIsUnavailable(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	t.Setenv("APP_URL", "https://env.example.test")
	if got := support.URL("docs"); got != "https://env.example.test/docs" {
		t.Fatalf("URL with APP_URL fallback = %q, want %q", got, "https://env.example.test/docs")
	}

	t.Setenv("APP_URL", "")
	if got := support.URL(""); got != "http://localhost:8080" {
		t.Fatalf("URL default fallback = %q, want %q", got, "http://localhost:8080")
	}
}

func TestPathHelpersUseCurrentApplicationBindings(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	root := t.TempDir()
	app := foundation.Configure(root).Create()
	t.Cleanup(func() { _ = app.Close() })

	if got := support.StoragePath("logs", "app.log"); got != filepath.Join(root, "storage", "logs", "app.log") {
		t.Fatalf("StoragePath(logs, app.log) = %q, want %q", got, filepath.Join(root, "storage", "logs", "app.log"))
	}
	if got := support.LangPath("en", "messages.json"); got != filepath.Join(root, "lang", "en", "messages.json") {
		t.Fatalf("LangPath(en, messages.json) = %q, want %q", got, filepath.Join(root, "lang", "en", "messages.json"))
	}
	if got := support.ConfigPath("app.go"); got != filepath.Join(root, "config", "app.go") {
		t.Fatalf("ConfigPath(app.go) = %q, want %q", got, filepath.Join(root, "config", "app.go"))
	}
}

func TestPathHelpersNormalizeConfiguredPathSegments(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	root := t.TempDir()
	app := foundation.Configure(root).Create()
	t.Cleanup(func() { _ = app.Close() })

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty storage segment", in: "", want: filepath.Join(root, "storage")},
		{name: "relative storage segment", in: "framework/cache/data", want: filepath.Join(root, "storage", "framework", "cache", "data")},
		{name: "storage-prefixed segment", in: "storage/framework/cache/data", want: filepath.Join(root, "storage", "framework", "cache", "data")},
		{name: "storage root segment", in: "storage", want: filepath.Join(root, "storage")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := support.StoragePath(tt.in); got != tt.want {
				t.Fatalf("StoragePath(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}

	absolute := filepath.Join(t.TempDir(), "absolute", "cache")
	if got := support.StoragePath(absolute); got != absolute {
		t.Fatalf("StoragePath(abs) = %q, want %q", got, absolute)
	}
	if got, want := support.PublicPath("public/assets"), filepath.Join(root, "public", "assets"); got != want {
		t.Fatalf("PublicPath(public/assets) = %q, want %q", got, want)
	}
	if got, want := support.ConfigPath("config/app.go"), filepath.Join(root, "config", "app.go"); got != want {
		t.Fatalf("ConfigPath(config/app.go) = %q, want %q", got, want)
	}
	if got, want := support.BasePath("storage/app.log"), filepath.Join(root, "storage", "app.log"); got != want {
		t.Fatalf("BasePath(storage/app.log) = %q, want %q", got, want)
	}
}

func TestPathHelpersDoNotReinferAfterApplicationStartup(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	root := t.TempDir()
	nested := filepath.Join(root, "cmd", "serve")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.work"), []byte("go 1.26.2\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	t.Chdir(nested)
	app := foundation.Configure().Create()
	t.Cleanup(func() { _ = app.Close() })

	other := t.TempDir()
	t.Chdir(other)

	if got := app.BasePath(".env"); got != filepath.Join(root, ".env") {
		t.Fatalf("Application BasePath after cwd change = %q, want %q", got, filepath.Join(root, ".env"))
	}
	if got := support.StoragePath("logs", "app.log"); got != filepath.Join(root, "storage", "logs", "app.log") {
		t.Fatalf("StoragePath after cwd change = %q, want %q", got, filepath.Join(root, "storage", "logs", "app.log"))
	}
	if got := support.LangPath("zh_CN", "validation.json"); got != filepath.Join(root, "lang", "zh_CN", "validation.json") {
		t.Fatalf("LangPath after cwd change = %q, want %q", got, filepath.Join(root, "lang", "zh_CN", "validation.json"))
	}
}

func TestPathHelpersFallbackToInferredRootWithoutCurrentApplication(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	root := t.TempDir()
	nested := filepath.Join(root, "pkg", "tool")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.work"), []byte("go 1.26.2\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	t.Chdir(nested)

	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "base", got: support.BasePath("go.work"), want: filepath.Join(root, "go.work")},
		{name: "app", got: support.AppPath("models", "user.go"), want: filepath.Join(root, "app", "models", "user.go")},
		{name: "database", got: support.DatabasePath("migrations"), want: filepath.Join(root, "database", "migrations")},
		{name: "public", got: support.PublicPath("index.html"), want: filepath.Join(root, "public", "index.html")},
		{name: "resource", got: support.ResourcePath("views"), want: filepath.Join(root, "resources", "views")},
		{name: "storage", got: support.StoragePath("logs"), want: filepath.Join(root, "storage", "logs")},
		{name: "lang", got: support.LangPath("en", "messages.json"), want: filepath.Join(root, "lang", "en", "messages.json")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("path = %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestPathHelpersFallbackRecognizesDeployedLayoutWithoutMarkers(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "storage"), 0o755); err != nil {
		t.Fatalf("mkdir storage: %v", err)
	}

	t.Chdir(root)

	if got := support.StoragePath("logs", "app.log"); got != filepath.Join(root, "storage", "logs", "app.log") {
		t.Fatalf("StoragePath deployed layout = %q, want %q", got, filepath.Join(root, "storage", "logs", "app.log"))
	}
}

func TestPathHelpersPreserveAbsolutePaths(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	root := t.TempDir()
	app := foundation.Configure(root).Create()
	t.Cleanup(func() { _ = app.Close() })

	absolute := filepath.Join(t.TempDir(), "logs", "app.log")
	if got := support.BasePath(absolute); got != absolute {
		t.Fatalf("BasePath(abs) = %q, want %q", got, absolute)
	}
}

func TestIsProductionDefaultFallsBackToProduction(t *testing.T) {
	t.Setenv("APP_ENV", "")
	if !support.IsProduction() {
		t.Fatal("IsProduction() should return true when APP_ENV is empty (conservative fallback)")
	}
}

func TestIsProductionExplicitProductionEnv(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	if !support.IsProduction() {
		t.Fatal("IsProduction() should return true for APP_ENV=production")
	}
}

func TestIsProductionExplicitProductionEnvCaseInsensitive(t *testing.T) {
	t.Setenv("APP_ENV", "PRODUCTION")
	if !support.IsProduction() {
		t.Fatal("IsProduction() should be case-insensitive")
	}
}

func TestIsProductionLocalEnv(t *testing.T) {
	t.Setenv("APP_ENV", "local")
	if support.IsProduction() {
		t.Fatal("IsProduction() should return false for APP_ENV=local")
	}
}

func TestIsProductionDevEnv(t *testing.T) {
	t.Setenv("APP_ENV", "dev")
	if support.IsProduction() {
		t.Fatal("IsProduction() should return false for APP_ENV=dev")
	}
}

func TestIsProductionTestingEnv(t *testing.T) {
	t.Setenv("APP_ENV", "testing")
	if support.IsProduction() {
		t.Fatal("IsProduction() should return false for APP_ENV=testing")
	}
}

func TestIsProductionStagingEnv(t *testing.T) {
	t.Setenv("APP_ENV", "staging")
	if support.IsProduction() {
		t.Fatal("IsProduction() should return false for APP_ENV=staging")
	}
}
