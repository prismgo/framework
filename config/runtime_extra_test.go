package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestFacadeFactoryAndTypedGetters(t *testing.T) {
	registry := useConfigTestContainer(t)

	factoryCfg := &Config{store: map[string]any{
		"typed": map[string]any{
			"name":  "factory",
			"count": 7,
			"ratio": 1.25,
			"large": int64(99),
			"flag":  true,
			"attrs": map[string]any{"zone": "cn"},
		},
	}}
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return factoryCfg, nil
	}); err != nil {
		t.Fatalf("register config factory failed: %v", err)
	}

	if got := Resolve(); got != factoryCfg {
		t.Fatalf("Resolve got %#v, want factory config", got)
	}
	if got := Get("typed.name"); got != "factory" {
		t.Fatalf("Get = %q, want factory", got)
	}
	if got := GetInt("typed.count"); got != 7 {
		t.Fatalf("GetInt = %d, want 7", got)
	}
	if got := GetFloat64("typed.ratio"); got != 1.25 {
		t.Fatalf("GetFloat64 = %v, want 1.25", got)
	}
	if got := GetInt64("typed.large"); got != 99 {
		t.Fatalf("GetInt64 = %d, want 99", got)
	}
	if got := GetUint("typed.count"); got != 7 {
		t.Fatalf("GetUint = %d, want 7", got)
	}
	if got := GetBool("typed.flag"); !got {
		t.Fatal("GetBool = false, want true")
	}
	if got := GetStringMapString("typed.attrs"); got["zone"] != "cn" {
		t.Fatalf("GetStringMapString = %#v, want zone cn", got)
	}
	if got := GetStringMap("typed.attrs"); got["zone"] != "cn" {
		t.Fatalf("GetStringMap = %#v, want zone cn", got)
	}
}

func TestFacadeGettersResolveRegisteredFactory(t *testing.T) {
	registry := useConfigTestContainer(t)

	factoryCfg := &Config{store: map[string]any{
		"lazy": map[string]any{
			"name": "loaded",
		},
	}}
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return factoryCfg, nil
	}); err != nil {
		t.Fatalf("register config factory failed: %v", err)
	}

	if got := GetString("lazy.name"); got != "loaded" {
		t.Fatalf("GetString should resolve registered factory, got %q", got)
	}
	if Resolve() != factoryCfg {
		t.Fatal("getter should resolve factory config")
	}
}

func TestFacadeResolveWithoutCurrentRegistryReturnsNil(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Resolve should panic without a current registry")
		}
	}()
	_ = Resolve()
}

func TestNewFromDefaultFileReturnsConfig(t *testing.T) {
	cfg, err := NewFromDefaultFile()
	if err != nil {
		t.Fatalf("NewFromDefaultFile failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("NewFromDefaultFile returned nil config")
	}
}

func TestFacadeReloadAndSetDefault(t *testing.T) {
	registry := useConfigTestContainer(t)

	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("LOAD_FLAG=loaded\n"), 0o644); err != nil {
		t.Fatalf("write env file failed: %v", err)
	}
	Add("load_extra", func() map[string]any {
		return map[string]any{"flag": Env("LOAD_FLAG", "missing")}
	})

	cfg, err := NewFromFile(path)
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}
	if err := registry.Instance(serviceKey, cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if Resolve() != cfg {
		t.Fatal("container should register provided config")
	}
	if got := GetString("load_extra.flag"); got != "loaded" {
		t.Fatalf("expected loaded flag, got %q", got)
	}
}

func TestReloadFromFileReplacesStoreWithExplicitEnvFile(t *testing.T) {
	// 覆盖实例级 ReloadFromFile 分支，确保显式文件重载会替换旧仓库而不是合并旧值。
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("RELOAD_FILE_VALUE=fresh\n"), 0o644); err != nil {
		t.Fatalf("write env file failed: %v", err)
	}
	Add("reload_file_extra", func() map[string]any {
		return map[string]any{"value": Env("RELOAD_FILE_VALUE", "missing")}
	})

	cfg := &Config{store: map[string]any{"reload_file_extra": map[string]any{"value": "stale"}}}
	if err := cfg.ReloadFromFile(path); err != nil {
		t.Fatalf("ReloadFromFile failed: %v", err)
	}
	if got := cfg.GetString("reload_file_extra.value"); got != "fresh" {
		t.Fatalf("ReloadFromFile value = %q, want fresh", got)
	}

	var nilCfg *Config
	if err := nilCfg.ReloadFromFile(path); err != nil {
		t.Fatalf("nil ReloadFromFile should still validate readable file, got %v", err)
	}
}

func TestReloadReadsDefaultEnvFromProjectRoot(t *testing.T) {
	registry := useConfigTestContainer(t)

	root := t.TempDir()
	nested := filepath.Join(root, "prismgo", "foundation")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested package dir failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.work"), []byte("go 1.26.2\n"), 0o644); err != nil {
		t.Fatalf("write go.work marker failed: %v", err)
	}

	const envName = "PRISMGO_CONFIG_ROOT_ENV_TEST_VALUE"
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte(envName+"=root\n"), 0o644); err != nil {
		t.Fatalf("write root env failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, "local.env"), []byte(envName+"=explicit\n"), 0o644); err != nil {
		t.Fatalf("write nested env failed: %v", err)
	}

	Add("root_env_resolution_extra", func() map[string]any {
		return map[string]any{"value": Env(envName, "missing")}
	})

	t.Chdir(nested)
	cfg := New()
	if err := registry.Instance(serviceKey, cfg); err != nil {
		t.Fatalf("bind config failed: %v", err)
	}
	if err := Reload(); err != nil {
		t.Fatalf("Reload failed: %v", err)
	}
	if got := GetString("root_env_resolution_extra.value"); got != "root" {
		t.Fatalf("Reload should read project root .env, got %q", got)
	}
}

func TestConfigEmptyReloadNilAndMissingMaps(t *testing.T) {
	var nilCfg *Config
	if !nilCfg.Empty() {
		t.Fatal("nil config should be empty")
	}
	cfg := New()
	if !cfg.Empty() {
		t.Fatal("new config should be empty")
	}
	if got := cfg.GetStringMapString("missing"); len(got) != 0 {
		t.Fatalf("missing string map = %#v, want empty", got)
	}
	if got := cfg.GetStringMap("missing"); len(got) != 0 {
		t.Fatalf("missing map = %#v, want empty", got)
	}

	cfg = &Config{store: map[string]any{"list": []any{"a", map[string]any{"b": "c"}}}}
	cloned := cfg.Clone()
	cloned.store["list"].([]any)[1].(map[string]any)["b"] = "changed"
	if cfg.store["list"].([]any)[1].(map[string]any)["b"] != "c" {
		t.Fatal("Clone should deep-copy slices and nested maps")
	}
}

func TestGetStringMapReturnsDeepCopiedData(t *testing.T) {
	cfg := &Config{store: map[string]any{
		"app": map[string]any{
			"server": map[string]any{
				"headers": map[string]any{"client": "X-Real-IP"},
				"proxies": []any{"10.0.0.1", map[string]any{"zone": "cn"}},
			},
		},
	}}

	got := cfg.GetStringMap("app.server")
	got["name"] = "mutated"
	got["headers"].(map[string]any)["client"] = "X-Forwarded-For"
	got["proxies"].([]any)[1].(map[string]any)["zone"] = "us"

	server := cfg.store["app"].(map[string]any)["server"].(map[string]any)
	if _, ok := server["name"]; ok {
		t.Fatal("GetStringMap should not expose top-level map mutations to store")
	}
	if server["headers"].(map[string]any)["client"] != "X-Real-IP" {
		t.Fatal("GetStringMap should deep-copy nested maps")
	}
	if server["proxies"].([]any)[1].(map[string]any)["zone"] != "cn" {
		t.Fatal("GetStringMap should deep-copy nested slices")
	}
}

func TestGetStringMapStringReturnsCopiedData(t *testing.T) {
	cfg := &Config{store: map[string]any{
		"app": map[string]any{
			"labels": map[string]string{"env": "test"},
		},
	}}

	got := cfg.GetStringMapString("app.labels")
	got["env"] = "prod"
	got["zone"] = "cn"

	labels := cfg.store["app"].(map[string]any)["labels"].(map[string]string)
	if labels["env"] != "test" {
		t.Fatal("GetStringMapString should not expose map mutations to store")
	}
	if _, ok := labels["zone"]; ok {
		t.Fatal("GetStringMapString should return an isolated copy")
	}
}

func TestRuntimeParsingHelpers(t *testing.T) {
	t.Setenv("PORT_TEXT", "8051")
	t.Setenv("PORT_EMPTY", "")
	t.Setenv("BOOL_TEXT", "true")
	t.Setenv("INT_TEXT", "42")
	t.Setenv("FLOAT_TEXT", "3.5")

	if got := Env("PORT_TEXT", 80); got != 8051 {
		t.Fatalf("Env int = %v, want 8051", got)
	}
	if got := Env("PORT_EMPTY", 81); got != 81 {
		t.Fatalf("Env empty fallback = %v, want 81", got)
	}
	if got := castEnvValue("true", false); got != true {
		t.Fatalf("castEnvValue bool = %v", got)
	}
	if got := castEnvValue("42", int64(0)); got != int64(42) {
		t.Fatalf("castEnvValue int64 = %v", got)
	}
	if got := castEnvValue("42", uint(0)); got != uint(42) {
		t.Fatalf("castEnvValue uint = %v", got)
	}
	if got := castEnvValue("3.5", float64(0)); got != 3.5 {
		t.Fatalf("castEnvValue float = %v", got)
	}
	if got := castEnvValue("abc", ""); got != "abc" {
		t.Fatalf("castEnvValue string = %v", got)
	}
	if got := castEnvValue("42", int16(0)); got != int16(42) {
		t.Fatalf("castEnvValue int16 = %v", got)
	}
	if got := castEnvValue("42", int8(0)); got != int8(42) {
		t.Fatalf("castEnvValue int8 = %v", got)
	}
	if got := castEnvValue("42", uint8(0)); got != uint8(42) {
		t.Fatalf("castEnvValue uint8 = %v", got)
	}
	if got := castEnvValue("3.5", float32(0)); got != float32(3.5) {
		t.Fatalf("castEnvValue float32 = %v", got)
	}
	if got := castByKind("true", reflect.Bool); got != true {
		t.Fatalf("castByKind bool = %v", got)
	}
	if got := castByKind("7", reflect.Uint16); got != uint64(7) {
		t.Fatalf("castByKind uint = %v", got)
	}
	if got := castByKind("2.5", reflect.Float32); got != 2.5 {
		t.Fatalf("castByKind float = %v", got)
	}
	if got := castByKind("name", reflect.String); got != "name" {
		t.Fatalf("castByKind string = %v", got)
	}
	if got := castByKind("x", reflect.Struct); got != "x" {
		t.Fatalf("castByKind default = %v", got)
	}
	if got := firstDefault(); got != nil {
		t.Fatalf("firstDefault without values = %#v, want nil", got)
	}
	if got := firstDefault("first", "second"); got != "first" {
		t.Fatalf("firstDefault = %#v, want first", got)
	}
}

func TestValueOrDefaultReturnsDefaultForNilValue(t *testing.T) {
	// 当配置项存在但值为 nil 时，valueOrDefault 应返回调用方提供的默认值，而非零值。
	cfg := &Config{store: map[string]any{"key": nil}}
	if got := cfg.Get("key", "fallback"); got != "fallback" {
		t.Fatalf("Get with nil value should return default, got %q", got)
	}
}

func TestNilConfigReloadDoesNotPanic(t *testing.T) {
	// nil Config 上调用 Reload/ReloadFromFile 应直接返回 nil，不执行文件加载。
	var nilCfg *Config
	if err := nilCfg.Reload(); err != nil {
		t.Fatalf("nil Reload should return nil, got %v", err)
	}
	if err := nilCfg.ReloadFromFile("/nonexistent/.env"); err != nil {
		t.Fatalf("nil ReloadFromFile should return nil, got %v", err)
	}
}

func TestReadEnvFileErrorsAndNormalizeValue(t *testing.T) {
	dir := t.TempDir()
	if err := readEnvFile(newViper(), filepath.Join(dir, "missing.env")); err != nil {
		t.Fatalf("missing env file should be ignored, got %v", err)
	}
	if err := readEnvFile(newViper(), dir); err == nil {
		t.Fatal("expected directory read to fail")
	}

	normalized := normalizeValue(map[any]any{"bad": "key"}, maxNormalizeDepth)
	if _, ok := normalized.(map[any]any); !ok {
		t.Fatalf("non-string map keys should be preserved, got %T", normalized)
	}
	if got := normalizeValue([]string{"a", "b"}, maxNormalizeDepth); !reflect.DeepEqual(got, []any{"a", "b"}) {
		t.Fatalf("normalize slice = %#v", got)
	}
	type NamedPort int
	if !isBlankValue(nil) || !isBlankValue("") || isBlankValue(false) || isBlankValue(1) || isBlankValue(NamedPort(0)) {
		t.Fatal("isBlankValue returned unexpected result for nil/empty/named-zero")
	}
	if isBlankValue(NamedPort(80)) {
		t.Fatal("isBlankValue should return false for non-zero named type")
	}

	var pathErr *os.PathError
	if !errors.As(readEnvFile(newViper(), dir), &pathErr) {
		t.Fatal("expected readEnvFile directory error to wrap *os.PathError")
	}
}

func TestConfigConcurrentAccessWithLock(t *testing.T) {
	cfg := &Config{store: map[string]any{
		"app.name": "test",
		"nested":   map[string]any{"a": map[string]any{"b": "c"}},
		"count":    int64(99),
	}}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = cfg.Get("app.name")
				_ = cfg.GetString("app.name")
				_ = cfg.GetInt("nested.x")
				_ = cfg.GetStringMap("nested")
				_ = cfg.Clone()
				_ = cfg.Empty()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		var nilCfg *Config
		for j := 0; j < 50; j++ {
			nilCfg.Clone()
			nilCfg.Empty()
		}
	}()

	wg.Wait()
}

func TestConfigConcurrentReloadAndRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("VERSION=initial\n"), 0o644); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	cfg, err := NewFromFile(path)
	if err != nil {
		t.Fatalf("NewFromFile: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				_ = cfg.Get("VERSION")
				_ = cfg.GetStringMapString("nested")
				_ = cfg.Clone()
				_ = cfg.Empty()
			}
		}()
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := fmt.Sprintf("VERSION=v%d_%d\n", id, j)
				if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
					t.Logf("write: %v", err)
				}
				if err := cfg.ReloadFromFile(path); err != nil {
					t.Logf("reload: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}
