package config

import "testing"

func TestFacadeCurrentAndDefaultShareSameConfig(t *testing.T) {
	cfg := New()
	bindConfigForTest(t, cfg)

	if Resolve() != cfg {
		t.Fatal("expected Resolve to return registered config")
	}
}

func TestContainerBindingReplacesResolvedConfig(t *testing.T) {
	cfg := &Config{store: map[string]any{"app": map[string]any{"name": "custom"}}}
	bindConfigForTest(t, cfg)

	if got := Resolve(); got != cfg {
		t.Fatal("expected Resolve to return the config passed to container")
	}
	if got := GetString("app.name"); got != "custom" {
		t.Fatalf("expected facade getter to use the injected config, got %q", got)
	}
	if Empty() {
		t.Fatal("expected facade Empty to use injected config")
	}
	cloned := Clone()
	cloned.store["app"].(map[string]any)["name"] = "cloned"
	if got := GetString("app.name"); got != "custom" {
		t.Fatalf("expected facade Clone to isolate current config, got %q", got)
	}
}

func TestUseNilFallsBackToFreshConfig(t *testing.T) {
	cfg := &Config{store: map[string]any{"app": map[string]any{"name": "custom"}}}
	registry := bindConfigForTest(t, cfg)
	if err := registry.Forget(serviceKey); err != nil {
		t.Fatalf("forget config service: %v", err)
	}
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected Resolve to panic after clearing container binding")
		}
	}()
	_ = Resolve()
}

func TestCloneCreatesIndependentStore(t *testing.T) {
	original := &Config{store: map[string]any{
		"app": map[string]any{
			"name": "origin",
		},
	}}
	cloned := original.Clone()
	cloned.store["app"].(map[string]any)["name"] = "changed"

	if got := original.GetString("app.name"); got != "origin" {
		t.Fatalf("expected clone to keep original store isolated, got %q", got)
	}
}

func TestConfigDelegatesRuntimeStoreGetters(t *testing.T) {
	cfg := &Config{store: map[string]any{
		"app": map[string]any{
			"name":    "facade-test",
			"port":    8051,
			"ratio":   1.5,
			"timeout": int64(60),
			"debug":   true,
			"flags": map[string]any{
				"env":  "test",
				"zone": "cn",
			},
		},
	}}

	if got := cfg.Get("app.name"); got != "facade-test" {
		t.Fatalf("expected Get to read app.name, got %q", got)
	}
	if got := cfg.GetString("app.name"); got != "facade-test" {
		t.Fatalf("expected GetString to read app.name, got %q", got)
	}
	if got := cfg.GetInt("app.port"); got != 8051 {
		t.Fatalf("expected GetInt to read app.port, got %d", got)
	}
	if got := cfg.GetFloat64("app.ratio"); got != 1.5 {
		t.Fatalf("expected GetFloat64 to read app.ratio, got %v", got)
	}
	if got := cfg.GetInt64("app.timeout"); got != 60 {
		t.Fatalf("expected GetInt64 to read app.timeout, got %d", got)
	}
	if got := cfg.GetUint("app.port"); got != 8051 {
		t.Fatalf("expected GetUint to read app.port, got %d", got)
	}
	if got := cfg.GetBool("app.debug"); !got {
		t.Fatal("expected GetBool to read app.debug")
	}

	flags := cfg.GetStringMapString("app.flags")
	if flags["env"] != "test" || flags["zone"] != "cn" {
		t.Fatalf("expected GetStringMapString to read app.flags, got %#v", flags)
	}

	app := cfg.GetStringMap("app")
	if app["name"] != "facade-test" {
		t.Fatalf("expected GetStringMap to read app node, got %#v", app)
	}
}
