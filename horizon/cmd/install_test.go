package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigStubIncludesCompleteDocumentedHorizonConfig(t *testing.T) {
	// 测试目的：horizon:install 生成的是业务侧接入模板，必须覆盖当前可解析的 Horizon 配置面，
	// 并在生成文件中使用英文注释解释参数用途，便于业务项目直接审阅和调整。
	stub := defaultConfigStub()
	for _, want := range []string{
		"// Dashboard path and internal API prefix base.",
		"// Store driver used by Horizon runtime state.",
		"// Default supervisor options shared by all environments.",
		"// Environment specific supervisor definitions.",
		`"path"`,
		`"environment"`,
		`"use"`,
		`"connection"`,
		`"prefix"`,
		`"heartbeat_ttl_seconds"`,
		`"waits"`,
		`"observability"`,
		`"metrics_window"`,
		`"event_metrics_sample_rate"`,
		`"high_value_detail_sample_rate"`,
		`"buffer_size"`,
		`"flush_interval"`,
		`"drop_policy"`,
		`"event_metrics_retention"`,
		`"high_value_detail_retention"`,
		`"sample_reservoir_size"`,
		`"max_aggregate_keys"`,
		`"aggregate_key_ttl"`,
		`"failed_detail_enabled"`,
		`"poison_detail_enabled"`,
		`"slow_job_detail_enabled"`,
		`"fast_termination"`,
		`"memory_limit"`,
		`"watch"`,
		`"defaults"`,
		`"environments"`,
		`"connection"`,
		`"queue"`,
		`"balance"`,
		`"min_processes"`,
		`"max_processes"`,
		`"tries"`,
		`"timeout"`,
		`"sleep"`,
		`"max_jobs"`,
		`"max_time"`,
		`"retry_after"`,
		`"backoff"`,
		`"stop_when_empty"`,
		`"memory"`,
		`"nice"`,
		`"auto_scaling_strategy"`,
		`"balance_max_shift"`,
		`"balance_cooldown"`,
	} {
		if !strings.Contains(stub, want) {
			t.Fatalf("default config stub missing %q:\n%s", want, stub)
		}
	}
	if strings.Contains(strings.ToLower(stub), "publish") {
		t.Fatalf("default config stub must not mention publish flow:\n%s", stub)
	}
	if strings.Contains(stub, `"block_for"`) {
		t.Fatalf("default config stub must not include supervisor block_for:\n%s", stub)
	}
	for _, removed := range []string{`"recent_jobs"`, `"job_history"`, `"queue_history"`, `"silenced"`, `"silenced_tags"`, `"success_detail_enabled"`, `"success_sample_rate"`, `"trim_snapshots"`} {
		if strings.Contains(stub, removed) {
			t.Fatalf("default config stub should not contain removed field %s:\n%s", removed, stub)
		}
	}
}

func TestInstallCommandCreatesConfigAndProviderWithoutOverwritingUserFiles(t *testing.T) {
	// 测试目的：horizon:install 只能补齐缺失的业务侧配置/provider 文件，不能覆盖用户已有配置，
	// 也不能把 Dashboard publish 流程写入生成文件或命令提示。
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get wd: %v", err)
	}
	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir temp: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(originalDir) })

	command := NewInstallCommand()
	if err := command.Handle(surfaceCommandContext(command, surfaceInput{})); err != nil {
		t.Fatalf("install command: %v", err)
	}
	configPath := filepath.Join(tempDir, "config", "horizon.go")
	providerPath := filepath.Join(tempDir, "app", "providers", "horizon_service_provider.go")
	configBody, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read generated config: %v", err)
	}
	providerBody, err := os.ReadFile(providerPath)
	if err != nil {
		t.Fatalf("read generated provider: %v", err)
	}
	if !strings.Contains(string(configBody), "// Dashboard path and internal API prefix base.") {
		t.Fatalf("generated config is missing English comments:\n%s", string(configBody))
	}
	if !strings.Contains(string(providerBody), "horizon.RegisterHTTPRoutes") || strings.Contains(strings.ToLower(string(providerBody)), "publish") {
		t.Fatalf("generated provider should register routes without publish flow:\n%s", string(providerBody))
	}

	if err := os.WriteFile(configPath, []byte("package config\n// custom\n"), 0o644); err != nil {
		t.Fatalf("seed custom config: %v", err)
	}
	if err := command.Handle(surfaceCommandContext(command, surfaceInput{})); err != nil {
		t.Fatalf("second install command: %v", err)
	}
	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read custom config: %v", err)
	}
	if string(after) != "package config\n// custom\n" {
		t.Fatalf("install command overwrote user config:\n%s", string(after))
	}
}

func TestListenCommandRunsLocalDevelopmentHelperAndRejectsMissingRuntime(t *testing.T) {
	// 测试目的：horizon:listen 当前是本地开发辅助命令，至少要稳定解析命令面并要求 Horizon
	// runtime 已接入，避免用户把未配置的 watcher 当成生产进程管理入口。
	runtime := &surfaceRuntime{}
	command := NewListenCommand(func(context.Context) (Runtime, error) { return runtime, nil })
	if err := command.Handle(surfaceCommandContext(command, surfaceInput{options: map[string]string{"environment": "testing"}, ints: map[string]int{"poll": 25}})); err != nil {
		t.Fatalf("listen command: %v", err)
	}
	if runtime.listen.Environment != "testing" || runtime.listen.Poll != 25*time.Millisecond {
		t.Fatalf("listen options = %#v", runtime.listen)
	}

	missingRuntime := NewListenCommand(func(context.Context) (Runtime, error) { return nil, nil })
	err := missingRuntime.Handle(surfaceCommandContext(missingRuntime, surfaceInput{}))
	if err == nil || !strings.Contains(err.Error(), ErrRuntimeNotConfigured.Error()) {
		t.Fatalf("expected missing runtime error, got %v", err)
	}
}
