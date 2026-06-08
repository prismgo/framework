package horizon

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigMergesCurrentEnvironmentSupervisors(t *testing.T) {
	// 需求背景：runtime retry contract 要求 supervisor 支持 defaults 与当前 environment 合并，并把 queue
	// 统一规范化为 []string。本测试通过公开 LoadConfigFrom 验证完整配置解析行为。
	// buffer config contract 后 trim 已移除，改用 observability.event_metrics_retention 等新 retention 字段。
	t.Setenv("APP_ENV", "ignored-by-app-env")
	cfg := fakeConfig{
		strings: map[string]string{
			"app.env": "local",
		},
		maps: map[string]map[string]any{
			"horizon": {
				"use":                   "memory",
				"connection":            "horizon",
				"prefix":                "demo_horizon",
				"heartbeat_ttl_seconds": 45,
				"observability": map[string]any{
					"event_metrics_retention": "48h",
				},
				"defaults": map[string]any{
					"supervisor-default": map[string]any{
						"connection":    "redis",
						"queue":         "default, emails",
						"balance":       "auto",
						"min_processes": 1,
						"max_processes": 3,
						"tries":         2,
					},
				},
				"environments": map[string]any{
					"local": map[string]any{
						"supervisor-default": map[string]any{
							"queue":         []any{"critical", "emails"},
							"max_processes": 5,
						},
					},
				},
			},
		},
	}

	loaded, err := LoadConfigFrom(cfg)
	if err != nil {
		t.Fatalf("LoadConfigFrom returned error: %v", err)
	}

	if loaded.Environment != "local" {
		t.Fatalf("environment = %q, want local", loaded.Environment)
	}
	if loaded.Store != "memory" || loaded.Prefix != "demo_horizon" {
		t.Fatalf("unexpected store or prefix: %#v", loaded)
	}
	if loaded.Connection != "horizon" || loaded.HeartbeatTTL != 45*time.Second {
		t.Fatalf("runtime store config was not normalized: %#v", loaded)
	}
	if loaded.Observability.EventMetricsRetention != 48*time.Hour {
		t.Fatalf("observability retention was not parsed: %#v", loaded.Observability)
	}
	supervisor := loaded.Supervisors["supervisor-default"]
	if supervisor.Connection != "redis" {
		t.Fatalf("connection = %q, want redis", supervisor.Connection)
	}
	if !reflect.DeepEqual(supervisor.Queues, []string{"critical", "emails"}) {
		t.Fatalf("queues = %#v", supervisor.Queues)
	}
	if supervisor.Balance != BalanceAuto || supervisor.MinProcesses != 1 || supervisor.MaxProcesses != 5 || supervisor.Tries != 2 {
		t.Fatalf("supervisor was not merged and normalized: %#v", supervisor)
	}
}

func TestLoadConfigParsesLaravelCompatibilityBaseline(t *testing.T) {
	// 需求背景：historical scenario 11 要求后续 runtime、观测、HTTP API 和 Dashboard 共用同一份 Horizon 配置合同。
	// buffer config contract 后 silenced/silenced_tags/metrics.trim_snapshots 已移除，改用新 observability 字段。
	cfg := fakeConfig{
		strings: map[string]string{"app.env": "prod-cn"},
		maps: map[string]map[string]any{
			"horizon": {
				"path":             "ops/horizon/",
				"waits":            map[string]any{"redis:default": 60, "rabbitmq:emails": "120"},
				"fast_termination": "true",
				"memory_limit":     "256",
				"watch":            []any{"app", "configs"},
				"observability": map[string]any{
					"event_metrics_sample_rate":   0.5,
					"high_value_detail_retention": "48h",
					"event_metrics_retention":     "24h",
				},
				"defaults": map[string]any{
					"supervisor-default": validSupervisorConfig(),
				},
				"environments": map[string]any{
					"prod-*": map[string]any{
						"supervisor-default": map[string]any{"max_processes": 3},
					},
					"*": map[string]any{
						"supervisor-default": map[string]any{"max_processes": 2},
					},
				},
			},
		},
	}

	loaded, err := LoadConfigFrom(cfg)
	if err != nil {
		t.Fatalf("LoadConfigFrom returned error: %v", err)
	}

	if loaded.Path != "ops/horizon" || loaded.APIPrefix() != "/ops/horizon/api" {
		t.Fatalf("path was not normalized: path=%q api=%q", loaded.Path, loaded.APIPrefix())
	}
	if loaded.Waits["redis:default"] != 60 || loaded.Waits["rabbitmq:emails"] != 120 {
		t.Fatalf("waits were not parsed: %#v", loaded.Waits)
	}
	if !loaded.FastTermination || loaded.MemoryLimit != 256 || !reflect.DeepEqual(loaded.Watch, []string{"app", "configs"}) {
		t.Fatalf("runtime flags were not parsed: %#v", loaded)
	}
	if loaded.Observability.EventMetricsSampleRate != 0.5 || loaded.Observability.HighValueDetailRetention != 48*time.Hour {
		t.Fatalf("observability fields were not parsed: %#v", loaded.Observability)
	}
	if loaded.Supervisors["supervisor-default"].MaxProcesses != 3 {
		t.Fatalf("prod-* environment should win over * fallback: %#v", loaded.Supervisors["supervisor-default"])
	}
}

func TestLoadConfigDefaultsLaravelCompatibilityBaseline(t *testing.T) {
	// 需求背景：缺省配置必须可预测，避免后续 Dashboard/API/runtime 各自发明默认值。
	// buffer config contract 后 metrics.trim_snapshots 已移除，改用 observability 下的 retention 字段。
	cfg := fakeConfig{maps: map[string]map[string]any{
		"horizon": {
			"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments": map[string]any{"production": map[string]any{}},
		},
	}}

	loaded, err := LoadConfigFrom(cfg)
	if err != nil {
		t.Fatalf("LoadConfigFrom returned error: %v", err)
	}

	if loaded.Path != "horizon" || loaded.DashboardPath() != "/horizon" || loaded.APIPrefix() != "/horizon/api" {
		t.Fatalf("unexpected default paths: path=%q dashboard=%q api=%q", loaded.Path, loaded.DashboardPath(), loaded.APIPrefix())
	}
	if loaded.FastTermination || loaded.MemoryLimit != 128 {
		t.Fatalf("unexpected runtime defaults: fast=%v memory=%d", loaded.FastTermination, loaded.MemoryLimit)
	}
	if loaded.Observability.EventMetricsRetention != 24*time.Hour {
		t.Fatalf("unexpected observability retention defaults: %#v", loaded.Observability)
	}
}

func TestLoadConfigFailsWhenNoEnvironmentMatches(t *testing.T) {
	// 需求背景：生产入口不能在环境配置缺失时静默以 0 个 supervisor 启动，必须给出稳定配置错误。
	cfg := fakeConfig{
		strings: map[string]string{"app.env": "qa"},
		maps: map[string]map[string]any{
			"horizon": {
				"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
				"environments": map[string]any{"production": map[string]any{}},
			},
		},
	}

	_, err := LoadConfigFrom(cfg)
	if err == nil {
		t.Fatal("expected missing environment match to fail")
	}
	if !strings.Contains(err.Error(), "no environment configuration matches") || !strings.Contains(err.Error(), "qa") {
		t.Fatalf("unexpected environment error: %v", err)
	}
}

func TestLoadConfigRejectsInvalidSupervisorConfig(t *testing.T) {
	// 需求背景：Horizon 后续会管理长驻 worker，错误配置不能静默回退。本测试覆盖必填字段、
	// balance、非负整数、backoff、bool 和 max/min 关系的 fail-fast 行为。
	cases := map[string]map[string]any{
		"empty connection":     {"connection": "", "queue": "default", "balance": "auto"},
		"empty queue":          {"connection": "redis", "queue": " , ", "balance": "auto"},
		"invalid balance":      {"connection": "redis", "queue": "default", "balance": "round-robin"},
		"negative min":         {"connection": "redis", "queue": "default", "balance": "auto", "min_processes": -1},
		"max below min":        {"connection": "redis", "queue": "default", "balance": "auto", "min_processes": 2, "max_processes": 1},
		"invalid backoff":      {"connection": "redis", "queue": "default", "balance": "auto", "backoff": "1,bad"},
		"invalid bool":         {"connection": "redis", "queue": "default", "balance": "auto", "stop_when_empty": "maybe"},
		"negative retry_after": {"connection": "redis", "queue": "default", "balance": "auto", "retry_after": -1},
	}
	for name, supervisor := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := fakeConfig{maps: map[string]map[string]any{
				"horizon": {
					"defaults": map[string]any{"supervisor-default": supervisor},
					"environments": map[string]any{
						"production": map[string]any{},
					},
				},
			}}
			_, err := LoadConfigFrom(cfg)
			if err == nil {
				t.Fatal("expected invalid supervisor config to return an error")
			}
			if !strings.Contains(err.Error(), "supervisor-default") {
				t.Fatalf("error %q does not include supervisor name", err.Error())
			}
		})
	}
}

func TestLoadConfigParsesLaravelScalingOptions(t *testing.T) {
	// 需求背景：historical scenario 07 要求 Prismgo Horizon 兼容 Laravel Horizon 的 autoscaler 配置命名，
	// 同时保留本项目 snake_case 风格和 Laravel 文档中的 camelCase 风格，避免迁移配置时出现静默丢失。
	cfg := fakeConfig{maps: map[string]map[string]any{
		"horizon": {
			"defaults": map[string]any{
				"snake": map[string]any{
					"connection":            "redis",
					"queue":                 "default",
					"balance":               "auto",
					"min_processes":         1,
					"max_processes":         5,
					"auto_scaling_strategy": "size",
					"balance_max_shift":     2,
					"balance_cooldown":      7,
				},
				"defaults": map[string]any{
					"connection":    "redis",
					"queue":         "low",
					"balance":       "false",
					"min_processes": 1,
					"max_processes": 2,
				},
			},
			"environments": map[string]any{"production": map[string]any{}},
		},
	}}

	loaded, err := LoadConfigFrom(cfg)
	if err != nil {
		t.Fatalf("LoadConfigFrom returned error: %v", err)
	}

	if got := loaded.Supervisors["snake"]; got.AutoScalingStrategy != AutoScalingStrategySize || got.BalanceMaxShift != 2 || got.BalanceCooldown != 7 {
		t.Fatalf("snake scaling options were not parsed: %#v", got)
	}
	if got := loaded.Supervisors["defaults"]; got.AutoScalingStrategy != AutoScalingStrategyTime || got.BalanceMaxShift != 1 || got.BalanceCooldown != 3 {
		t.Fatalf("default scaling options were not applied: %#v", got)
	}
}

func TestLoadConfigRejectsCamelCaseSupervisorFields(t *testing.T) {
	// 需求背景：historical scenario 11 明确 camelCase supervisor 字段是非兼容项，必须 fail fast 并提示对应 snake_case 字段。
	supervisor := validSupervisorConfig()
	supervisor["maxProcesses"] = 5
	cfg := fakeConfig{maps: map[string]map[string]any{
		"horizon": {
			"defaults":     map[string]any{"supervisor-default": supervisor},
			"environments": map[string]any{"production": map[string]any{}},
		},
	}}

	_, err := LoadConfigFrom(cfg)
	if err == nil {
		t.Fatal("expected camelCase supervisor field to fail")
	}
	if !strings.Contains(err.Error(), "maxProcesses") || !strings.Contains(err.Error(), "max_processes") {
		t.Fatalf("error should point to snake_case replacement, got %q", err.Error())
	}
}

func TestLoadConfigRejectsInvalidLaravelScalingOptions(t *testing.T) {
	// 逻辑说明：scaling 配置控制长期运行的 worker 数量，非法 strategy 或数字必须 fail fast，
	// 错误信息需要同时包含 supervisor 名称和具体字段，方便定位是哪一个队列组配置错误。
	cases := map[string]map[string]any{
		"invalid strategy": {"auto_scaling_strategy": "latency"},
		"negative shift":   {"balance_max_shift": -1},
		"bad cooldown":     {"balance_cooldown": "soon"},
	}
	for name, overrides := range cases {
		t.Run(name, func(t *testing.T) {
			supervisor := validSupervisorConfig()
			for key, value := range overrides {
				supervisor[key] = value
			}
			cfg := fakeConfig{maps: map[string]map[string]any{
				"horizon": {
					"defaults":     map[string]any{"supervisor-default": supervisor},
					"environments": map[string]any{"production": map[string]any{}},
				},
			}}
			_, err := LoadConfigFrom(cfg)
			if err == nil {
				t.Fatal("expected invalid scaling config to return an error")
			}
			if !strings.Contains(err.Error(), "supervisor-default") {
				t.Fatalf("error %q does not include supervisor name", err.Error())
			}
			for field := range overrides {
				if !strings.Contains(err.Error(), field) {
					t.Fatalf("error %q does not include field %q", err.Error(), field)
				}
			}
		})
	}
}

func TestLoadConfigResolvesEnvironmentFallbackOrder(t *testing.T) {
	// 需求背景：Horizon 需要兼容独立环境覆盖、应用环境和系统环境变量。本测试固定顺序为
	// horizon.environment -> app.env -> APP_ENV -> production。
	t.Setenv("APP_ENV", "staging")
	base := map[string]any{
		"defaults": map[string]any{
			"supervisor-default": validSupervisorConfig(),
		},
		"environments": map[string]any{
			"production": map[string]any{},
			"local":      map[string]any{},
			"testing":    map[string]any{},
			"staging":    map[string]any{},
		},
	}

	cfg, err := LoadConfigFrom(fakeConfig{
		strings: map[string]string{"app.env": "local"},
		maps: map[string]map[string]any{"horizon": mergeTestMap(base, map[string]any{
			"environment": "testing",
		})},
	})
	if err != nil {
		t.Fatalf("explicit horizon environment failed: %v", err)
	}
	if cfg.Environment != "testing" {
		t.Fatalf("explicit environment = %q, want testing", cfg.Environment)
	}

	cfg, err = LoadConfigFrom(fakeConfig{
		strings: map[string]string{"app.env": "local"},
		maps:    map[string]map[string]any{"horizon": base},
	})
	if err != nil {
		t.Fatalf("app.env environment failed: %v", err)
	}
	if cfg.Environment != "local" {
		t.Fatalf("app.env environment = %q, want local", cfg.Environment)
	}

	cfg, err = LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": base}})
	if err != nil {
		t.Fatalf("APP_ENV fallback failed: %v", err)
	}
	if cfg.Environment != "staging" {
		t.Fatalf("APP_ENV fallback = %q, want staging", cfg.Environment)
	}

	t.Setenv("APP_ENV", "")
	cfg, err = LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": base}})
	if err != nil {
		t.Fatalf("production fallback failed: %v", err)
	}
	if cfg.Environment != "production" {
		t.Fatalf("default fallback = %q, want production", cfg.Environment)
	}
}

func validSupervisorConfig() map[string]any {
	// 测试辅助配置保持最小合法 supervisor，避免环境 fallback 测试被字段校验细节干扰。
	return map[string]any{
		"connection":    "redis",
		"queue":         "default",
		"balance":       "auto",
		"min_processes": 1,
		"max_processes": 1,
	}
}

func mergeTestMap(base, override map[string]any) map[string]any {
	// 测试辅助函数用于构造 horizon.environment 覆盖场景，不复用生产 mergeMaps，避免测试和实现互相锁死。
	out := make(map[string]any, len(base)+len(override))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range override {
		out[key] = value
	}
	return out
}

type fakeConfig struct {
	// strings 保存 GetString 可读取的点路径配置。
	strings map[string]string
	// maps 保存 GetStringMap 可读取的顶层命名空间配置。
	maps map[string]map[string]any
}

func (f fakeConfig) GetString(path string, defaultValue ...any) string {
	// 参数说明：path 是 Laravel 风格点路径；defaultValue 兼容 ConfigReader 接口的默认值参数。
	if value, ok := f.strings[path]; ok {
		return value
	}
	if len(defaultValue) == 0 {
		return ""
	}
	value, _ := defaultValue[0].(string)
	return value
}

func (f fakeConfig) GetStringMap(path string) map[string]any {
	// 返回副本，避免被测代码修改 fakeConfig 内部状态后影响后续断言。
	value := f.maps[path]
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}
