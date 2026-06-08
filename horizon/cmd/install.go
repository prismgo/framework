package cmd

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/prismgo/framework/console"
)

// NewInstallCommand 创建 horizon:install 命令。
//
// 需求背景：对齐 Laravel Horizon 的安装体验，在业务侧缺少 Horizon 配置或 provider
// stub 时生成最小接入文件；Prismgo 的 Dashboard 资源已经内嵌，因此不会生成 publish 命令
// 或提示用户发布 public/vendor/horizon 静态资源。
func NewInstallCommand() console.Command {
	return installCommand{}
}

type installCommand struct{}

func (installCommand) Definition() *console.Definition {
	return console.MustDefinition("horizon:install", "Install Horizon config and provider stubs")
}

// Run 执行安装流程。
//
// 逻辑说明：只在文件不存在时创建 config/horizon.go 与 app/providers/horizon_service_provider.go；
// 已存在文件一律不覆盖，避免破坏业务侧配置。命令只输出需要手动注册 provider 和配置
// horizon.view 权限中间件的提示，不自动修改 bootstrap/app.go。
func (installCommand) Handle(ctx console.CommandContext) error {
	configCreated, err := writeFileIfMissing("config/horizon.go", defaultConfigStub())
	if err != nil {
		return err
	}
	providerCreated, err := writeFileIfMissing("app/providers/horizon_service_provider.go", defaultProviderStub())
	if err != nil {
		return err
	}
	if configCreated {
		ctx.IO().Success("Created config/horizon.go")
	} else {
		ctx.IO().Info("config/horizon.go already exists")
	}
	if providerCreated {
		ctx.IO().Success("Created app/providers/horizon_service_provider.go")
	} else {
		ctx.IO().Info("app/providers/horizon_service_provider.go already exists")
	}
	ctx.IO().Info("Register app/providers.HorizonServiceProvider{} in bootstrap providers if it is not already registered.")
	ctx.IO().Info("Wrap Horizon routes with project auth and horizon.HorizonView permission middleware.")
	ctx.IO().Info("Embedded Horizon dashboard is available at the configured horizon.path; no publish step is required.")
	return nil
}

func writeFileIfMissing(path string, body string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	return true, os.WriteFile(path, []byte(body), 0o644)
}

// defaultConfigStub 返回业务侧 Horizon 配置模板。
//
// 设计思路：模板只包含 path、store、connection、prefix 和 watch 这类接入必需项；
// 具体 supervisor、权限种子和生产参数由业务项目自行补充。
func defaultConfigStub() string {
	return `package config

func init() {
	Add("horizon", func() map[string]interface{} {
		return map[string]interface{}{
			// Dashboard path and internal API prefix base.
			"path": Env("HORIZON_PATH", "horizon"),

			// Runtime environment used to choose the supervisor overrides below.
			"environment": Env("APP_ENV", "production"),

			// Store driver used by Horizon runtime state.
			"use": Env("HORIZON_STORE", "redis"),

			// Redis connection name used when the store driver is redis.
			"connection": Env("HORIZON_CONNECTION", "default"),

			// Dedicated key prefix for Horizon state and metrics.
			"prefix": Env("HORIZON_PREFIX", "prismgo_horizon"),

			// Seconds before master, supervisor, and worker heartbeats are considered stale.
			"heartbeat_ttl_seconds": 60,

			// Long-wait thresholds in seconds, keyed by connection:queue.
			"waits": map[string]interface{}{
				"redis:default": 60,
			},

			// Observability controls event sampling, bounded memory, async flush, and per-channel retention.
			// Each field below is designed for billion-scale workloads where synchronous per-job Store writes
			// are not acceptable on the worker hot path.
			"observability": map[string]interface{}{
				// Preset applies a curated set of defaults: "full", "production_light", or "minimal".
				// Every explicit sub-field below overrides the preset default for that field.
				"preset": Env("HORIZON_OBSERVABILITY_PRESET", "full"),

				// Enable event-derived queue-level counters and runtime aggregation (processed/failed/released/poison).
				"event_metrics": Env("HORIZON_OBSERVABILITY_EVENT_METRICS", true),

				// Enable queued-wait observation and long-wait detection; depends on event_metrics.
				"waits": Env("HORIZON_OBSERVABILITY_WAITS", true),

				// Enable read-only projections for batch events dispatched by the queue system.
				"batch_summaries": Env("HORIZON_OBSERVABILITY_BATCH_SUMMARIES", true),

				// Master, supervisor, and worker heartbeat plus control-state tracking.
				"process_health": Env("HORIZON_OBSERVABILITY_PROCESS_HEALTH", true),

				// Periodic backend queue-length sampling and persistence.
				"queue_lengths": Env("HORIZON_OBSERVABILITY_QUEUE_LENGTHS", true),

				// Event-time aggregation bucket width for event_metrics windows (e.g. "1m", "30s").
				// Must be a positive duration. Defaults to flush_interval when not set.
				"metrics_window": Env("HORIZON_OBSERVABILITY_METRICS_WINDOW", "1m"),

				// Interval at which the async flusher wakes to write a batch to the Store.
				"flush_interval": Env("HORIZON_OBSERVABILITY_FLUSH_INTERVAL", "1m"),

				// Maximum time to wait for a single flush batch write; exceeded writes are discarded.
				"flush_timeout": Env("HORIZON_OBSERVABILITY_FLUSH_TIMEOUT", "5s"),

				// Max number of event_metrics increments and detail rows in a single flush batch.
				"batch_size": Env("HORIZON_OBSERVABILITY_BATCH_SIZE", 500),

				// Capacity of the bounded in-memory buffer between collector and async flusher, in items.
				"buffer_size": Env("HORIZON_OBSERVABILITY_BUFFER_SIZE", 10000),

				// Optional per-second rate limit at the collector entry point; 0 means no extra cap.
				"max_events_per_second": Env("HORIZON_OBSERVABILITY_MAX_EVENTS_PER_SECOND", 0),

				// Drop strategy when the bounded buffer is full: "drop_oldest" or "drop_newest".
				"drop_policy": Env("HORIZON_OBSERVABILITY_DROP_POLICY", "drop_oldest"),

				// Fraction of queue events that enter the event_metrics pipeline (0..1).
				// 0 is a valid explicit value and disables event_metrics collection.
				"event_metrics_sample_rate": Env("HORIZON_OBSERVABILITY_EVENT_METRICS_SAMPLE_RATE", 1),

				// Optional independent sampling rate for high-value failed/poison/slow-job detail (0..1).
				// When empty or unset, falls back to the effective event_metrics_sample_rate at runtime.
				"high_value_detail_sample_rate": Env("HORIZON_OBSERVABILITY_HIGH_VALUE_DETAIL_SAMPLE_RATE", ""),

				// Maximum number of runtime samples kept in the reservoir for P95/P99 estimation.
				"sample_reservoir_size": Env("HORIZON_OBSERVABILITY_SAMPLE_RESERVOIR_SIZE", 2048),

				// Hard limit on distinct aggregate keys (connection+queue / job type).
				// New keys beyond this limit enter the _overflow bucket and record a diagnostic.
				"max_aggregate_keys": Env("HORIZON_OBSERVABILITY_MAX_AGGREGATE_KEYS", 10000),

				// Time-to-live for low-activity aggregate keys before they are evicted.
				"aggregate_key_ttl": Env("HORIZON_OBSERVABILITY_AGGREGATE_KEY_TTL", "30m"),

				// How long event_metrics window-aggregation data is retained in the Store.
				"event_metrics_retention": Env("HORIZON_OBSERVABILITY_EVENT_METRICS_RETENTION", "24h"),

				// How long high-value failed/poison/slow-job diagnostic detail is retained.
				"high_value_detail_retention": Env("HORIZON_OBSERVABILITY_HIGH_VALUE_DETAIL_RETENTION", "24h"),

				// How long batch-summary read-model projections are retained.
				"batch_summary_retention": Env("HORIZON_OBSERVABILITY_BATCH_SUMMARY_RETENTION", "24h"),

				// How long drop/degradation diagnostic records are retained.
				"diagnostics_retention": Env("HORIZON_OBSERVABILITY_DIAGNOSTICS_RETENTION", "24h"),

				// Collect safe diagnostic summaries for failed jobs into the high-value detail channel.
				"failed_detail_enabled": Env("HORIZON_OBSERVABILITY_FAILED_DETAIL_ENABLED", true),

				// Collect safe diagnostic summaries for poison envelopes into the high-value detail channel.
				"poison_detail_enabled": Env("HORIZON_OBSERVABILITY_POISON_DETAIL_ENABLED", true),

				// Collect safe diagnostic summaries for jobs exceeding slow_job_threshold.
				"slow_job_detail_enabled": Env("HORIZON_OBSERVABILITY_SLOW_JOB_DETAIL_ENABLED", true),

				// Minimum job runtime before a job is considered "slow" for diagnostics.
				"slow_job_threshold": Env("HORIZON_OBSERVABILITY_SLOW_JOB_THRESHOLD", "30s"),

				// Max per-job queued-wait state entries; 0 disables queued-wait tracking entirely.
				"queued_waits_max": Env("HORIZON_OBSERVABILITY_QUEUED_WAITS_MAX", 10000),

				// Max in-flight processing spans used for runtime aggregation; 0 disables span storage.
				"processing_spans_max": Env("HORIZON_OBSERVABILITY_PROCESSING_SPANS_MAX", 10000),

				// Seconds between periodic TTL-cleanup sweeps of stale processing spans.
				"processing_cleanup_interval_seconds": Env("HORIZON_OBSERVABILITY_PROCESSING_CLEANUP_INTERVAL_SECONDS", 60),

				// Allow the sampling policy to lower effective sample rates under memory or throughput pressure.
				"dynamic_sampling_enabled": Env("HORIZON_OBSERVABILITY_DYNAMIC_SAMPLING_ENABLED", true),

				// Floor for dynamic sampling; explicit event_metrics_sample_rate=0 still keeps the rate at 0.
				"min_sample_rate": Env("HORIZON_OBSERVABILITY_MIN_SAMPLE_RATE", 0.01),
			},

			// Allows replacement masters to start before old workers have fully drained.
			"fast_termination": false,

			// Default worker memory limit in MB when a supervisor does not override memory.
			"memory_limit": 128,

			// Paths watched by horizon:listen during local development.
			"watch": []interface{}{"app", "configs", "routes"},

			// Default supervisor options shared by all environments.
			"defaults": map[string]interface{}{
				"supervisor-1": map[string]interface{}{
					"connection":            Env("HORIZON_QUEUE_CONNECTION", "redis"),
					"queue":                 []interface{}{"default"},
					"balance":               "auto",
					"min_processes":         1,
					"max_processes":         3,
					"tries":                 1,
					"timeout":               60,
					"sleep":                 3,
					"max_jobs":              0,
					"max_time":              0,
					"retry_after":           90,
					"backoff":               []interface{}{0},
					"stop_when_empty":       false,
					"memory":                128,
					"nice":                  0,
					"auto_scaling_strategy": "time",
					"balance_max_shift":     1,
					"balance_cooldown":      3,
				},
			},

			// Environment specific supervisor definitions.
			"environments": map[string]interface{}{
				"production": map[string]interface{}{
					"supervisor-1": map[string]interface{}{
						"max_processes": 10,
						"balance":       "auto",
					},
				},
				"local": map[string]interface{}{
					"supervisor-1": map[string]interface{}{
						"max_processes": 3,
					},
				},
				"*": map[string]interface{}{},
			},
		}
	})
}
`
}

// defaultProviderStub 返回业务侧 Horizon provider 模板。
//
// 设计思路：模板参考 Laravel HorizonServiceProvider 的 boot 接入点，但适配 Prismgo：
// 只保留路由注册和 auth middleware 占位，不包含 publish 逻辑，也不直接依赖业务 app 包。
func defaultProviderStub() string {
	return `package providers

import (
	"github.com/gin-gonic/gin"
	providercontract "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/horizon"
)

type HorizonServiceProvider struct{}

func (HorizonServiceProvider) Name() string { return "app.horizon" }

func (HorizonServiceProvider) Register(app providercontract.Application) error { return nil }

func (HorizonServiceProvider) Boot(app providercontract.Application) error {
	horizon.RegisterHTTPRoutes(horizon.HTTPOptions{
		Auth: horizonAuthMiddleware(),
	})
	return nil
}

func horizonAuthMiddleware() []gin.HandlerFunc {
	// Add project auth and permission middleware here, for example:
	// middleware.AuthRequired(), middleware.InjectUserPermissions(...),
	// middleware.RequirePermission(horizon.HorizonView)
	return nil
}
`
}
