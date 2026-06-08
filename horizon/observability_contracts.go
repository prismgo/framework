package horizon

import (
	"context"
	"time"
)

const (
	// EventMetricQualityExact 表示 window 由全量事件派生，无已知采样或丢弃降级。
	EventMetricQualityExact = "exact"
	// EventMetricQualityEstimated 表示 window 由采样事件估算派生。
	EventMetricQualityEstimated = "estimated"
	// EventMetricQualityDegraded 表示 window 受到丢弃、Store 失败或其他降级影响。
	EventMetricQualityDegraded = "degraded"
	// EventMetricQualityUnknown 表示窗口存在不可量化丢失，相关计数不能作为估算或精确值展示。
	EventMetricQualityUnknown = "unknown"
	// EventMetricQualityPartial 表示 shutdown best-effort flush 写入了未完成 window。
	EventMetricQualityPartial = "partial"

	// ObservabilityGapQuantifiable 表示丢失或折叠缺口可量化，读侧可展示 estimated/degraded。
	ObservabilityGapQuantifiable = "quantifiable"
	// ObservabilityGapUnknown 表示缺口不可量化，读侧必须把相关窗口视为 unknown。
	ObservabilityGapUnknown = "unknown"

	// HighValueDetailFailed 表示 failed job 的可丢弃诊断摘要。
	HighValueDetailFailed = "failed"
	// HighValueDetailPoison 表示 poison envelope 的可丢弃诊断摘要。
	HighValueDetailPoison = "poison"
	// HighValueDetailSlowJob 表示超过 slow_job_threshold 的慢任务诊断摘要。
	HighValueDetailSlowJob = "slow_job"

	// MemoryDropBufferFull 表示有界 buffer 已满导致观测数据被丢弃。
	MemoryDropBufferFull = "buffer_full"
	// MemoryDropRateLimited 表示 max_events_per_second 限流导致观测数据被丢弃。
	MemoryDropRateLimited = "rate_limited"
	// MemoryDropAggregateOverflow 表示聚合 key 达到上限后进入 overflow 或被降级。
	MemoryDropAggregateOverflow = "aggregate_key_overflow"
	// MemoryDropStoreUnavailable 表示后续 flush 写入 Store 失败或超时。
	MemoryDropStoreUnavailable = "store_unavailable"
	// MemoryDropFlushLagExceeded 表示周期 flush 距离上次成功或取样时间超过安全窗口。
	MemoryDropFlushLagExceeded = "flush_lag_exceeded"
	// MemoryDropFlushTimeoutNear 表示 flush 耗时接近或超过 flush_timeout。
	MemoryDropFlushTimeoutNear = "flush_timeout_near"
	// MemoryDropBatchSummaryLimit 表示 batch summary 达到内存或写入上限后被丢弃。
	MemoryDropBatchSummaryLimit = "batch_summary_limit"
	// MemoryDropCollectorPanic 表示 collector 主循环发生 panic 并已自动重启。
	MemoryDropCollectorPanic = "collector_panic"
	// MemoryDropFlusherPanic 表示 flusher 主循环发生 panic 并已自动重启。
	MemoryDropFlusherPanic = "flusher_panic"
	// EventMetricsSourceSupervisorUnknown 表示 queue event 缺少 Horizon worker runtime supervisor 来源。
	//
	// 用途：让读侧和运维人员区分“该来源没有 supervisor 维度”和“该来源没有流量”。
	// 使用方式：collector 在 SourceSupervisor 为空但事件仍进入 event_metrics window 时写入该诊断。
	// 设计原因：supervisor 只能来自当前 worker runtime 身份，不能从 queue/config/host 等字段推断。
	// 设计思路：保留空 SourceSupervisor 分片并附带稳定 reason，避免丢弃事件或伪造来源。
	// 需求背景：issue 43 要求缺少 supervisor runtime 上下文时继续采集并暴露可诊断原因。
	EventMetricsSourceSupervisorUnknown = "event_metrics_source_supervisor_unknown"
)

// CollectorInput 是后续非阻塞 collector 从 queue event 热路径接收的最小事件合同。
//
// 设计边界：该结构只携带展示安全元数据、采样决策和发生时间；不得要求 worker 同步等待
// Horizon Store，也不得携带 queue payload、raw envelope 或 broker 凭据。
type CollectorInput struct {
	// Event 是 queue event 名称，例如 queue.job_processed。
	Event string
	// Connection 是 queue connection 名称。
	Connection string
	// Queue 是 queue 名称。
	Queue string
	// JobID 是 queue job id，可为空。
	JobID string
	// JobName 是 queue job name/type，可为空。
	JobName string
	// Attempts 是事件可安全暴露的尝试次数。
	Attempts int
	// Runtime 是 processed/failed/slow job 可安全暴露的运行耗时。
	Runtime time.Duration
	// ErrorSummary 是截断后的错误摘要，不包含完整堆栈或 payload。
	ErrorSummary string
	// PoisonDriver 是 poison envelope 的安全 driver 摘要，不包含原始消息体。
	PoisonDriver string
	// PoisonAction 是 driver 对 poison envelope 采取的终结动作。
	PoisonAction string
	// PoisonBodySize 是 poison envelope 原始 body 字节数，仅用于容量排障。
	PoisonBodySize int
	// PoisonBodyTruncated 表示 driver 暴露的 raw body 前缀已被截断；raw body 本身不进入 Horizon。
	PoisonBodyTruncated bool
	// Tags 是入队时显式提供的展示安全标签。
	Tags []string
	// OccurredAt 是 queue event 的发生时间。
	OccurredAt time.Time
	// SourcePrefix 是 Horizon namespace/store prefix，作为多实例分片来源维度。
	SourcePrefix string
	// SourceHost 是采集事件的主机名，避免同名 supervisor 跨主机写入时互相覆盖。
	SourceHost string
	// SourceEnvironment 是 Horizon environment，避免不同环境的同名 supervisor 被合并为重复 runtime。
	SourceEnvironment string
	// SourceSupervisor 是采集事件的 supervisor 名称，用于 Dashboard/API 下钻到单个 runtime 来源。
	SourceSupervisor string
	// Sampling 保存 event_metrics 与 high-value detail 的入口采样结果。
	Sampling SamplingDecision
	// BatchSummary 保存 BatchEvent 派生的安全批次摘要；为空 ID 表示非批次事件。
	BatchSummary BatchSummary
}

// SamplingDecision 描述单个 queue event 是否进入 Horizon 观测管线。
//
// 语义说明：event_metrics_sample_rate 小于 1、动态采样降级或发生丢弃时，事件派生读模型应
// 标记为 Estimated Horizon metric；high-value detail 缺省回落到当前实际 event_metrics 采样率。
type SamplingDecision struct {
	// EventMetricsSampled 表示该事件是否进入 event_metrics 聚合/window 管线。
	EventMetricsSampled bool
	// EventMetricsSampleRate 是当前实际 event_metrics 采样率，已经包含动态采样调整。
	EventMetricsSampleRate float64
	// HighValueDetailSampled 表示该事件是否进入高价值诊断明细通道。
	HighValueDetailSampled bool
	// HighValueDetailRate 是当前实际 high-value detail 采样率。
	HighValueDetailRate float64
	// Estimated 表示由该采样结果派生的 counters/runtime/history 不是 exact。
	Estimated bool
}

// EventMetricIncrement 是 flush batch 中按 window 追加写入 Store 的 event_metrics 增量。
//
// 设计边界：history 和 queue/job read model 后续都应来自这些 window 增量，而不是 retained job detail。
type EventMetricIncrement struct {
	// WindowStart 是事件发生时间归属的 event_metrics window 开始时间。
	WindowStart time.Time
	// WindowEnd 是事件发生时间归属的 event_metrics window 结束时间。
	WindowEnd time.Time
	// FlushAt 是 flusher 将该增量写入 Store 的诊断时间，不参与 window 归属。
	FlushAt time.Time
	// MetricsWindowMS 是该事件桶的配置窗口宽度，单位毫秒；读侧用它检测跨实例配置漂移。
	MetricsWindowMS int64
	// SourcePrefix 是 Horizon Store prefix，用于跨实例来源区分。
	SourcePrefix string
	// SourceHost 是采集事件的主机名；不可得时为空。
	SourceHost string
	// SourceEnvironment 是 Horizon environment；不可得时为空。
	SourceEnvironment string
	// SourceSupervisor 是采集事件的 supervisor 名称；不可得时为空。
	SourceSupervisor string
	// Connection 是 queue connection 名称。
	Connection string
	// Queue 是 queue 名称。
	Queue string
	// JobName 是 job type 维度，可为空。
	JobName string
	// Processed 是采样后进入 event_metrics 的 processed 计数。
	Processed int64
	// Failed 是采样后进入 event_metrics 的 failed 计数。
	Failed int64
	// Released 是采样后进入 event_metrics 的 released 计数。
	Released int64
	// Poison 是采样后进入 event_metrics 的 poison envelope 计数。
	Poison int64
	// Queued 是采样后进入 event_metrics 的 queued 计数。
	Queued int64
	// RuntimeMS 是采样后进入 runtime 聚合的总耗时毫秒数。
	RuntimeMS int64
	// Samples 是采样命中的事件数量，包含 queued/processed/failed/released/poison 等所有进入 event_metrics 的事件。
	Samples int64
	// RuntimeSampleCount 是参与 runtime 平均值计算的样本数，只统计 processed/failed 且 runtimeMS>0 的事件。
	RuntimeSampleCount int64
	// EffectiveSampleRate 是当前 window 实际采样率。
	EffectiveSampleRate float64
	// EstimatedTotal 是按采样率估算的原始事件总量。
	EstimatedTotal int64
	// Estimated 表示该增量派生读模型必须标记为估算值。
	Estimated bool
	// Degraded 表示该 window 存在丢弃、overflow、Store 写入失败或其他不可精确量化缺口。
	Degraded bool
	// Unknown 表示该增量存在不可量化丢失，读模型不能把计数作为 exact 或 estimated 展示。
	Unknown bool
	// Partial 表示该 window 在 shutdown best-effort flush 时提前写入。
	Partial bool
	// Quality 是 exact、estimated、degraded、unknown 或 partial。
	Quality string
}

// EventMetricWindow 是 Store 中按事件时间追加保存的 event_metrics window。
//
// 设计边界：该模型是 event_metrics 的事实写入面；snapshot/counters/history 只是 read model。
// 它保留 window 边界、来源维度、有效采样率和质量标记，不保存 job payload 或 per-job 明细。
type EventMetricWindow struct {
	// WindowStart 是事件按 occurred_at 归属的窗口开始时间。
	WindowStart time.Time `json:"window_start"`
	// WindowEnd 是事件按 occurred_at 归属的窗口结束时间。
	WindowEnd time.Time `json:"window_end"`
	// FlushAt 是写入 Store 的诊断时间，不改变事件窗口归属。
	FlushAt time.Time `json:"flush_at"`
	// MetricsWindowMS 是写入实例使用的 metrics_window 配置，单位毫秒。
	MetricsWindowMS int64 `json:"metrics_window_ms,omitempty"`
	// SourcePrefix 是 Horizon Store prefix，用于区分同一 Redis 中的不同应用实例。
	SourcePrefix string `json:"source_prefix,omitempty"`
	// SourceHost 是采集实例主机名；不可得时为空。
	SourceHost string `json:"source_host,omitempty"`
	// SourceEnvironment 是 Horizon environment；不可得时为空。
	SourceEnvironment string `json:"source_environment,omitempty"`
	// SourceSupervisor 是采集事件的 supervisor 名称；不可得时为空。
	SourceSupervisor string `json:"source_supervisor,omitempty"`
	// Connection 是 queue connection 来源维度。
	Connection string `json:"connection"`
	// Queue 是 queue 来源维度。
	Queue string `json:"queue"`
	// JobName 是可折叠的 job type 维度；overflow 桶会使用 _overflow。
	JobName string `json:"job_name,omitempty"`
	// Processed 是窗口内采样命中的 processed 计数。
	Processed int64 `json:"processed"`
	// Failed 是窗口内采样命中的 failed 计数。
	Failed int64 `json:"failed"`
	// Released 是窗口内采样命中的 released 计数。
	Released int64 `json:"released"`
	// Poison 是窗口内采样命中的 poison envelope 计数。
	Poison int64 `json:"poison"`
	// Queued 是窗口内采样命中的 queued 计数。
	Queued int64 `json:"queued"`
	// RuntimeMS 是窗口内采样命中的 runtime 毫秒总量。
	RuntimeMS int64 `json:"runtime_ms"`
	// SampleCount 是窗口内实际进入 event_metrics 的事件数。
	SampleCount int64 `json:"sample_count"`
	// RuntimeSampleCount 是窗口内参与 runtime 平均值计算的样本数，只统计 processed/failed 且 runtimeMS>0 的事件。
	RuntimeSampleCount int64 `json:"runtime_sample_count"`
	// EffectiveSampleRate 是生成该窗口时实际使用的采样率。
	EffectiveSampleRate float64 `json:"effective_sample_rate"`
	// EstimatedTotal 是基于 SampleCount 和 EffectiveSampleRate 估算的原始事件量。
	EstimatedTotal int64 `json:"estimated_total"`
	// Estimated 表示该窗口由采样估算派生，不应作为 exact 事实展示。
	Estimated bool `json:"estimated"`
	// Degraded 表示该窗口存在丢弃、overflow 或 Store 写入降级。
	Degraded bool `json:"degraded"`
	// Unknown 表示该窗口存在不可量化缺口，读侧不得把计数当作精确或估算值展示。
	Unknown bool `json:"unknown"`
	// Partial 表示 shutdown best-effort flush 写入了尚未完整结束的窗口。
	Partial bool `json:"partial"`
	// Quality 是 exact、estimated、degraded、unknown 或 partial，用于 read model 直接展示质量。
	Quality string `json:"quality"`
}

// HighValueJobDetail 是 failed、poison 和 slow job 的可丢弃安全诊断摘要。
//
// 安全边界：该结构不表示可靠事实源；queue.FailedStore 仍然负责可靠失败记录。
// 该结构不保存 Successful Horizon job detail，也不保存 payload、BodyBase64 或 raw envelope。
type HighValueJobDetail struct {
	// ID 是诊断摘要 ID。
	ID string
	// Kind 是 failed、poison 或 slow_job。
	Kind string
	// Connection 是 queue connection 名称。
	Connection string
	// Queue 是 queue 名称。
	Queue string
	// JobID 是 queue job id。
	JobID string
	// JobName 是 queue job name/type。
	JobName string
	// RuntimeMS 是慢任务或失败任务可安全暴露的耗时毫秒数。
	RuntimeMS int64
	// ErrorSummary 是截断后的错误摘要。
	ErrorSummary string
	// PoisonDriver 是 poison envelope 的安全 driver 摘要；非 poison 明细为空。
	PoisonDriver string
	// PoisonAction 是 driver 对 poison envelope 采取的终结动作；非 poison 明细为空。
	PoisonAction string
	// PoisonBodySize 是 poison envelope 原始 body 字节数；不保存 BodyBase64 或 raw body。
	PoisonBodySize int
	// PoisonBodyTruncated 表示 poison envelope 原始 body 是否被 driver 截断后再暴露。
	PoisonBodyTruncated bool
	// OccurredAt 是诊断事件发生时间。
	OccurredAt time.Time
}

// ObservabilityDiagnostic 是 collector/aggregator/flusher 对丢弃和降级的安全诊断。
type ObservabilityDiagnostic struct {
	// Reason 是稳定机器可读原因，例如 buffer_full。
	Reason string
	// Count 是同类诊断在当前窗口内的累计次数。
	Count int64
	// ObservedAt 是最近一次观测到该诊断的时间。
	ObservedAt time.Time
	// Description 是可展示的短说明，不包含底层敏感错误原文。
	Description string
	// Gap 区分缺口是否可量化：quantifiable 可降级估算，unknown 必须让相关窗口 unknown。
	Gap string
}

// ObservabilityMemoryState 暴露采样数据内存控制状态，供读模型显示压力和降级原因。
type ObservabilityMemoryState struct {
	// BufferSize 是有界 buffer 容量。
	BufferSize int
	// BufferUsed 是当前 buffer 使用条数。
	BufferUsed int
	// SampleReservoirSize 是 runtime 样本池容量。
	SampleReservoirSize int
	// SampleReservoirUsed 是当前样本池使用条数。
	SampleReservoirUsed int
	// MaxAggregateKeys 是聚合 key 基数上限。
	MaxAggregateKeys int
	// AggregateKeyCount 是当前聚合 key 数量。
	AggregateKeyCount int
	// LastDropReason 是最近一次内存或降级丢弃原因。
	LastDropReason string
	// BufferHighWatermark 是调用方记录的 buffer 高水位比例。
	BufferHighWatermark float64
	// ReservoirUtilization 是调用方记录的样本池使用比例。
	ReservoirUtilization float64
}

// BufferUtilization 返回当前 buffer 使用率；容量为 0 时返回 0，避免除零。
func (s ObservabilityMemoryState) BufferUtilization() float64 {
	if s.BufferSize <= 0 {
		return 0
	}
	return float64(s.BufferUsed) / float64(s.BufferSize)
}

// AggregateKeyUtilization 返回当前聚合 key 使用率；容量为 0 时返回 0。
func (s ObservabilityMemoryState) AggregateKeyUtilization() float64 {
	if s.MaxAggregateKeys <= 0 {
		return 0
	}
	return float64(s.AggregateKeyCount) / float64(s.MaxAggregateKeys)
}

// FlushBatch 是 async flusher 后续按窗口批量写 Store 的内部合同。
//
// 设计边界：该结构把 event_metrics 增量、高价值明细、诊断和内存状态分开表达；
// Store 慢或不可用时允许丢弃 batch，worker 热路径不应同步等待该 batch 写入结果。
type FlushBatch struct {
	// WindowStart 是 event_metrics 聚合窗口开始时间。
	WindowStart time.Time
	// WindowEnd 是 event_metrics 聚合窗口结束时间。
	WindowEnd time.Time
	// Increments 保存采样后的 event_metrics 增量。
	Increments []EventMetricIncrement
	// EventMetricWindows 保存按事件时间归属、可追加写入 Store 的 event_metrics windows。
	EventMetricWindows []EventMetricWindow
	// HighValueDetails 保存 failed/poison/slow job 安全诊断摘要。
	HighValueDetails []HighValueJobDetail
	// BatchSummaries 保存 BatchEvent 派生的低频只读摘要，独立于 event_metrics 与 detail 通道。
	BatchSummaries []BatchSummary
	// Diagnostics 保存 drop/degradation 诊断。
	Diagnostics []ObservabilityDiagnostic
	// Memory 保存本批次对应的内存控制状态。
	Memory ObservabilityMemoryState
}

// HasDrops 判断该批次是否包含丢弃或降级诊断。
func (b FlushBatch) HasDrops() bool {
	for _, diagnostic := range b.Diagnostics {
		if diagnostic.Count > 0 {
			return true
		}
	}
	return false
}

// ObservabilityCollector 是 Horizon 非阻塞采集入口。
type ObservabilityCollector interface {
	// Collect 尝试接收单个 queue event；实现必须快速返回，不能同步等待 Store 写入。
	Collect(context.Context, CollectorInput) error
}

// ObservabilityFlusher 是后续 async flusher 写入 Store writer 的内部边界。
type ObservabilityFlusher interface {
	// Flush 写入一个批次；调用方可用 flush_timeout 控制等待上限，失败时允许丢弃观测数据。
	Flush(context.Context, FlushBatch) error
}
