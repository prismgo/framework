package horizon

import (
	"context"
	"sort"
	"strings"
	"time"
)

const (
	metricsWindowInconsistent = "metrics_window_inconsistent"
	metricsSourceStale        = "event_metrics_source_stale"
	metricsSourceMissing      = "event_metrics_source_missing"
	metricsSummaryWindow      = 24 * time.Hour
)

// EventMetricReadModel 是 API/Dashboard 使用的 event_metrics 估算读模型。
//
// 需求背景：Dashboard 默认展示 connection:queue 聚合值，但 API 需要保留 SourceDetails，
// 供调用方按 host/supervisor/environment/overflow 下钻定位单实例异常。
type EventMetricReadModel struct {
	// Key 是 connection:queue 形式的聚合键。
	Key string `json:"key"`
	// Connection 是队列连接维度。
	Connection string `json:"connection"`
	// Queue 是队列名称维度。
	Queue string `json:"queue"`
	// Estimate 是按每个实例/窗口自己的采样率估算后再求和的聚合结果。
	Estimate EventMetricEstimate `json:"estimate"`
	// RuntimePercentiles 当前只在具备样本分布时可用；缺样本时返回 unknown。
	RuntimePercentiles RuntimePercentileEstimate `json:"runtime_percentiles"`
	// SourceDetails 保存聚合结果背后的来源分片，供只读 API 下钻。
	SourceDetails []EventMetricSourceDetail `json:"source_details"`
}

// EventMetricSourceDetail 保留 event_metrics 聚合后的可下钻来源分片。
//
// 设计原因：Dashboard 默认展示 connection:queue 聚合值，但多实例排障需要看到 host、
// environment、supervisor 和 overflow/job 维度，不能在聚合后丢失来源。
type EventMetricSourceDetail struct {
	// SourcePrefix 是 Horizon namespace/store prefix。
	SourcePrefix string `json:"source_prefix,omitempty"`
	// SourceHost 是写入该窗口的主机名。
	SourceHost string `json:"source_host,omitempty"`
	// SourceEnvironment 是写入该窗口的 Horizon environment。
	SourceEnvironment string `json:"source_environment,omitempty"`
	// SourceSupervisor 是写入该窗口的 supervisor 名称。
	SourceSupervisor string `json:"source_supervisor,omitempty"`
	// Connection 是队列连接来源维度。
	Connection string `json:"connection"`
	// Queue 是队列名称来源维度。
	Queue string `json:"queue"`
	// JobName 是 job type 或 _overflow；_overflow 只表示高基数扩展维度折叠。
	JobName string `json:"job_name,omitempty"`
	// WindowStart 是事件窗口开始时间。
	WindowStart time.Time `json:"window_start"`
	// WindowEnd 是事件窗口结束时间。
	WindowEnd time.Time `json:"window_end"`
	// FlushAt 是写入诊断时间，不参与事件窗口归属。
	FlushAt time.Time `json:"flush_at"`
	// MetricsWindowMS 是写入实例配置的 metrics_window，单位毫秒。
	MetricsWindowMS int64 `json:"metrics_window_ms,omitempty"`
	// Processed 是该来源分片窗口内采样命中的 processed 计数。
	Processed int64 `json:"processed"`
	// Failed 是该来源分片窗口内采样命中的 failed 计数。
	Failed int64 `json:"failed"`
	// Released 是该来源分片窗口内采样命中的 released 计数。
	Released int64 `json:"released"`
	// Poison 是该来源分片窗口内采样命中的 poison envelope 计数。
	Poison int64 `json:"poison"`
	// Queued 是该来源分片窗口内采样命中的 queued 计数。
	Queued int64 `json:"queued"`
	// SampleCount 是该来源分片窗口实际进入 event_metrics 的事件数。
	SampleCount int64 `json:"sample_count"`
	// EffectiveSampleRate 是该来源分片窗口的实际采样率。
	EffectiveSampleRate float64 `json:"effective_sample_rate"`
	// Estimate 是该来源分片自己的估算结果，不与其他分片混算采样率。
	Estimate EventMetricEstimate `json:"estimate"`
	// Quality 是该来源分片的质量状态。
	Quality string `json:"quality"`
	// Degraded 表示该来源分片存在可诊断降级。
	Degraded bool `json:"degraded"`
	// Unknown 表示该来源分片存在不可量化缺口。
	Unknown bool `json:"unknown"`
}

// MetricsObservabilityReadModel 暴露监控链路降级诊断，不包含底层错误原文。
type MetricsObservabilityReadModel struct {
	BufferUtilization   float64   `json:"buffer_utilization"`
	DroppedCount        int64     `json:"dropped_count"`
	Degraded            bool      `json:"degraded"`
	LastFlushAt         time.Time `json:"last_flush_at,omitempty"`
	LastFlushErrorCode  string    `json:"last_flush_error_code,omitempty"`
	LastFlushDurationMS int64     `json:"last_flush_duration_ms,omitempty"`
	FlushLagMS          int64     `json:"flush_lag_ms,omitempty"`
}

// HighValueDetailReadModel 是 high_value_detail API 和 Dashboard 共享的安全摘要字段集合。
//
// 安全边界：该 DTO 只映射 HighValueJobDetail 的展示安全字段，不暴露 payload、raw envelope、
// poison body、broker credential 或完整错误堆栈。
type HighValueDetailReadModel struct {
	ID                  string    `json:"id"`
	Kind                string    `json:"kind"`
	Connection          string    `json:"connection"`
	Queue               string    `json:"queue"`
	JobID               string    `json:"job_id"`
	JobName             string    `json:"job_name"`
	RuntimeMS           int64     `json:"runtime_ms"`
	ErrorSummary        string    `json:"error_summary"`
	PoisonDriver        string    `json:"poison_driver"`
	PoisonAction        string    `json:"poison_action"`
	PoisonBodySize      int       `json:"poison_body_size"`
	PoisonBodyTruncated bool      `json:"poison_body_truncated"`
	OccurredAt          time.Time `json:"occurred_at"`
}

// MetricsCurrentReadModel 是 /metrics/current 的完整读模型。
//
// 设计原因：所有计数字段均从 EventMetricWindow 聚合计算，不再依赖已移除的 MetricsSnapshot。
type MetricsCurrentReadModel struct {
	// CapturedAt 是本次读取时间。
	CapturedAt time.Time `json:"captured_at"`
	// Totals 从 EventMetricWindow 跨窗口聚合计算。
	Totals MetricsCounters `json:"totals"`
	// Buckets 从 EventMetricWindow 按 connection:queue 聚合计算。
	Buckets []MetricsBucketSnapshot `json:"buckets"`
	// QueueWaits 从 collector 当前 queued state 实时计算。
	QueueWaits     []QueueWaitSnapshot                   `json:"queue_waits"`
	MetricsWindows PageEnvelope[EventMetricWindow]       `json:"metrics_windows"`
	Estimates      []EventMetricReadModel                `json:"estimates"`
	Diagnostics    PageEnvelope[ObservabilityDiagnostic] `json:"diagnostics"`
	Observability  MetricsObservabilityReadModel         `json:"observability"`
}

// buildMetricsCurrentReadModel 组合 event_metrics、等待时间和诊断形成 /metrics/current。
//
// 读取边界：metrics_windows 字段始终是 raw windows 的分页结果，供 API 使用者查看真实写入窗口；
// summary 计数、buckets 和默认 estimates 则优先读取 queue rollup，避免首屏请求扫描完整 raw 集合。
// 当请求 source_details=1 或带来源过滤时，读模型退回 raw windows，因为 rollup 已经丢弃
// host/environment/supervisor/jobName，无法支持来源下钻和 missing/stale source 诊断。
// extraDiagnostics 只在读侧派生，不写回 Store，避免 GET 请求产生副作用。
func buildMetricsCurrentReadModel(ctx context.Context, manager *Manager, store Store, query EventMetricWindowQuery) (MetricsCurrentReadModel, error) {
	query = normalizeEventMetricWindowQuery(query)
	windows, err := store.EventMetricWindows(ctx, query)
	if err != nil {
		return MetricsCurrentReadModel{}, err
	}
	summaryWindows, err := metricSummaryWindows(ctx, store, query)
	if err != nil {
		return MetricsCurrentReadModel{}, err
	}
	allWindows := summaryWindows
	if queryNeedsRawMetricWindows(query) {
		allWindows, err = allEventMetricWindows(ctx, store, query)
		if err != nil {
			return MetricsCurrentReadModel{}, err
		}
	}
	diagnostics, err := store.ObservabilityDiagnostics(ctx, query.Page)
	if err != nil {
		return MetricsCurrentReadModel{}, err
	}
	extraDiagnostics := metricsWindowDiagnostics(summaryWindows)
	if queryNeedsRawMetricWindows(query) {
		sourceDiagnostics, err := metricsSourceDiagnostics(ctx, store, allWindows, query)
		if err != nil {
			return MetricsCurrentReadModel{}, err
		}
		extraDiagnostics = append(extraDiagnostics, sourceDiagnostics...)
	}
	diagnostics.Items = append(diagnostics.Items, extraDiagnostics...)
	diagnostics.Total += len(extraDiagnostics)
	sortObservabilityDiagnostics(diagnostics.Items)

	estimates := buildEventMetricReadModels(allWindows, extraDiagnostics)
	if query.OmitSourceDetails {
		for i := range estimates {
			estimates[i].SourceDetails = nil
		}
	}
	// 所有计数字段从 EventMetricWindow 聚合计算。
	totals := aggregateMetricsTotals(allWindows)
	buckets := aggregateMetricsBuckets(allWindows)
	return MetricsCurrentReadModel{
		CapturedAt:     time.Now().UTC(),
		Totals:         totals,
		Buckets:        buckets,
		QueueWaits:     currentQueueWaitSnapshots(manager),
		MetricsWindows: windows,
		Estimates:      estimates,
		Diagnostics:    diagnostics,
		Observability:  metricsObservabilityReadModel(manager, diagnostics.Items, estimates),
	}, nil
}

// normalizeMetricsSummaryQuery 为内部 summary 读取补齐默认 24h 时间范围。
//
// 使用场景：HTTP 层已经为 `/metrics/current` 做了同样的默认化和 24h 上限校验；该函数主要服务
// `/queues` 等内部 read model，保证它们读取 rollup 时也只看最近 24h，不回到无限历史聚合。
func normalizeMetricsSummaryQuery(query EventMetricWindowQuery, now time.Time) EventMetricWindowQuery {
	query = normalizeEventMetricWindowQuery(query)
	if query.From.IsZero() && query.To.IsZero() {
		query.To = now
		query.From = now.Add(-metricsSummaryWindow)
	}
	return query
}

// metricSummaryWindows 选择 summary 聚合的数据源。
//
// 默认 summary 走 Store 维护好的 queue rollup；只有调用方需要来源维度时才分页遍历 raw windows。
// 这个分支是热点保护的核心：普通 Dashboard 首屏不再为了 Totals/Buckets/Estimates 扫完整原始窗口。
func metricSummaryWindows(ctx context.Context, store Store, query EventMetricWindowQuery) ([]EventMetricWindow, error) {
	if queryNeedsRawMetricWindows(query) {
		return allEventMetricWindows(ctx, store, query)
	}
	return store.EventMetricRollupWindows(ctx, query)
}

// queryNeedsRawMetricWindows 判断当前查询是否必须读取 raw event_metrics windows。
//
// OmitSourceDetails=false 表示响应需要 source_details；source_host/source_environment/source_supervisor
// 过滤也只能在 raw windows 上判断。connection/queue 过滤不触发 raw 读取，因为 rollup 仍保留这两个维度。
func queryNeedsRawMetricWindows(query EventMetricWindowQuery) bool {
	query = normalizeEventMetricWindowQuery(query)
	return !query.OmitSourceDetails ||
		query.SourceHost != "" || query.SourceEnvironment != "" || query.SourceSupervisor != ""
}

func buildMetricSourcesReadModel(ctx context.Context, store Store, query EventMetricWindowQuery) (PageEnvelope[EventMetricSourceDetail], error) {
	query = normalizeEventMetricWindowQuery(query)
	windows, err := allEventMetricWindows(ctx, store, query)
	if err != nil {
		return PageEnvelope[EventMetricSourceDetail]{}, err
	}
	diagnostics, err := metricsSourceDiagnostics(ctx, store, windows, query)
	if err != nil {
		return PageEnvelope[EventMetricSourceDetail]{}, err
	}
	items := buildEventMetricSourceDetails(windows, diagnostics)
	return makePageEnvelope(items, query.Page), nil
}

// buildMetricsHistoryReadModel 从 event_metrics windows 投影 queue history。
//
// 设计原因：queue history 是事件窗口读模型，不依赖旧 metrics history 覆盖语义；同一 key 下的
// 多实例窗口按各自采样率估算后再聚合，metrics_window 漂移时返回 unknown/degraded。
// 查询边界：只要调用方传入时间范围或来源过滤，就不能回退到旧 MetricsHistory，否则会返回未过滤数据。
func buildMetricsHistoryReadModel(ctx context.Context, store Store, kind, key string, query EventMetricWindowQuery) (PageEnvelope[MetricsHistorySnapshot], EventMetricEstimate, error) {
	query = normalizeEventMetricWindowQuery(query)
	if kind == MetricsHistoryQueue {
		windows, err := allEventMetricWindows(ctx, store, query)
		if err != nil {
			return PageEnvelope[MetricsHistorySnapshot]{}, EventMetricEstimate{}, err
		}
		items := eventMetricWindowsForKey(windows, key)
		if len(items) > 0 {
			diagnostics, err := metricsSourceDiagnostics(ctx, store, items, query)
			if err != nil {
				return PageEnvelope[MetricsHistorySnapshot]{}, EventMetricEstimate{}, err
			}
			history := make([]MetricsHistorySnapshot, 0, len(items))
			for _, window := range items {
				history = append(history, metricsHistoryFromWindow(kind, key, window))
			}
			sort.Slice(history, func(i, j int) bool {
				return history[i].WindowStart.Before(history[j].WindowStart)
			})
			estimate := EstimateEventMetricWindows(items)
			if metricsWindowInconsistentForWindows(items) || hasDiagnostic(diagnostics, metricsSourceStale) {
				estimate.Quality = EventMetricQualityUnknown
				estimate.Degraded = true
			}
			return makePageEnvelope(history, query.Page), estimate, nil
		}
		if eventMetricWindowQueryHasFilters(query) {
			return makePageEnvelope([]MetricsHistorySnapshot{}, query.Page), EventMetricEstimate{}, nil
		}
		// 已不再回退到旧 MetricsHistory；event_metrics_retention 窗口保留期
		// 可完整覆盖原有 history 保留需求，且旧 history 不支持来源下钻。
		return makePageEnvelope([]MetricsHistorySnapshot{}, query.Page), EventMetricEstimate{}, nil
	}
	return makePageEnvelope([]MetricsHistorySnapshot{}, query.Page), EventMetricEstimate{}, nil
}

// allEventMetricWindows 分页读取完整过滤窗口集合，供聚合读模型使用。
//
// 设计原因：Store 返回的 raw window 列表需要分页展示，但 estimates、quality 和 diagnostics
// 必须基于完整过滤集合计算；该函数保留 query 的范围/来源条件，只把分页扩大为内部有界遍历。
func allEventMetricWindows(ctx context.Context, store Store, query EventMetricWindowQuery) ([]EventMetricWindow, error) {
	query = normalizeEventMetricWindowQuery(query)
	query.Page = PageRequest{Page: 1, PageSize: maxPageSize}
	out := make([]EventMetricWindow, 0)
	for {
		page, err := store.EventMetricWindows(ctx, query)
		if err != nil {
			return nil, err
		}
		out = append(out, page.Items...)
		if len(out) >= page.Total || len(page.Items) == 0 {
			break
		}
		query.Page.Page++
	}
	return out, nil
}

// eventMetricWindowQueryHasFilters 判断查询是否包含范围或来源过滤。
//
// 设计原因：旧 MetricsHistory 只按 kind/key 保存聚合点，没有来源维度；当调用方请求过滤时，
// read model 必须避免回退到旧 history，否则会返回未过滤的兼容数据。
func eventMetricWindowQueryHasFilters(query EventMetricWindowQuery) bool {
	query = normalizeEventMetricWindowQuery(query)
	return !query.From.IsZero() || !query.To.IsZero() ||
		query.SourceHost != "" || query.SourceEnvironment != "" || query.SourceSupervisor != "" ||
		query.Connection != "" || query.Queue != ""
}

// aggregateMetricsTotals 从 EventMetricWindow 跨越多个窗口聚合全量 totals。
//
// 设计原因：totals 改为从 append-only event window 直接聚合，不再依赖 SaveMetricsSnapshot
// 写入的兼容 MetricsSnapshot.Totals，确保数值来源与 estimates/source_details 一致。
// 设计边界：该函数只按窗口自己的采样率和估算值求和；不同窗口或实例的采样率差异
// 在窗口粒度的 estimateEventMetricWindow 中已经处理。
func aggregateMetricsTotals(windows []EventMetricWindow) MetricsCounters {
	var totals MetricsCounters
	for _, window := range windows {
		totals.Processed += window.Processed
		totals.Failed += window.Failed
		totals.Released += window.Released
		totals.PoisonEnvelopes += window.Poison
		totals.Queued += window.Queued
	}
	return totals
}

// aggregateMetricsBuckets 从 EventMetricWindow 按 connection:queue 聚合为 bucket 快照。
//
// 设计原因：替代已移除的 MetricsSnapshot.Buckets，供 /metrics/current 和 Queue read model 使用。
func aggregateMetricsBuckets(windows []EventMetricWindow) []MetricsBucketSnapshot {
	buckets := make(map[string]*MetricsBucketSnapshot)
	for _, window := range windows {
		key := window.Connection + ":" + window.Queue
		bucket, ok := buckets[key]
		if !ok {
			bucket = &MetricsBucketSnapshot{
				Connection: window.Connection,
				Queue:      window.Queue,
			}
			buckets[key] = bucket
		}
		bucket.Processed += window.Processed
		bucket.Failed += window.Failed
		bucket.Released += window.Released
		bucket.PoisonEnvelopes += window.Poison
		bucket.Queued += window.Queued
		bucket.ProcessedRuntimeTotalMS += window.RuntimeMS
		bucket.ProcessedCount += window.RuntimeSampleCount
	}
	out := make([]MetricsBucketSnapshot, 0, len(buckets))
	for _, bucket := range buckets {
		out = append(out, *bucket)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Connection != out[j].Connection {
			return out[i].Connection < out[j].Connection
		}
		return out[i].Queue < out[j].Queue
	})
	return out
}

// buildEventMetricReadModels 按 connection:queue 聚合多个实例的 event_metrics windows。
//
// 逻辑说明：聚合值面向 Dashboard 默认视图；SourceDetails 保留原始来源粒度，包含 _overflow
// bucket 的 degraded/estimated 状态，避免把高基数压力来源合并成普通精确 key。
func buildEventMetricReadModels(windows []EventMetricWindow, diagnostics []ObservabilityDiagnostic) []EventMetricReadModel {
	groups := make(map[string][]EventMetricWindow)
	for _, window := range windows {
		key := eventMetricKey(window.Connection, window.Queue)
		if key == ":" {
			continue
		}
		groups[key] = append(groups[key], window)
	}
	inconsistent := hasDiagnostic(diagnostics, metricsWindowInconsistent)
	out := make([]EventMetricReadModel, 0, len(groups))
	for key, items := range groups {
		sort.Slice(items, func(i, j int) bool {
			return items[i].WindowStart.Before(items[j].WindowStart)
		})
		connection, queue := splitQueueWaitKey(key)
		estimate := EstimateEventMetricWindows(items)
		if inconsistent && metricsWindowInconsistentForWindows(items) {
			estimate.Quality = EventMetricQualityUnknown
			estimate.Degraded = true
		}
		if groupHasDiagnostic(diagnostics, metricsSourceStale, key) || groupHasDiagnostic(diagnostics, metricsSourceMissing, key) {
			estimate.Quality = EventMetricQualityUnknown
			estimate.Degraded = true
		}
		out = append(out, EventMetricReadModel{
			Key:                key,
			Connection:         connection,
			Queue:              queue,
			Estimate:           estimate,
			RuntimePercentiles: EstimateRuntimePercentiles(nil, 1),
			SourceDetails:      buildEventMetricSourceDetails(items, diagnostics),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func eventMetricWindowsForKey(windows []EventMetricWindow, key string) []EventMetricWindow {
	out := make([]EventMetricWindow, 0, len(windows))
	for _, window := range windows {
		if eventMetricKey(window.Connection, window.Queue) == key {
			out = append(out, window)
		}
	}
	return out
}

func metricsHistoryFromWindow(kind, key string, window EventMetricWindow) MetricsHistorySnapshot {
	item := estimateEventMetricWindow(window)
	return MetricsHistorySnapshot{
		Kind:                kind,
		Key:                 key,
		Timestamp:           window.WindowStart,
		Throughput:          window.Processed,
		RuntimeMS:           averageRuntimeMS(window.RuntimeMS, window.RuntimeSampleCount),
		Failed:              window.Failed,
		Released:            window.Released,
		PoisonEnvelopes:     window.Poison,
		WaitStatus:          QueueWaitUnknown,
		WindowStart:         window.WindowStart,
		WindowEnd:           window.WindowEnd,
		FlushAt:             window.FlushAt,
		EffectiveSampleRate: window.EffectiveSampleRate,
		SampleCount:         item.SampledCount,
		EstimatedTotal:      item.EstimatedTotal,
		Quality:             item.Quality,
		Degraded:            item.Degraded,
		Unknown:             item.Quality == EventMetricQualityUnknown,
	}
}

// metricsWindowDiagnostics 返回只读一致性诊断。
//
// 需求背景：同一 namespace/environment 的实例必须使用一致 metrics_window；检测到漂移时，
// 读模型返回 degraded/unknown，而不是尝试重采样或静默按 flush 时间重新分桶。
func metricsWindowDiagnostics(windows []EventMetricWindow) []ObservabilityDiagnostic {
	if !metricsWindowInconsistentForWindows(windows) {
		return nil
	}
	return []ObservabilityDiagnostic{{
		Reason:      metricsWindowInconsistent,
		Count:       1,
		ObservedAt:  latestWindowFlushAt(windows),
		Description: "event_metrics windows use inconsistent metrics_window values for the same namespace/environment",
		Gap:         ObservabilityGapUnknown,
	}}
}

// metricsWindowInconsistentForWindows 判断同一 namespace/environment 是否存在 metrics_window 漂移。
//
// 逻辑说明：不同 environment 可以使用不同配置；同一 environment 内不同 flush_at 也不构成漂移。
// 只有显式 metrics_window_ms 或兼容回退出的 window duration 不一致时才降级。
func metricsWindowInconsistentForWindows(windows []EventMetricWindow) bool {
	seen := map[string]time.Duration{}
	for _, window := range windows {
		namespace := strings.TrimSpace(window.SourcePrefix) + ":" + strings.TrimSpace(window.SourceEnvironment)
		duration := eventMetricWindowDuration(window)
		if namespace == ":" || duration <= 0 {
			continue
		}
		if current, ok := seen[namespace]; ok && current != duration {
			return true
		}
		seen[namespace] = duration
	}
	return false
}

// metricsSourceDiagnostics 返回来源 runtime 状态对 event_metrics 的读侧诊断。
//
// 设计边界：只有 Store 中已经存在对应 supervisor 记录且读取时派生为 stale，才把窗口标记为
// stale source。missing source 只基于 fresh supervisor heartbeat 和其声明的 queue 目标判断；
// 纯历史窗口或没有 heartbeat 事实的测试数据不被误判为缺失实例。
// 查询边界：诊断只基于过滤后的 runtime 来源范围计算，避免把已被 source_host/environment/queue
// 过滤掉的 supervisor 误判为缺失。
func metricsSourceDiagnostics(ctx context.Context, store Store, windows []EventMetricWindow, query EventMetricWindowQuery) ([]ObservabilityDiagnostic, error) {
	if store == nil {
		return nil, nil
	}
	supervisors, err := store.Supervisors(ctx, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	supervisors = filterSupervisorsForMetricQuery(supervisors, query)
	stale := staleSupervisorSources(supervisors)
	if len(stale) == 0 {
		return missingSourceDiagnostics(ctx, store, supervisors, windows), nil
	}
	out := make([]ObservabilityDiagnostic, 0)
	for _, window := range windows {
		if !stale[eventMetricRuntimeSourceKey(window.SourceHost, window.SourceEnvironment, window.SourceSupervisor)] {
			continue
		}
		out = append(out, ObservabilityDiagnostic{
			Reason:      metricsSourceStale,
			Count:       1,
			ObservedAt:  window.FlushAt,
			Description: eventMetricSourceDiagnosticDescription(window),
			Gap:         ObservabilityGapUnknown,
		})
	}
	out = append(out, missingSourceDiagnostics(ctx, store, supervisors, windows)...)
	return out, nil
}

// filterSupervisorsForMetricQuery 将 runtime supervisor 集合裁剪到当前 metrics 查询范围。
//
// 使用方式：missing/stale source 诊断调用该函数后再比对窗口来源；这样按 host/environment/supervisor
// 或 queue 下钻时，只诊断下钻范围内应出现但缺失的来源。
// 设计边界：该函数只过滤 supervisor heartbeat 投影，不修改窗口集合，也不承担 Store 查询职责。
func filterSupervisorsForMetricQuery(supervisors []SupervisorState, query EventMetricWindowQuery) []SupervisorState {
	query = normalizeEventMetricWindowQuery(query)
	out := make([]SupervisorState, 0, len(supervisors))
	for _, supervisor := range supervisors {
		if query.SourceHost != "" && supervisor.Host != query.SourceHost {
			continue
		}
		if query.SourceEnvironment != "" && supervisor.Environment != query.SourceEnvironment {
			continue
		}
		if query.SourceSupervisor != "" && supervisor.Name != query.SourceSupervisor {
			continue
		}
		if query.Connection != "" && supervisor.Connection != query.Connection {
			continue
		}
		if query.Queue != "" && !stringSliceContains(supervisor.Queues, query.Queue) {
			continue
		}
		out = append(out, supervisor)
	}
	return out
}

// stringSliceContains 判断 supervisor 声明的 queue 列表是否包含过滤目标。
//
// 需求背景：missing source 诊断需要知道某个 supervisor 是否负责当前 queue；这里使用精确匹配，
// 与 issue 44 的来源过滤语义保持一致。
func stringSliceContains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

// staleSupervisorSources 提取当前已知 stale supervisor 的来源 key。
func staleSupervisorSources(supervisors []SupervisorState) map[string]bool {
	out := map[string]bool{}
	for _, supervisor := range supervisors {
		if supervisor.Status == SupervisorStale {
			out[eventMetricRuntimeSourceKey(supervisor.Host, supervisor.Environment, supervisor.Name)] = true
		}
	}
	return out
}

// missingSourceDiagnostics 标记 fresh supervisor 负责但当前窗口集合缺失的 queue 分片。
//
// 设计原因：多实例聚合不能把缺失分片当作 0；同一读取窗口里已有其他实例数据时，
// fresh supervisor 的目标 queue 缺失必须通过诊断让聚合结果进入 unknown/degraded。
//
// 组合策略（修复空闲 supervisor 误报）：
//  1. 快速判断：同 connection:queue 是否有其他 supervisor 的窗口 → 有则报 missing
//  2. 兜底判断：查 Store 历史窗口，曾经有过但当前缺失 → 报 missing；从未有过 → 不报（空闲/刚启动）
func missingSourceDiagnostics(ctx context.Context, store Store, supervisors []SupervisorState, windows []EventMetricWindow) []ObservabilityDiagnostic {
	// 构建当前窗口中已出现的来源 key 集合和 connection:queue 集合
	seen := eventMetricWindowSourceSet(windows)
	activeQueues := activeQueueSet(windows)
	latest := latestWindowFlushAt(windows)
	out := make([]ObservabilityDiagnostic, 0)

	// 缓存 Store 历史查询结果，避免同一 connection:queue 重复查询
	historyCache := map[string]bool{}

	for _, supervisor := range supervisors {
		if supervisor.Status == SupervisorStale || supervisor.Host == "" || supervisor.Environment == "" || supervisor.Name == "" || supervisor.Connection == "" {
			continue
		}
		for _, queueName := range supervisor.Queues {
			if queueName == "" || seen[eventMetricQueueSourceKey(supervisor.Host, supervisor.Environment, supervisor.Name, supervisor.Connection, queueName)] {
				continue
			}

			// 组合策略：同 queue 有其他来源窗口 → 直接报 missing
			queueKey := supervisor.Connection + ":" + queueName
			if activeQueues[queueKey] {
				out = append(out, ObservabilityDiagnostic{
					Reason:      metricsSourceMissing,
					Count:       1,
					ObservedAt:  latest,
					Description: eventMetricQueueSourceDescription(supervisor.Host, supervisor.Environment, supervisor.Name, supervisor.Connection, queueName, ""),
					Gap:         ObservabilityGapUnknown,
				})
				continue
			}

			// 同 queue 无任何来源窗口 → 查 Store 历史确认是否有过数据
			if storeHasQueueHistory(ctx, store, historyCache, supervisor.Connection, queueName) {
				out = append(out, ObservabilityDiagnostic{
					Reason:      metricsSourceMissing,
					Count:       1,
					ObservedAt:  latest,
					Description: eventMetricQueueSourceDescription(supervisor.Host, supervisor.Environment, supervisor.Name, supervisor.Connection, queueName, ""),
					Gap:         ObservabilityGapUnknown,
				})
			}
			// Store 也没有该 queue 的历史 → 空闲/刚启动，不报 missing
		}
	}
	return out
}

// activeQueueSet 从当前窗口集合中提取所有活跃的 connection:queue 集合。
//
// 使用方式：missingSourceDiagnostics 用该集合快速判断同 queue 是否有其他来源的窗口，
// 避免对每个 supervisor 都查询 Store 历史。
func activeQueueSet(windows []EventMetricWindow) map[string]bool {
	out := make(map[string]bool, len(windows))
	for _, window := range windows {
		out[window.Connection+":"+window.Queue] = true
	}
	return out
}

// storeHasQueueHistory 查询 Store 中指定 connection:queue 是否存在历史窗口记录。
//
// 设计原因：区分"pipeline 断裂（曾有数据但当前缺失）"和"queue 空闲/刚启动（从未有过数据）"。
// 使用 cache 避免同一 connection:queue 被多个 supervisor 重复查询。
func storeHasQueueHistory(ctx context.Context, store Store, cache map[string]bool, connection, queue string) bool {
	if store == nil {
		return false
	}
	key := connection + ":" + queue
	if cached, ok := cache[key]; ok {
		return cached
	}
	result, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{
		Connection: connection,
		Queue:      queue,
		Page:       PageRequest{Page: 1, PageSize: 1},
	})
	hasHistory := err == nil && result.Total > 0
	cache[key] = hasHistory
	return hasHistory
}

func eventMetricWindowSourceSet(windows []EventMetricWindow) map[string]bool {
	out := make(map[string]bool, len(windows))
	for _, window := range windows {
		out[eventMetricQueueSourceKey(window.SourceHost, window.SourceEnvironment, window.SourceSupervisor, window.Connection, window.Queue)] = true
	}
	return out
}

// buildEventMetricSourceDetails 将聚合窗口投影为 API 下钻明细。
//
// 设计边界：该函数不再二次合并来源，确保 host、environment、supervisor、connection、queue
// 和 _overflow/jobName 都保持可见；每个明细只按自己的有效采样率计算 Estimate。
func buildEventMetricSourceDetails(windows []EventMetricWindow, diagnostics []ObservabilityDiagnostic) []EventMetricSourceDetail {
	out := make([]EventMetricSourceDetail, 0, len(windows))
	for _, window := range windows {
		item := estimateEventMetricWindow(window)
		estimate := EstimateEventMetricWindows([]EventMetricWindow{window})
		if sourceHasDiagnostic(diagnostics, metricsSourceStale, window) {
			item.Quality = EventMetricQualityUnknown
			item.Degraded = true
			estimate.Quality = EventMetricQualityUnknown
			estimate.Degraded = true
		}
		out = append(out, EventMetricSourceDetail{
			SourcePrefix:        window.SourcePrefix,
			SourceHost:          window.SourceHost,
			SourceEnvironment:   window.SourceEnvironment,
			SourceSupervisor:    window.SourceSupervisor,
			Connection:          window.Connection,
			Queue:               window.Queue,
			JobName:             window.JobName,
			WindowStart:         window.WindowStart,
			WindowEnd:           window.WindowEnd,
			FlushAt:             window.FlushAt,
			MetricsWindowMS:     eventMetricMetricsWindowMS(window.MetricsWindowMS, window.WindowStart, window.WindowEnd),
			Processed:           window.Processed,
			Failed:              window.Failed,
			Released:            window.Released,
			Poison:              window.Poison,
			Queued:              window.Queued,
			SampleCount:         item.SampledCount,
			EffectiveSampleRate: window.EffectiveSampleRate,
			Estimate:            estimate,
			Quality:             item.Quality,
			Degraded:            item.Degraded,
			Unknown:             item.Quality == EventMetricQualityUnknown,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceEnvironment != out[j].SourceEnvironment {
			return out[i].SourceEnvironment < out[j].SourceEnvironment
		}
		if out[i].SourceHost != out[j].SourceHost {
			return out[i].SourceHost < out[j].SourceHost
		}
		if out[i].SourceSupervisor != out[j].SourceSupervisor {
			return out[i].SourceSupervisor < out[j].SourceSupervisor
		}
		if !out[i].WindowStart.Equal(out[j].WindowStart) {
			return out[i].WindowStart.Before(out[j].WindowStart)
		}
		return out[i].JobName < out[j].JobName
	})
	return out
}

// eventMetricRuntimeSourceKey 返回 host/environment/supervisor 的可读匹配键。
func eventMetricRuntimeSourceKey(host, environment, supervisor string) string {
	return strings.Join([]string{strings.TrimSpace(host), strings.TrimSpace(environment), strings.TrimSpace(supervisor)}, "|")
}

// eventMetricQueueSourceKey 返回 runtime source 加 queue 目标的匹配键。
func eventMetricQueueSourceKey(host, environment, supervisor, connection, queue string) string {
	return strings.Join([]string{
		eventMetricRuntimeSourceKey(host, environment, supervisor),
		strings.TrimSpace(connection),
		strings.TrimSpace(queue),
	}, "|")
}

// eventMetricSourceDiagnosticDescription 生成稳定可匹配且可展示的 stale source 诊断描述。
func eventMetricSourceDiagnosticDescription(window EventMetricWindow) string {
	return eventMetricQueueSourceDescription(window.SourceHost, window.SourceEnvironment, window.SourceSupervisor, window.Connection, window.Queue, window.JobName)
}

// eventMetricQueueSourceDescription 生成 source 诊断的稳定描述字段。
func eventMetricQueueSourceDescription(host, environment, supervisor, connection, queue, jobName string) string {
	return strings.Join([]string{
		"source=" + eventMetricRuntimeSourceKey(host, environment, supervisor),
		"connection=" + strings.TrimSpace(connection),
		"queue=" + strings.TrimSpace(queue),
		"job=" + strings.TrimSpace(jobName),
	}, " ")
}

// groupHasDiagnostic 判断某个 queue 聚合 key 是否命中指定诊断。
func groupHasDiagnostic(items []ObservabilityDiagnostic, reason string, key string) bool {
	connection, queue := splitQueueWaitKey(key)
	for _, item := range items {
		if item.Reason == reason && item.Count > 0 &&
			strings.Contains(item.Description, "connection="+connection) &&
			strings.Contains(item.Description, "queue="+queue) {
			return true
		}
	}
	return false
}

// sourceHasDiagnostic 判断单个来源窗口是否命中指定诊断。
func sourceHasDiagnostic(items []ObservabilityDiagnostic, reason string, window EventMetricWindow) bool {
	source := "source=" + eventMetricRuntimeSourceKey(window.SourceHost, window.SourceEnvironment, window.SourceSupervisor)
	for _, item := range items {
		if item.Reason == reason && item.Count > 0 &&
			strings.Contains(item.Description, source) &&
			strings.Contains(item.Description, "connection="+strings.TrimSpace(window.Connection)) &&
			strings.Contains(item.Description, "queue="+strings.TrimSpace(window.Queue)) &&
			strings.Contains(item.Description, "job="+strings.TrimSpace(window.JobName)) {
			return true
		}
	}
	return false
}

// eventMetricWindowDuration 返回用于配置漂移判断的窗口宽度。
//
// 语义说明：优先使用写入端携带的 metrics_window_ms；只有旧数据缺失该字段时，才回退到
// WindowEnd-WindowStart。FlushAt 永远不参与该计算。
func eventMetricWindowDuration(window EventMetricWindow) time.Duration {
	if window.MetricsWindowMS > 0 {
		return time.Duration(window.MetricsWindowMS) * time.Millisecond
	}
	if window.WindowEnd.After(window.WindowStart) {
		return window.WindowEnd.Sub(window.WindowStart)
	}
	return 0
}

func latestWindowFlushAt(windows []EventMetricWindow) time.Time {
	var latest time.Time
	for _, window := range windows {
		if window.FlushAt.After(latest) {
			latest = window.FlushAt
		}
	}
	return latest
}

func metricsObservabilityReadModel(manager *Manager, diagnostics []ObservabilityDiagnostic, estimates []EventMetricReadModel) MetricsObservabilityReadModel {
	var memory ObservabilityMemoryState
	if manager != nil && manager.Collector() != nil {
		memory = manager.Collector().MemoryState()
	}
	var flusher FlusherDiagnostics
	if manager != nil && manager.Flusher() != nil {
		flusher = manager.Flusher().Diagnostics()
	}
	out := MetricsObservabilityReadModel{
		BufferUtilization:   memory.BufferUtilization(),
		LastFlushAt:         flusher.LastFlushAt,
		LastFlushErrorCode:  flushErrorCode(flusher),
		LastFlushDurationMS: int64(flusher.LastFlushDuration / time.Millisecond),
		FlushLagMS:          int64(flusher.FlushLag / time.Millisecond),
		Degraded:            flusher.Degraded,
	}
	for _, diagnostic := range diagnostics {
		if strings.HasPrefix(diagnostic.Reason, "buffer_") ||
			strings.HasPrefix(diagnostic.Reason, "rate_") ||
			strings.HasPrefix(diagnostic.Reason, "aggregate_") ||
			diagnostic.Reason == MemoryDropStoreUnavailable ||
			diagnostic.Reason == MemoryDropFlushLagExceeded ||
			diagnostic.Reason == MemoryDropFlushTimeoutNear {
			out.DroppedCount += diagnostic.Count
		}
		if diagnostic.Count > 0 {
			out.Degraded = true
		}
	}
	for _, estimate := range estimates {
		if estimate.Estimate.Degraded || estimate.Estimate.Quality == EventMetricQualityUnknown {
			out.Degraded = true
			break
		}
	}
	return out
}

func flushErrorCode(diag FlusherDiagnostics) string {
	if diag.LastFlushError == "" {
		return ""
	}
	if diag.DegradedReason != "" {
		return diag.DegradedReason
	}
	return MemoryDropStoreUnavailable
}

func hasDiagnostic(items []ObservabilityDiagnostic, reason string) bool {
	for _, item := range items {
		if item.Reason == reason && item.Count > 0 {
			return true
		}
	}
	return false
}

func eventMetricKey(connection, queue string) string {
	return strings.TrimSpace(connection) + ":" + strings.TrimSpace(queue)
}

// BatchDetailReadModel 是批次详情的安全展示模型。
//
// 设计思路：批次详情只表达进度、状态和关键时间戳，不读取批次内 job payload 或 broker 字段。
type BatchDetailReadModel struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Total       int       `json:"total"`
	Pending     int       `json:"pending"`
	Processed   int       `json:"processed"`
	Failed      int       `json:"failed"`
	Cancelled   bool      `json:"cancelled"`
	CreatedAt   time.Time `json:"created_at"`
	FinishedAt  time.Time `json:"finished_at,omitempty"`
	CancelledAt time.Time `json:"cancelled_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func batchDetailReadModel(batch BatchSummary) BatchDetailReadModel {
	return BatchDetailReadModel{
		ID:          batch.ID,
		Name:        batch.Name,
		Status:      batch.Status,
		Total:       batch.Total,
		Pending:     batch.Pending,
		Processed:   batch.Processed,
		Failed:      batch.Failed,
		Cancelled:   batch.Cancelled,
		CreatedAt:   batch.CreatedAt,
		FinishedAt:  batch.FinishedAt,
		CancelledAt: batch.CancelledAt,
		UpdatedAt:   batch.UpdatedAt,
	}
}

func highValueDetailReadModel(detail HighValueJobDetail) HighValueDetailReadModel {
	return HighValueDetailReadModel{
		ID:                  detail.ID,
		Kind:                detail.Kind,
		Connection:          detail.Connection,
		Queue:               detail.Queue,
		JobID:               detail.JobID,
		JobName:             detail.JobName,
		RuntimeMS:           detail.RuntimeMS,
		ErrorSummary:        detail.ErrorSummary,
		PoisonDriver:        detail.PoisonDriver,
		PoisonAction:        detail.PoisonAction,
		PoisonBodySize:      detail.PoisonBodySize,
		PoisonBodyTruncated: detail.PoisonBodyTruncated,
		OccurredAt:          detail.OccurredAt,
	}
}

func highValueDetailReadModels(details []HighValueJobDetail) []HighValueDetailReadModel {
	out := make([]HighValueDetailReadModel, 0, len(details))
	for _, detail := range details {
		out = append(out, highValueDetailReadModel(detail))
	}
	return out
}
