package horizon

import (
	"context"
	"sort"
	"strings"
	"time"

	goprocess "github.com/prismgo/framework/process"
)

// QueueReadModel 是 Queues tab 与只读 API 复用的聚合读模型。
//
// 需求背景：issue 28 要求后端把 queue length snapshot、metrics bucket、wait snapshot 和配置声明合并成单一只读视图，
// 前端不得自行拼接多个观测接口。该模型只暴露安全聚合值，不读取 payload、broker 内部结构或历史扫描结果。
type QueueReadModel struct {
	Key        string           `json:"key"`
	Connection string           `json:"connection"`
	Queue      string           `json:"queue"`
	Size       goprocess.Metric `json:"size"`
	AvgRuntime goprocess.Metric `json:"avg_runtime"`
	MaxRuntime goprocess.Metric `json:"max_runtime"`
	AvgMemory  goprocess.Metric `json:"avg_memory"`
	MaxMemory  goprocess.Metric `json:"max_memory"`
	WaitTime   goprocess.Metric `json:"wait_time"`
	Throughput goprocess.Metric `json:"throughput"`
	Processed  goprocess.Metric `json:"processed"`
	Failed     goprocess.Metric `json:"failed"`
	Released   goprocess.Metric `json:"released"`
	SampledAt  time.Time        `json:"sampled_at"`
}

type queueAggregateSource struct {
	lengths QueueLengthSnapshot
	buckets []MetricsBucketSnapshot
	waits   []QueueWaitSnapshot
}

type queueAggregateKey struct {
	Connection string
	Queue      string
}

// buildQueueReadModels 聚合配置、队列长度快照和 metrics 快照，生成稳定排序的 queue 只读模型。
//
// 逻辑说明：来源集合取 supervisor 配置、queue length snapshot、metrics bucket 和 wait snapshot 的有界并集；
// 单个来源缺失时只降级对应字段，不隐藏整个队列。
func buildQueueReadModels(ctx context.Context, cfg Config, store Store, manager *Manager) ([]QueueReadModel, error) {
	sources, err := loadQueueAggregateSource(ctx, cfg, store, manager, currentQueueWaitSnapshots(manager))
	if err != nil {
		return nil, err
	}
	keys := collectQueueAggregateKeys(cfg, sources)
	items := make([]QueueReadModel, 0, len(keys))
	for _, key := range keys {
		items = append(items, queueReadModelFromSource(cfg, sources, key))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Connection == items[j].Connection {
			return items[i].Queue < items[j].Queue
		}
		return items[i].Connection < items[j].Connection
	})
	return items, nil
}

// countQueueAggregateKeys 从配置和队列长度快照计算队列数量，不加载 EventMetricWindows。
//
// 设计原因：/status 端点只需要队列数量，不应为计数而加载全量 event windows。
func countQueueAggregateKeys(cfg Config, lengths QueueLengthSnapshot) int {
	set := map[queueAggregateKey]bool{}
	for _, supervisor := range cfg.Supervisors {
		for _, queueName := range supervisor.Queues {
			key := queueAggregateKey{
				Connection: strings.TrimSpace(supervisor.Connection),
				Queue:      strings.TrimSpace(queueName),
			}
			if key.Connection != "" && key.Queue != "" {
				set[key] = true
			}
		}
	}
	for _, item := range lengths.Queues {
		key := queueAggregateKey{Connection: strings.TrimSpace(item.Connection), Queue: strings.TrimSpace(item.Queue)}
		if key.Connection != "" && key.Queue != "" {
			set[key] = true
		}
	}
	return len(set)
}

// queueLengthSnapshotForRead 返回读接口可用的队列长度快照。
//
// 逻辑说明：正常路径优先复用 Store 中由 horizon:snapshot 持久化的快照；当队列长度能力开启但快照尚未产生时，
// 允许读接口按 supervisor 配置补采一次当前长度并写回 Store，避免 Dashboard 在启动早期把已配置队列展示为 null。
func queueLengthSnapshotForRead(ctx context.Context, cfg Config, store Store, manager *Manager, now time.Time) (QueueLengthSnapshot, error) {
	lengths, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		return QueueLengthSnapshot{}, err
	}
	if !shouldRefreshQueueLengthSnapshot(cfg, lengths, manager) {
		return lengths, nil
	}
	refreshed, err := captureQueueLengthSnapshotForRead(ctx, cfg, manager, now)
	if err != nil {
		// 补采是启动期兜底，不应把 /status 或 /queues 变成 broker 强依赖；失败时返回已有快照让字段级指标降级。
		return lengths, nil
	}
	if err := store.SaveQueueLengthSnapshot(ctx, refreshed); err != nil {
		// 写回失败不影响本次响应，调用方仍可展示刚采到的当前长度。
		return refreshed, nil
	}
	return refreshed, nil
}

// shouldRefreshQueueLengthSnapshot 只在快照完全缺失时触发读侧补采。
//
// 设计原因：已有快照即使队列列表为空也可能是一次有效采样结果，不能在每个读请求里反复打到 queue backend。
func shouldRefreshQueueLengthSnapshot(cfg Config, lengths QueueLengthSnapshot, manager *Manager) bool {
	if !cfg.Observability.Enabled(ObservabilityQueueLengths) || manager == nil || manager.QueueManager() == nil {
		return false
	}
	if !lengths.CapturedAt.IsZero() || len(lengths.Queues) > 0 {
		return false
	}
	return len(toCommandQueueTargets(cfg.Supervisors)) > 0
}

// captureQueueLengthSnapshotForRead 使用与 horizon:snapshot 相同的 supervisor 目标集合采样队列长度。
func captureQueueLengthSnapshotForRead(ctx context.Context, cfg Config, manager *Manager, now time.Time) (QueueLengthSnapshot, error) {
	targets := toCommandQueueTargets(cfg.Supervisors)
	snapshot := QueueLengthSnapshot{CapturedAt: now, Queues: make([]QueueLengthBucket, 0, len(targets))}
	queueManager := manager.QueueManager()
	for _, target := range targets {
		connection, err := queueManager.Queue(target.Connection)
		if err != nil {
			return QueueLengthSnapshot{}, queueOperationError("size", target, err)
		}
		size, err := connection.Size(ctx, target.Queue)
		if err != nil {
			return QueueLengthSnapshot{}, queueOperationError("size", target, err)
		}
		snapshot.Queues = append(snapshot.Queues, QueueLengthBucket{
			Connection: target.Connection,
			Queue:      target.Queue,
			Size:       size,
		})
	}
	return snapshot, nil
}

func loadQueueAggregateSource(ctx context.Context, cfg Config, store Store, manager *Manager, waits []QueueWaitSnapshot) (queueAggregateSource, error) {
	lengths, err := queueLengthSnapshotForRead(ctx, cfg, store, manager, time.Now().UTC())
	if err != nil {
		return queueAggregateSource{}, err
	}
	query := normalizeMetricsSummaryQuery(EventMetricWindowQuery{}, time.Now().UTC())
	windows, err := store.EventMetricRollupWindows(ctx, query)
	if err != nil {
		return queueAggregateSource{}, err
	}
	buckets := aggregateMetricsBuckets(windows)
	return queueAggregateSource{lengths: lengths, buckets: buckets, waits: waits}, nil
}

func currentQueueWaitSnapshots(manager *Manager) []QueueWaitSnapshot {
	if manager == nil || manager.coll == nil {
		return nil
	}
	return manager.coll.ComputeWaits(manager.Config().Waits, time.Now().UTC())
}

func collectQueueAggregateKeys(cfg Config, source queueAggregateSource) []queueAggregateKey {
	set := map[queueAggregateKey]bool{}
	for _, supervisor := range cfg.Supervisors {
		for _, queueName := range supervisor.Queues {
			key := queueAggregateKey{
				Connection: strings.TrimSpace(supervisor.Connection),
				Queue:      strings.TrimSpace(queueName),
			}
			if key.Connection != "" && key.Queue != "" {
				set[key] = true
			}
		}
	}
	for _, item := range source.lengths.Queues {
		key := queueAggregateKey{Connection: strings.TrimSpace(item.Connection), Queue: strings.TrimSpace(item.Queue)}
		if key.Connection != "" && key.Queue != "" {
			set[key] = true
		}
	}
	for _, item := range source.buckets {
		key := queueAggregateKey{Connection: strings.TrimSpace(item.Connection), Queue: strings.TrimSpace(item.Queue)}
		if key.Connection != "" && key.Queue != "" {
			set[key] = true
		}
	}
	for _, item := range source.waits {
		key := queueAggregateKey{Connection: strings.TrimSpace(item.Connection), Queue: strings.TrimSpace(item.Queue)}
		if key.Connection != "" && key.Queue != "" {
			set[key] = true
		}
	}
	keys := make([]queueAggregateKey, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	return keys
}

func queueReadModelFromSource(cfg Config, source queueAggregateSource, key queueAggregateKey) QueueReadModel {
	item := QueueReadModel{
		Key:        key.Connection + ":" + key.Queue,
		Connection: key.Connection,
		Queue:      key.Queue,
		AvgMemory:  queueUnsupportedMemoryMetric(),
		MaxMemory:  queueUnsupportedMemoryMetric(),
	}
	if bucket, ok := queueLengthBucketByKey(source.lengths.Queues, key); ok {
		item.Size = queueAvailableIntMetric(bucket.Size, goprocess.UnitCount)
		item.SampledAt = newerTime(item.SampledAt, source.lengths.CapturedAt)
	} else if cfg.Observability.Enabled(ObservabilityQueueLengths) {
		item.Size = queueUnavailableMetric(goprocess.UnitCount, "queue size unavailable")
	} else {
		item.Size = queueDisabledMetric(goprocess.UnitCount, "queue_lengths disabled")
	}

	if bucket, ok := metricsBucketByKey(source.buckets, key); ok {
		// 需求背景：runtime 新口径已经把 event window 的分母收敛为 runtime sample，
		// 队列读模型的平均值和最大值必须来自同一批样本，避免 AvgRuntime 只看 sample
		// 而 MaxRuntime 仍混入 failed runtime 历史字段，造成口径不一致。
		runtimeCount := bucket.ProcessedCount
		runtimeTotal := bucket.ProcessedRuntimeTotalMS
		runtimeMax := bucket.ProcessedRuntimeMaxMS
		if runtimeCount > 0 {
			item.AvgRuntime = queueAvailableIntMetric(runtimeTotal/runtimeCount, goprocess.UnitMilliseconds)
			item.MaxRuntime = queueAvailableIntMetric(runtimeMax, goprocess.UnitMilliseconds)
		} else {
			item.AvgRuntime = queueUnavailableMetric(goprocess.UnitMilliseconds, "queue runtime metrics unavailable")
			item.MaxRuntime = queueUnavailableMetric(goprocess.UnitMilliseconds, "queue runtime metrics unavailable")
		}
		item.Throughput = queueAvailableIntMetric(bucket.Processed, goprocess.UnitCount)
		item.Processed = item.Throughput
		item.Failed = queueAvailableIntMetric(bucket.Failed, goprocess.UnitCount)
		item.Released = queueAvailableIntMetric(bucket.Released, goprocess.UnitCount)
		item.SampledAt = newerTime(item.SampledAt, source.lengths.CapturedAt)
	} else if cfg.Observability.Enabled(ObservabilityEventMetrics) {
		item.AvgRuntime = queueUnavailableMetric(goprocess.UnitMilliseconds, "queue runtime metrics unavailable")
		item.MaxRuntime = queueUnavailableMetric(goprocess.UnitMilliseconds, "queue runtime metrics unavailable")
		item.Throughput = queueUnavailableMetric(goprocess.UnitCount, "queue throughput metrics unavailable")
		item.Processed = item.Throughput
		item.Failed = queueUnavailableMetric(goprocess.UnitCount, "queue failed metrics unavailable")
		item.Released = queueUnavailableMetric(goprocess.UnitCount, "queue released metrics unavailable")
	} else {
		item.AvgRuntime = queueDisabledMetric(goprocess.UnitMilliseconds, "event_metrics disabled")
		item.MaxRuntime = queueDisabledMetric(goprocess.UnitMilliseconds, "event_metrics disabled")
		item.Throughput = queueDisabledMetric(goprocess.UnitCount, "event_metrics disabled")
		item.Processed = item.Throughput
		item.Failed = queueDisabledMetric(goprocess.UnitCount, "event_metrics disabled")
		item.Released = queueDisabledMetric(goprocess.UnitCount, "event_metrics disabled")
	}

	if wait, ok := queueWaitByAggregateKey(source.waits, key); ok {
		switch wait.Status {
		case QueueWaitKnown:
			item.WaitTime = queueAvailableIntMetric(wait.WaitMS, goprocess.UnitMilliseconds)
		case QueueWaitUnsupported:
			item.WaitTime = queueUnsupportedMetric(goprocess.UnitMilliseconds, "queue wait metrics unsupported")
		default:
			item.WaitTime = queueUnavailableMetric(goprocess.UnitMilliseconds, "queue wait metrics unavailable")
		}
		item.SampledAt = newerTime(item.SampledAt, wait.SampledAt)
	} else if cfg.Observability.Enabled(ObservabilityWaits) {
		item.WaitTime = queueUnavailableMetric(goprocess.UnitMilliseconds, "queue wait metrics unavailable")
	} else {
		item.WaitTime = queueDisabledMetric(goprocess.UnitMilliseconds, "waits disabled")
	}
	return item
}

func queueLengthBucketByKey(items []QueueLengthBucket, key queueAggregateKey) (QueueLengthBucket, bool) {
	for _, item := range items {
		if item.Connection == key.Connection && item.Queue == key.Queue {
			return item, true
		}
	}
	return QueueLengthBucket{}, false
}

func metricsBucketByKey(items []MetricsBucketSnapshot, key queueAggregateKey) (MetricsBucketSnapshot, bool) {
	for _, item := range items {
		if item.Connection == key.Connection && item.Queue == key.Queue {
			return item, true
		}
	}
	return MetricsBucketSnapshot{}, false
}

func queueWaitByAggregateKey(items []QueueWaitSnapshot, key queueAggregateKey) (QueueWaitSnapshot, bool) {
	for _, item := range items {
		if item.Connection == key.Connection && item.Queue == key.Queue {
			return item, true
		}
	}
	return QueueWaitSnapshot{}, false
}

func queueAvailableIntMetric(value int64, unit string) goprocess.Metric {
	return goprocess.Metric{Value: value, Unit: unit, Status: goprocess.StatusAvailable, Reason: ""}
}

func queueDisabledMetric(unit string, reason string) goprocess.Metric {
	return goprocess.Metric{Value: nil, Unit: unit, Status: goprocess.StatusDisabled, Reason: reason}
}

func queueUnsupportedMetric(unit string, reason string) goprocess.Metric {
	return goprocess.Metric{Value: nil, Unit: unit, Status: goprocess.StatusUnsupported, Reason: reason}
}

func queueUnavailableMetric(unit string, reason string) goprocess.Metric {
	return goprocess.Metric{Value: nil, Unit: unit, Status: goprocess.StatusUnavailable, Reason: reason}
}

func queueUnsupportedMemoryMetric() goprocess.Metric {
	return queueUnsupportedMetric(goprocess.UnitBytes, "memory metrics are not recorded by the current queue event model")
}

func newerTime(current time.Time, candidate time.Time) time.Time {
	if candidate.After(current) {
		return candidate
	}
	return current
}
