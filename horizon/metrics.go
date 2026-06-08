package horizon

import (
	"strings"
	"time"
)

const maxErrorSummaryLength = 512

// MetricsCounters 保存 Horizon metrics 的聚合计数。
//
// 需求背景：issue 03 要求 metrics 至少记录 queued、processed、released、failed、poison envelope
// 以及 consumer 生命周期计数；这些字段既用于全局汇总，也用于 connection+queue bucket。
type MetricsCounters struct {
	// Queued 是 queue.job_queued 事件数量。
	Queued int64 `json:"queued"`
	// Processed 是 queue.job_processed 事件数量。
	Processed int64 `json:"processed"`
	// Released 是 queue.job_released 事件数量。
	Released int64 `json:"released"`
	// Failed 是 queue.job_failed 事件数量。
	Failed int64 `json:"failed"`
	// PoisonEnvelopes 是 queue.poison_envelope 事件数量。
	PoisonEnvelopes int64 `json:"poison_envelopes"`
	// ConsumerStarted 是 queue.consumer_started 事件数量。
	ConsumerStarted int64 `json:"consumer_started"`
	// ConsumerStopped 是 queue.consumer_stopped 事件数量。
	ConsumerStopped int64 `json:"consumer_stopped"`
	// ConsumerStopFailed 是 queue.consumer_stop_failed 事件数量。
	ConsumerStopFailed int64 `json:"consumer_stop_failed"`
}

// MetricsBucketSnapshot 表示单个 connection+queue 维度的 metrics 快照。
type MetricsBucketSnapshot struct {
	// Connection 是 queue connection 名称。
	Connection string `json:"connection"`
	// Queue 是 queue 名称；consumer 事件缺少队列时使用 _unknown。
	Queue string `json:"queue"`
	MetricsCounters
	// ProcessedCount 是参与 processed runtime 统计的任务数量。
	ProcessedCount int64 `json:"processed_count"`
	// ProcessedRuntimeTotalMS 是 processed runtime 总耗时毫秒数。
	ProcessedRuntimeTotalMS int64 `json:"processed_runtime_total_ms"`
	// ProcessedRuntimeMaxMS 是 processed runtime 最大耗时毫秒数。
	ProcessedRuntimeMaxMS int64 `json:"processed_runtime_max_ms"`
	// FailedRuntimeTotalMS 是 failed runtime 总耗时毫秒数。
	FailedRuntimeTotalMS int64 `json:"failed_runtime_total_ms"`
	// FailedRuntimeMaxMS 是 failed runtime 最大耗时毫秒数。
	FailedRuntimeMaxMS int64 `json:"failed_runtime_max_ms"`
}

// PoisonEnvelopeSummary 是 poison envelope 的安全展示摘要。
//
// 安全边界：该结构明确不包含 BodyBase64、原始 body、queue.Envelope 或 broker 凭据。
type PoisonEnvelopeSummary struct {
	// ID 是 Horizon metrics 内部摘要 ID。
	ID string `json:"id"`
	// Connection 是 queue connection 名称。
	Connection string `json:"connection"`
	// Driver 是触发 poison envelope 的 queue driver。
	Driver string `json:"driver"`
	// Queue 是收到坏消息的队列名。
	Queue string `json:"queue"`
	// Status 是 Horizon 对该 poison envelope 的记录状态。
	Status string `json:"status"`
	// Action 是 driver 对坏消息采取的终结动作。
	Action string `json:"action"`
	// BodySize 是原始 body 字节数，仅用于容量和排障判断。
	BodySize int `json:"body_size"`
	// BodyTruncated 表示 driver 是否只暴露了原始 body 前缀。
	BodyTruncated bool `json:"body_truncated"`
	// ErrorSummary 是截断后的错误摘要。
	ErrorSummary string `json:"error_summary"`
	// OccurredAt 是 poison envelope 事件发生时间。
	OccurredAt time.Time `json:"occurred_at"`
}

// MetricsSnapshot 是 Horizon collector/flusher 生成并由 Store 持久化的 metrics 展示模型。
//
// 设计思路：该模型只包含事件派生的安全摘要和聚合计数，不包含 queue payload、queue.Envelope、
// poison body 或 queue length；队列长度会在后续 issue 04 单独扩展。
type MetricsSnapshot struct {
	// CapturedAt 是本次 snapshot 生成时间。
	CapturedAt time.Time `json:"captured_at"`
	// Buckets 按 connection+queue 拆分保存聚合计数。
	Buckets []MetricsBucketSnapshot `json:"buckets"`
	// Totals 是从全部 bucket 推导出的全局汇总。
	Totals MetricsCounters `json:"totals"`
	// PoisonEnvelopes 保存可展示的 poison envelope 安全摘要。
	PoisonEnvelopes []PoisonEnvelopeSummary `json:"poison_envelopes"`
	// QueueWaits 保存 connection:queue 等待时间能力和观测值。
	QueueWaits []QueueWaitSnapshot `json:"queue_waits"`
	// Batches 保存 BatchEvent 派生出的只读批次摘要，不包含批次内 job payload。
	Batches []BatchSummary `json:"batches"`
}

func durationMS(value time.Duration) int64 {
	if value <= 0 {
		return 0
	}
	return int64(value / time.Millisecond)
}

func truncateSummary(value string) string {
	if len(value) <= maxErrorSummaryLength {
		return value
	}
	return value[:maxErrorSummaryLength]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func splitQueueWaitKey(key string) (string, string) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return key, ""
}
