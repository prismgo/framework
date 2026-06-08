package horizon

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/prismgo/framework/event"
	goprocess "github.com/prismgo/framework/process"
	"github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/queue/payload"
)

// TestCollectorNonBlockingHotPath 验证 Collect 在 buffer 满时不阻塞调用方。
//
// 需求背景：historical scenario 34 要求 collector 事件接收路径为非阻塞，不能在 worker 热路径执行 Store 写入或同步等待。
func TestCollectorNonBlockingHotPath(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 10
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()
	input := CollectorInput{Event: "queue.job_processed", Connection: "redis", Queue: "default"}

	// 发送远超 buffer 容量的事件，不应阻塞
	var wg sync.WaitGroup
	wg.Add(1000)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			wg.Done()
			_ = coll.Collect(ctx, input)
		}
		close(done)
	}()

	// 等待所有发送完成或超时
	select {
	case <-done:
		// 成功：所有发送均未阻塞
	case <-time.After(5 * time.Second):
		t.Fatal("Collect blocked on full buffer")
	}
}

// TestCollectorBoundedBuffer 验证 buffer 容量受配置控制，不无限增长。
//
// 需求背景：historical scenario 34 要求 collector 使用 bounded buffer，容量受配置控制，不能无限增长。
func TestCollectorBoundedBuffer(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 5
	coll := newCollector(cfg)

	if cap(coll.buffer) != 5 {
		t.Fatalf("expected buffer capacity 5, got %d", cap(coll.buffer))
	}
}

func TestCollectorMemoryEstimateReportsAvailableBytes(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 5
	coll := newCollector(cfg)

	coll.mu.Lock()
	coll.windows["manual"] = &eventMetricsWindow{connection: "redis", queue: "default", jobName: "Job"}
	coll.aggKeys["redis:default:Job"] = &aggregateKeyState{key: "redis:default:Job", lastActive: time.Now()}
	coll.queued["job-1"] = queuedJobCollectorState{connection: "redis", queue: "default", jobID: "job-1", jobName: "Job"}
	coll.drops["buffer_full"] = 2
	coll.mu.Unlock()

	metric := coll.MemoryEstimate()
	if metric.Status != goprocess.StatusAvailable || metric.Unit != goprocess.UnitBytes {
		t.Fatalf("collector memory metric = %#v", metric)
	}
	got, ok := metric.Value.(int64)
	if !ok {
		t.Fatalf("collector memory value type = %T", metric.Value)
	}
	want := int64(unsafe.Sizeof(*coll))
	want += int64(cap(coll.buffer)) * int64(unsafe.Sizeof(collectorItem{}))
	want += int64(len(coll.windows)) * (int64(unsafe.Sizeof("")+unsafe.Sizeof(&eventMetricsWindow{})) + 16)
	want += int64(len(coll.windows)) * int64(unsafe.Sizeof(eventMetricsWindow{}))
	want += int64(len(coll.aggKeys)) * (int64(unsafe.Sizeof("")+unsafe.Sizeof(&aggregateKeyState{})) + 16)
	want += int64(len(coll.aggKeys)) * int64(unsafe.Sizeof(aggregateKeyState{}))
	want += int64(len(coll.queued)) * (int64(unsafe.Sizeof("")+unsafe.Sizeof(queuedJobCollectorState{})) + 16)
	want += int64(len(coll.drops)) * (int64(unsafe.Sizeof("")+unsafe.Sizeof(int64(0))) + 16)
	want += int64(cap(coll.details)) * int64(unsafe.Sizeof(HighValueJobDetail{}))
	want += int64(cap(coll.batchSummaries)) * int64(unsafe.Sizeof(BatchSummary{}))
	want += int64(cap(coll.rtSamples)) * int64(unsafe.Sizeof(int64(0)))
	if got != want {
		t.Fatalf("collector memory estimate=%d, want %d", got, want)
	}
}

// TestCollectorSampling 验证 event_metrics 采样发生在 collector 入口。
//
// 需求背景：historical scenario 34 要求被采样掉的 queue event 不参与 event_metrics 内存聚合。
func TestCollectorSampling(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 1000
	cfg.EventMetricsSampleRate = 0.0 // 全部不采样
	cfg.EventMetrics = true
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()
	input := CollectorInput{
		Event:      "queue.job_processed",
		Connection: "redis",
		Queue:      "default",
		Sampling: SamplingDecision{
			EventMetricsSampled:    false,
			EventMetricsSampleRate: 0.0,
		},
	}

	// 发送多个事件
	for i := 0; i < 100; i++ {
		_ = coll.Collect(ctx, input)
	}

	// 等待后台处理
	time.Sleep(100 * time.Millisecond)

	// 检查聚合状态：不应该有 event_metrics 数据
	snapshot := coll.FlushSnapshot(time.Now())
	totalIncrements := 0
	for _, w := range snapshot.windows {
		totalIncrements += int(w.processed)
	}
	if totalIncrements > 0 {
		t.Errorf("expected 0 event_metrics increments with sample_rate=0, got %d", totalIncrements)
	}
}

// TestCollectorEventMetricsAggregation 验证 event_metrics 窗口聚合行为。
//
// 需求背景：historical scenario 34 要求 event_metrics 使用 window 聚合，记录窗口边界、来源维度。
func TestCollectorEventMetricsAggregation(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 1000
	cfg.EventMetricsSampleRate = 1.0 // 全部采样
	cfg.EventMetrics = true
	cfg.MetricsWindow = time.Minute
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()
	now := time.Now()

	for i := 0; i < 10; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
			JobName:    "SendEmail",
			Runtime:    100 * time.Millisecond,
			OccurredAt: now,
			Sampling: SamplingDecision{
				EventMetricsSampled:    true,
				EventMetricsSampleRate: 1.0,
			},
		})
	}

	// 等待后台处理
	time.Sleep(200 * time.Millisecond)

	snapshot := coll.FlushSnapshot(now.Add(time.Minute))
	if len(snapshot.windows) == 0 {
		t.Fatal("expected at least one event_metrics window")
	}

	found := false
	for _, w := range snapshot.windows {
		if w.jobName == "SendEmail" && w.connection == "redis" && w.queue == "default" {
			found = true
			if w.processed != 10 {
				t.Errorf("expected 10 processed, got %d", w.processed)
			}
		}
	}
	if !found {
		t.Error("expected window for SendEmail/redis/default")
	}
}

// TestCollectorAggregateKeyLimit 验证聚合 key 上限触发后 overflow 行为。
//
// 需求背景：historical scenario 34 要求 aggregate key 达到上限后，已存在 key 继续更新；新 key 写入 _overflow 聚合桶。
//
// 测试步骤：
//  1. 设置 MaxAggregateKeys=2，只允许 2 个不同的 job 维度 key
//  2. 发送 3 种不同 job（JobA/JobB/JobC）的事件各 5 次
//  3. 验证前两种 job 的 key 正常更新，第三种 job 写入 _overflow 桶
//  4. 验证 aggregate_key_overflow 诊断记录
func TestCollectorAggregateKeyLimit(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 1000
	cfg.EventMetricsSampleRate = 1.0
	cfg.EventMetrics = true
	cfg.MaxAggregateKeys = 2
	cfg.AggregateKeyTTL = time.Hour
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()
	now := time.Now()

	// 发送 3 种不同 job 的事件：前两种进入常规窗口，第三种因 key 上限触发 overflow
	jobs := []string{"JobA", "JobB", "JobC"}
	for i := 0; i < 5; i++ {
		for _, job := range jobs {
			_ = coll.Collect(ctx, CollectorInput{
				Event:      "queue.job_processed",
				Connection: "redis",
				Queue:      "default",
				JobName:    job,
				Runtime:    50 * time.Millisecond,
				OccurredAt: now,
				Sampling: SamplingDecision{
					EventMetricsSampled:    true,
					EventMetricsSampleRate: 1.0,
				},
			})
		}
	}

	time.Sleep(200 * time.Millisecond)
	snapshot := coll.FlushSnapshot(now.Add(time.Minute))

	// 检查是否有 _overflow 桶
	hasOverflow := false
	for _, w := range snapshot.windows {
		if w.jobName == "_overflow" {
			hasOverflow = true
		}
	}
	if !hasOverflow {
		t.Error("expected _overflow bucket when aggregate keys exceed limit")
	}

	// 检查 aggregate_key_overflow 诊断
	hasOverflowDiag := false
	for _, d := range snapshot.diags {
		if d.Reason == MemoryDropAggregateOverflow {
			hasOverflowDiag = true
		}
	}
	if !hasOverflowDiag {
		t.Error("expected aggregate_key_overflow diagnostic")
	}
}

// TestCollectorWaitsAndLongWait 验证 waits/long_wait 由新 collector 管理。
//
// 需求背景：historical scenario 34 要求 waits/long wait 由新 collector/aggregator 迁移为 event_metrics window 聚合和诊断能力。
func TestCollectorWaitsComputesFromQueuedState(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 1000
	cfg.EventMetrics = true
	cfg.Waits = true
	cfg.QueuedWaitsMax = 100
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()
	queuedAt := time.Now().Add(-120 * time.Second) // 2 分钟前入队

	// 入队事件（有显式 queued_at）
	_ = coll.Collect(ctx, CollectorInput{
		Event:      "queue.job_queued",
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-wait-1",
		JobName:    "SlowJob",
		OccurredAt: queuedAt,
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 1.0,
		},
	})

	time.Sleep(100 * time.Millisecond)

	thresholds := map[string]int{"redis:default": 60} // 60 秒阈值
	waits := coll.ComputeWaits(thresholds, time.Now())

	if len(waits) == 0 {
		t.Fatal("expected at least one wait snapshot")
	}
	if waits[0].Status != QueueWaitKnown {
		t.Errorf("expected known status, got %s", waits[0].Status)
	}
	if waits[0].LongWait != true {
		t.Error("expected long_wait=true since wait exceeds 60s threshold")
	}
}

// TestCollectorQueuedWaitMemoryBound 验证 queued wait 状态受内存上限约束。
//
// 需求背景：historical scenario 34 要求 queued wait 状态必须受结构化内存上限约束，超过上限时记录 degraded/unknown。
func TestCollectorQueuedWaitMemoryBound(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 1000
	cfg.EventMetrics = true
	cfg.Waits = true
	cfg.QueuedWaitsMax = 3
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()
	now := time.Now()

	// 发送超过上限的入队事件
	for i := 0; i < 10; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_queued",
			Connection: "redis",
			Queue:      "default",
			JobID:      "job-" + string(rune('a'+i)),
			JobName:    "TestJob",
			OccurredAt: now,
			Sampling: SamplingDecision{
				EventMetricsSampled:    true,
				EventMetricsSampleRate: 1.0,
			},
		})
	}

	time.Sleep(100 * time.Millisecond)

	coll.mu.Lock()
	queuedCount := len(coll.queued)
	coll.mu.Unlock()

	if queuedCount > cfg.QueuedWaitsMax {
		t.Errorf("queued count %d exceeds limit %d", queuedCount, cfg.QueuedWaitsMax)
	}
}

// TestCollectorHighValueDetailCollection 验证高价值明细采集。
//
// 需求背景：historical scenario 34 要求 high-value detail 由新 collector 管理，不得依赖旧 Monitor。
func TestCollectorHighValueDetailCollection(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 1000
	cfg.FailedDetailEnabled = true
	cfg.PoisonDetailEnabled = true
	cfg.SlowJobDetailEnabled = true
	cfg.SlowJobThreshold = 30 * time.Second
	// 高价值明细采样率
	rate := 1.0
	cfg.HighValueDetailSampleRate = &rate

	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()
	now := time.Now()

	// 发送失败事件
	_ = coll.Collect(ctx, CollectorInput{
		Event:        "queue.job_failed",
		Connection:   "redis",
		Queue:        "default",
		JobID:        "job-failed-1",
		JobName:      "FailingJob",
		ErrorSummary: "connection refused",
		OccurredAt:   now,
		Sampling: SamplingDecision{
			HighValueDetailSampled: true,
			HighValueDetailRate:    1.0,
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 1.0,
		},
	})

	time.Sleep(100 * time.Millisecond)
	snapshot := coll.FlushSnapshot(now.Add(time.Minute))

	foundFailedDetail := false
	for _, detail := range snapshot.details {
		if detail.Kind == HighValueDetailFailed && detail.JobID == "job-failed-1" {
			foundFailedDetail = true
			if detail.ErrorSummary != "connection refused" {
				t.Errorf("expected error summary 'connection refused', got %q", detail.ErrorSummary)
			}
		}
	}
	if !foundFailedDetail {
		t.Error("expected failed job high-value detail")
	}
}

// TestCollectorDropOnFullBuffer 验证 buffer 满时丢弃行为。
//
// 需求背景：historical scenario 34/35 要求 buffer 满时记录丢弃，不得阻塞 worker。
func TestCollectorDropOnFullBuffer(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 2
	cfg.DropPolicy = ObservabilityDropNewest
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()

	// 填满 buffer
	for i := 0; i < 5; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
		})
	}

	time.Sleep(100 * time.Millisecond)
	snapshot := coll.FlushSnapshot(time.Now())

	if snapshot.drops[MemoryDropBufferFull] == 0 {
		t.Error("expected buffer_full drop to be recorded")
	}
}

// TestCollectorDropOldestPolicy 验证 drop_oldest 策略。
//
// 需求背景：drop_oldest 策略丢弃最旧事件腾出空间给新事件。
func TestCollectorDropOldestPolicy(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 2
	cfg.DropPolicy = ObservabilityDropOldest
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
		})
	}

	time.Sleep(100 * time.Millisecond)

	// drop_oldest 下 buffer 满后丢弃最旧，放入最新
	// 应记录 buffer_full drop
	snapshot := coll.FlushSnapshot(time.Now())
	if snapshot.drops[MemoryDropBufferFull] == 0 {
		t.Error("expected buffer_full drop count > 0 with drop_oldest policy")
	}
}

func TestCollectorRateLimitRecordsDegradedDrop(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 100
	cfg.MaxEventsPerSecond = 2
	coll := newCollector(cfg)

	for i := 0; i < 5; i++ {
		_ = coll.Collect(context.Background(), CollectorInput{
			Event:      queue.EventJobProcessed,
			Connection: "redis",
			Queue:      "default",
		})
	}

	snapshot := coll.FlushSnapshot(time.Now())
	if snapshot.drops[MemoryDropRateLimited] == 0 {
		t.Fatalf("expected rate_limited drops, got %#v", snapshot.drops)
	}
	if !snapshot.degraded || snapshot.degradedReason != "drops_detected" {
		t.Fatalf("expected rate limit to mark snapshot degraded, got %#v", snapshot)
	}
}

// TestCollectorShutdownBestEffort 验证 shutdown 路径排空 buffer。
//
// 需求背景：historical scenario 34 要求 shutdown/cancel 路径支持 best-effort flush。
func TestCollectorShutdownDrainsBuffer(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 100
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1.0
	coll := newCollector(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	coll.Start(ctx)

	// 发送事件
	for i := 0; i < 10; i++ {
		_ = coll.Collect(context.Background(), CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
			JobName:    "ShutdownJob",
			Sampling: SamplingDecision{
				EventMetricsSampled:    true,
				EventMetricsSampleRate: 1.0,
			},
		})
	}

	// 触发 shutdown
	cancel()
	coll.Stop()

	// shutdown 后 buffer 应排空
	snapshot := coll.FlushSnapshot(time.Now())
	totalProcessed := int64(0)
	for _, w := range snapshot.windows {
		totalProcessed += w.processed
	}
	if totalProcessed != 10 {
		t.Errorf("expected 10 processed events drained on shutdown, got %d", totalProcessed)
	}
}

// TestCollectorMemoryState 验证内存状态暴露。
//
// 需求背景：historical scenario 34 要求结构化容量上限状态暴露给读模型。
func TestCollectorMemoryState(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 100
	cfg.SampleReservoirSize = 50
	cfg.MaxAggregateKeys = 20
	coll := newCollector(cfg)

	state := coll.MemoryState()
	if state.BufferSize != 100 {
		t.Errorf("expected BufferSize 100, got %d", state.BufferSize)
	}
	if state.SampleReservoirSize != 50 {
		t.Errorf("expected SampleReservoirSize 50, got %d", state.SampleReservoirSize)
	}
	if state.MaxAggregateKeys != 20 {
		t.Errorf("expected MaxAggregateKeys 20, got %d", state.MaxAggregateKeys)
	}
}

// TestNilCollector 验证 nil collector 安全。
func TestNilCollector(t *testing.T) {
	var coll *collector
	if err := coll.Collect(context.Background(), CollectorInput{}); err != nil {
		t.Error("nil collector Collect should return nil")
	}
	if snap := coll.FlushSnapshot(time.Now()); snap != nil {
		t.Error("nil collector FlushSnapshot should return nil")
	}
}

func observabilityPresetConfigOrFull() ObservabilityConfig {
	cfg, _ := observabilityPresetConfig(ObservabilityPresetFull)
	return normalizeObservabilityConfig(cfg)
}

// TestShouldSample 验证采样判断函数。
func TestShouldSample(t *testing.T) {
	if !shouldSample(1.0) {
		t.Error("rate=1.0 should always sample")
	}
	if shouldSample(0.0) {
		t.Error("rate=0.0 should never sample")
	}
	// rate=0.5: 多次调用应该有大概一半命中
	hits := 0
	for i := 0; i < 10000; i++ {
		if shouldSample(0.5) {
			hits++
		}
	}
	if hits == 0 || hits == 10000 {
		t.Errorf("rate=0.5 should have ~50%% hits, got %d/10000", hits)
	}
}

// TestCollectorInputFromEvent 验证队列事件到 CollectorInput 的转换。
func TestCollectorInputFromEvent(t *testing.T) {
	obs := observabilityPresetConfigOrFull()
	obs.EventMetricsSampleRate = 1.0

	tests := []struct {
		name  string
		event interface{}
	}{
		{"JobQueued", queue.JobQueued{Connection: "redis", Queue: "default", JobID: "j1", JobName: "Test"}},
		{"JobProcessing", queue.JobProcessing{Connection: "redis", Queue: "default", JobID: "j1", JobName: "Test", Attempts: 1}},
		{"JobProcessed", queue.JobProcessed{Connection: "redis", Queue: "default", JobID: "j1", JobName: "Test"}},
		{"JobReleased", queue.JobReleased{Connection: "redis", Queue: "default", JobID: "j1", JobName: "Test"}},
		{"JobFailed", queue.JobFailed{FailedJob: payload.FailedJob{Connection: "redis", Queue: "default", JobID: "j1", JobName: "Test"}}},
		{"PoisonEnvelope", queue.PoisonEnvelope{Connection: "redis", Queue: "default"}},
		{"InfrastructureEvent", queue.InfrastructureEvent{EventName: "consumer_started", Connection: "redis", Queue: "default"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := collectorInputFromEvent(tt.event.(event.Event), obs)
			if input.Event == "" {
				t.Errorf("expected non-empty Event for %s", tt.name)
			}
		})
	}

	// nil event
	input := collectorInputFromEvent(nil, obs)
	if input.Event != "" {
		t.Error("nil event should produce empty input")
	}
}

func TestCollectorMissingSupervisorSourceKeepsWindowAndRecordsDiagnostic(t *testing.T) {
	// 需求背景：historical scenario 43 要求缺少 Horizon worker runtime supervisor 上下文时，事件仍进入
	// event_metrics；SourceSupervisor 保持空字符串，并记录稳定诊断，不能从 queue/host/env/config 推断。
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1
	cfg.QueuedWaitsMax = 0
	coll := newCollector(cfg)
	now := time.Now().Truncate(time.Minute)

	coll.processItem(collectorItem{input: CollectorInput{
		Event:             queue.EventJobProcessed,
		Connection:        "redis",
		Queue:             "supervisor-looking-queue",
		JobName:           "EmailJob",
		SourcePrefix:      "prefix-a",
		SourceHost:        "host-a",
		SourceEnvironment: "production",
		OccurredAt:        now,
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 1,
		},
	}, receivedAt: now})

	snapshot := coll.FlushSnapshot(now.Add(time.Minute))
	if snapshot == nil || len(snapshot.windows) != 1 {
		t.Fatalf("expected event_metrics window despite missing supervisor, got %#v", snapshot)
	}
	window := snapshot.windows[0]
	if window.supervisor != "" {
		t.Fatalf("collector must not infer supervisor from other dimensions, got %#v", window)
	}
	found := false
	for _, diagnostic := range snapshot.diags {
		if diagnostic.Reason == EventMetricsSourceSupervisorUnknown {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing supervisor diagnostic not recorded: %#v", snapshot.diags)
	}
}

// TestCollectorRuntimeSamples 验证 runtime 样本池访问。
func TestCollectorRuntimeSamples(t *testing.T) {
	var nilColl *collector
	if samples := nilColl.RuntimeSamples(); samples != nil {
		t.Error("nil collector RuntimeSamples should return nil")
	}

	cfg := observabilityPresetConfigOrFull()
	cfg.SampleReservoirSize = 10
	coll := newCollector(cfg)
	samples := coll.RuntimeSamples()
	if len(samples) != 0 {
		t.Error("empty collector RuntimeSamples should be empty slice")
	}
}

// TestCollectorReservoirSampling 验证 runtime 样本池满后会按 reservoir sampling 替换任意槽位。
func TestCollectorReservoirSampling(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 1000
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1.0
	cfg.SampleReservoirSize = 3
	coll := newCollector(cfg)
	// 固定随机序列：
	// - 第 4 个样本先命中进入概率，再在 [0,3) 中抽到槽位 1，必须替换非 0 槽位；
	// - 第 5 个样本不命中进入概率，不进入 reservoir。
	coll.sampler = &sequenceSampler{values: []float64{0.26, 0.50, 0.99}}

	coll.mu.Lock()
	for _, runtimeMS := range []int64{100, 101, 102, 200, 300} {
		coll.recordRuntimeSampleLocked(runtimeMS)
	}
	got := append([]int64(nil), coll.rtSamples...)
	coll.mu.Unlock()

	if len(got) != cfg.SampleReservoirSize {
		t.Fatalf("samples count %d, want %d", len(got), cfg.SampleReservoirSize)
	}
	want := []int64{100, 200, 102}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reservoir samples = %#v, want %#v", got, want)
		}
	}
}

// TestCollectorCleanupExpiredKeys 验证聚合 key TTL 清理。
func TestCollectorCleanupExpiredKeys(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.MaxAggregateKeys = 100
	cfg.AggregateKeyTTL = 50 * time.Millisecond
	coll := newCollector(cfg)

	coll.mu.Lock()
	coll.aggKeys["test:key:A"] = &aggregateKeyState{key: "test:key:A", lastActive: time.Now().Add(-time.Hour)}
	coll.aggKeys["test:key:B"] = &aggregateKeyState{key: "test:key:B", lastActive: time.Now()}
	coll.mu.Unlock()

	coll.cleanupExpiredKeysLocked(time.Now())

	coll.mu.Lock()
	if _, ok := coll.aggKeys["test:key:A"]; ok {
		t.Error("expired key should be cleaned up")
	}
	if _, ok := coll.aggKeys["test:key:B"]; !ok {
		t.Error("active key should not be cleaned up")
	}
	coll.mu.Unlock()
}

func TestCollectorBatchSummaryLimitDeduplicatesAndRecordsDrop(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BatchSummarySize = 2
	coll := newCollector(cfg)
	now := time.Now()

	for _, input := range []CollectorInput{
		{Event: "queue.batch_updated", OccurredAt: now, BatchSummary: BatchSummary{ID: "batch-1", Status: BatchStatusRunning}},
		{Event: "queue.batch_updated", OccurredAt: now.Add(time.Second), BatchSummary: BatchSummary{ID: "batch-2", Status: BatchStatusRunning}},
		{Event: "queue.batch_updated", OccurredAt: now.Add(2 * time.Second), BatchSummary: BatchSummary{ID: "batch-1", Status: BatchStatusFinished}},
		{Event: "queue.batch_updated", OccurredAt: now.Add(3 * time.Second), BatchSummary: BatchSummary{ID: "batch-3", Status: BatchStatusRunning}},
	} {
		coll.processItem(collectorItem{input: input, receivedAt: input.OccurredAt})
	}

	snapshot := coll.FlushSnapshot(now.Add(time.Minute))
	if len(snapshot.batchSummaries) != 2 {
		t.Fatalf("expected batch summaries capped at 2, got %#v", snapshot.batchSummaries)
	}
	if snapshot.batchSummaries[0].ID != "batch-1" || snapshot.batchSummaries[0].Status != BatchStatusFinished {
		t.Fatalf("same batch ID should keep latest status, got %#v", snapshot.batchSummaries[0])
	}
	if snapshot.drops[MemoryDropBatchSummaryLimit] != 1 {
		t.Fatalf("expected batch summary limit drop, got %#v", snapshot.drops)
	}
	if !snapshot.degraded || snapshot.degradedReason != "drops_detected" {
		t.Fatalf("snapshot should be degraded after batch summary drop, got degraded=%v reason=%q", snapshot.degraded, snapshot.degradedReason)
	}
}

// TestCollectorProcessLoopRecoversFromPanic 验证 processLoop 内部 panic 被 recover 捕获，
// 主循环记录 collector_panic 后自动重启，后续事件仍能进入 flush snapshot。
func TestCollectorProcessLoopRecoversFromPanic(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1

	coll := newCollector(cfg)
	// 故意置空 windows 触发 nil map panic
	coll.mu.Lock()
	coll.windows = nil
	coll.mu.Unlock()

	ctx := context.Background()
	coll.Start(ctx)

	// 发送事件触发 processItem
	_ = coll.Collect(ctx, CollectorInput{
		Event:      "queue.job_processed",
		Connection: "redis",
		Queue:      "default",
		SourceHost: "test-host",
		OccurredAt: time.Now(),
		Sampling:   SamplingDecision{EventMetricsSampled: true},
	})

	// 等待 processLoop 处理事件并 panic
	deadline := time.Now().Add(time.Second)
	for {
		coll.mu.Lock()
		panicDropCount := coll.drops[MemoryDropCollectorPanic]
		coll.mu.Unlock()
		if panicDropCount > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for collector_panic drop")
		}
		time.Sleep(10 * time.Millisecond)
	}

	coll.mu.Lock()
	coll.windows = make(map[string]*eventMetricsWindow)
	coll.mu.Unlock()
	_ = coll.Collect(ctx, CollectorInput{
		Event:      "queue.job_processed",
		Connection: "redis",
		Queue:      "default",
		SourceHost: "test-host",
		OccurredAt: time.Now(),
		Sampling:   SamplingDecision{EventMetricsSampled: true},
	})

	var snapshot *flushSnapshot
	deadline = time.Now().Add(time.Second)
	for {
		coll.mu.Lock()
		windowCount := len(coll.windows)
		panicDropCount := coll.drops[MemoryDropCollectorPanic]
		coll.mu.Unlock()
		if windowCount > 0 && panicDropCount > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for restarted collector to process event")
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot = coll.FlushSnapshot(time.Now())
	coll.Stop()

	if snapshot.drops[MemoryDropCollectorPanic] == 0 {
		t.Fatal("processLoop panic should be recorded as collector_panic drop")
	}
	foundDescription := false
	for _, diag := range snapshot.diags {
		if diag.Reason == MemoryDropCollectorPanic && strings.Contains(diag.Description, "nil map") {
			foundDescription = true
			break
		}
	}
	if !foundDescription {
		t.Fatalf("panic diagnostic should contain cause, got %#v", snapshot.diags)
	}
}
