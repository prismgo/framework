package horizon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
)

func TestPoisonHighValueDetailRetainsOnlySafeEnvelopeSummary(t *testing.T) {
	// 需求背景：poison envelope 的 high-value detail 只能保留安全摘要字段，不能保存 raw body 或 BodyBase64。
	cfg := observabilityPresetConfigOrFull()
	cfg.PoisonDetailEnabled = true
	rate := 1.0
	cfg.HighValueDetailSampleRate = &rate
	coll := newCollector(cfg)

	input := collectorInputFromEvent(queue.PoisonEnvelope{
		Connection:    "rabbitmq-primary",
		Driver:        "rabbitmq",
		Queue:         "critical",
		Action:        queue.PoisonEnvelopeActionReject,
		Error:         "decode envelope: invalid character",
		BodyBase64:    "eyJzZWNyZXQiOiJkb25vdC1sZWFrIn0=",
		BodyEncoding:  "base64",
		BodySize:      4097,
		BodyTruncated: true,
		Timestamp:     time.Date(2026, 5, 15, 9, 0, 0, 0, time.UTC),
	}, cfg)
	input.Sampling.HighValueDetailSampled = true

	coll.processItem(collectorItem{input: input, receivedAt: input.OccurredAt})
	snapshot := coll.FlushSnapshot(input.OccurredAt.Add(time.Second))

	if len(snapshot.details) != 1 {
		t.Fatalf("expected one poison high-value detail, got %#v", snapshot.details)
	}
	detail := snapshot.details[0]
	if detail.Kind != HighValueDetailPoison || detail.Connection != "rabbitmq-primary" || detail.Queue != "critical" {
		t.Fatalf("poison detail was not classified with stable dimensions: %#v", detail)
	}
	if detail.PoisonDriver != "rabbitmq" || detail.PoisonAction != queue.PoisonEnvelopeActionReject ||
		detail.PoisonBodySize != 4097 || !detail.PoisonBodyTruncated {
		t.Fatalf("poison safe summary fields were not retained: %#v", detail)
	}
	if strings.Contains(detail.ErrorSummary, "eyJzZWNyZXQi") || strings.Contains(detail.ErrorSummary, "donot-leak") {
		t.Fatalf("poison detail leaked payload-like data in error summary: %#v", detail)
	}
}

func TestClassifiesOnlyFailedPoisonAndSlowHighValueDetails(t *testing.T) {
	// 需求背景：本阶段只保留 failed、poison、slow job 诊断明细，不实现 Successful/Completed job detail。
	cfg := observabilityPresetConfigOrFull()
	cfg.FailedDetailEnabled = true
	cfg.PoisonDetailEnabled = true
	cfg.SlowJobDetailEnabled = true
	cfg.SlowJobThreshold = 2 * time.Second
	rate := 1.0
	cfg.HighValueDetailSampleRate = &rate
	now := time.Date(2026, 5, 15, 9, 15, 0, 0, time.UTC)
	coll := newCollector(cfg)

	inputs := []CollectorInput{
		{Event: queue.EventJobFailed, Connection: "redis", Queue: "default", JobID: "failed", JobName: "CriticalJob", OccurredAt: now},
		{Event: queue.EventPoisonEnvelope, Connection: "redis", Queue: "default", OccurredAt: now},
		{Event: queue.EventJobProcessed, Connection: "redis", Queue: "default", JobID: "slow", JobName: "SlowJob", Runtime: 3 * time.Second, OccurredAt: now},
		{Event: queue.EventJobProcessed, Connection: "redis", Queue: "default", JobID: "success", JobName: "FastJob", Runtime: 100 * time.Millisecond, OccurredAt: now},
	}
	for _, input := range inputs {
		input.Sampling = SamplingDecision{HighValueDetailSampled: true, HighValueDetailRate: 1}
		coll.processItem(collectorItem{input: input, receivedAt: now})
	}

	snapshot := coll.FlushSnapshot(now.Add(time.Second))
	counts := map[string]int{}
	for _, detail := range snapshot.details {
		counts[detail.Kind]++
		if detail.JobID == "success" {
			t.Fatalf("successful processed job must not produce detail: %#v", snapshot.details)
		}
	}
	if counts[HighValueDetailFailed] != 1 || counts[HighValueDetailPoison] != 1 || counts[HighValueDetailSlowJob] != 1 || len(snapshot.details) != 3 {
		t.Fatalf("expected failed/poison/slow details only, got counts=%#v details=%#v", counts, snapshot.details)
	}
}

func TestZeroSlowJobThresholdDisablesSlowHighValueDetail(t *testing.T) {
	// 需求背景：慢任务 detail 必须先通过阈值识别；阈值显式为 0 时表示关闭慢任务分类。
	root := map[string]any{
		"observability": map[string]any{
			"slow_job_threshold": 0,
		},
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
	if err != nil {
		t.Fatalf("slow_job_threshold=0 should be a valid disabled value: %v", err)
	}

	rate := 1.0
	loaded.Observability.HighValueDetailSampleRate = &rate
	coll := newCollector(loaded.Observability)
	now := time.Date(2026, 5, 15, 9, 20, 0, 0, time.UTC)
	coll.processItem(collectorItem{input: CollectorInput{
		Event:      queue.EventJobProcessed,
		Connection: "redis",
		Queue:      "default",
		JobID:      "slow-disabled",
		JobName:    "SlowJob",
		Runtime:    time.Minute,
		OccurredAt: now,
		Sampling: SamplingDecision{
			HighValueDetailSampled: true,
			HighValueDetailRate:    1,
		},
	}, receivedAt: now})

	snapshot := coll.FlushSnapshot(now.Add(time.Second))
	if len(snapshot.details) != 0 {
		t.Fatalf("zero slow_job_threshold should disable slow detail, got %#v", snapshot.details)
	}
}

func TestSlowJobThresholdAcceptsNonNegativeDurationForms(t *testing.T) {
	// 需求背景：slow_job_threshold 只有该字段允许 0 表示关闭；其他非法负值仍必须 fail fast。
	cases := []struct {
		name string
		raw  any
		want time.Duration
	}{
		{name: "int zero", raw: 0, want: 0},
		{name: "int64 seconds", raw: int64(2), want: 2 * time.Second},
		{name: "float seconds", raw: 1.5, want: 1500 * time.Millisecond},
		{name: "duration", raw: 3 * time.Second, want: 3 * time.Second},
		{name: "string zero", raw: "0s", want: 0},
		{name: "string seconds", raw: "4s", want: 4 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": highValueDetailConfigWithSlowThreshold(tc.raw)}})
			if err != nil {
				t.Fatalf("load slow_job_threshold=%v: %v", tc.raw, err)
			}
			if loaded.Observability.SlowJobThreshold != tc.want {
				t.Fatalf("slow_job_threshold = %v, want %v", loaded.Observability.SlowJobThreshold, tc.want)
			}
		})
	}

	for _, raw := range []any{-1, int64(-1), -0.5, "-1s", ""} {
		_, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": highValueDetailConfigWithSlowThreshold(raw)}})
		if err == nil {
			t.Fatalf("slow_job_threshold=%v should be rejected", raw)
		}
	}
}

func TestDroppedFailedDetailDoesNotAffectFailedStoreLifecycle(t *testing.T) {
	// 需求背景：Horizon high-value detail 是可丢弃诊断通道，可靠失败事实源仍是 queue.FailedStore。
	failedStore := queue.NewMemoryFailedStore()
	ctx := context.Background()
	failed := payload.FailedJob{
		ID:       "failed-1",
		JobID:    "job-1",
		JobName:  "CriticalJob",
		Queue:    "default",
		Error:    "boom",
		FailedAt: time.Date(2026, 5, 15, 9, 0, 0, 0, time.UTC),
	}
	if err := failedStore.Record(ctx, failed); err != nil {
		t.Fatalf("record failed job: %v", err)
	}

	cfg := observabilityPresetConfigOrFull()
	cfg.FailedDetailEnabled = true
	rate := 1.0
	cfg.HighValueDetailSampleRate = &rate
	cfg.MaxAggregateKeys = 1
	coll := newCollector(cfg)
	for _, id := range []string{"job-a", "job-b"} {
		coll.processItem(collectorItem{input: CollectorInput{
			Event:      queue.EventJobFailed,
			Connection: "redis",
			Queue:      "default",
			JobID:      id,
			JobName:    "CriticalJob",
			OccurredAt: failed.FailedAt,
			Sampling: SamplingDecision{
				HighValueDetailSampled: true,
				HighValueDetailRate:    1,
			},
		}, receivedAt: failed.FailedAt})
	}
	snapshot := coll.FlushSnapshot(failed.FailedAt.Add(time.Second))
	if len(snapshot.details) != 1 || snapshot.drops["high_value_detail_limit"] != 1 {
		t.Fatalf("expected one retained detail and one dropped detail, got details=%#v drops=%#v", snapshot.details, snapshot.drops)
	}

	if found, err := failedStore.Find(ctx, "failed-1"); err != nil || found.JobID != "job-1" {
		t.Fatalf("failed store record should survive Horizon detail drop, found=%#v err=%v", found, err)
	}
	if err := failedStore.Forget(ctx, "failed-1"); err != nil {
		t.Fatalf("forget failed job: %v", err)
	}
	if _, err := failedStore.Find(ctx, "failed-1"); err != queue.ErrEmpty {
		t.Fatalf("forgotten failed job should be absent, err=%v", err)
	}
	if err := failedStore.Record(ctx, failed); err != nil {
		t.Fatalf("record failed job again: %v", err)
	}
	if err := failedStore.Flush(ctx); err != nil {
		t.Fatalf("flush failed store: %v", err)
	}
	page, err := failedStore.Page(ctx, state.PageRequest{Page: 1, PageSize: 10})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("failed store flush should clear records, all=%#v err=%v", page.Items, err)
	}
}

func TestHighValueDetailsWithSameTimestampRemainDistinctInStore(t *testing.T) {
	// 需求背景：高吞吐下同一纳秒也可能出现多个同类 detail；ID 不能只由 kind+timestamp 派生后互相覆盖。
	cfg := observabilityPresetConfigOrFull()
	cfg.FailedDetailEnabled = true
	rate := 1.0
	cfg.HighValueDetailSampleRate = &rate
	now := time.Date(2026, 5, 15, 9, 30, 0, 0, time.UTC)
	coll := newCollector(cfg)

	for _, id := range []string{"job-1", "job-2"} {
		coll.processItem(collectorItem{input: CollectorInput{
			Event:      queue.EventJobFailed,
			Connection: "redis",
			Queue:      "default",
			JobID:      id,
			JobName:    "CriticalJob",
			OccurredAt: now,
			Sampling: SamplingDecision{
				HighValueDetailSampled: true,
				HighValueDetailRate:    1,
			},
		}, receivedAt: now})
	}
	snapshot := coll.FlushSnapshot(now.Add(time.Second))
	if len(snapshot.details) != 2 {
		t.Fatalf("expected two collector details, got %#v", snapshot.details)
	}

	store := NewMemoryStore(StoreOptions{})
	if err := store.SaveHighValueDetails(context.Background(), snapshot.details, 365*24*time.Hour); err != nil {
		t.Fatalf("save high-value details: %v", err)
	}
	page, err := store.HighValueDetails(context.Background(), HighValueDetailQuery{Kind: HighValueDetailFailed, Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read high-value details: %v", err)
	}
	if page.Total != 2 {
		t.Fatalf("expected both failed details to remain distinct, got page=%#v details=%#v", page, snapshot.details)
	}
}

func highValueDetailConfigWithSlowThreshold(value any) map[string]any {
	return map[string]any{
		"observability": map[string]any{
			"slow_job_threshold": value,
		},
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
}
