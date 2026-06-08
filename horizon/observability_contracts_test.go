package horizon

import (
	"testing"
	"time"
)

func TestObservabilityContractsCarrySamplingDropAndMemoryState(t *testing.T) {
	// 需求背景：buffer config contract 只建立后续 collector/flusher 合同；这些 DTO 必须能表达采样、
	// 有界 buffer、聚合 key 基数和丢弃诊断，而不要求调用方等待 Store 写入结果。
	now := time.Date(2026, 5, 15, 9, 0, 0, 0, time.UTC)
	input := CollectorInput{
		Event:      "queue.job_failed",
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-1",
		JobName:    "MailJob",
		OccurredAt: now,
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 0.25,
			HighValueDetailSampled: true,
			HighValueDetailRate:    0.5,
			Estimated:              true,
		},
	}
	if !input.Sampling.Estimated {
		t.Fatalf("sampled input should be able to mark derived metrics estimated: %#v", input)
	}

	batch := FlushBatch{
		WindowStart: now.Truncate(time.Minute),
		WindowEnd:   now.Truncate(time.Minute).Add(time.Minute),
		Increments: []EventMetricIncrement{{
			Connection: "redis",
			Queue:      "default",
			JobName:    "MailJob",
			Processed:  1,
			RuntimeMS:  120,
			Estimated:  true,
		}},
		HighValueDetails: []HighValueJobDetail{{
			ID:           "failed-1",
			Kind:         HighValueDetailFailed,
			Connection:   "redis",
			Queue:        "default",
			JobID:        "job-1",
			JobName:      "MailJob",
			ErrorSummary: "timeout",
			OccurredAt:   now,
		}},
		Diagnostics: []ObservabilityDiagnostic{{
			Reason:      MemoryDropBufferFull,
			Count:       2,
			ObservedAt:  now,
			Description: "buffer full",
		}},
		Memory: ObservabilityMemoryState{
			BufferSize:           100,
			BufferUsed:           80,
			SampleReservoirSize:  1000,
			SampleReservoirUsed:  250,
			MaxAggregateKeys:     500,
			AggregateKeyCount:    400,
			LastDropReason:       MemoryDropBufferFull,
			BufferHighWatermark:  0.8,
			ReservoirUtilization: 0.25,
		},
	}
	if batch.Memory.BufferUtilization() != 0.8 || batch.Memory.AggregateKeyUtilization() != 0.8 {
		t.Fatalf("memory utilization helpers should expose bounded state: %#v", batch.Memory)
	}
	if !batch.HasDrops() {
		t.Fatalf("batch should expose drop diagnostics: %#v", batch)
	}
}
