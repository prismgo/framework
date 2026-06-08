package horizon

import (
	"testing"

	"github.com/prismgo/framework/queue"
)

type sequenceSampler struct {
	values []float64
	index  int
}

func (s *sequenceSampler) Float64() float64 {
	if len(s.values) == 0 {
		return 0
	}
	value := s.values[s.index%len(s.values)]
	s.index++
	return value
}

func TestCollectorSamplingUsesInjectedRandomSequence(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 0.5
	highValueRate := 0.25
	cfg.HighValueDetailSampleRate = &highValueRate
	coll := newCollector(cfg)
	coll.sampler = &sequenceSampler{values: []float64{0.49, 0.50, 0.24, 0.25}}

	input := coll.inputFromEventWithPressure(queue.JobProcessed{
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-1",
		JobName:    "SampledJob",
	}, SamplingPressure{})

	if !input.Sampling.EventMetricsSampled {
		t.Fatalf("event_metrics should sample when random value is below rate: %#v", input.Sampling)
	}
	if input.Sampling.HighValueDetailSampled {
		t.Fatalf("high-value detail should not sample when random value equals rate: %#v", input.Sampling)
	}
	if input.Sampling.EventMetricsSampleRate != 0.5 || input.Sampling.HighValueDetailRate != 0.25 {
		t.Fatalf("sampling rates should keep policy values: %#v", input.Sampling)
	}

	input = coll.inputFromEventWithPressure(queue.JobProcessed{
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-2",
		JobName:    "SampledJob",
	}, SamplingPressure{})

	if !input.Sampling.EventMetricsSampled {
		t.Fatalf("event_metrics should sample when random sequence wraps below rate: %#v", input.Sampling)
	}
	if input.Sampling.HighValueDetailSampled {
		t.Fatalf("high-value detail should remain governed by the injected sequence: %#v", input.Sampling)
	}
}

func TestCollectorSamplingKeepsBoundaryRatesDeterministic(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 0
	highValueRate := 1.0
	cfg.HighValueDetailSampleRate = &highValueRate
	coll := newCollector(cfg)
	coll.sampler = &sequenceSampler{values: []float64{0.0, 0.999999}}

	input := coll.inputFromEventWithPressure(queue.JobProcessed{
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-1",
		JobName:    "BoundaryJob",
	}, SamplingPressure{})

	if input.Sampling.EventMetricsSampled {
		t.Fatalf("event_metrics sample rate 0 should never sample: %#v", input.Sampling)
	}
	if !input.Sampling.HighValueDetailSampled {
		t.Fatalf("high-value sample rate 1 should always sample: %#v", input.Sampling)
	}

	cfg.EventMetricsSampleRate = 1
	highValueRate = 0
	cfg.HighValueDetailSampleRate = &highValueRate
	coll = newCollector(cfg)
	coll.sampler = &sequenceSampler{values: []float64{0.999999, 0.0}}

	input = coll.inputFromEventWithPressure(queue.JobProcessed{
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-2",
		JobName:    "BoundaryJob",
	}, SamplingPressure{})

	if !input.Sampling.EventMetricsSampled {
		t.Fatalf("event_metrics sample rate 1 should always sample: %#v", input.Sampling)
	}
	if input.Sampling.HighValueDetailSampled {
		t.Fatalf("high-value sample rate 0 should never sample: %#v", input.Sampling)
	}
}

func TestCollectorSamplingKeepsStableDistributionWithinOneWindow(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 0.25
	coll := newCollector(cfg)
	values := make([]float64, 1000)
	for i := range values {
		values[i] = float64(i) / 1000
	}
	coll.sampler = &sequenceSampler{values: values}

	sampled := 0
	for i := 0; i < 1000; i++ {
		input := coll.inputFromEventWithPressure(queue.JobProcessed{
			Connection: "redis",
			Queue:      "default",
			JobID:      "job-stable",
			JobName:    "StableSamplingJob",
		}, SamplingPressure{})
		if input.Sampling.EventMetricsSampled {
			sampled++
		}
	}

	if sampled != 250 {
		t.Fatalf("expected fixed sequence to keep a stable 25%% distribution, got %d/1000", sampled)
	}
}

func TestCollectorSamplingReportsDynamicPressureMetadata(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 1
	cfg.DynamicSamplingEnabled = true
	cfg.MinSampleRate = 0.2
	coll := newCollector(cfg)
	coll.sampler = &sequenceSampler{values: []float64{0.1, 0.9}}

	input := coll.inputFromEventWithPressure(queue.JobProcessed{
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-pressured",
		JobName:    "PressureAwareJob",
	}, SamplingPressure{BufferUtilization: 0.82})

	if input.Sampling.EventMetricsSampleRate >= 1 || input.Sampling.EventMetricsSampleRate < cfg.MinSampleRate {
		t.Fatalf("dynamic pressure should lower event_metrics rate within configured bounds: %#v", input.Sampling)
	}
	if !input.Sampling.EventMetricsSampled {
		t.Fatalf("fixed random value below lowered rate should sample: %#v", input.Sampling)
	}
	if !input.Sampling.Estimated {
		t.Fatalf("lowered dynamic sampling must expose estimated metadata: %#v", input.Sampling)
	}
	if input.Sampling.HighValueDetailRate != input.Sampling.EventMetricsSampleRate {
		t.Fatalf("high-value fallback should follow effective event_metrics rate: %#v", input.Sampling)
	}
}
