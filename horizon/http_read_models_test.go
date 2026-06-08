package horizon

import (
	"context"
	"testing"
	"time"
)

// TestStringSliceContains 覆盖 stringSliceContains 辅助函数的真/假分支。
//
// 需求背景：missing source 诊断中需要队列级精确过滤，该函数被
// filterSupervisorsForMetricQuery 调用以判断 supervisor 是否负责指定 queue。
func TestStringSliceContains(t *testing.T) {
	tests := []struct {
		name   string
		items  []string
		target string
		want   bool
	}{
		{name: "empty nil", items: nil, target: "a", want: false},
		{name: "empty slice", items: []string{}, target: "a", want: false},
		{name: "single match", items: []string{"a"}, target: "a", want: true},
		{name: "single miss", items: []string{"b"}, target: "a", want: false},
		{name: "first of many", items: []string{"a", "b", "c"}, target: "a", want: true},
		{name: "last of many", items: []string{"b", "c", "a"}, target: "a", want: true},
		{name: "mid miss", items: []string{"b", "c", "d"}, target: "a", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringSliceContains(tt.items, tt.target); got != tt.want {
				t.Fatalf("stringSliceContains(%v, %q) = %v, want %v", tt.items, tt.target, got, tt.want)
			}
		})
	}
}

// TestEventMetricWindowQueryHasFilters 覆盖 eventMetricWindowQueryHasFilters
// 的真/假分支：无过滤、时间范围过滤、来源字段过滤、组合过滤。
//
// 需求背景：该函数决定 buildMetricsHistoryReadModel 是否可以回退到旧 MetricsHistory；
// 必须准确判断任一非空过滤条件。
func TestEventMetricWindowQueryHasFilters(t *testing.T) {
	t0 := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	tests := []struct {
		name  string
		query EventMetricWindowQuery
		want  bool
	}{
		{
			name:  "empty query no filter",
			query: EventMetricWindowQuery{},
			want:  false,
		},
		{
			name:  "only from set",
			query: EventMetricWindowQuery{From: t0},
			want:  true,
		},
		{
			name:  "only to set",
			query: EventMetricWindowQuery{To: t1},
			want:  true,
		},
		{
			name:  "both from and to set",
			query: EventMetricWindowQuery{From: t0, To: t1},
			want:  true,
		},
		{
			name:  "source_host set",
			query: EventMetricWindowQuery{SourceHost: "host-a"},
			want:  true,
		},
		{
			name:  "source_environment set",
			query: EventMetricWindowQuery{SourceEnvironment: "production"},
			want:  true,
		},
		{
			name:  "source_supervisor set",
			query: EventMetricWindowQuery{SourceSupervisor: "supervisor-a"},
			want:  true,
		},
		{
			name:  "connection set",
			query: EventMetricWindowQuery{Connection: "redis"},
			want:  true,
		},
		{
			name:  "queue set",
			query: EventMetricWindowQuery{Queue: "default"},
			want:  true,
		},
		{
			name: "all fields set",
			query: EventMetricWindowQuery{
				From:              t0,
				To:                t1,
				SourceHost:        "host-a",
				SourceEnvironment: "production",
				SourceSupervisor:  "supervisor-a",
				Connection:        "redis",
				Queue:             "default",
			},
			want: true,
		},
		{
			name: "only page fields no filter",
			query: EventMetricWindowQuery{
				Page:              PageRequest{Page: 1, PageSize: 10},
				OmitSourceDetails: true,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eventMetricWindowQueryHasFilters(tt.query); got != tt.want {
				t.Fatalf("eventMetricWindowQueryHasFilters(%+v) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestFilterSupervisorsForMetricQuery 覆盖 filterSupervisorsForMetricQuery
// 的全部过滤分支，包括 host/environment/supervisor/connection/queue 过滤，
// 从而间接触发 stringSliceContains 的分支覆盖。
func TestFilterSupervisorsForMetricQuery(t *testing.T) {
	supervisors := []SupervisorState{
		{Name: "s1", Host: "h1", Environment: "prod", Connection: "redis", Queues: []string{"default", "critical"}},
		{Name: "s2", Host: "h2", Environment: "prod", Connection: "redis", Queues: []string{"default"}},
		{Name: "s3", Host: "h1", Environment: "staging", Connection: "beanstalkd", Queues: []string{"critical"}},
	}

	tests := []struct {
		name  string
		query EventMetricWindowQuery
		want  int
	}{
		{name: "no filter all pass", query: EventMetricWindowQuery{}, want: 3},
		{name: "filter by host", query: EventMetricWindowQuery{SourceHost: "h1"}, want: 2},
		{name: "filter by host no match", query: EventMetricWindowQuery{SourceHost: "h9"}, want: 0},
		{name: "filter by env", query: EventMetricWindowQuery{SourceEnvironment: "prod"}, want: 2},
		{name: "filter by env no match", query: EventMetricWindowQuery{SourceEnvironment: "dev"}, want: 0},
		{name: "filter by supervisor", query: EventMetricWindowQuery{SourceSupervisor: "s1"}, want: 1},
		{name: "filter by supervisor no match", query: EventMetricWindowQuery{SourceSupervisor: "s9"}, want: 0},
		{name: "filter by connection", query: EventMetricWindowQuery{Connection: "redis"}, want: 2},
		{name: "filter by connection no match", query: EventMetricWindowQuery{Connection: "sqs"}, want: 0},
		{name: "filter by queue default", query: EventMetricWindowQuery{Queue: "default"}, want: 2},
		{name: "filter by queue critical", query: EventMetricWindowQuery{Queue: "critical"}, want: 2},
		{name: "filter by queue no match", query: EventMetricWindowQuery{Queue: "missing"}, want: 0},
		{name: "host and queue combined", query: EventMetricWindowQuery{SourceHost: "h2", Queue: "critical"}, want: 0},
		{name: "host env and queue combined", query: EventMetricWindowQuery{SourceHost: "h1", SourceEnvironment: "staging", Queue: "critical"}, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSupervisorsForMetricQuery(supervisors, tt.query)
			if len(got) != tt.want {
				t.Fatalf("filterSupervisorsForMetricQuery len=%d, want %d; query=%+v results=%+v", len(got), tt.want, tt.query, got)
			}
		})
	}
}

func TestAggregateMetricsBucketsUseRuntimeSampleCountForRuntimeAverage(t *testing.T) {
	// 需求背景：event_metrics 的 SampleCount 表示全事件样本数，不能再被当作平均 runtime 的分母；
	// read-model 必须改用 RuntimeSampleCount，与 collector/autoscaler 的 runtime 口径保持一致。
	windows := []EventMetricWindow{{
		Connection:         "redis",
		Queue:              "default",
		Processed:          1,
		Failed:             1,
		Released:           2,
		Poison:             1,
		Queued:             1,
		RuntimeMS:          1000,
		SampleCount:        6,
		RuntimeSampleCount: 2,
	}}

	buckets := aggregateMetricsBuckets(windows)
	if len(buckets) != 1 {
		t.Fatalf("expected one bucket, got %#v", buckets)
	}
	if buckets[0].ProcessedCount != 2 {
		t.Fatalf("processed count should use runtime sample count, got %#v", buckets[0])
	}
	if got := RuntimeForQueue(MetricsSnapshot{Buckets: buckets}, "redis", "default"); got != 500*time.Millisecond {
		t.Fatalf("runtime average should ignore non-runtime events, got %s", got)
	}
}

func TestBuildMetricSourcesReadModelFiltersAndPaginatesSources(t *testing.T) {
	// 需求背景：Metric Sources 是 Dashboard 下钻入口，read model 必须复用 event_metrics
	// 查询合同，按来源过滤后再对可见来源分片分页。
	ctx := context.Background()
	base := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{})
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{
		eventMetricWindowFixture(base, base.Add(time.Minute), base.Add(time.Second), "host-a", "production", "supervisor-a", "redis", "default", 5),
		eventMetricWindowFixture(base.Add(time.Minute), base.Add(2*time.Minute), base.Add(2*time.Second), "host-a", "production", "supervisor-a", "redis", "critical", 7),
		eventMetricWindowFixture(base.Add(2*time.Minute), base.Add(3*time.Minute), base.Add(3*time.Second), "host-b", "production", "supervisor-b", "redis", "default", 11),
	}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}

	page, err := buildMetricSourcesReadModel(ctx, store, EventMetricWindowQuery{
		Page:       PageRequest{Page: 1, PageSize: 1},
		SourceHost: "host-a",
		Connection: "redis",
	})
	if err != nil {
		t.Fatalf("build metric sources: %v", err)
	}
	if page.Total != 2 || page.Page != 1 || page.PageSize != 1 || len(page.Items) != 1 {
		t.Fatalf("unexpected metric sources page: %#v", page)
	}
	item := page.Items[0]
	if item.SourceHost != "host-a" || item.Connection != "redis" || item.SourceEnvironment != "production" {
		t.Fatalf("source filters were not preserved in detail: %#v", item)
	}
	if item.Estimate.EstimatedTotal != item.Processed || item.Quality != EventMetricQualityExact || item.Degraded {
		t.Fatalf("source detail should expose per-window estimate quality, got %#v", item)
	}
}
