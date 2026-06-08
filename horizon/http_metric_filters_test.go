package horizon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestMetricsCurrentFiltersWindowsByRangeAndSource(t *testing.T) {
	// 需求背景：event metric window contract 要求 /metrics/current 把时间范围和来源过滤下推到 Store；
	// 范围必须按事件窗口半开重叠判断，不能用 flush_at 改变归属。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	base := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{
		eventMetricWindowFixture(base.Add(-5*time.Minute), base.Add(-4*time.Minute), base.Add(2*time.Hour), "host-a", "production", "supervisor-a", "redis", "default", 3),
		eventMetricWindowFixture(base.Add(-time.Minute), base.Add(time.Minute), base.Add(3*time.Hour), "host-a", "production", "supervisor-a", "redis", "default", 5),
		eventMetricWindowFixture(base.Add(-time.Minute), base.Add(time.Minute), base.Add(3*time.Hour), "host-b", "production", "supervisor-a", "redis", "default", 7),
		eventMetricWindowFixture(base.Add(-time.Minute), base.Add(time.Minute), base.Add(3*time.Hour), "host-a", "staging", "supervisor-a", "redis", "default", 11),
		eventMetricWindowFixture(base.Add(-time.Minute), base.Add(time.Minute), base.Add(3*time.Hour), "host-a", "production", "supervisor-a", "redis", "critical", 13),
	}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	params := url.Values{}
	params.Set("from", base.Add(-30*time.Second).Format(time.RFC3339))
	params.Set("to", base.Add(30*time.Second).Format(time.RFC3339))
	params.Set("source_host", "host-a")
	params.Set("source_environment", "production")
	params.Set("source_supervisor", "supervisor-a")
	params.Set("connection", "redis")
	params.Set("queue", "default")
	params.Set("source_details", "1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/current?"+params.Encode(), nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET metrics/current code=%d body=%s", response.Code, response.Body.String())
	}
	var current struct {
		Windows   PageEnvelope[EventMetricWindow] `json:"metrics_windows"`
		Estimates []EventMetricReadModel          `json:"estimates"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &current); err != nil {
		t.Fatalf("decode current metrics: %v body=%s", err, response.Body.String())
	}
	if current.Windows.Total != 1 || len(current.Windows.Items) != 1 {
		t.Fatalf("expected exactly one filtered window, got %#v body=%s", current.Windows, response.Body.String())
	}
	window := current.Windows.Items[0]
	if !window.WindowStart.Equal(base.Add(-time.Minute)) || window.FlushAt.Equal(window.WindowStart) || window.Processed != 5 {
		t.Fatalf("range must use event window boundaries and preserve flush_at diagnostic, got %#v", window)
	}
	estimate := findEventMetricEstimate(t, current.Estimates, "redis:default")
	if estimate.Estimate.EstimatedTotal != 5 || len(estimate.SourceDetails) != 1 ||
		estimate.SourceDetails[0].SourceEnvironment != "production" {
		t.Fatalf("estimate should be based on filtered source windows, got %#v", estimate)
	}
}

func TestMetricsAPIsRejectInvalidRangeAndSourceParameters(t *testing.T) {
	// 需求背景：event metric window contract 要求无效时间、缺少时区、from >= to 和空来源参数都返回稳定 400，
	// 不能静默回退成无过滤查询。
	gin.SetMode(gin.TestMode)
	store := NewMemoryStore(StoreOptions{})
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)
	base := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		path  string
		field string
	}{
		{name: "bad from", path: "/horizon/api/metrics/current?from=soon", field: "from"},
		{name: "missing timezone", path: "/horizon/api/metrics/current?from=2026-05-15T10:00:00", field: "from"},
		{name: "from after to", path: "/horizon/api/metrics/current?from=" + url.QueryEscape(base.Format(time.RFC3339)) + "&to=" + url.QueryEscape(base.Add(-time.Minute).Format(time.RFC3339)), field: "from"},
		{name: "empty source", path: "/horizon/api/metrics/current?source_host=", field: "source_host"},
		{name: "history rejects empty queue", path: "/horizon/api/metrics/history/queue/redis:default?queue=", field: "queue"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got code=%d body=%s", response.Code, response.Body.String())
			}
			var body struct {
				Error string `json:"error"`
				Field string `json:"field"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error response: %v body=%s", err, response.Body.String())
			}
			if body.Error != "invalid_parameter" || body.Field != tc.field {
				t.Fatalf("unexpected error response: %#v body=%s", body, response.Body.String())
			}
		})
	}
}

func TestMetricsHistoryAppliesRangeAndSourceFilters(t *testing.T) {
	// 需求背景：history API 与 current API 必须共享同一查询合同；history 聚合不能从未过滤窗口里取数。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	base := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{})
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{
		eventMetricWindowFixture(base.Add(-2*time.Minute), base.Add(-time.Minute), base.Add(5*time.Hour), "host-a", "production", "supervisor-a", "redis", "default", 2),
		eventMetricWindowFixture(base, base.Add(time.Minute), base.Add(6*time.Hour), "host-a", "production", "supervisor-a", "redis", "default", 5),
		eventMetricWindowFixture(base, base.Add(time.Minute), base.Add(6*time.Hour), "host-b", "production", "supervisor-a", "redis", "default", 7),
	}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	params := url.Values{}
	params.Set("from", base.Add(-30*time.Second).Format(time.RFC3339))
	params.Set("to", base.Add(30*time.Second).Format(time.RFC3339))
	params.Set("source_host", "host-a")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/history/queue/redis:default?"+params.Encode(), nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET metrics history code=%d body=%s", response.Code, response.Body.String())
	}
	var history struct {
		Items    []MetricsHistorySnapshot `json:"items"`
		Estimate EventMetricEstimate      `json:"estimate"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &history); err != nil {
		t.Fatalf("decode history: %v body=%s", err, response.Body.String())
	}
	if len(history.Items) != 1 || history.Estimate.EstimatedTotal != 5 {
		t.Fatalf("history should use only filtered windows, got %#v body=%s", history, response.Body.String())
	}
	if !history.Items[0].WindowStart.Equal(base) || history.Items[0].FlushAt.Before(base.Add(time.Hour)) {
		t.Fatalf("history should expose event window and preserve flush diagnostic, got %#v", history.Items[0])
	}
}

func TestMetricSourcesEndpointFiltersAndPaginatesSourceDetails(t *testing.T) {
	// 需求背景：/metrics/sources 是 Metric Sources 页面直接依赖的 HTTP 读模型入口，
	// 必须把 query 参数传给 read model，并返回稳定分页 envelope。
	gin.SetMode(gin.TestMode)
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
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	params := url.Values{}
	params.Set("page", "1")
	params.Set("page_size", "1")
	params.Set("source_host", "host-a")
	params.Set("connection", "redis")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/sources?"+params.Encode(), nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET metrics/sources code=%d body=%s", response.Code, response.Body.String())
	}
	var page PageEnvelope[EventMetricSourceDetail]
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode metric sources: %v body=%s", err, response.Body.String())
	}
	if page.Total != 2 || page.Page != 1 || page.PageSize != 1 || len(page.Items) != 1 {
		t.Fatalf("unexpected metric sources page: %#v body=%s", page, response.Body.String())
	}
	item := page.Items[0]
	if item.SourceHost != "host-a" || item.Connection != "redis" || item.SourceEnvironment != "production" {
		t.Fatalf("unexpected metric source detail: %#v", item)
	}
	if item.Estimate.EstimatedTotal != item.Processed || item.Quality != EventMetricQualityExact {
		t.Fatalf("metric source should expose per-source estimate, got %#v", item)
	}
}

func TestMetricsAggregationUsesFilteredSetNotRawPageBoundary(t *testing.T) {
	// 需求背景：raw/source detail 表可以分页，但 estimate、quality 和 diagnostics 必须基于完整过滤集合，
	// 不能因为 page_size=1 就只聚合第一页窗口。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	base := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{})
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{
		eventMetricWindowFixture(base, base.Add(time.Minute), base.Add(time.Second), "host-a", "production", "supervisor-a", "redis", "default", 5),
		eventMetricWindowFixture(base.Add(time.Minute), base.Add(2*time.Minute), base.Add(2*time.Second), "host-a", "production", "supervisor-a", "redis", "default", 7),
	}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	response := httptest.NewRecorder()
	path := "/horizon/api/metrics/current?page_size=1&source_details=1&source_host=host-a&from=" + base.Add(-time.Minute).Format(time.RFC3339) + "&to=" + base.Add(3*time.Minute).Format(time.RFC3339)
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET metrics/current code=%d body=%s", response.Code, response.Body.String())
	}
	var current struct {
		Windows   PageEnvelope[EventMetricWindow] `json:"metrics_windows"`
		Estimates []EventMetricReadModel          `json:"estimates"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &current); err != nil {
		t.Fatalf("decode current metrics: %v body=%s", err, response.Body.String())
	}
	if len(current.Windows.Items) != 1 || current.Windows.Total != 2 {
		t.Fatalf("raw windows should be paged but total preserved, got %#v", current.Windows)
	}
	estimate := findEventMetricEstimate(t, current.Estimates, "redis:default")
	if estimate.Estimate.EstimatedTotal != 12 || len(estimate.SourceDetails) != 2 {
		t.Fatalf("estimate should aggregate all filtered windows, got %#v", estimate)
	}
}

func TestMetricsSourceDiagnosticsRespectFilteredRuntimeSources(t *testing.T) {
	// 需求背景：按 host/environment/supervisor/queue 下钻时，missing source 诊断只应基于过滤后的来源范围，
	// 不得把已被过滤掉的 supervisor 误报为缺失。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	for _, supervisor := range []SupervisorState{
		{Name: "supervisor-a", Host: "host-a", Environment: "production", Status: SupervisorRunning, Connection: "redis", Queues: []string{"default"}, LastHeartbeatAt: now},
		{Name: "supervisor-b", Host: "host-b", Environment: "production", Status: SupervisorRunning, Connection: "redis", Queues: []string{"default"}, LastHeartbeatAt: now},
	} {
		if err := store.HeartbeatSupervisor(ctx, supervisor); err != nil {
			t.Fatalf("heartbeat supervisor: %v", err)
		}
	}
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{
		eventMetricWindowFixture(now.Add(-time.Minute), now, now, "host-a", "production", "supervisor-a", "redis", "default", 5),
	}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/current?source_host=host-a", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET metrics/current code=%d body=%s", response.Code, response.Body.String())
	}
	var current struct {
		Diagnostics PageEnvelope[ObservabilityDiagnostic] `json:"diagnostics"`
		Estimates   []EventMetricReadModel                `json:"estimates"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &current); err != nil {
		t.Fatalf("decode current metrics: %v body=%s", err, response.Body.String())
	}
	if hasDiagnostic(current.Diagnostics.Items, metricsSourceMissing) {
		t.Fatalf("filtered-out host must not be reported missing, diagnostics=%#v", current.Diagnostics.Items)
	}
	estimate := findEventMetricEstimate(t, current.Estimates, "redis:default")
	if estimate.Estimate.Quality != EventMetricQualityExact || estimate.Estimate.Degraded {
		t.Fatalf("filtered source estimate should stay exact, got %#v", estimate.Estimate)
	}
}

func eventMetricWindowFixture(start, end, flushAt time.Time, host, environment, supervisor, connection, queue string, processed int64) EventMetricWindow {
	return EventMetricWindow{
		WindowStart:         start,
		WindowEnd:           end,
		FlushAt:             flushAt,
		MetricsWindowMS:     int64(end.Sub(start) / time.Millisecond),
		SourcePrefix:        "tenant-a",
		SourceHost:          host,
		SourceEnvironment:   environment,
		SourceSupervisor:    supervisor,
		Connection:          connection,
		Queue:               queue,
		JobName:             "EmailJob",
		Processed:           processed,
		SampleCount:         processed,
		EffectiveSampleRate: 1,
		EstimatedTotal:      processed,
		Quality:             EventMetricQualityExact,
	}
}
