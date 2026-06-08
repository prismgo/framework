package horizon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestRegisterHTTPRoutesMetricsExposeWindowEstimatesAndDegradation(t *testing.T) {
	// 需求背景：metric window estimate contract 要求 API 不能把采样估算、丢弃缺口或不一致 metrics_window
	// 展示为完整精确数据；读模型必须按事件窗口聚合，并把 flush_at 仅作为诊断时间暴露。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	now := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{})
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{
		{
			WindowStart:         now,
			WindowEnd:           now.Add(time.Minute),
			FlushAt:             now.Add(10 * time.Second),
			SourcePrefix:        "tenant-a",
			SourceEnvironment:   "production",
			Connection:          "redis",
			Queue:               "default",
			Processed:           10,
			SampleCount:         10,
			EffectiveSampleRate: 0.5,
			Quality:             EventMetricQualityEstimated,
			Estimated:           true,
		},
		{
			WindowStart:         now.Add(time.Minute),
			WindowEnd:           now.Add(3 * time.Minute),
			FlushAt:             now.Add(3*time.Minute + 5*time.Second),
			SourcePrefix:        "tenant-a",
			SourceEnvironment:   "production",
			Connection:          "redis",
			Queue:               "default",
			Processed:           4,
			SampleCount:         4,
			EffectiveSampleRate: 0,
			Quality:             EventMetricQualityExact,
		},
		{
			WindowStart:         now,
			WindowEnd:           now.Add(time.Minute),
			FlushAt:             now.Add(15 * time.Second),
			SourcePrefix:        "tenant-a",
			SourceEnvironment:   "production",
			Connection:          "redis",
			Queue:               "critical",
			Failed:              2,
			SampleCount:         2,
			EffectiveSampleRate: 1,
			Quality:             EventMetricQualityDegraded,
			Degraded:            true,
		},
	}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}
	if err := store.SaveObservabilityDiagnostics(ctx, []ObservabilityDiagnostic{
		{Reason: MemoryDropBufferFull, Count: 3, ObservedAt: now.Add(20 * time.Second), Gap: ObservabilityGapQuantifiable},
	}, 0); err != nil {
		t.Fatalf("save diagnostics: %v", err)
	}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	response := httptest.NewRecorder()
	path := "/horizon/api/metrics/current?source_details=1&from=" + now.Add(-time.Minute).Format(time.RFC3339) + "&to=" + now.Add(4*time.Minute).Format(time.RFC3339)
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET metrics/current code=%d body=%s", response.Code, response.Body.String())
	}
	var current struct {
		Estimates     []EventMetricReadModel                `json:"estimates"`
		Windows       PageEnvelope[EventMetricWindow]       `json:"metrics_windows"`
		Diagnostics   PageEnvelope[ObservabilityDiagnostic] `json:"diagnostics"`
		Observability MetricsObservabilityReadModel         `json:"observability"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &current); err != nil {
		t.Fatalf("decode current metrics: %v body=%s", err, response.Body.String())
	}
	if current.Windows.Total != 3 || len(current.Windows.Items) != 3 {
		t.Fatalf("current metrics should expose event windows, got %#v body=%s", current.Windows, response.Body.String())
	}
	if current.Diagnostics.Total != 2 {
		t.Fatalf("diagnostics should include stored drop plus inconsistent-window diagnostic, got %#v body=%s", current.Diagnostics, response.Body.String())
	}
	if current.Observability.DroppedCount != 3 || !current.Observability.Degraded || current.Observability.BufferUtilization != 0 {
		t.Fatalf("observability diagnostics = %#v", current.Observability)
	}
	estimate := findEventMetricEstimate(t, current.Estimates, "redis:default")
	if estimate.Estimate.Quality != EventMetricQualityUnknown || !estimate.Estimate.Degraded ||
		estimate.Estimate.EstimatedTotal != 20 || len(estimate.Estimate.Windows) != 2 {
		t.Fatalf("redis:default estimate should preserve per-window estimate and unknown quality, got %#v", estimate)
	}
	if !estimate.RuntimePercentiles.QualityIsUnknown() {
		t.Fatalf("runtime percentiles must be unknown without a sample distribution, got %#v", estimate.RuntimePercentiles)
	}
	if estimate.Estimate.Windows[0].EffectiveSampleRate != 0.5 || estimate.Estimate.Windows[0].EstimatedTotal != 20 {
		t.Fatalf("first window should estimate with its own rate, got %#v", estimate.Estimate.Windows[0])
	}
	if estimate.Estimate.Windows[1].Quality != EventMetricQualityUnknown || estimate.Estimate.Windows[1].EstimatedTotal != 0 {
		t.Fatalf("zero sample-rate window must be unknown and not estimated as 0, got %#v", estimate.Estimate.Windows[1])
	}

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/history/queue/redis:default?page=1&page_size=100", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET metrics history code=%d body=%s", response.Code, response.Body.String())
	}
	var history struct {
		Kind     string                   `json:"kind"`
		Key      string                   `json:"key"`
		Items    []MetricsHistorySnapshot `json:"items"`
		Total    int                      `json:"total"`
		Page     int                      `json:"page"`
		PageSize int                      `json:"page_size"`
		Estimate EventMetricEstimate      `json:"estimate"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &history); err != nil {
		t.Fatalf("decode history: %v body=%s", err, response.Body.String())
	}
	if history.Kind != MetricsHistoryQueue || history.Key != "redis:default" || len(history.Items) != 2 || history.Total != 2 || history.Page != 1 || history.PageSize != 100 {
		t.Fatalf("history response = %#v body=%s", history, response.Body.String())
	}
	if history.Estimate.Quality != EventMetricQualityUnknown || history.Estimate.EstimatedTotal != 20 {
		t.Fatalf("history estimate should aggregate per event window, got %#v", history.Estimate)
	}
	if history.Items[0].WindowStart.IsZero() || history.Items[0].FlushAt.IsZero() ||
		history.Items[0].Quality == "" || history.Items[0].EstimatedTotal == 0 {
		t.Fatalf("history item should expose window boundary, flush diagnostic and quality, got %#v", history.Items[0])
	}
}

func findEventMetricEstimate(t *testing.T, items []EventMetricReadModel, key string) EventMetricReadModel {
	t.Helper()
	for _, item := range items {
		if item.Key == key {
			return item
		}
	}
	t.Fatalf("estimate %s not found in %#v", key, items)
	return EventMetricReadModel{}
}

func (e RuntimePercentileEstimate) QualityIsUnknown() bool {
	return e.Quality == EventMetricQualityUnknown && e.P95 == 0 && e.P99 == 0
}

func TestRegisterHTTPRoutesStatusDoesNotLoadMetricWindowsOrDiagnostics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &httpMetricWindowEstimateStatusStore{Store: NewMemoryStore(StoreOptions{})}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET status code=%d body=%s", response.Code, response.Body.String())
	}
	if store.windowCalls != 0 || store.diagnosticCalls != 0 {
		t.Fatalf("/status must stay summary-only for metric window estimate contract, window_calls=%d diagnostic_calls=%d", store.windowCalls, store.diagnosticCalls)
	}
}

type httpMetricWindowEstimateStatusStore struct {
	Store
	windowCalls     int
	diagnosticCalls int
}

func (s *httpMetricWindowEstimateStatusStore) EventMetricWindows(ctx context.Context, query EventMetricWindowQuery) (PageEnvelope[EventMetricWindow], error) {
	s.windowCalls++
	return s.Store.EventMetricWindows(ctx, query)
}

func (s *httpMetricWindowEstimateStatusStore) ObservabilityDiagnostics(ctx context.Context, page PageRequest) (PageEnvelope[ObservabilityDiagnostic], error) {
	s.diagnosticCalls++
	return s.Store.ObservabilityDiagnostics(ctx, page)
}

func TestFlushErrorCode(t *testing.T) {
	// 需求背景：flushErrorCode 将 FlusherDiagnostics 映射为稳定机器可读错误码，
	// 用于 /metrics/current 响应中的 LastFlushErrorCode 字段。
	// 三个分支：无错误、有错误+已知降级原因、有错误+未知降级原因。

	// 分支 1：LastFlushError 为空 → 返回空字符串
	if code := flushErrorCode(FlusherDiagnostics{}); code != "" {
		t.Fatalf("empty error: got %q, want empty", code)
	}

	// 分支 2：LastFlushError 非空 + DegradedReason 非空 → 返回 DegradedReason
	if code := flushErrorCode(FlusherDiagnostics{
		LastFlushError: "write timeout",
		DegradedReason: "flush_lag_exceeded",
	}); code != "flush_lag_exceeded" {
		t.Fatalf("error + degraded reason: got %q, want 'flush_lag_exceeded'", code)
	}

	// 分支 3：LastFlushError 非空 + DegradedReason 为空 → 返回 MemoryDropStoreUnavailable
	if code := flushErrorCode(FlusherDiagnostics{
		LastFlushError: "store write failed",
	}); code != MemoryDropStoreUnavailable {
		t.Fatalf("error + no degraded reason: got %q, want %q", code, MemoryDropStoreUnavailable)
	}
}
