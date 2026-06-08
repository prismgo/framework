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

func TestMetricsCurrentCanDeferSourceDetailsUntilRequested(t *testing.T) {
	// 需求背景：Dashboard Metrics tab 默认展示 queue 聚合 summary，来源分片只能在用户下钻时懒加载。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	base := time.Date(2026, 5, 16, 11, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{})
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{
		eventMetricWindowFixture(base, base.Add(time.Minute), base.Add(2*time.Minute), "host-a", "production", "supervisor-a", "redis", "default", 5),
		eventMetricWindowFixture(base, base.Add(time.Minute), base.Add(2*time.Minute), "host-b", "production", "supervisor-b", "redis", "default", 7),
	}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}
	router := highValueDetailRouter(t, store)
	rangeQuery := "&from=" + base.Add(-time.Minute).Format(time.RFC3339) + "&to=" + base.Add(2*time.Minute).Format(time.RFC3339)

	summary := httptest.NewRecorder()
	router.ServeHTTP(summary, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/current?summary_only=1"+rangeQuery, nil))
	if summary.Code != http.StatusOK {
		t.Fatalf("GET summary metrics code=%d body=%s", summary.Code, summary.Body.String())
	}
	var summaryBody struct {
		Estimates []EventMetricReadModel `json:"estimates"`
	}
	if err := json.Unmarshal(summary.Body.Bytes(), &summaryBody); err != nil {
		t.Fatalf("decode summary metrics: %v body=%s", err, summary.Body.String())
	}
	estimate := findEventMetricEstimate(t, summaryBody.Estimates, "redis:default")
	if estimate.Estimate.EstimatedTotal != 12 {
		t.Fatalf("summary should still expose aggregate estimate, got %#v", estimate)
	}
	if len(estimate.SourceDetails) != 0 {
		t.Fatalf("summary_only must omit source details, got %#v", estimate.SourceDetails)
	}

	details := httptest.NewRecorder()
	router.ServeHTTP(details, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/current?source_details=1&connection=redis&queue=default"+rangeQuery, nil))
	if details.Code != http.StatusOK {
		t.Fatalf("GET source metrics code=%d body=%s", details.Code, details.Body.String())
	}
	var detailBody struct {
		Estimates []EventMetricReadModel `json:"estimates"`
	}
	if err := json.Unmarshal(details.Body.Bytes(), &detailBody); err != nil {
		t.Fatalf("decode source metrics: %v body=%s", err, details.Body.String())
	}
	detailEstimate := findEventMetricEstimate(t, detailBody.Estimates, "redis:default")
	if len(detailEstimate.SourceDetails) != 2 ||
		detailEstimate.SourceDetails[0].Connection != "redis" ||
		detailEstimate.SourceDetails[0].Queue != "default" {
		t.Fatalf("source_details request should include queue source shards, got %#v", detailEstimate.SourceDetails)
	}
}
