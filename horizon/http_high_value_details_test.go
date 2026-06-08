package horizon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestHighValueDetailListFiltersPaginatesAndReturnsSafeFields(t *testing.T) {
	// 需求背景：high_value_detail 是 failed/poison/slow_job 的只读安全摘要列表，不恢复 recent jobs。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	base := time.Date(2026, 5, 16, 9, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{})
	if err := store.SaveHighValueDetails(ctx, []HighValueJobDetail{
		{ID: "failed-old", Kind: HighValueDetailFailed, Connection: "redis", Queue: "default", JobID: "job-old", JobName: "OldJob", ErrorSummary: "old boom", OccurredAt: base.Add(-2 * time.Hour)},
		{ID: "failed-new", Kind: HighValueDetailFailed, Connection: "redis", Queue: "critical", JobID: "job-new", JobName: "CriticalJob", RuntimeMS: 123, ErrorSummary: "short boom", OccurredAt: base.Add(2 * time.Minute)},
		{ID: "failed-later", Kind: HighValueDetailFailed, Connection: "redis", Queue: "critical", JobID: "job-later", JobName: "CriticalJob", RuntimeMS: 456, ErrorSummary: "later boom", OccurredAt: base.Add(3 * time.Minute)},
		{ID: "poison-1", Kind: HighValueDetailPoison, Connection: "rabbit", Queue: "critical", PoisonDriver: "rabbitmq", PoisonAction: "reject", PoisonBodySize: 4096, PoisonBodyTruncated: true, ErrorSummary: "poison summary", OccurredAt: base.Add(time.Minute)},
	}, 0); err != nil {
		t.Fatalf("save high-value details: %v", err)
	}
	router := highValueDetailRouter(t, store)

	params := url.Values{}
	params.Set("kind", HighValueDetailFailed)
	params.Set("occurred_from", base.Format(time.RFC3339))
	params.Set("occurred_to", base.Add(10*time.Minute).Format(time.RFC3339))
	params.Set("page", "2")
	params.Set("page_size", "1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/high-value-detail?"+params.Encode(), nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET high-value-detail code=%d body=%s", response.Code, response.Body.String())
	}
	var page struct {
		Items    []map[string]any `json:"items"`
		Total    int              `json:"total"`
		Page     int              `json:"page"`
		PageSize int              `json:"page_size"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode high-value detail page: %v body=%s", err, response.Body.String())
	}
	if page.Total != 2 || page.Page != 2 || page.PageSize != 1 || len(page.Items) != 1 {
		t.Fatalf("unexpected pagination: %#v body=%s", page, response.Body.String())
	}
	item := page.Items[0]
	if item["id"] != "failed-new" || item["kind"] != HighValueDetailFailed || item["queue"] != "critical" || item["runtime_ms"].(float64) != 123 {
		t.Fatalf("unexpected filtered item: %#v body=%s", item, response.Body.String())
	}
	assertSafeHighValueFields(t, item)
	if strings.Contains(response.Body.String(), "failed-old") || strings.Contains(response.Body.String(), "poison-1") {
		t.Fatalf("list should apply kind and occurred_at filters body=%s", response.Body.String())
	}
}

func TestHighValueDetailRejectsInvalidKindAndTime(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := highValueDetailRouter(t, NewMemoryStore(StoreOptions{}))

	cases := []struct {
		path  string
		field string
	}{
		{path: "/horizon/api/high-value-detail?kind=recent_jobs", field: "kind"},
		{path: "/horizon/api/high-value-detail?occurred_from=soon", field: "occurred_from"},
		{path: "/horizon/api/high-value-detail?occurred_from=2026-05-16T10:00:00", field: "occurred_from"},
		{path: "/horizon/api/high-value-detail?occurred_from=2026-05-16T10%3A00%3A00Z&occurred_to=2026-05-16T09%3A00%3A00Z", field: "occurred_from"},
	}
	for _, tc := range cases {
		t.Run(tc.field+" "+tc.path, func(t *testing.T) {
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
				t.Fatalf("unexpected error body: %#v response=%s", body, response.Body.String())
			}
		})
	}
}

func TestHighValueDetailDetailReturnsSafeSummaryOr404(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	base := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{})
	if err := store.SaveHighValueDetails(ctx, []HighValueJobDetail{{
		ID:                  "poison-1",
		Kind:                HighValueDetailPoison,
		Connection:          "rabbit",
		Queue:               "critical",
		JobID:               "job-1",
		JobName:             "PoisonJob",
		ErrorSummary:        "decode failed summary",
		PoisonDriver:        "rabbitmq",
		PoisonAction:        "reject",
		PoisonBodySize:      4097,
		PoisonBodyTruncated: true,
		OccurredAt:          base,
	}}, 0); err != nil {
		t.Fatalf("save high-value detail: %v", err)
	}
	router := highValueDetailRouter(t, store)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/high-value-detail/poison-1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET high-value-detail detail code=%d body=%s", response.Code, response.Body.String())
	}
	var detail map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail: %v body=%s", err, response.Body.String())
	}
	if detail["id"] != "poison-1" || detail["poison_body_size"].(float64) != 4097 || detail["poison_body_truncated"] != true {
		t.Fatalf("unexpected detail summary: %#v body=%s", detail, response.Body.String())
	}
	assertSafeHighValueFields(t, detail)
	for _, forbidden := range []string{"payload", "body_base64", "raw_envelope", "credential", "stack", "recent_jobs", "completed", "silenced"} {
		if strings.Contains(strings.ToLower(response.Body.String()), forbidden) {
			t.Fatalf("detail must not expose forbidden field/content %q body=%s", forbidden, response.Body.String())
		}
	}

	missing := httptest.NewRecorder()
	router.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/horizon/api/high-value-detail/missing", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing detail code=%d body=%s", missing.Code, missing.Body.String())
	}
}

func TestDoesNotRestoreLegacyJobListRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := highValueDetailRouter(t, NewMemoryStore(StoreOptions{}))
	for _, path := range []string{
		"/horizon/api/recent-jobs",
		"/horizon/api/jobs",
		"/horizon/api/jobs/completed",
		"/horizon/api/jobs/silenced",
	} {
		assertRouteNotFound(t, router, path)
	}
}

func highValueDetailRouter(t *testing.T, store Store) *gin.Engine {
	t.Helper()
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mountHorizonRoutes(t, manager)
}

func assertSafeHighValueFields(t *testing.T, item map[string]any) {
	t.Helper()
	allowed := map[string]bool{
		"id": true, "kind": true, "connection": true, "queue": true, "job_id": true, "job_name": true,
		"runtime_ms": true, "error_summary": true, "poison_driver": true, "poison_action": true,
		"poison_body_size": true, "poison_body_truncated": true, "occurred_at": true,
	}
	for key := range item {
		if !allowed[key] {
			t.Fatalf("high-value detail exposed unexpected field %q in %#v", key, item)
		}
	}
	for key := range allowed {
		if _, ok := item[key]; !ok {
			t.Fatalf("high-value detail missing safe field %q in %#v", key, item)
		}
	}
}
