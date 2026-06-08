package horizon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	goprocess "github.com/prismgo/framework/process"
	"github.com/prismgo/framework/route"
)

func TestRegisterHTTPRoutesServesReadOnlyAPIUnderConfiguredPrefix(t *testing.T) {
	gin.SetMode(gin.TestMode)
	setupRouteContainer(t)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: 24 * time.Hour})
	// 设计说明：/status 在运行时使用真实当前时间判断 stale，这里使用相对当前时间避免测试受日期漂移影响。
	now := time.Now().UTC()
	if err := store.HeartbeatMaster(context.Background(), MasterState{ID: "master-1", Status: MasterRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("heartbeat master: %v", err)
	}
	if err := store.HeartbeatSupervisor(context.Background(), SupervisorState{Name: "supervisor-1", Status: SupervisorRunning, LastHeartbeatAt: now, Connection: "redis", Queues: []string{"default"}}); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(context.Background(), WorkerState{ID: "worker-1", Supervisor: "supervisor-1", Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}
	if err := store.AppendEventMetricWindows(context.Background(), []EventMetricWindow{{
		WindowStart: now.Truncate(time.Minute),
		WindowEnd:   now.Truncate(time.Minute).Add(time.Minute),
		FlushAt:     now,
		Connection:  "redis",
		Queue:       "default",
		Processed:   3,
		Quality:     EventMetricQualityExact,
	}}, 24*time.Hour); err != nil {
		t.Fatalf("save event windows: %v", err)
	}

	manager, err := NewManager(Config{Path: "ops/horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := gin.New()
	seenAuth := false
	RegisterHTTPRoutes(HTTPOptions{
		Manager: manager,
		Auth: []gin.HandlerFunc{func(c *gin.Context) {
			seenAuth = true
			c.Next()
		}},
	})
	if err := route.Mount(router); err != nil {
		t.Fatalf("mount routes: %v", err)
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/ops/horizon/api/status", nil)
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET status code = %d body=%s", response.Code, response.Body.String())
	}
	if !seenAuth {
		t.Fatal("auth middleware was not applied to Horizon API")
	}
	var status struct {
		Status       StatusSnapshot    `json:"status"`
		Capabilities map[string]string `json:"capabilities"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.Status.Status != GlobalRunning || status.Status.SupervisorCount != 1 || status.Status.WorkerCount != 1 {
		t.Fatalf("status payload = %#v", status)
	}
	var rawStatus map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &rawStatus); err != nil {
		t.Fatalf("decode raw status: %v", err)
	}
	for _, forbidden := range []string{"masters", "supervisors", "workers"} {
		if _, ok := rawStatus[forbidden]; ok {
			t.Fatalf("status payload must not include %s details: %s", forbidden, response.Body.String())
		}
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/ops/horizon/api/metrics/history/queue/redis:default", nil)
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET history code = %d body=%s", response.Code, response.Body.String())
	}
	var history struct {
		Kind  string                   `json:"kind"`
		Key   string                   `json:"key"`
		Items []MetricsHistorySnapshot `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &history); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if history.Kind != MetricsHistoryQueue || history.Key != "redis:default" || len(history.Items) != 1 || history.Items[0].Throughput != 3 {
		t.Fatalf("history payload = %#v", history)
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/ops/horizon/api/pause", nil)
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("write API code = %d, want 404", response.Code)
	}
}

func TestRegisterHTTPRoutesStatusUsesSummaryOnly(t *testing.T) {
	// 测试目的：Dashboard 首屏只需要 Store 汇总快照和 queue length snapshot，不能在同一个 /status 响应里继续读取
	// masters/supervisors/workers 明细；否则大规模运行时会把首屏变成全量扫描。
	gin.SetMode(gin.TestMode)
	store := &httpSummaryOnlyStore{
		Store: NewMemoryStore(StoreOptions{}),
		status: StatusSnapshot{
			Status:               GlobalRunning,
			SupervisorCount:      12,
			WorkerCount:          64,
			StaleSupervisorCount: 2,
			StaleWorkerCount:     3,
			TerminateRequested:   true,
			GlobalPaused:         false,
		},
	}
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
	if store.statusCalls != 1 || store.mastersCalls != 0 || store.supervisorsCalls != 0 || store.workersCalls != 0 {
		t.Fatalf("status should use only StatusSnapshot, calls status=%d masters=%d supervisors=%d workers=%d",
			store.statusCalls, store.mastersCalls, store.supervisorsCalls, store.workersCalls)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode status payload: %v", err)
	}
	for _, forbidden := range []string{"masters", "supervisors", "workers"} {
		if _, ok := payload[forbidden]; ok {
			t.Fatalf("status payload must not include %s details: %s", forbidden, response.Body.String())
		}
	}
	if _, ok := payload["queue_lengths"]; !ok {
		t.Fatalf("status payload should include queue_lengths snapshot: %s", response.Body.String())
	}
}

func TestRegisterHTTPRoutesPaginatesReadModelsAndDisabledCapabilities(t *testing.T) {
	// 测试目的：列表类只读接口必须返回稳定分页 envelope，并在观测能力关闭时返回
	// 200 + 空列表，方便 Dashboard 按 tab 独立展示 disabled 状态。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{})
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	for i := 1; i <= 3; i++ {
		id := "worker-" + string(rune('0'+i))
		if err := store.HeartbeatWorker(ctx, WorkerState{ID: id, Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
			t.Fatalf("heartbeat worker %s: %v", id, err)
		}
	}
	if err := store.SaveBatchSummary(ctx, BatchSummary{ID: "batch-1", Name: "Daily", CreatedAt: now}); err != nil {
		t.Fatalf("save first batch: %v", err)
	}
	if err := store.SaveBatchSummary(ctx, BatchSummary{ID: "batch-2", Name: "Weekly", CreatedAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("save second batch: %v", err)
	}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	assertPaginatedLength(t, router, "/horizon/api/workers?page=2&page_size=2", 3, 2, 2, 1)
	assertPaginatedLength(t, router, "/horizon/api/batches?query=week&page=1&page_size=10", 1, 1, 10, 1)

	disabledManager, err := NewManager(Config{
		Path:          "horizon",
		Store:         "memory",
		Observability: ObservabilityConfig{Preset: ObservabilityPresetMinimal},
	}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new disabled manager: %v", err)
	}
	disabledRouter := mountHorizonRoutes(t, disabledManager)
	assertRouteNotFound(t, disabledRouter, "/horizon/api/jobs/recent")
	assertCapabilityPayload(t, disabledRouter, "/horizon/api/batches", "disabled", "batch_summaries disabled")
}

func TestRegisterHTTPRoutesServesDashboardReadModelsAndEmptyBatches(t *testing.T) {
	// 测试目的：Dashboard 依赖的只读端点必须全部挂载在 horizon.path 下，并且对暂不支持的 batch
	// 能力返回稳定降级结构，而不是注册写接口或模拟成功数据。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	now := time.Now().UTC()
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "fresh", Status: SupervisorRunning, LastHeartbeatAt: now, Connection: "redis", Queues: []string{"default"}}); err != nil {
		t.Fatalf("heartbeat fresh supervisor: %v", err)
	}
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "stale", Status: SupervisorRunning, LastHeartbeatAt: now.Add(-2 * time.Hour)}); err != nil {
		t.Fatalf("heartbeat stale supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "fresh-worker", Supervisor: "fresh", Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("heartbeat fresh worker: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "stale-worker", Supervisor: "stale", Status: WorkerIdle, LastHeartbeatAt: now.Add(-2 * time.Hour)}); err != nil {
		t.Fatalf("heartbeat stale worker: %v", err)
	}
	if err := store.SaveQueueLengthSnapshot(ctx, QueueLengthSnapshot{CapturedAt: now, Queues: []QueueLengthBucket{{Connection: "redis", Queue: "default", Size: 2}}}); err != nil {
		t.Fatalf("save queue lengths: %v", err)
	}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	for _, path := range []string{
		"/horizon",
		"/horizon/api/masters",
		"/horizon/api/supervisors",
		"/horizon/api/workers",
		"/horizon/api/stale",
		"/horizon/api/queues",
		"/horizon/api/metrics/current",
		"/horizon/api/batches",
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s code=%d body=%s", path, response.Code, response.Body.String())
		}
	}

	for _, removed := range []string{
		"/horizon/api/jobs/pending",
		"/horizon/api/jobs/completed",
		"/horizon/api/jobs/recent",
		"/horizon/api/jobs/failed",
		"/horizon/api/jobs/recent-1",
		"/horizon/api/jobs/failed/failed-1",
	} {
		assertRouteNotFound(t, router, removed)
	}
	assertCapability(t, router, "/horizon/api/batches", "supported")
	assertStatusCode(t, router, "/horizon/api/batches/batch-1", http.StatusNotFound)
	assertRouteNotFound(t, router, "/horizon/api/monitoring")
}

func TestRegisterHTTPRoutesServesBatchListSearchAndDetail(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{})
	created := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	if err := store.SaveBatchSummary(ctx, BatchSummary{
		ID:        "batch-1",
		Name:      "Daily reports",
		Status:    BatchStatusFinished,
		Total:     3,
		Pending:   0,
		Processed: 3,
		Failed:    1,
		CreatedAt: created,
		UpdatedAt: created.Add(time.Minute),
	}); err != nil {
		t.Fatalf("save batch summary: %v", err)
	}
	if err := store.SaveBatchSummary(ctx, BatchSummary{
		ID:        "batch-2",
		Name:      "Weekly exports",
		Status:    BatchStatusRunning,
		Total:     2,
		Pending:   1,
		Processed: 1,
		CreatedAt: created.Add(time.Hour),
		UpdatedAt: created.Add(time.Hour),
	}); err != nil {
		t.Fatalf("save batch summary: %v", err)
	}
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/batches?query=weekly", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET batches code=%d body=%s", response.Code, response.Body.String())
	}
	var list struct {
		Capability string         `json:"capability"`
		Items      []BatchSummary `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode batch list: %v", err)
	}
	if list.Capability != "supported" || len(list.Items) != 1 || list.Items[0].ID != "batch-2" {
		t.Fatalf("batch list = %#v", list)
	}

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/batches/batch-1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET batch detail code=%d body=%s", response.Code, response.Body.String())
	}
	var detail struct {
		Capability string               `json:"capability"`
		Batch      BatchDetailReadModel `json:"batch"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode batch detail: %v", err)
	}
	if detail.Capability != "summary" || detail.Batch.ID != "batch-1" || detail.Batch.Status != BatchStatusFinished {
		t.Fatalf("batch detail = %#v", detail)
	}

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/horizon/api/batches/batch-1/retry", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("batch write API code=%d, want 404", response.Code)
	}

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/batches/missing", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing batch detail code=%d body=%s", response.Code, response.Body.String())
	}
}

func TestRegisterHTTPRoutesDoesNotServeLegacyJobDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := NewMemoryStore(StoreOptions{})
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	for _, removed := range []string{
		"/horizon/api/jobs/processed-1",
		"/horizon/api/jobs/failed/failed-1",
		"/horizon/api/jobs/failed/processed-1",
		"/horizon/api/jobs/missing",
	} {
		assertRouteNotFound(t, router, removed)
	}
}

func TestRegisterHTTPRoutesQueuesReadModelAggregatesBoundedSources(t *testing.T) {
	// 测试目的：/queues 必须返回后端聚合后的分页只读模型，来源取配置、队列长度快照、metrics bucket 和 wait snapshot 的有界并集；
	// 单个来源缺失时只能降级对应字段，不能隐藏整条队列，也不能把缺失值伪装成 0。
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	store := NewMemoryStore(StoreOptions{})
	if err := store.SaveQueueLengthSnapshot(ctx, QueueLengthSnapshot{
		CapturedAt: now.Add(-time.Minute),
		Queues: []QueueLengthBucket{
			{Connection: "redis", Queue: "alpha", Size: 7},
			{Connection: "redis", Queue: "beta", Size: 2},
		},
	}); err != nil {
		t.Fatalf("save queue lengths: %v", err)
	}
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{
		{
			WindowStart:        now.Truncate(time.Minute),
			WindowEnd:          now.Truncate(time.Minute).Add(time.Minute),
			FlushAt:            now,
			Connection:         "redis",
			Queue:              "alpha",
			Processed:          4,
			Failed:             1,
			Released:           2,
			RuntimeMS:          620,
			SampleCount:        5,
			RuntimeSampleCount: 5,
			Quality:            EventMetricQualityExact,
		},
		{
			WindowStart:        now.Truncate(time.Minute),
			WindowEnd:          now.Truncate(time.Minute).Add(time.Minute),
			FlushAt:            now,
			Connection:         "redis",
			Queue:              "gamma",
			Processed:          3,
			Released:           1,
			RuntimeMS:          90,
			SampleCount:        3,
			RuntimeSampleCount: 3,
			Quality:            EventMetricQualityExact,
		},
		{
			WindowStart:        now.Truncate(time.Minute),
			WindowEnd:          now.Truncate(time.Minute).Add(time.Minute),
			FlushAt:            now,
			Connection:         "redis",
			Queue:              "submillisecond",
			Processed:          2,
			RuntimeMS:          0,
			SampleCount:        2,
			RuntimeSampleCount: 0,
			Quality:            EventMetricQualityExact,
		},
	}, 24*time.Hour); err != nil {
		t.Fatalf("save event windows: %v", err)
	}

	manager, err := NewManager(Config{
		Path:  "horizon",
		Store: "memory",
		Waits: map[string]int{"redis:alpha": 1},
		Supervisors: map[string]SupervisorConfig{
			"mail": {Name: "mail", Connection: "redis", Queues: []string{"alpha", "zeta"}},
		},
	}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	manager.coll = newCollector(manager.Config().Observability)
	queuedAt := time.Now().UTC().Add(-2 * time.Second)
	manager.coll.processItem(collectorItem{input: CollectorInput{
		Event:      "queue.job_queued",
		Connection: "redis",
		Queue:      "alpha",
		JobID:      "waiting-job",
		JobName:    "WaitingJob",
		OccurredAt: queuedAt,
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			HighValueDetailSampled: false,
		},
	}, receivedAt: queuedAt})
	router := mountHorizonRoutes(t, manager)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/queues?page=1&page_size=2", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET queues code=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Items      []QueueReadModel `json:"items"`
		Total      int              `json:"total"`
		Page       int              `json:"page"`
		PageSize   int              `json:"page_size"`
		Capability string           `json:"capability"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode queues payload: %v", err)
	}
	if payload.Capability != "supported" || payload.Total != 5 || payload.Page != 1 || payload.PageSize != 2 || len(payload.Items) != 2 {
		t.Fatalf("queues pagination = %#v body=%s", payload, response.Body.String())
	}
	if payload.Items[0].Key != "redis:alpha" || payload.Items[1].Key != "redis:beta" {
		t.Fatalf("queues should sort by connection+queue, got %#v", payload.Items)
	}
	assertMetricValue(t, payload.Items[0].Size, 7, goprocess.UnitCount, "redis:alpha size")
	assertMetricValue(t, payload.Items[0].AvgRuntime, 124, goprocess.UnitMilliseconds, "redis:alpha avg runtime")
	// EventMetricWindow 不保留 per-event 最大 runtime，故 MaxRuntime 为 0。
	assertMetricValue(t, payload.Items[0].MaxRuntime, 0, goprocess.UnitMilliseconds, "redis:alpha max runtime")
	if payload.Items[0].WaitTime.Status != goprocess.StatusAvailable || payload.Items[0].WaitTime.Value == nil {
		t.Fatalf("redis:alpha wait should be available from collector queued state: %#v", payload.Items[0].WaitTime)
	}
	assertMetricValue(t, payload.Items[0].Throughput, 4, goprocess.UnitCount, "redis:alpha throughput")
	assertMetricValue(t, payload.Items[0].Processed, 4, goprocess.UnitCount, "redis:alpha processed")
	assertMetricValue(t, payload.Items[0].Failed, 1, goprocess.UnitCount, "redis:alpha failed")
	assertMetricValue(t, payload.Items[0].Released, 2, goprocess.UnitCount, "redis:alpha released")
	assertMetricStatus(t, payload.Items[0].AvgMemory, goprocess.StatusUnsupported, "memory metrics are not recorded by the current queue event model", "redis:alpha avg memory")
	assertMetricStatus(t, payload.Items[0].MaxMemory, goprocess.StatusUnsupported, "memory metrics are not recorded by the current queue event model", "redis:alpha max memory")
	assertMetricValue(t, payload.Items[1].Size, 2, goprocess.UnitCount, "redis:beta size")
	assertMetricStatus(t, payload.Items[1].AvgRuntime, goprocess.StatusUnavailable, "queue runtime metrics unavailable", "redis:beta avg runtime")
	assertMetricStatus(t, payload.Items[1].WaitTime, goprocess.StatusUnavailable, "queue wait metrics unavailable", "redis:beta wait")

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/queues?page=4&page_size=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET queues submillisecond code=%d body=%s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode submillisecond queues payload: %v", err)
	}
	if len(payload.Items) != 1 || payload.Items[0].Key != "redis:submillisecond" {
		t.Fatalf("submillisecond queue page = %#v body=%s", payload, response.Body.String())
	}
	assertMetricStatus(t, payload.Items[0].AvgRuntime, goprocess.StatusUnavailable, "queue runtime metrics unavailable", "redis:submillisecond avg runtime")
	assertMetricStatus(t, payload.Items[0].MaxRuntime, goprocess.StatusUnavailable, "queue runtime metrics unavailable", "redis:submillisecond max runtime")

	metricsResponse := httptest.NewRecorder()
	router.ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/current?page=1&page_size=5", nil))
	if metricsResponse.Code != http.StatusOK {
		t.Fatalf("GET metrics/current code=%d body=%s", metricsResponse.Code, metricsResponse.Body.String())
	}
	var metricsPayload MetricsCurrentReadModel
	if err := json.Unmarshal(metricsResponse.Body.Bytes(), &metricsPayload); err != nil {
		t.Fatalf("decode metrics/current payload: %v", err)
	}
	if len(metricsPayload.QueueWaits) != 1 || metricsPayload.QueueWaits[0].Key != "redis:alpha" || metricsPayload.QueueWaits[0].Status != QueueWaitKnown {
		t.Fatalf("metrics/current queue waits = %#v body=%s", metricsPayload.QueueWaits, metricsResponse.Body.String())
	}

	statusResponse := httptest.NewRecorder()
	router.ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/horizon/api/status", nil))
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("GET status code=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	var status struct {
		Status struct {
			QueueCount     int      `json:"queue_count"`
			JobsPerMinute  *float64 `json:"jobs_per_minute"`
			JobsPastHour   *int64   `json:"jobs_past_hour"`
			TotalProcessed *int64   `json:"total_processed"`
		} `json:"status"`
		QueueLengths QueueLengthSnapshot `json:"queue_lengths"`
	}
	if err := json.Unmarshal(statusResponse.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status payload: %v", err)
	}
	if status.Status.QueueCount != 3 {
		t.Fatalf("queue_count = %d, want 3 body=%s", status.Status.QueueCount, statusResponse.Body.String())
	}
	if status.Status.TotalProcessed == nil || *status.Status.TotalProcessed != 9 ||
		status.Status.JobsPastHour == nil || *status.Status.JobsPastHour != 9 ||
		status.Status.JobsPerMinute == nil || *status.Status.JobsPerMinute != 0.15 {
		t.Fatalf("status dashboard metrics = %#v body=%s", status.Status, statusResponse.Body.String())
	}
	if len(status.QueueLengths.Queues) != 2 || status.QueueLengths.Queues[0].Queue != "alpha" {
		t.Fatalf("queue_lengths snapshot = %#v body=%s", status.QueueLengths, statusResponse.Body.String())
	}
}

func TestRegisterHTTPRoutesCapsQueueLengthsInStatusSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	now := time.Now().UTC()
	queues := make([]QueueLengthBucket, 0, 101)
	for i := 0; i < 101; i++ {
		queues = append(queues, QueueLengthBucket{Connection: "redis", Queue: "queue-" + string(rune('a'+(i%26))) + string(rune('a'+((i/26)%26))) + string(rune('a'+((i/676)%26))), Size: int64(i + 1)})
	}
	if err := store.SaveQueueLengthSnapshot(ctx, QueueLengthSnapshot{CapturedAt: now, Queues: queues}); err != nil {
		t.Fatalf("save queue lengths: %v", err)
	}
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
	var status struct {
		Status struct {
			QueueCount int `json:"queue_count"`
		} `json:"status"`
		QueueLengths QueueLengthSnapshot `json:"queue_lengths"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status payload: %v", err)
	}
	if status.Status.QueueCount != 101 {
		t.Fatalf("queue_count = %d, want 101 body=%s", status.Status.QueueCount, response.Body.String())
	}
	if len(status.QueueLengths.Queues) != 100 {
		t.Fatalf("queue_lengths should be capped at 100, got %d body=%s", len(status.QueueLengths.Queues), response.Body.String())
	}
}

func TestRegisterHTTPRoutesRefreshesMissingQueueLengthSnapshotFromQueueBackend(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, err := NewManager(Config{
		Path:          "horizon",
		Store:         "memory",
		Observability: ObservabilityConfig{QueueLengths: true},
		Supervisors: map[string]SupervisorConfig{
			"rabbit": {Name: "rabbit", Connection: "rabbitmq", Queues: []string{"test1", "test2"}},
			"redis":  {Name: "redis", Connection: "redis", Queues: []string{"default"}},
		},
	}, WithStoreFactory(httpStaticStoreResolver{store: store}), WithQueueManager(&fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{
		"rabbitmq": {sizes: map[string]int64{"test1": 2, "test2": 0}},
		"redis":    {sizes: map[string]int64{"default": 1}},
	}}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	statusResponse := httptest.NewRecorder()
	router.ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/horizon/api/status", nil))
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("GET status code=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	if strings.Contains(statusResponse.Body.String(), `"queues":null`) {
		t.Fatalf("status queue_lengths queues must not be null: %s", statusResponse.Body.String())
	}
	var status struct {
		Status struct {
			QueueCount int `json:"queue_count"`
		} `json:"status"`
		QueueLengths QueueLengthSnapshot `json:"queue_lengths"`
	}
	if err := json.Unmarshal(statusResponse.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status payload: %v", err)
	}
	if status.Status.QueueCount != 3 || len(status.QueueLengths.Queues) != 3 {
		t.Fatalf("status queue lengths = %#v body=%s", status, statusResponse.Body.String())
	}

	queuesResponse := httptest.NewRecorder()
	router.ServeHTTP(queuesResponse, httptest.NewRequest(http.MethodGet, "/horizon/api/queues?page=1&page_size=10", nil))
	if queuesResponse.Code != http.StatusOK {
		t.Fatalf("GET queues code=%d body=%s", queuesResponse.Code, queuesResponse.Body.String())
	}
	var queues PageEnvelope[QueueReadModel]
	if err := json.Unmarshal(queuesResponse.Body.Bytes(), &queues); err != nil {
		t.Fatalf("decode queues payload: %v", err)
	}
	if queues.Total != 3 || len(queues.Items) != 3 {
		t.Fatalf("queues payload = %#v body=%s", queues, queuesResponse.Body.String())
	}
	for _, item := range queues.Items {
		if item.Size.Status != "available" || item.Size.Value == nil {
			t.Fatalf("queue size should be available after refresh: %#v body=%s", item, queuesResponse.Body.String())
		}
	}
}

func TestRegisterHTTPRoutesDoesNotRegisterUnusedLaravelAliases(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	for _, path := range []string{
		"/horizon/api/stats",
		"/horizon/api/workload",
		"/horizon/api/metrics/jobs",
		"/horizon/api/metrics/jobs/MailJob",
		"/horizon/api/metrics/queues",
		"/horizon/api/metrics/queues/redis/default",
		"/horizon/api/monitoring",
		"/horizon/api/retry/job-1",
	} {
		assertRouteNotFound(t, router, path)
	}
}

func TestRegisterHTTPRoutesSharesAuthMiddlewareAndStableStoreErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	setupRouteContainer(t)
	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{
		store: httpMetricsErrorStore{Store: NewMemoryStore(StoreOptions{})},
	}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	router := gin.New()
	RegisterHTTPRoutes(HTTPOptions{
		Manager: manager,
		Auth: []gin.HandlerFunc{
			func(c *gin.Context) {
				mode := c.GetHeader("X-Horizon-Auth")
				if mode == "unauthorized" {
					c.AbortWithStatus(http.StatusUnauthorized)
					return
				}
				if mode == "forbidden" {
					c.AbortWithStatus(http.StatusForbidden)
					return
				}
				c.Next()
			},
		},
	})
	if err := route.Mount(router); err != nil {
		t.Fatalf("mount routes: %v", err)
	}

	for _, tc := range []struct {
		path   string
		header string
		want   int
	}{
		{path: "/horizon", header: "unauthorized", want: http.StatusUnauthorized},
		{path: "/horizon/api/status", header: "forbidden", want: http.StatusForbidden},
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, tc.path, nil)
		request.Header.Set("X-Horizon-Auth", tc.header)
		router.ServeHTTP(response, request)
		if response.Code != tc.want {
			t.Fatalf("%s auth code=%d want=%d", tc.path, response.Code, tc.want)
		}
	}

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/metrics/current", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("metrics error code=%d body=%s", response.Code, response.Body.String())
	}
	if !json.Valid(response.Body.Bytes()) {
		t.Fatalf("metrics error must be json body=%s", response.Body.String())
	}
}

func TestRegisterHTTPRoutesCoversResolverAndReadOnlyErrorBranches(t *testing.T) {
	gin.SetMode(gin.TestMode)

	manager, err := NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpFailingStoreResolver{err: context.DeadlineExceeded}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)
	for _, path := range []string{
		"/horizon/api/status",
		"/horizon/api/masters",
		"/horizon/api/supervisors",
		"/horizon/api/workers",
		"/horizon/api/stale",
		"/horizon/api/queues",
		"/horizon/api/metrics/current",
		"/horizon/api/metrics/history/queue/redis:default",
		"/horizon/api/batches?query=nightly",
		"/horizon/api/batches/batch-1",
	} {
		assertStatusCode(t, router, path, http.StatusServiceUnavailable)
	}

	base := NewMemoryStore(StoreOptions{})
	manager, err = NewManager(Config{Path: "horizon", Store: "memory"}, WithStoreFactory(httpStaticStoreResolver{store: httpEndpointErrorStore{
		Store:                 base,
		statusErr:             context.Canceled,
		mastersErr:            context.Canceled,
		supervisorsErr:        context.Canceled,
		workersErr:            context.Canceled,
		eventMetricWindowsErr: context.Canceled,
		metricsErr:            context.Canceled,
		batchesErr:            context.Canceled,
		batchErr:              context.Canceled,
	}}))
	if err != nil {
		t.Fatalf("new manager with error store: %v", err)
	}
	router = mountHorizonRoutes(t, manager)
	for _, path := range []string{
		"/horizon/api/status",
		"/horizon/api/masters",
		"/horizon/api/supervisors",
		"/horizon/api/workers",
		"/horizon/api/stale",
		"/horizon/api/metrics/history/queue/redis:default",
		"/horizon/api/batches?query=nightly",
		"/horizon/api/batches/batch-1",
	} {
		assertStatusCode(t, router, path, http.StatusInternalServerError)
	}
	for _, removed := range []string{"/horizon/api/jobs/recent", "/horizon/api/jobs/example"} {
		assertRouteNotFound(t, router, removed)
	}

}

func mountHorizonRoutes(t *testing.T, manager *Manager) *gin.Engine {
	t.Helper()
	setupRouteContainer(t)
	router := gin.New()
	RegisterHTTPRoutes(HTTPOptions{Manager: manager})
	if err := route.Mount(router); err != nil {
		t.Fatalf("mount routes: %v", err)
	}
	return router
}

func assertPaginatedLength(t *testing.T, router *gin.Engine, path string, wantTotal, wantPage, wantPageSize, wantItems int) {
	t.Helper()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("%s code=%d body=%s", path, response.Code, response.Body.String())
	}
	var payload struct {
		Items    []json.RawMessage `json:"items"`
		Total    int               `json:"total"`
		Page     int               `json:"page"`
		PageSize int               `json:"page_size"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if payload.Total != wantTotal || payload.Page != wantPage || payload.PageSize != wantPageSize || len(payload.Items) != wantItems {
		t.Fatalf("%s pagination got total=%d page=%d page_size=%d items=%d body=%s",
			path, payload.Total, payload.Page, payload.PageSize, len(payload.Items), response.Body.String())
	}
}

func assertCapabilityPayload(t *testing.T, router *gin.Engine, path, wantCapability, wantReason string) {
	t.Helper()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("%s code=%d body=%s", path, response.Code, response.Body.String())
	}
	var payload struct {
		Items      []json.RawMessage `json:"items"`
		Total      int               `json:"total"`
		Capability string            `json:"capability"`
		Reason     string            `json:"reason"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if payload.Capability != wantCapability || payload.Reason != wantReason || payload.Total != 0 || len(payload.Items) != 0 {
		t.Fatalf("%s capability payload=%#v body=%s", path, payload, response.Body.String())
	}
}

func assertCapability(t *testing.T, router *gin.Engine, path string, want string) {
	t.Helper()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	var payload struct {
		Capability string `json:"capability"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if payload.Capability != want {
		t.Fatalf("%s capability=%q want=%q", path, payload.Capability, want)
	}
}

func assertStatusCode(t *testing.T, router *gin.Engine, path string, want int) {
	t.Helper()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != want {
		t.Fatalf("%s code=%d want=%d body=%s", path, response.Code, want, response.Body.String())
	}
	if !json.Valid(response.Body.Bytes()) {
		t.Fatalf("%s should return json body=%s", path, response.Body.String())
	}
}

func assertRouteNotFound(t *testing.T, router *gin.Engine, path string) {
	t.Helper()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("%s code=%d want=%d body=%s", path, response.Code, http.StatusNotFound, response.Body.String())
	}
}

func assertMetricValue(t *testing.T, metric goprocess.Metric, want int64, wantUnit string, label string) {
	t.Helper()
	// 断言目的：字段级可用指标必须返回 available 状态、稳定单位和可读数值，避免测试直接耦合 JSON 解码细节。
	if metric.Status != goprocess.StatusAvailable || metric.Unit != wantUnit || metric.Reason != "" {
		t.Fatalf("%s metric state=%#v", label, metric)
	}
	value, ok := metric.Value.(float64)
	if !ok || int64(value) != want {
		t.Fatalf("%s value=%#v want=%d", label, metric.Value, want)
	}
}

func assertMetricStatus(t *testing.T, metric goprocess.Metric, wantStatus string, wantReason string, label string) {
	t.Helper()
	// 断言目的：字段级降级必须返回 nil value 和稳定 reason，避免前端把 unavailable/disabled/unsupported 误判为真实 0。
	if metric.Status != wantStatus || metric.Reason != wantReason {
		t.Fatalf("%s metric=%#v want status=%q reason=%q", label, metric, wantStatus, wantReason)
	}
	if metric.Value != nil {
		t.Fatalf("%s value=%#v want nil", label, metric.Value)
	}
}

type httpStaticStoreResolver struct {
	store Store
}

func (r httpStaticStoreResolver) ResolveStore(context.Context, Config) (Store, error) {
	return r.store, nil
}

type httpFailingStoreResolver struct {
	err error
}

func (r httpFailingStoreResolver) ResolveStore(context.Context, Config) (Store, error) {
	return nil, r.err
}

type httpMetricsErrorStore struct {
	Store
}

func (s httpMetricsErrorStore) EventMetricWindows(context.Context, EventMetricWindowQuery) (PageEnvelope[EventMetricWindow], error) {
	return PageEnvelope[EventMetricWindow]{}, context.DeadlineExceeded
}

type httpEndpointErrorStore struct {
	Store
	statusErr             error
	mastersErr            error
	supervisorsErr        error
	workersErr            error
	metricsErr            error
	eventMetricWindowsErr error
	batchesErr            error
	allBatchesErr         error
	batchErr              error
	queryBatches          []BatchSummary
}

func (s httpEndpointErrorStore) StatusSnapshot(context.Context, time.Time) (StatusSnapshot, error) {
	if s.statusErr != nil {
		return StatusSnapshot{}, s.statusErr
	}
	return s.Store.StatusSnapshot(context.Background(), time.Now().UTC())
}

func (s httpEndpointErrorStore) Masters(context.Context, time.Time) ([]MasterState, error) {
	if s.mastersErr != nil {
		return nil, s.mastersErr
	}
	return s.Store.Masters(context.Background(), time.Now().UTC())
}

func (s httpEndpointErrorStore) Supervisors(context.Context, time.Time) ([]SupervisorState, error) {
	if s.supervisorsErr != nil {
		return nil, s.supervisorsErr
	}
	return s.Store.Supervisors(context.Background(), time.Now().UTC())
}

func (s httpEndpointErrorStore) Workers(context.Context, time.Time) ([]WorkerState, error) {
	if s.workersErr != nil {
		return nil, s.workersErr
	}
	return s.Store.Workers(context.Background(), time.Now().UTC())
}

func (s httpEndpointErrorStore) EventMetricWindows(ctx context.Context, query EventMetricWindowQuery) (PageEnvelope[EventMetricWindow], error) {
	if s.eventMetricWindowsErr != nil {
		return PageEnvelope[EventMetricWindow]{}, s.eventMetricWindowsErr
	}
	return s.Store.EventMetricWindows(ctx, query)
}

func (s httpEndpointErrorStore) BatchesPage(_ context.Context, query string, page PageRequest) (PageEnvelope[BatchSummary], error) {
	if strings.TrimSpace(query) != "" {
		if s.batchesErr != nil {
			return PageEnvelope[BatchSummary]{}, s.batchesErr
		}
		if s.queryBatches != nil {
			items := append([]BatchSummary(nil), s.queryBatches...)
			return PageEnvelope[BatchSummary]{
				Items:    pageSlice(items, page),
				Total:    len(items),
				Page:     page.Page,
				PageSize: page.PageSize,
			}, nil
		}
	}
	if strings.TrimSpace(query) == "" && s.allBatchesErr != nil {
		return PageEnvelope[BatchSummary]{}, s.allBatchesErr
	}
	return s.Store.BatchesPage(context.Background(), query, page)
}

func (s httpEndpointErrorStore) Batch(context.Context, string) (BatchSummary, bool, error) {
	if s.batchErr != nil {
		return BatchSummary{}, false, s.batchErr
	}
	return s.Store.Batch(context.Background(), "batch-1")
}

type httpSummaryOnlyStore struct {
	Store
	status           StatusSnapshot
	statusCalls      int
	mastersCalls     int
	supervisorsCalls int
	workersCalls     int
}

func (s *httpSummaryOnlyStore) StatusSnapshot(context.Context, time.Time) (StatusSnapshot, error) {
	s.statusCalls++
	return s.status, nil
}

func (s *httpSummaryOnlyStore) Masters(context.Context, time.Time) ([]MasterState, error) {
	s.mastersCalls++
	return nil, nil
}

func (s *httpSummaryOnlyStore) Supervisors(context.Context, time.Time) ([]SupervisorState, error) {
	s.supervisorsCalls++
	return nil, nil
}

func (s *httpSummaryOnlyStore) Workers(context.Context, time.Time) ([]WorkerState, error) {
	s.workersCalls++
	return nil, nil
}

type horizonTestApp struct {
	container *container.Container
}

func (a horizonTestApp) Container() containercontract.Container { return a.container }

func setupRouteContainer(t *testing.T) {
	t.Helper()
	c := container.NewContainer()
	container.SetProvider(func() *container.Container { return c })
	t.Cleanup(func() { container.SetProvider(nil) })
	_ = route.ServiceProvider{}.Register(horizonTestApp{container: c})
}
