package horizon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	goprocess "github.com/prismgo/framework/process"
)

// TestHTTPProcessListsUseReadModelsAndSampleCurrentPageOnly 覆盖 process observability contract 的核心 API 行为。
// 需求背景：/status 必须继续保持轻量摘要，不能触发 OS 进程采样；进程列表只对当前分页 PID 做有界采样。
// 同时验证 read model 中 CPU、内存百分比和 queue 字段都使用字段级状态，避免暴露 broker 细节或误导性 0。
func TestHTTPProcessListsUseReadModelsAndSampleCurrentPageOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	now := time.Date(2026, 5, 14, 15, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Hour})
	for _, worker := range []WorkerState{
		{ID: "worker-1", Supervisor: "supervisor-a", Host: "host-a", PID: 111, Status: WorkerIdle, LastHeartbeatAt: now, ConfiguredQueues: []string{"high"}},
		{ID: "worker-2", Supervisor: "supervisor-a", Host: "host-a", PID: 222, Status: WorkerIdle, LastHeartbeatAt: now, ConfiguredQueues: []string{"default", "low"}, GoroutineCount: goprocess.Metric{Value: 17, Unit: goprocess.UnitCount, Status: goprocess.StatusAvailable}, CollectorMemoryBytes: goprocess.Metric{Value: int64(8192), Unit: goprocess.UnitBytes, Status: goprocess.StatusAvailable}},
	} {
		if err := store.HeartbeatWorker(ctx, worker); err != nil {
			t.Fatalf("heartbeat worker: %v", err)
		}
	}
	observer := &fakeProcessObserver{snapshots: map[int]goprocess.Snapshot{
		222: {
			PID:            222,
			SampledAt:      now,
			SampleWindowMS: 7,
			CPUPercent:     goprocess.Metric{Value: 12.5, Unit: goprocess.UnitPercent, Status: goprocess.StatusAvailable},
			MemoryRSSBytes: goprocess.Metric{Value: int64(4096), Unit: goprocess.UnitBytes, Status: goprocess.StatusAvailable},
			MemoryPercent:  goprocess.Metric{Value: nil, Unit: goprocess.UnitPercent, Status: goprocess.StatusUnavailable, Reason: "system total memory unavailable"},
			GoroutineCount: goprocess.Metric{Value: nil, Unit: goprocess.UnitCount, Status: goprocess.StatusUnavailable, Reason: "external goroutine count unavailable"},
		},
	}}
	manager, err := NewManager(
		Config{Path: "horizon", Store: "memory"},
		WithStoreFactory(httpStaticStoreResolver{store: store}),
		WithProcessObserver(observer),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	router := mountHorizonRoutes(t, manager)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", response.Code, response.Body.String())
	}
	if observer.calls != 0 {
		t.Fatalf("/status must not sample process resources, calls=%d", observer.calls)
	}

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/horizon/api/workers?page=2&page_size=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("workers code=%d body=%s", response.Code, response.Body.String())
	}
	if !reflect.DeepEqual(observer.pids, []int{222}) {
		t.Fatalf("sampled pids=%v, want current page pid only", observer.pids)
	}
	var payload struct {
		Items []ProcessReadModel `json:"items"`
		Total int                `json:"total"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode workers: %v", err)
	}
	if payload.Total != 2 || len(payload.Items) != 1 {
		t.Fatalf("workers page payload=%#v body=%s", payload, response.Body.String())
	}
	item := payload.Items[0]
	if item.ID != "worker-2" || item.Kind != ProcessKindWorker || item.PID != 222 || item.LastHeartbeatAt.IsZero() {
		t.Fatalf("worker read model identity = %#v", item)
	}
	if item.CPUPercent.Status != goprocess.StatusAvailable || item.CPUPercent.Value == nil {
		t.Fatalf("cpu metric = %#v", item.CPUPercent)
	}
	if item.MemoryPercent.Status != goprocess.StatusUnavailable || item.MemoryPercent.Value != nil || item.MemoryPercent.Reason == "" {
		t.Fatalf("memory percent metric = %#v", item.MemoryPercent)
	}
	if item.GoroutineCount.Status != goprocess.StatusAvailable || item.GoroutineCount.Value != float64(17) {
		t.Fatalf("goroutine metric should prefer heartbeat self-observation over external sample = %#v", item.GoroutineCount)
	}
	if item.CollectorMemoryBytes.Status != goprocess.StatusAvailable || item.CollectorMemoryBytes.Value != float64(8192) {
		t.Fatalf("collector memory metric = %#v", item.CollectorMemoryBytes)
	}
	// worker current_queue 已移除，始终返回 unavailable
	if item.CurrentQueue.Status != goprocess.StatusUnavailable || len(item.ConfiguredQueues) != 2 {
		t.Fatalf("queue fields = current:%#v configured:%#v", item.CurrentQueue, item.ConfiguredQueues)
	}
}

// fakeProcessObserver 是测试专用进程观测器。
// 设计思路：通过可注入 Observer 固定采样结果和 PID 调用顺序，避免测试依赖真实 OS 进程负载。
type fakeProcessObserver struct {
	snapshots map[int]goprocess.Snapshot
	pids      []int
	calls     int
}

// Observe 记录本次被采样的 PID，并按测试预置快照返回字段级结果。
// 参数说明：pids 必须来自当前分页，缺失预置值时返回 unavailable，用于验证 UI/API 不把缺失数据显示为 0。
func (o *fakeProcessObserver) Observe(_ context.Context, pids []int) (map[int]goprocess.Snapshot, error) {
	o.calls++
	o.pids = append([]int(nil), pids...)
	out := make(map[int]goprocess.Snapshot, len(pids))
	for _, pid := range pids {
		if snapshot, ok := o.snapshots[pid]; ok {
			out[pid] = snapshot
			continue
		}
		out[pid] = goprocess.Snapshot{
			PID:              pid,
			CPUPercent:       goprocess.Metric{Value: nil, Unit: goprocess.UnitPercent, Status: goprocess.StatusUnavailable, Reason: "missing fake sample"},
			MemoryRSSBytes:   goprocess.Metric{Value: nil, Unit: goprocess.UnitBytes, Status: goprocess.StatusUnavailable, Reason: "missing fake sample"},
			MemoryPercent:    goprocess.Metric{Value: nil, Unit: goprocess.UnitPercent, Status: goprocess.StatusUnavailable, Reason: "missing fake sample"},
			GoroutineCount:   goprocess.Metric{Value: nil, Unit: goprocess.UnitCount, Status: goprocess.StatusUnavailable, Reason: "missing fake sample"},
			PlatformProvider: "fake",
		}
	}
	return out, nil
}
