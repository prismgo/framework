package horizon

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryStore 是测试和本地开发可用的 Horizon Store 实现。
//
// 需求背景：issue 02 要求本地测试可不依赖 Redis 验证 Store 语义；该实现只在单进程内保存状态，
// 不适合作为生产跨进程可见的 Horizon 状态存储。
type MemoryStore struct {
	// mu 保护所有 map 与 control 状态，保证并发命令读写不会数据竞争。
	mu sync.RWMutex
	// options 保存 prefix 与 heartbeat TTL，prefix 主要用于和 Redis Store 配置保持一致。
	options StoreOptions
	// masters 保存 horizon 顶层 master heartbeat 原始状态，读取时再派生 stale。
	masters map[string]MasterState
	// supervisors 保存 supervisor heartbeat 原始状态，读取时再派生 stale/pause/terminating。
	supervisors map[string]SupervisorState
	// workers 保存 worker heartbeat 原始状态，读取时再派生 stale/pause/terminating。
	workers map[string]WorkerState
	// control 保存运行时控制命令写入的标记。
	control ControlState
	// eventMetricWindows 保存按事件时间追加写入的 event_metrics windows。
	eventMetricWindows []EventMetricWindow
	// eventMetricRollups 保存按 connection:queue 聚合后的窗口，供 summary 读取路径使用。
	eventMetricRollups []EventMetricWindow
	// queueLengths 保存最近一次从 queue backend 采样得到的队列长度快照，独立于事件派生 metrics。
	queueLengths QueueLengthSnapshot
	// orphans 保存 master -> pid -> first_seen 的 Horizon orphan worker tracking 记录。
	orphans map[string]map[int]time.Time
	// highValueDetails 保存 failed/poison/slow job 的可丢弃安全诊断摘要。
	highValueDetails map[string]HighValueJobDetail
	// diagnostics 保存 collector/flusher drop/degradation 诊断。
	diagnostics []ObservabilityDiagnostic
	// batches 保存 BatchEvent 投影出的只读批次摘要，不包含 job payload 或 broker 内部字段。
	batches map[string]BatchSummary
}

// NewMemoryStore 创建内存 Horizon Store；该实现不适合作为生产跨进程状态存储。
//
// 参数说明：options 提供与 Redis Store 一致的 prefix/heartbeat TTL 语义，缺省值由 normalizeStoreOptions 补齐。
func NewMemoryStore(options StoreOptions) *MemoryStore {
	options = normalizeStoreOptions(options)
	return &MemoryStore{
		options:          options,
		masters:          make(map[string]MasterState),
		supervisors:      make(map[string]SupervisorState),
		workers:          make(map[string]WorkerState),
		orphans:          make(map[string]map[int]time.Time),
		highValueDetails: make(map[string]HighValueJobDetail),
		batches:          make(map[string]BatchSummary),
		control: ControlState{
			PausedSupervisors: make(map[string]bool),
		},
	}
}

// AcquireMasterLease 在同一把锁内完成 fresh 冲突检查和首个 heartbeat 写入。
//
// 设计原因：测试和本地 memory store 也必须暴露与 Redis 一致的原子语义，否则并发启动用例只能
// 覆盖生产路径的一半。fast termination 抢跑由 runtime 层显式处理，Store 租约本身保持保守互斥。
func (s *MemoryStore) AcquireMasterLease(_ context.Context, state MasterState) (bool, error) {
	id := strings.TrimSpace(state.ID)
	if id == "" {
		return false, fmt.Errorf("horizon: master id is required")
	}
	state.ID = id
	s.mu.Lock()
	defer s.mu.Unlock()
	now := state.LastHeartbeatAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	for _, existing := range s.masters {
		derived := masterWithDerivedStatus(existing, s.options.HeartbeatTTL, now)
		if derived.Status != MasterStale && existing.Host == state.Host && existing.Environment == state.Environment {
			return false, nil
		}
	}
	s.masters[id] = state
	return true, nil
}

// HeartbeatSupervisor 写入 supervisor heartbeat 原始状态。
//
// 参数说明：state.Name 必填；Queues 会被复制，避免调用方后续修改切片影响 Store 内部状态。
// HeartbeatMaster 写入 master heartbeat 原始状态。
//
// 参数说明：state.ID 必填；master 独立于 supervisor 保存，避免顶层进程状态和子进程状态混淆。
func (s *MemoryStore) HeartbeatMaster(_ context.Context, state MasterState) error {
	id := strings.TrimSpace(state.ID)
	if id == "" {
		return fmt.Errorf("horizon: master id is required")
	}
	state.ID = id
	s.mu.Lock()
	s.masters[id] = state
	s.mu.Unlock()
	return nil
}

// AcquireSupervisorLease 在同一把锁内完成 fresh 冲突检查和首个 supervisor heartbeat 写入。
//
// 逻辑边界：冲突范围只包含 host、environment 和 supervisor name，不跨主机或环境阻塞；pause/terminate
// 等控制状态仍在读取路径派生，不改变租约判断的基础运行域。
func (s *MemoryStore) AcquireSupervisorLease(_ context.Context, state SupervisorState) (bool, error) {
	name := strings.TrimSpace(state.Name)
	if name == "" {
		return false, fmt.Errorf("horizon: supervisor name is required")
	}
	state.Name = name
	state.Queues = append([]string(nil), state.Queues...)
	state.Pools = cloneProcessPools(state.Pools)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := state.LastHeartbeatAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	for _, existing := range s.supervisors {
		derived := supervisorWithDerivedStatus(existing, s.control, s.options.HeartbeatTTL, now)
		if derived.Status != SupervisorStale && existing.Name == state.Name && existing.Host == state.Host && existing.Environment == state.Environment {
			return false, nil
		}
	}
	s.supervisors[supervisorInstanceID(state)] = state
	return true, nil
}

// Master 读取单个 master，并按 heartbeat TTL 派生 stale 状态。
func (s *MemoryStore) Master(_ context.Context, id string, now time.Time) (MasterState, bool, error) {
	s.mu.RLock()
	state, ok := s.masters[strings.TrimSpace(id)]
	s.mu.RUnlock()
	if !ok {
		return MasterState{}, false, nil
	}
	return masterWithDerivedStatus(state, s.options.HeartbeatTTL, now), true, nil
}

// Masters 读取全部 master，并按 heartbeat TTL 派生 stale 状态。
func (s *MemoryStore) Masters(_ context.Context, now time.Time) ([]MasterState, error) {
	s.mu.RLock()
	items := make([]MasterState, 0, len(s.masters))
	for _, state := range s.masters {
		items = append(items, masterWithDerivedStatus(state, s.options.HeartbeatTTL, now))
	}
	s.mu.RUnlock()
	sortMasterStates(items)
	return items, nil
}

func (s *MemoryStore) HeartbeatSupervisor(_ context.Context, state SupervisorState) error {
	name := strings.TrimSpace(state.Name)
	if name == "" {
		return fmt.Errorf("horizon: supervisor name is required")
	}
	state.Name = name
	state.Queues = append([]string(nil), state.Queues...)
	state.Pools = cloneProcessPools(state.Pools)
	s.mu.Lock()
	// 同名 supervisor 可以在不同 host/environment 同时存在；列表必须保留每个实例。
	s.supervisors[supervisorInstanceID(state)] = state
	s.mu.Unlock()
	return nil
}

// HeartbeatWorker 写入 worker heartbeat 原始状态。
//
// 参数说明：state.ID 必填，Supervisor 用于后续继承指定 supervisor 的 pause 标记。
func (s *MemoryStore) HeartbeatWorker(_ context.Context, state WorkerState) error {
	id := strings.TrimSpace(state.ID)
	if id == "" {
		return fmt.Errorf("horizon: worker id is required")
	}
	state.ID = id
	state.ConfiguredQueues = append([]string(nil), state.ConfiguredQueues...)
	s.mu.Lock()
	// worker slot ID 只在单个 supervisor 内稳定；跨 host/environment 复用时不能互相覆盖。
	s.workers[workerInstanceID(state)] = state
	s.mu.Unlock()
	return nil
}

// Supervisor 读取单个 supervisor，并按当前控制标记和 heartbeat TTL 派生状态。
func (s *MemoryStore) Supervisor(_ context.Context, name string, now time.Time) (SupervisorState, bool, error) {
	name = strings.TrimSpace(name)
	s.mu.RLock()
	control := cloneControlState(s.control)
	var state SupervisorState
	ok := false
	for _, candidate := range s.supervisors {
		if candidate.Name != name {
			continue
		}
		if !ok || candidate.LastHeartbeatAt.After(state.LastHeartbeatAt) {
			state = candidate
			ok = true
		}
	}
	s.mu.RUnlock()
	if !ok {
		return SupervisorState{}, false, nil
	}
	return supervisorWithDerivedStatus(state, control, s.options.HeartbeatTTL, now), true, nil
}

// Supervisors 读取所有 supervisor，并按当前控制标记和 heartbeat TTL 派生状态。
func (s *MemoryStore) Supervisors(_ context.Context, now time.Time) ([]SupervisorState, error) {
	s.mu.RLock()
	control := cloneControlState(s.control)
	items := make([]SupervisorState, 0, len(s.supervisors))
	for _, state := range s.supervisors {
		items = append(items, supervisorWithDerivedStatus(state, control, s.options.HeartbeatTTL, now))
	}
	s.mu.RUnlock()
	sortSupervisorStates(items)
	return items, nil
}

// Worker 读取单个 worker，并按当前控制标记和 heartbeat TTL 派生状态。
func (s *MemoryStore) Worker(_ context.Context, id string, now time.Time) (WorkerState, bool, error) {
	id = strings.TrimSpace(id)
	s.mu.RLock()
	control := cloneControlState(s.control)
	var state WorkerState
	ok := false
	for _, candidate := range s.workers {
		if candidate.ID != id {
			continue
		}
		if !ok || candidate.LastHeartbeatAt.After(state.LastHeartbeatAt) {
			state = candidate
			ok = true
		}
	}
	s.mu.RUnlock()
	if !ok {
		return WorkerState{}, false, nil
	}
	return workerWithDerivedStatus(state, control, s.options.HeartbeatTTL, now), true, nil
}

func supervisorInstanceID(state SupervisorState) string {
	// NUL 分隔避免 host/env/name 中的普通标点造成身份碰撞。
	return strings.TrimSpace(state.Host) + "\x00" + strings.TrimSpace(state.Environment) + "\x00" + strings.TrimSpace(state.Name)
}

func workerInstanceID(state WorkerState) string {
	// worker ID 可能是固定 slot 名；实例身份必须带上运行域和归属 supervisor。
	return strings.TrimSpace(state.Host) + "\x00" + strings.TrimSpace(state.Environment) + "\x00" + strings.TrimSpace(state.Supervisor) + "\x00" + strings.TrimSpace(state.ID)
}

// Workers 读取所有 worker，并按当前控制标记和 heartbeat TTL 派生状态。
func (s *MemoryStore) Workers(_ context.Context, now time.Time) ([]WorkerState, error) {
	s.mu.RLock()
	control := cloneControlState(s.control)
	items := make([]WorkerState, 0, len(s.workers))
	for _, state := range s.workers {
		items = append(items, workerWithDerivedStatus(state, control, s.options.HeartbeatTTL, now))
	}
	s.mu.RUnlock()
	sortWorkerStates(items)
	return items, nil
}

// Control 返回控制标记副本，避免外部修改内部 map。
func (s *MemoryStore) Control(context.Context) (ControlState, error) {
	s.mu.RLock()
	control := cloneControlState(s.control)
	s.mu.RUnlock()
	return control, nil
}

// SetGlobalPaused 写入或清除全局暂停标记。
func (s *MemoryStore) SetGlobalPaused(_ context.Context, paused bool) error {
	s.mu.Lock()
	s.control.GlobalPaused = paused
	s.mu.Unlock()
	return nil
}

// SetSupervisorPaused 写入或清除指定 supervisor 的暂停标记。
//
// 参数说明：name 是 supervisor 名称，空值视为调用方错误。
func (s *MemoryStore) SetSupervisorPaused(_ context.Context, name string, paused bool) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("horizon: supervisor name is required")
	}
	s.mu.Lock()
	if s.control.PausedSupervisors == nil {
		s.control.PausedSupervisors = make(map[string]bool)
	}
	if paused {
		s.control.PausedSupervisors[name] = true
	} else {
		delete(s.control.PausedSupervisors, name)
	}
	s.mu.Unlock()
	return nil
}

// RequestTerminate 写入一次性 terminate 请求时间。
//
// 逻辑说明：空时间会替换为当前时间；horizon:continue 不会调用 ClearTerminateRequest。
func (s *MemoryStore) RequestTerminate(_ context.Context, at time.Time, wait bool) error {
	if at.IsZero() {
		at = time.Now()
	}
	s.mu.Lock()
	s.control.TerminateRequestedAt = at
	s.control.TerminateShouldWait = wait
	s.mu.Unlock()
	return nil
}

// ClearTerminateRequest 清除 terminate 请求，供后续 supervisor 启动流程使用。
func (s *MemoryStore) ClearTerminateRequest(context.Context) error {
	s.mu.Lock()
	s.control.TerminateRequestedAt = time.Time{}
	s.control.TerminateShouldWait = false
	s.mu.Unlock()
	return nil
}

// Trim 清理超过 heartbeat TTL 的 supervisor/worker 记录。
func (s *MemoryStore) Trim(_ context.Context, now time.Time) error {
	s.mu.Lock()
	for id, state := range s.masters {
		if heartbeatStale(state.LastHeartbeatAt, s.options.HeartbeatTTL, now) {
			delete(s.masters, id)
		}
	}
	for name, state := range s.supervisors {
		if heartbeatStale(state.LastHeartbeatAt, s.options.HeartbeatTTL, now) {
			delete(s.supervisors, name)
		}
	}
	for id, state := range s.workers {
		if heartbeatStale(state.LastHeartbeatAt, s.options.HeartbeatTTL, now) {
			delete(s.workers, id)
		}
	}
	s.mu.Unlock()
	return nil
}

// StatusSnapshot 聚合控制标记和 heartbeat 状态，派生 horizon:status 需要的全局快照。
func (s *MemoryStore) StatusSnapshot(ctx context.Context, now time.Time) (StatusSnapshot, error) {
	control, err := s.Control(ctx)
	if err != nil {
		return StatusSnapshot{}, err
	}
	supervisors, err := s.Supervisors(ctx, now)
	if err != nil {
		return StatusSnapshot{}, err
	}
	workers, err := s.Workers(ctx, now)
	if err != nil {
		return StatusSnapshot{}, err
	}
	return deriveStatusSnapshot(control, supervisors, workers), nil
}

// ClearMetrics 清理事件派生 metrics，不影响 heartbeat、control flags 或 queue 数据。
func (s *MemoryStore) ClearMetrics(context.Context) error {
	s.mu.Lock()
	s.eventMetricWindows = nil
	s.eventMetricRollups = nil
	s.mu.Unlock()
	return nil
}

// AppendEventMetricWindows 追加 event_metrics window 数据，并按 retention 清理旧 window。
//
// 设计思路：memory store 也保持追加语义，避免测试和本地开发只看到最后一次 snapshot 覆盖结果。
func (s *MemoryStore) AppendEventMetricWindows(_ context.Context, windows []EventMetricWindow, retention time.Duration) error {
	if len(windows) == 0 {
		return nil
	}
	now := time.Now().UTC()
	// 以数据和当前时间中最晚的时刻作为 retention 截断参考，避免测试或延迟
	// 场景下过去的事件窗口被 wall clock 错误修剪。
	reference := time.Time{}
	for _, window := range windows {
		if !window.WindowEnd.IsZero() && window.WindowEnd.After(reference) {
			reference = window.WindowEnd
		}
	}
	for _, window := range s.eventMetricWindows {
		if !window.WindowEnd.IsZero() && window.WindowEnd.After(reference) {
			reference = window.WindowEnd
		}
	}
	if reference.IsZero() {
		reference = now
	}
	cutoff := reference.Add(-retention)
	s.mu.Lock()
	for _, window := range windows {
		if window.FlushAt.IsZero() {
			window.FlushAt = now
		}
		s.eventMetricWindows = append(s.eventMetricWindows, window)
	}
	s.eventMetricRollups = append(s.eventMetricRollups, queueEventMetricRollups(windows, now)...)
	if retention > 0 {
		out := s.eventMetricWindows[:0]
		for _, window := range s.eventMetricWindows {
			// retention 基于事件窗口结束时间，而不是 flush_at，避免慢 flush 改变业务窗口归属。
			if window.WindowEnd.IsZero() || !window.WindowEnd.Before(cutoff) {
				out = append(out, window)
			}
		}
		s.eventMetricWindows = out
		rollups := s.eventMetricRollups[:0]
		for _, window := range s.eventMetricRollups {
			if window.WindowEnd.IsZero() || !window.WindowEnd.Before(cutoff) {
				rollups = append(rollups, window)
			}
		}
		s.eventMetricRollups = rollups
	}
	s.mu.Unlock()
	return nil
}

// EventMetricWindows 返回按窗口时间倒序排列的 event_metrics 追加窗口。
func (s *MemoryStore) EventMetricWindows(_ context.Context, query EventMetricWindowQuery) (PageEnvelope[EventMetricWindow], error) {
	query = normalizeEventMetricWindowQuery(query)
	s.mu.RLock()
	items := make([]EventMetricWindow, 0, len(s.eventMetricWindows))
	for _, window := range s.eventMetricWindows {
		if eventMetricWindowMatchesQuery(window, query) {
			items = append(items, window)
		}
	}
	s.mu.RUnlock()
	sortEventMetricWindows(items)
	return PageEnvelope[EventMetricWindow]{
		Items:    pageSlice(items, query.Page),
		Total:    len(items),
		Page:     query.Page.Page,
		PageSize: query.Page.PageSize,
	}, nil
}

// EventMetricRollupWindows 返回 queue 级聚合窗口，不包含来源和 job 维度。
//
// 设计边界：MemoryStore 与 RedisStore 保持同一读语义，summary 路径可以直接使用该结果；
// source_details、Metric Sources 和 raw windows 分页仍必须调用 EventMetricWindows。
func (s *MemoryStore) EventMetricRollupWindows(_ context.Context, query EventMetricWindowQuery) ([]EventMetricWindow, error) {
	query = normalizeEventMetricWindowQuery(query)
	s.mu.RLock()
	items := make([]EventMetricWindow, 0, len(s.eventMetricRollups))
	for _, window := range s.eventMetricRollups {
		if eventMetricWindowMatchesQuery(window, query) {
			items = append(items, window)
		}
	}
	s.mu.RUnlock()
	sortEventMetricWindows(items)
	return cloneEventMetricWindows(items), nil
}

// SaveQueueLengthSnapshot 保存最近一次队列长度采样结果。
//
// 参数说明：snapshot 由 horizon:snapshot 在所有 Size 调用成功后传入；memory store 会复制切片，避免调用方后续修改影响 Store 状态。
func (s *MemoryStore) SaveQueueLengthSnapshot(_ context.Context, snapshot QueueLengthSnapshot) error {
	s.mu.Lock()
	s.queueLengths = cloneQueueLengthSnapshot(snapshot)
	s.mu.Unlock()
	return nil
}

// QueueLengthSnapshot 读取最近一次队列长度采样结果；没有保存过时返回零值快照。
func (s *MemoryStore) QueueLengthSnapshot(context.Context) (QueueLengthSnapshot, error) {
	s.mu.RLock()
	snapshot := cloneQueueLengthSnapshot(s.queueLengths)
	s.mu.RUnlock()
	return snapshot, nil
}

// SaveHighValueDetails 保存 failed/poison/slow job 的可丢弃安全诊断摘要。
func (s *MemoryStore) SaveHighValueDetails(_ context.Context, details []HighValueJobDetail, retention time.Duration) error {
	if len(details) == 0 {
		return nil
	}
	now := time.Now().UTC()
	cutoff := now.Add(-retention)
	s.mu.Lock()
	for _, detail := range details {
		id := strings.TrimSpace(detail.ID)
		if id == "" {
			continue
		}
		detail.ID = id
		if detail.OccurredAt.IsZero() {
			detail.OccurredAt = now
		}
		s.highValueDetails[id] = detail
	}
	if retention > 0 {
		for id, detail := range s.highValueDetails {
			if !detail.OccurredAt.IsZero() && detail.OccurredAt.Before(cutoff) {
				delete(s.highValueDetails, id)
			}
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) HighValueDetails(_ context.Context, query HighValueDetailQuery) (PageEnvelope[HighValueJobDetail], error) {
	query = normalizeHighValueDetailQuery(query)
	s.mu.RLock()
	items := make([]HighValueJobDetail, 0, len(s.highValueDetails))
	for _, detail := range s.highValueDetails {
		if highValueDetailMatchesQuery(detail, query) {
			items = append(items, detail)
		}
	}
	s.mu.RUnlock()
	sortHighValueJobDetails(items)
	items = cloneHighValueJobDetails(items)
	return PageEnvelope[HighValueJobDetail]{
		Items:    pageSlice(items, query.Page),
		Total:    len(items),
		Page:     query.Page.Page,
		PageSize: query.Page.PageSize,
	}, nil
}

func (s *MemoryStore) HighValueDetail(_ context.Context, id string) (HighValueJobDetail, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return HighValueJobDetail{}, false, nil
	}
	s.mu.RLock()
	detail, ok := s.highValueDetails[id]
	s.mu.RUnlock()
	return detail, ok, nil
}

func (s *MemoryStore) SaveObservabilityDiagnostics(_ context.Context, diagnostics []ObservabilityDiagnostic, retention time.Duration) error {
	if len(diagnostics) == 0 {
		return nil
	}
	now := time.Now().UTC()
	cutoff := now.Add(-retention)
	s.mu.Lock()
	for _, diagnostic := range diagnostics {
		if diagnostic.Count <= 0 {
			continue
		}
		if diagnostic.ObservedAt.IsZero() {
			diagnostic.ObservedAt = now
		}
		s.diagnostics = append(s.diagnostics, diagnostic)
	}
	if retention > 0 {
		out := s.diagnostics[:0]
		for _, diagnostic := range s.diagnostics {
			if diagnostic.ObservedAt.IsZero() || !diagnostic.ObservedAt.Before(cutoff) {
				out = append(out, diagnostic)
			}
		}
		s.diagnostics = out
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ObservabilityDiagnostics(_ context.Context, page PageRequest) (PageEnvelope[ObservabilityDiagnostic], error) {
	s.mu.RLock()
	items := cloneObservabilityDiagnostics(s.diagnostics)
	s.mu.RUnlock()
	sortObservabilityDiagnostics(items)
	return PageEnvelope[ObservabilityDiagnostic]{
		Items:    pageSlice(items, page),
		Total:    len(items),
		Page:     page.Page,
		PageSize: page.PageSize,
	}, nil
}

// SaveBatchSummary 保存 BatchEvent 派生出的展示安全批次摘要。
//
// 参数说明：summary.ID 必须非空；UpdatedAt 为空时使用当前时间，便于按最近变化排序。
func (s *MemoryStore) SaveBatchSummary(_ context.Context, summary BatchSummary) error {
	id := strings.TrimSpace(summary.ID)
	if id == "" {
		return fmt.Errorf("horizon: batch id is required")
	}
	summary.ID = id
	if summary.UpdatedAt.IsZero() {
		summary.UpdatedAt = time.Now().UTC()
	}
	s.mu.Lock()
	s.batches[id] = summary
	s.mu.Unlock()
	return nil
}

// SaveBatchSummaries 批量保存 batch summary，并按 batch_summary_retention 清理旧摘要。
func (s *MemoryStore) SaveBatchSummaries(ctx context.Context, items []BatchSummary, retention time.Duration) error {
	for _, item := range items {
		if err := s.SaveBatchSummary(ctx, item); err != nil {
			return err
		}
	}
	if retention <= 0 {
		return nil
	}
	// 以数据中最晚的 retention time 作为截断参考，避免 wall clock 误修剪过去数据。
	trimRef := time.Time{}
	for _, item := range items {
		if rt := batchSummaryRetentionTime(item); rt.After(trimRef) {
			trimRef = rt
		}
	}
	for _, item := range s.batches {
		if rt := batchSummaryRetentionTime(item); rt.After(trimRef) {
			trimRef = rt
		}
	}
	if trimRef.IsZero() {
		trimRef = time.Now().UTC()
	}
	cutoff := trimRef.Add(-retention)
	s.mu.Lock()
	for id, item := range s.batches {
		if batchSummaryRetentionTime(item).Before(cutoff) {
			delete(s.batches, id)
		}
	}
	s.mu.Unlock()
	return nil
}

// Batches 按 ID 或名称搜索批次摘要，并按创建时间倒序返回。
func (s *MemoryStore) Batches(_ context.Context, query string) ([]BatchSummary, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	s.mu.RLock()
	items := make([]BatchSummary, 0, len(s.batches))
	for _, summary := range s.batches {
		if batchMatchesQuery(summary, query) {
			items = append(items, summary)
		}
	}
	s.mu.RUnlock()
	sortBatchSummaries(items)
	return cloneBatchSummaries(items), nil
}

// BatchesPage 在内存中按 query 与分页窗口返回批次摘要。
//
// 逻辑说明：memory store 允许直接扫描全部数据，但仍保持与 Redis store 相同的分页 envelope，
// 便于 HTTP 层和测试复用一套响应结构。
func (s *MemoryStore) BatchesPage(_ context.Context, query string, page PageRequest) (PageEnvelope[BatchSummary], error) {
	query = strings.ToLower(strings.TrimSpace(query))
	s.mu.RLock()
	items := make([]BatchSummary, 0, len(s.batches))
	for _, summary := range s.batches {
		if batchMatchesQuery(summary, query) {
			items = append(items, summary)
		}
	}
	s.mu.RUnlock()
	sortBatchSummaries(items)
	total := len(items)
	return PageEnvelope[BatchSummary]{
		Items:    pageSlice(cloneBatchSummaries(items), page),
		Total:    total,
		Page:     page.Page,
		PageSize: page.PageSize,
	}, nil
}

// Batch 读取单个批次摘要；不存在时返回 ok=false。
func (s *MemoryStore) Batch(_ context.Context, id string) (BatchSummary, bool, error) {
	s.mu.RLock()
	summary, ok := s.batches[strings.TrimSpace(id)]
	s.mu.RUnlock()
	return summary, ok, nil
}

// RecordOrphanProcess 记录 master 下首次发现 orphan worker PID 的时间。
//
// 参数说明：masterID 是 Horizon master 标识；pid 是 OS 进程 ID；firstSeenAt 为空时使用当前时间。
func (s *MemoryStore) RecordOrphanProcess(_ context.Context, masterID string, pid int, firstSeenAt time.Time) error {
	masterID = strings.TrimSpace(masterID)
	if masterID == "" {
		return fmt.Errorf("horizon: master id is required")
	}
	if pid <= 0 {
		return fmt.Errorf("horizon: orphan pid must be positive")
	}
	if firstSeenAt.IsZero() {
		firstSeenAt = time.Now().UTC()
	}
	s.mu.Lock()
	if s.orphans[masterID] == nil {
		s.orphans[masterID] = make(map[int]time.Time)
	}
	if s.orphans[masterID][pid].IsZero() {
		s.orphans[masterID][pid] = firstSeenAt
	}
	s.mu.Unlock()
	return nil
}

// OrphanProcesses 列出指定 master 下的 orphan PID tracking 记录。
func (s *MemoryStore) OrphanProcesses(_ context.Context, masterID string) ([]OrphanProcess, error) {
	masterID = strings.TrimSpace(masterID)
	s.mu.RLock()
	items := cloneOrphanProcesses(masterID, s.orphans[masterID])
	s.mu.RUnlock()
	return items, nil
}

// OrphanProcessesOlderThan 返回 first_seen 早于指定 age 的 orphan 记录。
func (s *MemoryStore) OrphanProcessesOlderThan(_ context.Context, masterID string, age time.Duration, now time.Time) ([]OrphanProcess, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := now.Add(-age)
	s.mu.RLock()
	all := cloneOrphanProcesses(masterID, s.orphans[masterID])
	s.mu.RUnlock()
	out := all[:0]
	for _, item := range all {
		if !item.FirstSeenAt.After(cutoff) {
			out = append(out, item)
		}
	}
	return out, nil
}

// ForgetOrphanProcess 删除指定 master 下的 orphan PID tracking 记录。
func (s *MemoryStore) ForgetOrphanProcess(_ context.Context, masterID string, pid int) error {
	masterID = strings.TrimSpace(masterID)
	s.mu.Lock()
	if s.orphans[masterID] != nil {
		delete(s.orphans[masterID], pid)
		if len(s.orphans[masterID]) == 0 {
			delete(s.orphans, masterID)
		}
	}
	s.mu.Unlock()
	return nil
}

func cloneOrphanProcesses(masterID string, values map[int]time.Time) []OrphanProcess {
	items := make([]OrphanProcess, 0, len(values))
	for pid, firstSeenAt := range values {
		items = append(items, OrphanProcess{MasterID: masterID, PID: pid, FirstSeenAt: firstSeenAt})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].PID < items[j].PID })
	return items
}
