package horizon

import (
	"context"
	"time"

	goprocess "github.com/prismgo/framework/process"
)

const (
	// ProcessKindMaster 表示 Horizon 顶层 master 进程。
	ProcessKindMaster = "master"
	// ProcessKindSupervisor 表示 Horizon supervisor 进程。
	ProcessKindSupervisor = "supervisor"
	// ProcessKindWorker 表示 Horizon worker 进程。
	ProcessKindWorker = "worker"
)

// ProcessReadModel 是 Dashboard/API 展示进程明细时使用的只读模型。
//
// 需求背景：issue 27 要求 masters、supervisors、workers、stale 视图返回统一形状，且 CPU、
// 内存、协程数等字段必须带字段级能力状态。该 DTO 不暴露 job payload、broker credential、
// Redis/RabbitMQ 内部连接参数或完整错误对象。
type ProcessReadModel struct {
	ID                   string           `json:"id"`
	Name                 string           `json:"name"`
	Kind                 string           `json:"kind"`
	Status               string           `json:"status"`
	Host                 string           `json:"host"`
	PID                  int              `json:"pid"`
	Supervisor           string           `json:"supervisor,omitempty"`
	LastHeartbeatAt      time.Time        `json:"last_heartbeat_at"`
	SampledAt            time.Time        `json:"sampled_at"`
	SampleWindowMS       int64            `json:"sample_window_ms"`
	CPUPercent           goprocess.Metric `json:"cpu_percent"`
	MemoryRSSBytes       goprocess.Metric `json:"memory_rss_bytes"`
	MemoryPercent        goprocess.Metric `json:"memory_percent"`
	CollectorMemoryBytes goprocess.Metric `json:"collector_memory_bytes"`
	GoroutineCount       goprocess.Metric `json:"goroutine_count"`
	CurrentQueue         goprocess.Metric `json:"current_queue"`
	ConfiguredQueues     []string         `json:"configured_queues"`
	HeartbeatError       HeartbeatError   `json:"heartbeat_error,omitempty"`
}

// HeartbeatError 是 worker 最近一次 heartbeat 写入失败的安全诊断摘要。
type HeartbeatError struct {
	Code     string    `json:"code"`
	Message  string    `json:"message"`
	FailedAt time.Time `json:"failed_at"`
}

// buildProcessReadModels 将当前分页中的 Store 状态投影为只读进程明细，并仅对这些 PID 做有界采样。
//
// 参数说明：observer 是 prismgo/process 的公共采样接口；items 是已经分页后的进程基础状态；
// toModel 负责把不同 Store 状态转换为统一 read model。采样失败不会阻塞列表展示，而是回退到
// 字段级 unavailable，避免 Dashboard 因单个 PID 退出或平台不支持而整体失败。
func buildProcessReadModels[T any](ctx context.Context, observer goprocess.Observer, items []T, toModel func(T) ProcessReadModel) []ProcessReadModel {
	out := make([]ProcessReadModel, 0, len(items))
	pids := make([]int, 0, len(items))
	for _, item := range items {
		model := toModel(item)
		out = append(out, model)
		if model.PID > 0 {
			pids = append(pids, model.PID)
		}
	}
	snapshots := map[int]goprocess.Snapshot{}
	if observer != nil && len(pids) > 0 {
		if observed, err := observer.Observe(ctx, pids); err == nil {
			snapshots = observed
		}
	}
	for i := range out {
		applyProcessSnapshot(&out[i], snapshots[out[i].PID])
	}
	return out
}

// masterProcessReadModel 将 master heartbeat 状态转换为统一进程 read model。
// master 不消费 queue，因此 current_queue 明确返回 unavailable，避免 UI 把空字符串当成真实队列名。
func masterProcessReadModel(master MasterState) ProcessReadModel {
	return ProcessReadModel{
		ID:              master.ID,
		Name:            master.ID,
		Kind:            ProcessKindMaster,
		Status:          master.Status,
		Host:            master.Host,
		PID:             master.PID,
		LastHeartbeatAt: master.LastHeartbeatAt,
		MemoryRSSBytes:  master.MemoryRSSBytes,
		MemoryPercent:   master.MemoryPercent,
		GoroutineCount:  master.GoroutineCount,
		CurrentQueue:    fieldUnavailable("queue", "master is not bound to a queue"),
	}
}

// supervisorProcessReadModel 将 supervisor 状态转换为统一进程 read model。
// 设计思路：supervisor 有配置队列但不直接消费当前 job，所以只暴露 configured_queues 和不可用的 current_queue。
func supervisorProcessReadModel(supervisor SupervisorState) ProcessReadModel {
	return ProcessReadModel{
		ID:               supervisor.Name,
		Name:             supervisor.Name,
		Kind:             ProcessKindSupervisor,
		Status:           supervisor.Status,
		Host:             supervisor.Host,
		PID:              supervisor.PID,
		LastHeartbeatAt:  supervisor.LastHeartbeatAt,
		MemoryRSSBytes:   supervisor.MemoryRSSBytes,
		MemoryPercent:    supervisor.MemoryPercent,
		GoroutineCount:   supervisor.GoroutineCount,
		CurrentQueue:     fieldUnavailable("queue", "supervisor is configured for queues but does not consume a current job"),
		ConfiguredQueues: append([]string(nil), supervisor.Queues...),
	}
}

// workerProcessReadModel 将 worker heartbeat 状态转换为统一进程 read model。
//
// 设计原因（issue 50）：worker 不再保存 CurrentQueue，因为当前任务明细已从 worker heartbeat state 中移除；
// 任务量统计走 collector + flusher 的 event_metrics 聚合通道。worker 继续暴露 ConfiguredQueues
// 描述启动消费范围。
func workerProcessReadModel(worker WorkerState) ProcessReadModel {
	return ProcessReadModel{
		ID:                   worker.ID,
		Name:                 worker.ID,
		Kind:                 ProcessKindWorker,
		Status:               worker.Status,
		Host:                 worker.Host,
		PID:                  worker.PID,
		Supervisor:           worker.Supervisor,
		LastHeartbeatAt:      worker.LastHeartbeatAt,
		MemoryRSSBytes:       worker.MemoryRSSBytes,
		MemoryPercent:        worker.MemoryPercent,
		CollectorMemoryBytes: preferSnapshotMetric(goprocess.Metric{}, worker.CollectorMemoryBytes, goprocess.UnitBytes, "collector memory unavailable"),
		GoroutineCount:       worker.GoroutineCount,
		CurrentQueue:         fieldUnavailable("queue", "worker current queue unavailable"),
		ConfiguredQueues:     append([]string(nil), worker.ConfiguredQueues...),
		HeartbeatError:       heartbeatErrorReadModel(worker),
	}
}

func heartbeatErrorReadModel(worker WorkerState) HeartbeatError {
	if worker.LastHeartbeatErrorCode == "" {
		return HeartbeatError{}
	}
	return HeartbeatError{
		Code:     worker.LastHeartbeatErrorCode,
		Message:  worker.LastHeartbeatErrorMessage,
		FailedAt: worker.LastHeartbeatErrorAt,
	}
}

// applyProcessSnapshot 将 prismgo/process 的 OS 采样结果合并到 Horizon read model。
// 复杂逻辑说明：请求时采样优先于 Store 中的 heartbeat 自省字段；如果没有采样结果，则保留已存字段或补 unavailable。
func applyProcessSnapshot(model *ProcessReadModel, snapshot goprocess.Snapshot) {
	if model == nil {
		return
	}
	if snapshot.PID == 0 {
		if model.CPUPercent.Status == "" {
			model.CPUPercent = fieldUnavailable(goprocess.UnitPercent, "process sample unavailable")
		}
		fillStoredOrUnavailable(model)
		return
	}
	model.SampledAt = snapshot.SampledAt
	model.SampleWindowMS = snapshot.SampleWindowMS
	model.CPUPercent = snapshot.CPUPercent
	model.MemoryRSSBytes = preferSnapshotMetric(snapshot.MemoryRSSBytes, model.MemoryRSSBytes, goprocess.UnitBytes, "rss unavailable")
	model.MemoryPercent = preferSnapshotMetric(snapshot.MemoryPercent, model.MemoryPercent, goprocess.UnitPercent, "memory percent unavailable")
	model.GoroutineCount = preferSnapshotMetric(snapshot.GoroutineCount, model.GoroutineCount, goprocess.UnitCount, "goroutine count unavailable")
}

// fillStoredOrUnavailable 在 OS 采样缺失时补齐字段级状态。
// 设计思路：heartbeat 已上报的低成本字段可以继续展示，完全缺失的字段再返回 unavailable reason。
func fillStoredOrUnavailable(model *ProcessReadModel) {
	model.MemoryRSSBytes = preferSnapshotMetric(goprocess.Metric{}, model.MemoryRSSBytes, goprocess.UnitBytes, "rss unavailable")
	model.MemoryPercent = preferSnapshotMetric(goprocess.Metric{}, model.MemoryPercent, goprocess.UnitPercent, "memory percent unavailable")
	model.GoroutineCount = preferSnapshotMetric(goprocess.Metric{}, model.GoroutineCount, goprocess.UnitCount, "goroutine count unavailable")
}

// preferSnapshotMetric 按“请求采样 > heartbeat 存储值 > unavailable”选择最终字段。
// 参数说明：sample 是本次请求采样值；stored 是 Store 中已有值；unit/reason 用于构造兜底 unavailable。
func preferSnapshotMetric(sample goprocess.Metric, stored goprocess.Metric, unit string, reason string) goprocess.Metric {
	if sample.Status == goprocess.StatusAvailable {
		return sample
	}
	if stored.Status == goprocess.StatusAvailable {
		return stored
	}
	if sample.Status != "" {
		return sample
	}
	if stored.Status != "" {
		return stored
	}
	return fieldUnavailable(unit, reason)
}

// fieldUnavailable 构造 Horizon read model 的字段级不可用状态，保持与 prismgo/process 的 Metric 形状一致。
func fieldUnavailable(unit string, reason string) goprocess.Metric {
	return goprocess.Metric{Value: nil, Unit: unit, Status: goprocess.StatusUnavailable, Reason: reason}
}

// applySelfObservationToMaster 把当前 Go 进程可低成本获得的自省字段写入 master heartbeat。
//
// 需求背景：issue 27 要求 heartbeat 路径上报 goroutine_count 与 RSS，但不能在 heartbeat 中阻塞采样 CPU。
// 因此这里只消费 prismgo/process.SelfSnapshot 的低成本字段，CPU% 仍由列表 API 的按需采样补齐。
func applySelfObservationToMaster(state *MasterState) {
	snapshot := goprocess.SelfSnapshot()
	state.GoroutineCount = snapshot.GoroutineCount
	state.MemoryRSSBytes = snapshot.MemoryRSSBytes
	state.MemoryPercent = snapshot.MemoryPercent
}

// applySelfObservationToSupervisor 把当前 supervisor 进程的低成本自省字段写入 heartbeat。
func applySelfObservationToSupervisor(state *SupervisorState) {
	snapshot := goprocess.SelfSnapshot()
	state.GoroutineCount = snapshot.GoroutineCount
	state.MemoryRSSBytes = snapshot.MemoryRSSBytes
	state.MemoryPercent = snapshot.MemoryPercent
}

// applySelfObservationToWorker 把当前 worker 进程的低成本自省字段写入 heartbeat。
func applySelfObservationToWorker(state *WorkerState) {
	snapshot := goprocess.SelfSnapshot()
	state.GoroutineCount = snapshot.GoroutineCount
	state.MemoryRSSBytes = snapshot.MemoryRSSBytes
	state.MemoryPercent = snapshot.MemoryPercent
}
