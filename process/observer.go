package process

import (
	"context"
	"os"
	"runtime"
	"time"
)

const (
	// StatusAvailable 表示该字段已成功采集，可直接展示 value。
	StatusAvailable = "available"
	// StatusDisabled 预留给调用方显式关闭某项采集能力时使用。
	StatusDisabled = "disabled"
	// StatusUnsupported 表示当前平台或运行环境不支持该字段。
	StatusUnsupported = "unsupported"
	// StatusUnavailable 表示字段理论可用但本次采集失败或数据缺失。
	StatusUnavailable = "unavailable"

	// UnitPercent 表示百分比指标，CPU 和内存占比统一使用该单位。
	UnitPercent = "percent"
	// UnitBytes 表示字节数指标，RSS 内存统一使用该单位。
	UnitBytes = "bytes"
	// UnitCount 表示计数指标，goroutine 数统一使用该单位。
	UnitCount = "count"
	// UnitMilliseconds 表示采样窗口时长，便于 UI 解释瞬时 CPU 值。
	UnitMilliseconds = "milliseconds"
)

// Metric 是进程观测 read model 中所有成本敏感字段的统一形状。
// 设计思路：不可用值必须用 nil + 稳定 reason 表达，避免调用方把缺失数据误判为真实 0。
type Metric struct {
	Value  any    `json:"value"`
	Unit   string `json:"unit"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// Snapshot 保存单个 OS 进程一次有界观测结果。
// 需求背景：Horizon 只消费该 read model，不在 HTTP handler 或 Dashboard 内重复实现跨平台采样逻辑。
type Snapshot struct {
	PID              int       `json:"pid"`
	SampledAt        time.Time `json:"sampled_at"`
	SampleWindowMS   int64     `json:"sample_window_ms"`
	CPUPercent       Metric    `json:"cpu_percent"`
	MemoryRSSBytes   Metric    `json:"memory_rss_bytes"`
	MemoryPercent    Metric    `json:"memory_percent"`
	GoroutineCount   Metric    `json:"goroutine_count"`
	Platform         string    `json:"platform"`
	PlatformProvider string    `json:"platform_provider"`
}

// ObserverOptions 控制 OS 进程采样成本边界。
type ObserverOptions struct {
	// SampleWindow 是 CPU 短窗口采样时长，避免请求路径出现无界等待。
	SampleWindow time.Duration
}

// Observer 提供按 PID 读取进程观测快照的公共接口。
type Observer interface {
	Observe(context.Context, []int) (map[int]Snapshot, error)
}

type observer struct {
	options ObserverOptions
	sampler platformSampler
}

// platformSampler 隔离不同操作系统的进程读取细节。
// 设计思路：Horizon 只依赖 Observer 接口，Linux 的 /proc 实现和其他平台降级实现都放在 prismgo/process 内部。
type platformSampler interface {
	observe(context.Context, []int, time.Duration) (map[int]Snapshot, error)
	selfSnapshot() Snapshot
}

// NewObserver 创建默认 OS 进程观测器。
// 参数说明：SampleWindow 小于等于 0 时使用短窗口默认值，避免请求路径产生无界等待。
func NewObserver(options ObserverOptions) Observer {
	if options.SampleWindow <= 0 {
		options.SampleWindow = 100 * time.Millisecond
	}
	return &observer{options: options, sampler: newPlatformSampler()}
}

// Observe 按 PID 列表返回一次有界采样结果。
// 参数说明：ctx 用于请求取消；pids 是当前分页中需要观测的进程 ID。函数会去重但保留无效 PID，
// 让调用方获得字段级 unavailable，而不是因为单个 PID 异常导致整个列表失败。
func (o *observer) Observe(ctx context.Context, pids []int) (map[int]Snapshot, error) {
	if o == nil {
		o = &observer{options: ObserverOptions{SampleWindow: 100 * time.Millisecond}, sampler: newPlatformSampler()}
	}
	if o.sampler == nil {
		o.sampler = newPlatformSampler()
	}
	if o.options.SampleWindow <= 0 {
		o.options.SampleWindow = 100 * time.Millisecond
	}
	return o.sampler.observe(ctx, normalizePIDs(pids), o.options.SampleWindow)
}

// SelfSnapshot 返回当前 Go 进程可低成本自省的字段，供 heartbeat 路径使用。
func SelfSnapshot() Snapshot {
	return newPlatformSampler().selfSnapshot()
}

// normalizePIDs 去除重复 PID，避免同一分页中重复进程触发重复采样。
// 这里不丢弃非正数 PID，原因是测试和 API 需要稳定返回 unavailable 形状来表达错误输入。
func normalizePIDs(pids []int) []int {
	out := make([]int, 0, len(pids))
	seen := map[int]bool{}
	for _, pid := range pids {
		if seen[pid] {
			continue
		}
		seen[pid] = true
		out = append(out, pid)
	}
	return out
}

// available 构造可用字段，available 状态下 reason 固定为空，方便前端无分支渲染。
func available(value any, unit string) Metric {
	return Metric{Value: value, Unit: unit, Status: StatusAvailable, Reason: ""}
}

// unavailable 构造本次不可用字段，value 必须为 nil，避免调用方把缺失值误判为真实 0。
func unavailable(unit string, reason string) Metric {
	return Metric{Value: nil, Unit: unit, Status: StatusUnavailable, Reason: reason}
}

// unsupported 构造平台不支持字段，用于非 Linux 等无法低成本读取 OS 进程资源的平台。
func unsupported(unit string, reason string) Metric {
	return Metric{Value: nil, Unit: unit, Status: StatusUnsupported, Reason: reason}
}

// baseSnapshot 生成所有进程快照的安全默认值。
// 需求背景：issue 27 要求字段级降级，所以默认值先设为 unavailable，再由平台采样器逐项覆盖可用字段。
func baseSnapshot(pid int, sampleWindow time.Duration) Snapshot {
	return Snapshot{
		PID:              pid,
		SampledAt:        time.Now().UTC(),
		SampleWindowMS:   sampleWindow.Milliseconds(),
		CPUPercent:       unavailable(UnitPercent, "process metric unavailable"),
		MemoryRSSBytes:   unavailable(UnitBytes, "process metric unavailable"),
		MemoryPercent:    unavailable(UnitPercent, "process metric unavailable"),
		GoroutineCount:   unavailable(UnitCount, "goroutine count is only available for the current Go process"),
		Platform:         runtime.GOOS,
		PlatformProvider: "prismgo/process",
	}
}

// currentRuntimeSnapshot 只读取 Go runtime 自身可低成本提供的信息。
// 设计思路：heartbeat 路径可以使用该函数上报 goroutine 数，但不能在这里阻塞采样 CPU。
func currentRuntimeSnapshot() Snapshot {
	snapshot := baseSnapshot(os.Getpid(), 0)
	snapshot.GoroutineCount = available(runtime.NumGoroutine(), UnitCount)
	return snapshot
}
