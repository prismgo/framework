package horizon

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/prismgo/framework/event"
	goprocess "github.com/prismgo/framework/process"
	"github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/queue/payload"
)

// collectorItem 是从 Collect 热路径传入后台处理的事件包装。
//
// 设计思路：把 event 发生时间和 collector 接收时间分开记录，确保 window 归属不受 channel 排队延迟影响。
type collectorItem struct {
	input      CollectorInput
	receivedAt time.Time
}

// eventMetricsWindow 保存单个 window 内按 connection+queue+jobName 维度的 event_metrics 聚合增量。
//
// 需求背景：多实例 Horizon 会把相同 supervisor 名称部署到不同 host 或 environment。
// 因此内存聚合 key 必须先按 namespace/host/environment/supervisor 分片，再在读模型阶段聚合展示。
// 设计边界：该结构只保存安全聚合值和来源维度，不保存 job payload、raw envelope 或 broker 凭据。
type eventMetricsWindow struct {
	windowStart  time.Time
	sourcePrefix string
	sourceHost   string
	environment  string
	supervisor   string
	connection   string
	queue        string
	jobName      string
	processed    int64
	failed       int64
	released     int64
	poison       int64
	queued       int64
	runtimeMS    int64
	samples      int64
	eventSamples int64
	sampleRate   float64
	estimated    bool
	degraded     bool
}

// aggregateKeyState 跟踪聚合 key 的最后活跃时间，用于 TTL 清理。
type aggregateKeyState struct {
	key        string
	lastActive time.Time
}

// queuedJobCollectorState 保存 collector 内部用于 waits/long_wait 的入队任务状态。
//
// 设计边界：该结构不保存 job payload、raw envelope 或 broker credential。
type queuedJobCollectorState struct {
	connection string
	queue      string
	jobID      string
	jobName    string
	queuedAt   time.Time
	recordedAt time.Time
	unknown    bool
}

type queuedWaitHeapItem struct {
	id string
	at time.Time
}

type queuedWaitHeap []queuedWaitHeapItem

func (h queuedWaitHeap) Len() int           { return len(h) }
func (h queuedWaitHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h queuedWaitHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *queuedWaitHeap) Push(value any) {
	*h = append(*h, value.(queuedWaitHeapItem))
}

func (h *queuedWaitHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// eventRateSlot 保存单个秒级槽位的归属时间和事件数。
//
// 设计原因：rate tracker 使用固定 60 槽环形数组，槽位会被不同分钟里的同一秒下标复用。
// 如果只保存 count，长时间 idle 后旧计数仍会被 rate/total 读取，导致动态采样误判当前压力。
// 因此每个槽位必须携带 sec，读取时只统计当前滑动窗口内且归属秒匹配的计数。
type eventRateSlot struct {
	sec   int64
	count int64
}

// eventRateTracker 使用滑动窗口估算事件速率。
//
// 使用方式：recordAt 是 O(1) 热路径，只更新当前秒对应的固定槽位；rateAt/totalAt
// 是 O(60) 读取路径，用调用方给定的 now 排除窗口外旧槽位。
// 语义说明：latestSec 记录 tracker 见过的最大秒值。明显早于当前窗口的非单调输入会被忽略，
// 避免时钟回退或测试注入旧时间时复活过期压力；读取永远不返回负数。
type eventRateTracker struct {
	samples   [60]eventRateSlot // 最近 60 秒的事件计数
	latestSec int64
	seen      bool
}

// sampleRandomSource 是 collector event sampling 使用的最小随机源接口。
//
// 设计边界：采样只需要 [0,1) 的浮点数，不暴露 seed、全局 rand 或更宽接口。
// 生产路径每个 collector 持有自己的 lockedSampleRandomSource；测试可替换为固定序列，
// 从而稳定验证采样边界和动态采样后的有效 rate。
type sampleRandomSource interface {
	Float64() float64
}

// lockedSampleRandomSource 是 collector 级轻量 PRNG 的并发安全包装。
//
// 设计原因：Horizon queue event 可从 manager listener 与 worker runtime 桥接路径进入，
// 采样决策可能发生在不同 goroutine；math/rand.Rand 本身不是并发安全的。
// 使用 per-collector PRNG 避免依赖纳秒时间戳低位取模，也避免多个 collector
// 共享全局随机状态导致测试不可控或采样分布互相影响。
type lockedSampleRandomSource struct {
	mu  sync.Mutex
	rnd *rand.Rand
}

var collectorSamplerSeed int64
var defaultSampleRandomSource sampleRandomSource = newSampleRandomSource()

// newSampleRandomSource 创建 collector 独占的采样随机源。
//
// 逻辑说明：seed 使用当前时间叠加递增序号，避免同一纳秒内创建多个 collector 时拿到完全相同序列。
// 这里不使用加密随机数；event_metrics 采样只需要轻量、稳定分布的概率源，不承担安全用途。
func newSampleRandomSource() *lockedSampleRandomSource {
	seed := time.Now().UnixNano() + atomic.AddInt64(&collectorSamplerSeed, 1)
	return &lockedSampleRandomSource{rnd: rand.New(rand.NewSource(seed))}
}

// Float64 返回 [0,1) 随机值。
//
// 语义说明：nil receiver 返回 0，保持采样判断在异常替换随机源时仍可执行且不 panic。
func (s *lockedSampleRandomSource) Float64() float64 {
	if s == nil || s.rnd == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rnd.Float64()
}

// collector 实现 ObservabilityCollector 接口，提供非阻塞的 Horizon 事件采集。
//
// 设计思路：
//  1. 使用有界 channel 解耦 worker 热路径与内存聚合，确保 Collect 永不阻塞。
//  2. 后台 goroutine 从 channel 消费事件并更新内存聚合状态。
//  3. 所有内存结构均有容量/TTL 限制，触发上限时按 drop policy 处理。
//  4. 窗口按事件 OccurredAt 归属，缺失时使用 collector 接收时间。
//
// 使用方式：Manager 创建 collector 后调用 Start 启动后台处理，通过 FlushSnapshot 获取
// 当前聚合状态供 flusher 写入 Store。
type collector struct {
	mu  sync.Mutex
	cfg ObservabilityConfig

	// 有界事件缓冲，容量由 buffer_size 控制
	buffer chan collectorItem

	// event_metrics 窗口聚合：key 为 "windowStart:connection:queue:jobName"
	windows map[string]*eventMetricsWindow

	// 聚合 key 活跃度跟踪，用于 TTL 清理
	aggKeys map[string]*aggregateKeyState

	// queued 等待状态，用于 waits/long_wait 计算
	queued      map[string]queuedJobCollectorState
	queuedIndex queuedWaitHeap

	// 高价值明细（失败/poison/慢任务）
	details []HighValueJobDetail
	// detailSeq 为同一时间戳内产生的 high-value detail 提供本地唯一后缀。
	detailSeq int64

	// batchSummaries 保存 BatchEvent 派生出的低频安全摘要。
	batchSummaries []BatchSummary
	// batchIndex 将 batch ID 映射到 batchSummaries 下标，O(1) 查找替代 O(n) 扫描。
	batchIndex map[string]int

	// runtime 样本池，使用 reservoir sampling 维护
	rtSamples []int64
	rtIdx     int64

	// 按原因分类的丢弃计数
	drops map[string]int64

	// 诊断记录（最近 N 条）
	diags []ObservabilityDiagnostic

	// 内存控制状态（暴露给读模型）
	memState ObservabilityMemoryState

	// 事件速率追踪，用于入口限流、flush 诊断和动态采样压力输入。
	rateTracker eventRateTracker

	// 采样随机源；每个 collector 独立持有，测试可替换为固定序列。
	//
	// 设计原因：event_metrics 采样不能依赖时间戳低位取模。高并发同一时间窗口内的事件
	// 可能共享相近纳秒低位分布，导致采样偏差；collector 级随机源让生产路径分布稳定，
	// 也让测试能固定采样序列验证边界语义。
	sampler sampleRandomSource

	// 最近一次 flushSnapshot 被取走的时间
	lastFlushAt time.Time

	// 生命周期控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// flushSnapshot 是 collector 提供给 flusher 的一次性聚合快照。
//
// 设计思路：快照取出后 collector 内部计数器重置，避免重复写入。
// 快照包含完整的 event_metrics 窗口增量、高价值明细、诊断和内存状态。
type flushSnapshot struct {
	windows        []*eventMetricsWindow
	details        []HighValueJobDetail
	batchSummaries []BatchSummary
	diags          []ObservabilityDiagnostic
	drops          map[string]int64
	memState       ObservabilityMemoryState
	windowStart    time.Time
	windowEnd      time.Time
	lastFlushAt    time.Time
	lastFlushError string
	flushLag       time.Duration
	dropRate       float64
	degraded       bool
	degradedReason string
}

// newCollector 创建未启动的 Horizon 事件采集器。
//
// 参数说明：cfg 是已规范化的观测配置，提供 buffer 容量、采样率、内存上限等参数。
func newCollector(cfg ObservabilityConfig) *collector {
	cfg = normalizeObservabilityConfig(cfg)
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = 10000
	}
	return &collector{
		cfg:        cfg,
		buffer:     make(chan collectorItem, bufSize),
		windows:    make(map[string]*eventMetricsWindow),
		aggKeys:    make(map[string]*aggregateKeyState),
		queued:     make(map[string]queuedJobCollectorState),
		drops:      make(map[string]int64),
		batchIndex: make(map[string]int),
		sampler:    newSampleRandomSource(),
		memState: ObservabilityMemoryState{
			BufferSize: bufSize,
		},
	}
}

// Start 启动后台事件处理 goroutine。
//
// 参数说明：ctx 用于控制整个 collector 生命周期，取消时后台 goroutine 退出。
func (c *collector) Start(ctx context.Context) {
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.wg.Add(1)
	startRestartingTrackedGoroutineWithPanicHandler(c.ctx, &c.wg, "collector", nil, func(err error) {
		c.recordDrop(MemoryDropCollectorPanic)
		c.mu.Lock()
		c.diags = append(c.diags, ObservabilityDiagnostic{
			Reason:      MemoryDropCollectorPanic,
			Count:       1,
			ObservedAt:  time.Now(),
			Description: fmt.Sprintf("collector loop panic: %v", err),
			Gap:         ObservabilityGapUnknown,
		})
		c.mu.Unlock()
	}, c.processLoop)
}

// Stop 优雅关闭 collector：取消 context，等待后台 goroutine 退出，关闭 buffer channel。
func (c *collector) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// MemoryEstimate 返回 collector 当前内存占用的低成本估算值。
//
// 设计边界：Go runtime 不提供“某个对象图占用多少 RSS”的精确实时接口，因此这里按 collector
// 自有的 channel/map/slice 容量和状态条目数计算结构化估算值。该方法不遍历 job payload，不触发 GC，
// 只读取长度、容量和固定结构体大小，适合 worker heartbeat 路径低频调用。
func (c *collector) MemoryEstimate() goprocess.Metric {
	if c == nil {
		return goprocess.Metric{Value: nil, Unit: goprocess.UnitBytes, Status: goprocess.StatusUnavailable, Reason: "collector unavailable"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	size := int64(unsafe.Sizeof(*c))
	size += int64(cap(c.buffer)) * int64(unsafe.Sizeof(collectorItem{}))
	size += estimateMapBytes(len(c.windows), int64(unsafe.Sizeof("")+unsafe.Sizeof(&eventMetricsWindow{})))
	size += int64(len(c.windows)) * int64(unsafe.Sizeof(eventMetricsWindow{}))
	size += estimateMapBytes(len(c.aggKeys), int64(unsafe.Sizeof("")+unsafe.Sizeof(&aggregateKeyState{})))
	size += int64(len(c.aggKeys)) * int64(unsafe.Sizeof(aggregateKeyState{}))
	size += estimateMapBytes(len(c.queued), int64(unsafe.Sizeof("")+unsafe.Sizeof(queuedJobCollectorState{})))
	size += estimateMapBytes(len(c.drops), int64(unsafe.Sizeof("")+unsafe.Sizeof(int64(0))))
	size += int64(cap(c.details)) * int64(unsafe.Sizeof(HighValueJobDetail{}))
	size += int64(cap(c.batchSummaries)) * int64(unsafe.Sizeof(BatchSummary{}))
	size += int64(cap(c.rtSamples)) * int64(unsafe.Sizeof(int64(0)))
	return goprocess.Metric{Value: size, Unit: goprocess.UnitBytes, Status: goprocess.StatusAvailable}
}

func estimateMapBytes(entries int, entryBytes int64) int64 {
	if entries <= 0 {
		return 0
	}
	const mapEntryOverhead = int64(16)
	return int64(entries) * (entryBytes + mapEntryOverhead)
}

// Collect 尝试接收单个 queue event，永不阻塞。
//
// 逻辑说明：使用 select/default 实现非阻塞发送到有界 buffer。
// buffer 满时根据 drop_policy 丢弃最旧或当前事件，记录丢弃诊断。
//
// 参数说明：ctx 保留用于未来超时控制；input 是 queue event 的安全投影。
func (c *collector) Collect(_ context.Context, input CollectorInput) error {
	if c == nil {
		return nil
	}
	if c.recordEventAndShouldDrop() {
		return nil
	}

	select {
	case c.buffer <- collectorItem{input: input, receivedAt: time.Now()}:
		c.updateBufferUsed(1)
		return nil
	default:
		// buffer 满，按 drop policy 处理
		c.recordDrop(MemoryDropBufferFull)
		if c.cfg.DropPolicy == ObservabilityDropOldest {
			// 丢弃最旧事件，腾出空间放入当前事件
			select {
			case <-c.buffer:
				c.updateBufferUsed(-1)
				select {
				case c.buffer <- collectorItem{input: input, receivedAt: time.Now()}:
					c.updateBufferUsed(1)
				default:
				}
			default:
			}
		}
		// drop_newest: 静默丢弃当前事件
		return nil
	}
}

// recordEventAndShouldDrop 记录入口事件速率，并在超过 max_events_per_second 时执行限流丢弃。
//
// 设计原因：限流发生在写入有界 buffer 之前，避免高压下通过堆积观测事件反向影响 worker 热路径。
func (c *collector) recordEventAndShouldDrop() bool {
	c.mu.Lock()
	count := c.rateTracker.recordAt(time.Now())
	if c.cfg.MaxEventsPerSecond > 0 && count > int64(c.cfg.MaxEventsPerSecond) {
		c.drops[MemoryDropRateLimited]++
		c.memState.LastDropReason = MemoryDropRateLimited
		c.mu.Unlock()
		return true
	}
	c.mu.Unlock()
	return false
}

// FlushSnapshot 返回当前聚合状态快照并重置内部计数器，供 flusher 写入 Store。
//
// 逻辑说明：调用方在 flush 间隔期间调用此方法获取待写入数据。
// 快照取出后 windows/drops 等计数器会重置，避免重复写入。
func (c *collector) FlushSnapshot(now time.Time) *flushSnapshot {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupExpiredKeysLocked(now)

	windowStart := now.Add(-c.cfg.MetricsWindow)
	windowEnd := now

	snapshot := &flushSnapshot{
		windows:        make([]*eventMetricsWindow, 0, len(c.windows)),
		details:        make([]HighValueJobDetail, len(c.details)),
		batchSummaries: make([]BatchSummary, len(c.batchSummaries)),
		drops:          make(map[string]int64, len(c.drops)),
		diags:          make([]ObservabilityDiagnostic, len(c.diags)),
		memState:       c.memState,
		windowStart:    windowStart,
		windowEnd:      windowEnd,
		lastFlushAt:    c.lastFlushAt,
	}

	for _, w := range c.windows {
		cp := *w
		snapshot.windows = append(snapshot.windows, &cp)
	}
	copy(snapshot.details, c.details)
	copy(snapshot.batchSummaries, c.batchSummaries)
	for k, v := range c.drops {
		snapshot.drops[k] = v
	}
	copy(snapshot.diags, c.diags)

	if len(snapshot.drops) > 0 {
		snapshot.degraded = true
		snapshot.degradedReason = "drops_detected"
	}
	totalEvents := c.rateTracker.totalAt(now)
	if totalEvents > 0 {
		snapshot.dropRate = float64(totalDropCount(snapshot.drops)) / float64(totalEvents)
	}

	// 更新内存状态
	c.memState.BufferUsed = 0
	c.memState.BufferHighWatermark = 0
	c.memState.SampleReservoirSize = c.cfg.SampleReservoirSize
	c.memState.SampleReservoirUsed = len(c.rtSamples)
	if c.memState.SampleReservoirSize > 0 {
		c.memState.ReservoirUtilization = float64(len(c.rtSamples)) / float64(c.memState.SampleReservoirSize)
	}
	c.memState.MaxAggregateKeys = c.cfg.MaxAggregateKeys
	c.memState.AggregateKeyCount = len(c.aggKeys)

	if reason := dropReasonFromMap(c.drops); reason != "" {
		c.memState.LastDropReason = reason
	}
	snapshot.memState = c.memState

	// 重置计数器
	c.windows = make(map[string]*eventMetricsWindow)
	c.details = nil
	c.batchSummaries = nil
	c.batchIndex = make(map[string]int)
	c.drops = make(map[string]int64)
	c.diags = nil
	c.lastFlushAt = now

	return snapshot
}

// SnapshotPeek 返回当前聚合状态副本但不重置计数器，供 supervisor 运行时循环读取。
//
// 设计思路：supervisorWorkloads 在每 tick 读取 runtime 数据用于 autoscale 决策，
// 不能重置 collector 内部计数器（否则 flusher 的定期 flush 将拿到空数据）。
// 该方法只复制当前 window 聚合状态，不影响后续 FlushSnapshot 的结果。
func (c *collector) SnapshotPeek(now time.Time) *flushSnapshot {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupExpiredKeysLocked(now)

	windowStart := now.Add(-c.cfg.MetricsWindow)
	snapshot := &flushSnapshot{
		windows:     make([]*eventMetricsWindow, 0, len(c.windows)),
		windowStart: windowStart,
		windowEnd:   now,
	}

	for _, w := range c.windows {
		cp := *w
		snapshot.windows = append(snapshot.windows, &cp)
	}
	return snapshot
}

// MemoryState 返回当前内存控制状态，供读模型展示。
func (c *collector) MemoryState() ObservabilityMemoryState {
	if c == nil {
		return ObservabilityMemoryState{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ms := c.memState
	ms.BufferSize = c.cfg.BufferSize
	ms.BufferUsed = c.currentBufferUsed()
	ms.SampleReservoirSize = c.cfg.SampleReservoirSize
	ms.SampleReservoirUsed = len(c.rtSamples)
	ms.MaxAggregateKeys = c.cfg.MaxAggregateKeys
	ms.AggregateKeyCount = len(c.aggKeys)
	return ms
}

// SamplingPressure 返回当前 collector 可观测的动态采样压力输入。
func (c *collector) SamplingPressure() SamplingPressure {
	if c == nil {
		return SamplingPressure{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.samplingPressureAt(time.Now())
}

func (c *collector) samplingPressureAt(now time.Time) SamplingPressure {
	if c == nil {
		return SamplingPressure{}
	}
	bufferSize := c.cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = cap(c.buffer)
	}
	bufferUsed := c.currentBufferUsed()
	if c.memState.BufferUsed > bufferUsed {
		bufferUsed = c.memState.BufferUsed
	}
	reservoirUsed := len(c.rtSamples)
	aggregateCount := len(c.aggKeys)
	eventRate := c.rateTracker.rateAt(now)
	totalEvents := c.rateTracker.totalAt(now)
	dropRate := 0.0
	if totalEvents > 0 {
		dropRate = float64(totalDropCount(c.drops)) / float64(totalEvents)
	}
	return SamplingPressure{
		BufferUtilization:    ratio(bufferUsed, bufferSize),
		ReservoirUtilization: ratio(reservoirUsed, c.cfg.SampleReservoirSize),
		AggregateKeyCount:    aggregateCount,
		MaxAggregateKeys:     c.cfg.MaxAggregateKeys,
		EventRate:            eventRate,
		MaxEventsPerSecond:   c.cfg.MaxEventsPerSecond,
		DropRate:             dropRate,
	}
}

// processLoop 是后台事件处理主循环。
//
// 逻辑说明：从 buffer channel 读取事件，应用采样后更新内存聚合状态。
// context 取消时排空 buffer 后退出。
// 容错设计：Start 通过 startRecoveringGoroutineWithPanicHandler 捕获 panic 并上报。
func (c *collector) processLoop() {
	for {
		select {
		case <-c.ctx.Done():
			// 排空 buffer 中剩余事件
			c.drainBuffer()
			return
		case item := <-c.buffer:
			c.updateBufferUsed(-1)
			c.processItem(item)
		}
	}
}

// drainBuffer 排空 buffer 中剩余事件，用于 shutdown 路径。
func (c *collector) drainBuffer() {
	for {
		select {
		case item := <-c.buffer:
			c.updateBufferUsed(-1)
			c.processItem(item)
		default:
			return
		}
	}
}

// processItem 处理单个 queue event：应用采样、更新聚合、收集明细。
//
// 逻辑说明：该方法在后台 goroutine 中执行，不阻塞 worker 热路径。
func (c *collector) processItem(item collectorItem) {
	c.mu.Lock()
	defer c.mu.Unlock()

	input := item.input
	occurredAt := input.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = item.receivedAt
	}

	// 确定 event_metrics 是否采样
	eventMetricsSampled := input.Sampling.EventMetricsSampled
	if c.cfg.EventMetricsSampleRate <= 0 {
		eventMetricsSampled = false
	} else if !eventMetricsSampled && c.cfg.EventMetricsSampleRate >= 1.0 {
		eventMetricsSampled = true
	}

	// 处理 event_metrics（计数和 runtime 聚合）
	if c.cfg.EventMetrics && eventMetricsSampled && isEventMetricsEvent(input.Event) {
		c.processEventMetricsLocked(input, occurredAt)
	}

	// 处理 waits/long_wait（仅 event_metrics 开启时）
	if c.cfg.EventMetrics && c.cfg.Waits && c.cfg.QueuedWaitsMax > 0 {
		c.processWaitsLocked(input, occurredAt)
	}

	// 处理高价值明细
	if c.cfg.Enabled(ObservabilityHighValueDetail) {
		c.processHighValueDetailLocked(input, occurredAt)
	}

	if c.cfg.Enabled(ObservabilityBatchSummaries) {
		c.processBatchSummaryLocked(input, occurredAt)
	}
}

// processEventMetricsLocked 更新 event_metrics 窗口聚合。
//
// 逻辑说明：窗口归属只由事件发生时间和 metrics_window 决定；host/environment/supervisor
// 参与内存聚合 key，防止两个实例在 flush 前就被合并成一个不可下钻的全局 bucket。
// overflow 只折叠 jobName 这类高基数扩展维度，保留来源维度用于定位压力来源。
func (c *collector) processEventMetricsLocked(input CollectorInput, occurredAt time.Time) {
	if strings.TrimSpace(input.SourceSupervisor) == "" {
		c.recordSourceSupervisorUnknownLocked(input, occurredAt)
	}
	windowStart := occurredAt.Truncate(c.cfg.MetricsWindow)
	baseAggKey := aggKey(input.SourcePrefix, input.SourceHost, input.SourceEnvironment, input.SourceSupervisor, input.Connection, input.Queue, input.JobName)

	w := c.resolveEventMetricsWindowLocked(input, occurredAt, windowStart, baseAggKey)

	w.eventSamples++
	if rate := effectiveEventMetricsSampleRate(input.Sampling.EventMetricsSampleRate, c.cfg.EventMetricsSampleRate); rate > 0 {
		w.sampleRate = rate
	}
	if input.Sampling.Estimated || w.sampleRate < 1.0 {
		w.estimated = true
	}

	switch input.Event {
	case "queue.job_queued":
		w.queued++
	case "queue.job_processed":
		w.processed++
		runtimeMS := durationMS(input.Runtime)
		if runtimeMS > 0 {
			w.runtimeMS += runtimeMS
			w.samples++
			c.recordRuntimeSampleLocked(runtimeMS)
		}
	case "queue.job_failed":
		w.failed++
		runtimeMS := durationMS(input.Runtime)
		if runtimeMS > 0 {
			w.runtimeMS += runtimeMS
			w.samples++
			c.recordRuntimeSampleLocked(runtimeMS)
		}
	case "queue.job_released":
		w.released++
	case "queue.poison_envelope":
		w.poison++
	}
}

// resolveEventMetricsWindowLocked 查找或创建当前窗口对应的 eventMetricsWindow。
//
// 逻辑说明：先按完整 key 查找；若不存在，执行聚合 key 基数控制（超限时折叠到 _overflow），
// 然后创建新窗口并更新聚合 key 活跃时间。
func (c *collector) resolveEventMetricsWindowLocked(input CollectorInput, occurredAt time.Time, windowStart time.Time, baseAggKey string) *eventMetricsWindow {
	key := eventMetricsWindowKey(windowStart, input.SourcePrefix, input.SourceHost, input.SourceEnvironment, input.SourceSupervisor, input.Connection, input.Queue, input.JobName)

	w, ok := c.windows[key]
	if ok {
		return w
	}
	// 聚合 key 基数控制
	c.enforceAggregateKeyLimitLocked(input, occurredAt)
	// 上限触发后新 key 写入 _overflow
	effectiveJobName := input.JobName
	if len(c.aggKeys) >= c.cfg.MaxAggregateKeys && c.cfg.MaxAggregateKeys > 0 {
		if _, exists := c.aggKeys[baseAggKey]; !exists {
			effectiveJobName = "_overflow"
			c.recordDropLocked(MemoryDropAggregateOverflow)
			key = eventMetricsWindowKey(windowStart, input.SourcePrefix, input.SourceHost, input.SourceEnvironment, input.SourceSupervisor, input.Connection, input.Queue, "_overflow")
			w, ok = c.windows[key]
		}
	}
	if !ok {
		w = &eventMetricsWindow{
			windowStart:  windowStart,
			sourcePrefix: input.SourcePrefix,
			sourceHost:   input.SourceHost,
			environment:  input.SourceEnvironment,
			supervisor:   input.SourceSupervisor,
			connection:   input.Connection,
			queue:        input.Queue,
			jobName:      effectiveJobName,
			sampleRate:   effectiveEventMetricsSampleRate(input.Sampling.EventMetricsSampleRate, c.cfg.EventMetricsSampleRate),
			estimated:    input.Sampling.Estimated || c.cfg.EventMetricsSampleRate < 1.0,
		}
		c.windows[key] = w
	}
	// 更新聚合 key 活跃时间
	c.aggKeys[baseAggKey] = &aggregateKeyState{
		key:        baseAggKey,
		lastActive: occurredAt,
	}
	return w
}

// processWaitsLocked 更新 queued 等待状态和 long_wait 判断。
func (c *collector) processWaitsLocked(input CollectorInput, occurredAt time.Time) {
	switch input.Event {
	case "queue.job_queued":
		if input.JobID == "" {
			return
		}
		unknown := input.OccurredAt.IsZero()
		c.queued[input.JobID] = queuedJobCollectorState{
			connection: input.Connection,
			queue:      input.Queue,
			jobID:      input.JobID,
			jobName:    input.JobName,
			queuedAt:   input.OccurredAt,
			recordedAt: occurredAt,
			unknown:    unknown,
		}
		heap.Push(&c.queuedIndex, queuedWaitHeapItem{id: input.JobID, at: occurredAt})
		c.enforceQueuedLimitLocked()
	case "queue.job_processing", "queue.job_failed":
		delete(c.queued, input.JobID)
	}
}

// processHighValueDetailLocked 收集高价值诊断明细。
func (c *collector) processHighValueDetailLocked(input CollectorInput, occurredAt time.Time) {
	if !input.Sampling.HighValueDetailSampled {
		return
	}
	var kind string
	switch input.Event {
	case "queue.job_failed":
		if c.cfg.FailedDetailEnabled {
			kind = HighValueDetailFailed
		}
	case "queue.poison_envelope":
		if c.cfg.PoisonDetailEnabled {
			kind = HighValueDetailPoison
		}
	case "queue.job_processed":
		if c.cfg.SlowJobDetailEnabled && c.cfg.SlowJobThreshold > 0 && input.Runtime >= c.cfg.SlowJobThreshold {
			kind = HighValueDetailSlowJob
		}
	}
	if kind == "" {
		return
	}

	c.detailSeq++
	detail := HighValueJobDetail{
		ID:                  detailID(kind, occurredAt, c.detailSeq),
		Kind:                kind,
		Connection:          input.Connection,
		Queue:               input.Queue,
		JobID:               input.JobID,
		JobName:             input.JobName,
		RuntimeMS:           durationMS(input.Runtime),
		ErrorSummary:        input.ErrorSummary,
		PoisonDriver:        input.PoisonDriver,
		PoisonAction:        input.PoisonAction,
		PoisonBodySize:      input.PoisonBodySize,
		PoisonBodyTruncated: input.PoisonBodyTruncated,
		OccurredAt:          occurredAt,
	}

	limit := c.cfg.MaxAggregateKeys
	if limit <= 0 {
		limit = 1000
	}
	if len(c.details) >= limit {
		c.recordDropLocked("high_value_detail_limit")
		return
	}
	c.details = append(c.details, detail)
}

// processBatchSummaryLocked 收集 BatchEvent 派生的低频 summary。
//
// 设计边界：batch summary 只保存批次聚合进度和窗口质量，不保存批次内 job payload 或 per-job 明细。
func (c *collector) processBatchSummaryLocked(input CollectorInput, occurredAt time.Time) {
	if strings.TrimSpace(input.BatchSummary.ID) == "" {
		return
	}
	summary := input.BatchSummary
	if summary.UpdatedAt.IsZero() {
		summary.UpdatedAt = occurredAt
	}
	if summary.WindowStart.IsZero() {
		summary.WindowStart = occurredAt.Truncate(c.cfg.MetricsWindow)
	}
	if summary.WindowEnd.IsZero() {
		summary.WindowEnd = summary.WindowStart.Add(c.cfg.MetricsWindow)
	}
	if summary.Quality == "" {
		summary.Quality = EventMetricQualityExact
	}
	// 同一个 batch 在一个 flush 窗口内可能多次更新；只保留最后状态，避免低频摘要放大内存。
	// 使用 batchIndex 进行 O(1) 查找，避免 O(n) 线性扫描。
	if idx, exists := c.batchIndex[summary.ID]; exists {
		c.batchSummaries[idx] = summary
		return
	}
	limit := c.cfg.BatchSummarySize
	if limit <= 0 {
		limit = c.cfg.BatchSize
	}
	if limit > 0 && len(c.batchSummaries) >= limit {
		c.recordDropLocked(MemoryDropBatchSummaryLimit)
		return
	}
	c.batchIndex[summary.ID] = len(c.batchSummaries)
	c.batchSummaries = append(c.batchSummaries, summary)
}

// enforceAggregateKeyLimitLocked 检查聚合 key 是否超限，超限时记录溢出诊断。
//
// 逻辑说明：超限后已存在的 key 继续更新；新 key 将被折叠到 _overflow 桶。
func (c *collector) enforceAggregateKeyLimitLocked(input CollectorInput, occurredAt time.Time) {
	limit := c.cfg.MaxAggregateKeys
	if limit <= 0 {
		return
	}
	if len(c.aggKeys) < limit {
		return
	}
	key := aggKey(input.SourcePrefix, input.SourceHost, input.SourceEnvironment, input.SourceSupervisor, input.Connection, input.Queue, input.JobName)
	if _, exists := c.aggKeys[key]; exists {
		return
	}
	c.recordOverflowDiagnosticLocked(input, occurredAt)
}

// enforceQueuedLimitLocked 限制 queued 等待状态数量。
func (c *collector) enforceQueuedLimitLocked() {
	limit := c.cfg.QueuedWaitsMax
	if limit <= 0 {
		c.queued = make(map[string]queuedJobCollectorState)
		c.queuedIndex = nil
		return
	}
	if len(c.queued) <= limit {
		return
	}
	// queuedIndex 可能保留已经处理完成或同一 job 重新入队前的旧条目。
	// 只有 heap 条目的 recordedAt 与 map 中当前状态完全一致时，才允许淘汰该 job；
	// 这样可以保留 O(log n) 淘汰路径，同时避免过期 heap 条目误删新状态。
	for len(c.queued) > limit && c.queuedIndex.Len() > 0 {
		item := heap.Pop(&c.queuedIndex).(queuedWaitHeapItem)
		state, ok := c.queued[item.id]
		if !ok || !state.recordedAt.Equal(item.at) {
			continue
		}
		delete(c.queued, item.id)
	}
	c.recordDropLocked("queued_waits_limit")
}

// recordRuntimeSampleLocked 使用 reservoir sampling 维护 runtime 样本池。
func (c *collector) recordRuntimeSampleLocked(runtimeMS int64) {
	limit := c.cfg.SampleReservoirSize
	if limit <= 0 {
		return
	}
	c.rtIdx++
	if len(c.rtSamples) < limit {
		c.rtSamples = append(c.rtSamples, runtimeMS)
		return
	}
	// 逻辑说明：第 rtIdx 个样本以 limit/rtIdx 的概率进入 reservoir；
	// 一旦命中，再在现有槽位中均匀选择一个位置替换。
	if !shouldSampleWithSource(float64(limit)/float64(c.rtIdx), c.sampler) {
		return
	}
	idx := int(float64(limit) * sampleFloat64(c.sampler))
	if idx >= limit {
		idx = limit - 1
	}
	c.rtSamples[idx] = runtimeMS
}

// cleanupExpiredKeysLocked 清理超过 TTL 的低活跃聚合 key。
func (c *collector) cleanupExpiredKeysLocked(now time.Time) {
	ttl := c.cfg.AggregateKeyTTL
	if ttl <= 0 {
		return
	}
	for key, state := range c.aggKeys {
		if now.Sub(state.lastActive) > ttl {
			delete(c.aggKeys, key)
		}
	}
}

// recordDrop 记录一次丢弃事件（非锁版本，供 Collect 热路径使用）。
func (c *collector) recordDrop(reason string) {
	c.mu.Lock()
	c.drops[reason]++
	c.memState.LastDropReason = reason
	c.mu.Unlock()
}

// recordDropLocked 记录一次丢弃事件（调用方已持有锁）。
func (c *collector) recordDropLocked(reason string) {
	c.drops[reason]++
	c.memState.LastDropReason = reason
}

// recordOverflowDiagnosticLocked 记录聚合 key 溢出诊断。
func (c *collector) recordOverflowDiagnosticLocked(input CollectorInput, occurredAt time.Time) {
	desc := "aggregate key overflow: connection=" + input.Connection + " queue=" + input.Queue + " job=" + input.JobName
	c.diags = append(c.diags, ObservabilityDiagnostic{
		Reason:      MemoryDropAggregateOverflow,
		Count:       1,
		ObservedAt:  occurredAt,
		Description: desc,
	})
}

// recordSourceSupervisorUnknownLocked 记录缺少 Horizon supervisor runtime 来源的 event_metrics 诊断。
//
// 用途：当普通 queue event 不是从 horizon:work runtime 边界进入，或 runtime 没有携带 --supervisor
// 身份时，保留该来源分片仍然存在但 supervisor 为空的事实。
// 使用方式：processEventMetricsLocked 在持有 collector.mu 时调用；调用方已经确认该事件会进入
// event_metrics window，因此诊断与窗口在同一次 FlushSnapshot 中落盘。
// 设计原因：不能从 queue name、host、environment、connection 或配置列表反推 supervisor，否则多实例
// 下钻会把真实来源污染为猜测值；但也不能丢弃事件或把该分片展示为 0 流量。
// 设计思路：使用稳定 reason 和 quantifiable gap，把“来源维度未知”表达为诊断，而不是改变计数质量。
// 需求背景：issue 43 要求缺少 supervisor runtime 上下文时事件仍采集，SourceSupervisor 为空，并产生
// event_metrics_source_supervisor_unknown 或等价 reason。
func (c *collector) recordSourceSupervisorUnknownLocked(input CollectorInput, occurredAt time.Time) {
	c.diags = append(c.diags, ObservabilityDiagnostic{
		Reason:      EventMetricsSourceSupervisorUnknown,
		Count:       1,
		ObservedAt:  occurredAt,
		Description: "event metrics source supervisor is unknown: connection=" + input.Connection + " queue=" + input.Queue,
		Gap:         ObservabilityGapQuantifiable,
	})
}

// updateBufferUsed 更新 buffer 使用计数（非锁版本，供热路径使用）。
func (c *collector) updateBufferUsed(delta int) {
	// 使用原子操作或快速锁更新
	c.mu.Lock()
	c.memState.BufferUsed += delta
	if c.memState.BufferUsed < 0 {
		c.memState.BufferUsed = 0
	}
	util := c.memState.BufferUtilization()
	if util > c.memState.BufferHighWatermark {
		c.memState.BufferHighWatermark = util
	}
	c.mu.Unlock()
}

func (c *collector) currentBufferUsed() int {
	return len(c.buffer)
}

// ComputeWaits 基于当前 queued 状态计算等待时间快照。
//
// 参数说明：thresholds 是 waits 配置；now 是计算基准时间。
// 返回值是 connection:queue 维度的 QueueWaitSnapshot 列表。
func (c *collector) ComputeWaits(thresholds map[string]int, now time.Time) []QueueWaitSnapshot {
	if c == nil || len(thresholds) == 0 {
		return nil
	}
	if !c.cfg.Enabled(ObservabilityWaits) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	byKey := make(map[string]*QueueWaitSnapshot, len(thresholds))
	for key, seconds := range thresholds {
		connection, queueName := splitQueueWaitKey(key)
		byKey[key] = &QueueWaitSnapshot{
			Key:        key,
			Connection: connection,
			Queue:      queueName,
			Status:     QueueWaitUnknown,
			Threshold:  int64(seconds) * 1000,
			SampledAt:  now,
		}
	}

	for _, queued := range c.queued {
		key := queued.connection + ":" + queued.queue
		current, ok := byKey[key]
		if !ok {
			continue
		}
		if queued.unknown || queued.queuedAt.IsZero() {
			current.Status = QueueWaitUnknown
			continue
		}
		waitMS := durationMS(now.Sub(queued.queuedAt))
		if current.Status != QueueWaitKnown || waitMS > current.WaitMS {
			current.Status = QueueWaitKnown
			current.WaitMS = waitMS
			current.LongWait = current.Threshold > 0 && waitMS >= current.Threshold
		}
	}

	out := make([]QueueWaitSnapshot, 0, len(byKey))
	for _, item := range byKey {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// RuntimeSamples 返回当前 runtime 样本池副本，用于 flusher 计算 P50/P95/P99。
func (c *collector) RuntimeSamples() []int64 {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int64, len(c.rtSamples))
	copy(out, c.rtSamples)
	return out
}

// --- eventRateTracker 方法 ---

// recordAt 记录指定秒内的事件数量，并返回该秒当前计数。
//
// 调用方负责加锁；collector 需要同时用这个计数做入口限流判断。
// 语义说明：槽位下标由 Unix 秒对 60 取模得到，但是否有效由槽位保存的 sec 决定。
// 当 now 明显早于 latestSec 当前窗口时，说明输入发生时钟回退或非单调旧时间，直接忽略并返回 0；
// 这样不会 panic、不会产生负数，也不会把历史高流量重新写回当前压力窗口。
func (t *eventRateTracker) recordAt(now time.Time) int64 {
	sec := now.Truncate(time.Second).Unix()
	if t.seen && sec < t.latestSec-59 {
		return 0
	}
	if !t.seen || sec > t.latestSec {
		t.latestSec = sec
		t.seen = true
	}

	idx := eventRateSlotIndex(sec)
	if t.samples[idx].sec != sec {
		t.samples[idx] = eventRateSlot{sec: sec}
	}
	t.samples[idx].count++
	return t.samples[idx].count
}

// rateAt 返回以 now 为窗口终点的 60 秒平均事件率（EPS，浮点精度）。
//
// 设计原因：返回 float64 而非 int64，避免低流量（<60 EPS）时整数除法截断为 0，
// 导致动态采样策略无法感知压力。例如 55 事件/60 秒 = 0.917 EPS。
func (t *eventRateTracker) rateAt(now time.Time) float64 {
	return float64(t.totalAt(now)) / 60.0
}

// totalAt 返回以 now 为窗口终点的 60 秒事件总数。
//
// 语义说明：只统计 [now-59s, now] 内的槽位；长时间 idle 后旧秒槽会自然过期。
// 如果读取时间短暂回退到 latestSec 之前，以 latestSec 作为窗口终点，避免窗口边界倒退造成
// 旧槽位被错误复活或 total 出现不稳定跳变。
func (t *eventRateTracker) totalAt(now time.Time) int64 {
	if !t.seen {
		return 0
	}
	windowEnd := now.Truncate(time.Second).Unix()
	if windowEnd < t.latestSec {
		windowEnd = t.latestSec
	}
	cutoff := windowEnd - 59
	var sum int64
	for _, slot := range t.samples {
		if slot.count <= 0 {
			continue
		}
		if slot.sec < cutoff || slot.sec > windowEnd {
			continue
		}
		sum += slot.count
	}
	return sum
}

func eventRateSlotIndex(sec int64) int {
	idx := sec % 60
	if idx < 0 {
		idx += 60
	}
	return int(idx)
}

// --- 辅助函数 ---

// eventMetricsWindowKey 返回 collector 内存窗口聚合 key。
//
// 设计原因：同名 supervisor 在不同 host/environment 下不是重复 runtime；这里把来源维度纳入 key，
// 让 flush batch 写出多个 EventMetricWindow，而不是在内存阶段提前相加。
func eventMetricsWindowKey(windowStart time.Time, prefix, host, environment, supervisor, connection, queue, jobName string) string {
	var b strings.Builder
	b.Grow(64 + len(prefix) + len(host) + len(environment) + len(supervisor) + len(connection) + len(queue) + len(jobName))
	b.WriteString(windowStart.Format(time.RFC3339))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(prefix))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(host))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(environment))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(supervisor))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(connection))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(queue))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(jobName))
	return b.String()
}

// aggKey 返回聚合基数控制使用的稳定 key。
//
// 逻辑说明：来源维度不属于高基数扩展维度，不能被 _overflow 折叠；jobName 才是当前
// aggregate overflow 的主要折叠目标。
func aggKey(prefix, host, environment, supervisor, connection, queue, jobName string) string {
	var b strings.Builder
	b.Grow(len(prefix) + len(host) + len(environment) + len(supervisor) + len(connection) + len(queue) + len(jobName) + 6)
	b.WriteString(strings.TrimSpace(prefix))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(host))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(environment))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(supervisor))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(connection))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(queue))
	b.WriteByte(':')
	b.WriteString(strings.TrimSpace(jobName))
	return b.String()
}

func detailID(kind string, at time.Time, seq int64) string {
	return kind + "-" + at.Format(time.RFC3339Nano) + "-" + strconv.FormatInt(seq, 10)
}

func totalDropCount(drops map[string]int64) int64 {
	var total int64
	for _, v := range drops {
		total += v
	}
	return total
}

func ratio(used int, size int) float64 {
	if used <= 0 || size <= 0 {
		return 0
	}
	return float64(used) / float64(size)
}

func effectiveEventMetricsSampleRate(inputRate float64, configuredRate float64) float64 {
	rate := inputRate
	if rate <= 0 {
		rate = configuredRate
	}
	if rate < 0 {
		return 0
	}
	if rate > 1 {
		return 1
	}
	return rate
}

func dropReasonFromMap(drops map[string]int64) string {
	for reason, count := range drops {
		if count > 0 {
			return reason
		}
	}
	return ""
}

func isEventMetricsEvent(name string) bool {
	switch name {
	case queue.EventJobQueued,
		queue.EventJobProcessed,
		queue.EventJobReleased,
		queue.EventJobFailed,
		queue.EventPoisonEnvelope:
		return true
	default:
		return false
	}
}

// collectorInputFromEvent 将 queue event 转换为 collector 可处理的 CollectorInput。
//
// 设计边界：该函数只提取展示安全元数据和采样决策，不携带 queue payload、raw envelope 或 broker 凭据。
// 采样决策由 event_metrics_sample_rate 和 high_value_detail_sample_rate 配置决定。
func collectorInputFromEvent(ev event.Event, obs ObservabilityConfig) CollectorInput {
	return collectorInputFromEventWithSampler(ev, obs, SamplingPressure{}, nil)
}

// inputFromEventWithPressure 使用当前 collector 的随机源构造 CollectorInput。
//
// 使用方式：生产事件入口统一走该方法，让动态采样压力和随机源都来自当前 collector。
// 设计原因：采样随机源属于 collector 内部观测状态；每个 collector 持有独立 PRNG 后，
// 多个 Horizon manager/worker 在同一进程内不会共享采样序列，测试也可以通过替换 c.sampler
// 固定 event_metrics 与 high-value detail 的采样命中结果。
func (c *collector) inputFromEventWithPressure(ev event.Event, pressure SamplingPressure) CollectorInput {
	if c == nil {
		return collectorInputFromEventWithSampler(ev, ObservabilityConfig{}, pressure, nil)
	}
	return collectorInputFromEventWithSampler(ev, c.cfg, pressure, c.sampler)
}

// collectorInputFromEventWithSampler 将 queue event 投影为 collector 输入，并在入口完成采样决策。
//
// 逻辑说明：动态采样先根据配置和压力计算本次有效采样率；随后用传入随机源分别判断
// event_metrics 与 high-value detail 是否命中。rate=0 和 rate=1 在 shouldSampleWithSource
// 内短路，不消耗随机序列，保证边界值语义稳定且测试可预测。
// 设计边界：这里暂不做基于 job id、poison envelope 字段或其他稳定标识的哈希采样，
// 避免缺失字段和拼接规范为 Horizon 事件语义引入额外约束。
func collectorInputFromEventWithSampler(ev event.Event, obs ObservabilityConfig, pressure SamplingPressure, sampler sampleRandomSource) CollectorInput {
	if ev == nil {
		return CollectorInput{}
	}
	obs = normalizeObservabilityConfig(obs)
	input := buildSamplingInput(ev, obs, pressure, sampler)
	return populateInputFromEvent(input, ev)
}

// buildSamplingInput 构建采样决策和基础输入字段。
//
// 逻辑说明：根据配置和压力计算有效采样率，使用随机源判断是否命中采样。
func buildSamplingInput(ev event.Event, obs ObservabilityConfig, pressure SamplingPressure, sampler sampleRandomSource) CollectorInput {
	policy := EvaluateSamplingPolicy(obs, pressure)
	input := CollectorInput{
		Sampling: SamplingDecision{
			EventMetricsSampleRate: policy.EventMetricsRate,
		},
	}
	// event_metrics 采样判断
	input.Sampling.EventMetricsSampled = shouldSampleWithSource(policy.EventMetricsRate, sampler)
	input.Sampling.Estimated = policy.EventMetricsRate < 1.0 || policy.State != SamplingStateNormal

	// high_value_detail 采样判断
	input.Sampling.HighValueDetailRate = policy.HighValueDetailRate
	input.Sampling.HighValueDetailSampled = shouldSampleWithSource(policy.HighValueDetailRate, sampler)

	return input
}

// populateInputFromEvent 根据事件类型填充具体字段。
//
// 逻辑说明：将 queue event 的具体字段映射到 CollectorInput，不同事件类型有不同的字段集合。
func populateInputFromEvent(input CollectorInput, ev event.Event) CollectorInput {
	switch typed := ev.(type) {
	case queue.JobQueued:
		input.Event = queue.EventJobQueued
		input.Connection = typed.Connection
		input.Queue = typed.Queue
		input.JobID = typed.JobID
		input.JobName = typed.JobName
		input.Tags = normalizeStrings(typed.Tags)
		input.OccurredAt = typed.QueuedAt
	case queue.JobProcessing:
		input.Event = queue.EventJobProcessing
		input.Connection = typed.Connection
		input.Queue = typed.Queue
		input.JobID = typed.JobID
		input.JobName = typed.JobName
		input.Attempts = typed.Attempts
	case queue.JobProcessed:
		input.Event = queue.EventJobProcessed
		input.Connection = typed.Connection
		input.Queue = typed.Queue
		input.JobID = typed.JobID
		input.JobName = typed.JobName
		input.Runtime = typed.Duration
		input.Tags = normalizeStrings(typed.Tags)
	case queue.JobReleased:
		input.Event = queue.EventJobReleased
		input.Connection = typed.Connection
		input.Queue = typed.Queue
		input.JobID = typed.JobID
		input.JobName = typed.JobName
		input.ErrorSummary = truncateSummary(typed.Err)
		input.Tags = normalizeStrings(typed.Tags)
	case queue.JobFailed:
		input.Event = queue.EventJobFailed
		input.Connection = typed.Connection
		input.Queue = typed.Queue
		input.JobID = typed.JobID
		input.JobName = typed.JobName
		input.ErrorSummary = truncateSummary(typed.Error)
		input.Tags = normalizeStrings(typed.Tags)
		input.OccurredAt = typed.FailedAt
	case queue.PoisonEnvelope:
		input.Event = queue.EventPoisonEnvelope
		input.Connection = typed.Connection
		input.Queue = typed.Queue
		input.ErrorSummary = truncateSummary(typed.Error)
		input.PoisonDriver = typed.Driver
		input.PoisonAction = typed.Action
		input.PoisonBodySize = typed.BodySize
		input.PoisonBodyTruncated = typed.BodyTruncated
		input.OccurredAt = typed.Timestamp
	case queue.BatchEvent:
		input.Event = typed.EventName
		input.BatchSummary, input.OccurredAt = batchSummaryFromEvent(typed)
	case queue.InfrastructureEvent:
		input.Event = typed.EventName
		input.Connection = typed.Connection
		input.Queue = typed.Queue
	}
	return input
}

func batchSummaryFromEvent(ev queue.BatchEvent) (BatchSummary, time.Time) {
	status := ev.Batch
	occurredAt := batchEventOccurredAt(ev.EventName, status)
	summary := BatchSummary{
		ID:          status.ID,
		Name:        status.Name,
		Status:      batchStatusFromEvent(ev.EventName, status),
		Total:       status.Total,
		Pending:     status.Pending,
		Processed:   status.Processed,
		Failed:      status.Failed,
		Cancelled:   status.Cancelled,
		CreatedAt:   status.CreatedAt,
		FinishedAt:  status.FinishedAt,
		CancelledAt: status.CancelledAt,
		UpdatedAt:   occurredAt,
		Quality:     EventMetricQualityExact,
	}
	return summary, occurredAt
}

func batchStatusFromEvent(name string, status payload.BatchStatus) string {
	switch {
	case status.Cancelled || name == queue.EventBatchCancelled:
		return BatchStatusCancelled
	case !status.FinishedAt.IsZero() || name == queue.EventBatchFinished || (status.Total > 0 && status.Pending == 0):
		return BatchStatusFinished
	default:
		return BatchStatusRunning
	}
}

func batchEventOccurredAt(name string, status payload.BatchStatus) time.Time {
	switch {
	case name == queue.EventBatchCancelled && !status.CancelledAt.IsZero():
		return status.CancelledAt
	case name == queue.EventBatchFinished && !status.FinishedAt.IsZero():
		return status.FinishedAt
	case name == queue.EventBatchCreated && !status.CreatedAt.IsZero():
		return status.CreatedAt
	default:
		return time.Time{}
	}
}

func shouldSample(rate float64) bool {
	return shouldSampleWithSource(rate, nil)
}

// shouldSampleWithSource 基于采样率返回是否采样命中。
//
// 语义说明：采样率 1.0 总是命中，0 总是不命中；中间值比较随机源的 [0,1)
// 输出和 rate。命中条件使用 value < rate，因此随机值刚好等于 rate 时不命中，
// 与概率区间 [0, rate) 保持一致。
// 设计原因：旧实现使用纳秒时间戳低位取模，事件密集到达时会把采样结果绑定到低位时间分布，
// 既不可测试，也可能在同一时间窗口内产生偏差。传入 collector 级随机源后，
// 生产路径保持轻量 PRNG，测试路径可以注入固定序列验证采样边界。
func shouldSampleWithSource(rate float64, sampler sampleRandomSource) bool {
	if rate >= 1.0 {
		return true
	}
	if rate <= 0 {
		return false
	}
	return sampleFloat64(sampler) < rate
}

// sampleFloat64 统一处理 collector 采样随机源的默认回退。
//
// 设计原因：reservoir sampling 与 event_metrics 入口采样都依赖同一随机边界。
// 把 nil 回退集中到这里，避免不同调用点各自处理后出现边界不一致。
func sampleFloat64(sampler sampleRandomSource) float64 {
	if sampler == nil {
		sampler = defaultSampleRandomSource
	}
	return sampler.Float64()
}
