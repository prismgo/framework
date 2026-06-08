package horizon

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	goprocess "github.com/prismgo/framework/process"
)

const (
	// MasterRunning 表示 horizon master 进程正在上报 heartbeat。
	MasterRunning = "running"
	// MasterStale 表示 master heartbeat 已超过 TTL，仅在读取时派生，不写回 Store。
	MasterStale = "stale"

	// SupervisorRunning 表示 supervisor 正常上报 heartbeat 且未被控制标记覆盖。
	SupervisorRunning = "running"
	// SupervisorPaused 表示全局或指定 supervisor pause 标记生效后的派生状态。
	SupervisorPaused = "paused"
	// SupervisorTerminating 表示 terminate 控制标记生效后的派生状态。
	SupervisorTerminating = "terminating"
	// SupervisorStale 表示 heartbeat 已超过 TTL，仅读取时派生，不写入 Store。
	SupervisorStale = "stale"

	// WorkerIdle 表示 worker 进程存活且可消费/正在循环。
	WorkerIdle = "idle"
	// WorkerPaused 表示控制标记要求 worker 暂停消费。
	WorkerPaused = "paused"
	// WorkerTerminating 表示控制标记要求 worker 优雅退出。
	WorkerTerminating = "terminating"
	// WorkerStale 表示 worker heartbeat 已超过 TTL，仅读取时派生。
	WorkerStale = "stale"

	// GlobalRunning 表示至少存在一个非 stale supervisor。
	GlobalRunning = "running"
	// GlobalPaused 表示全局 pause 标记优先于 running 状态。
	GlobalPaused = "paused"
	// GlobalTerminating 表示 terminate 请求优先于 pause 和 running 状态。
	GlobalTerminating = "terminating"
	// GlobalInactive 表示没有可用的非 stale supervisor。
	GlobalInactive = "inactive"

	// QueueWaitKnown 表示 wait time 来自显式 queued_at 元数据，可用于长等待告警。
	QueueWaitKnown = "known"
	// QueueWaitUnknown 表示 driver 或历史数据未提供 queued_at，不能把等待时间默认为 0。
	QueueWaitUnknown = "unknown"
	// QueueWaitUnsupported 表示当前 driver 明确不支持等待时间采样。
	QueueWaitUnsupported = "unsupported"

	// MetricsHistoryQueue 表示按 connection:queue 维度保存 metrics history。
	MetricsHistoryQueue = "queue"

	// EventLongWait 表示 Horizon 在 snapshot 维护流程中检测到队列长等待。
	EventLongWait = "horizon.long_wait"

	// BatchStatusRunning 表示批次仍有未完成任务，Dashboard 只能展示只读进度。
	BatchStatusRunning = "running"
	// BatchStatusFinished 表示批次所有任务已完成，包含失败计数但不暴露任务 payload。
	BatchStatusFinished = "finished"
	// BatchStatusCancelled 表示批次已取消，后续未执行任务由 queue worker 跳过。
	BatchStatusCancelled = "cancelled"
)

// LongWaitEvent 是 Horizon 基于 waits 配置发出的安全长等待事件。
//
// 使用方式：业务应用可以通过 prismgo/event 监听 EventLongWait，把事件转换为站内通知、
// 日志或外部告警。事件只包含 connection、queue、阈值、观测值和采样时间，不包含 job payload。
type LongWaitEvent struct {
	Connection string    `json:"connection"`
	Queue      string    `json:"queue"`
	Threshold  int64     `json:"threshold_ms"`
	WaitMS     int64     `json:"wait_ms"`
	SampledAt  time.Time `json:"sampled_at"`
}

// Name 返回 Horizon 长等待事件名称。
func (e LongWaitEvent) Name() string { return EventLongWait }

// StoreOptions 保存 Horizon Store 的最小运行时配置。
//
// 需求背景：issue 02 要求 memory store 与 redis store 使用一致的 heartbeat TTL 与 key prefix 语义，
// issue 05 在此基础上把 Redis 持久化 record 纳入 Payload Encoding，因此运行时 Store 构造需要
// 接收已经从 Horizon Config 解析出的编码名称，避免 Store 实现自行读取全局配置。
//
// 设计思路：StoreOptions 只承载 Store 创建所需的稳定运行参数；MemoryStore 继续保存 Go struct，
// RedisStore 使用 Encoding 编解码实体 record，但 control hash、索引 set/zset 和 orphan pid 分数仍
// 保持 Redis 原生命令语义，避免把控制标记误接入 payload 编码。
type StoreOptions struct {
	// Prefix 是 Horizon 独占的 Store key 前缀，不复用 queue/cache/session prefix。
	//
	// 使用方式：RedisStore 用它生成所有 Horizon key；MemoryStore 用它做进程内复用边界。
	Prefix string
	// HeartbeatTTL 是 supervisor/worker heartbeat 被视为存活的时间窗口。
	//
	// 使用方式：写入 Redis heartbeat entity 时作为 key TTL；读取时也用于派生 stale 状态。
	HeartbeatTTL time.Duration
	// Encoding 是 Horizon Redis Store records 使用的 Payload Encoding；空值最终使用 msgpack。
	//
	// 需求背景：Horizon Dashboard/API/CLI 仍输出 JSON，但 Redis Store 内部记录需要跟随
	// horizon.encoding。显式 json 用于回滚和人工排障；默认 msgpack 不读取旧 JSON record。
	Encoding string
}

// SupervisorState 是 supervisor heartbeat 写入和状态命令读取的基础状态模型。
//
// 设计思路：该模型只承载控制与基础状态命令需要的字段，不放 recent jobs、吞吐、队列长度或失败任务详情，
// 防止 issue 02 提前耦合后续 metrics 与 worker loop 切片。
// MasterState 表示 horizon 顶层 master 进程的 heartbeat 状态。
//
// 需求背景：issue 05 将 Horizon 改为 master -> supervisor -> worker 的真实进程树，master 不再能被
// SupervisorState 混合表达。该模型只保存进程身份、心跳和派生 supervisor 数量，避免提前承载后续控制语义。
type MasterState struct {
	// ID 是本次 horizon master 进程启动生成的稳定逻辑 ID，用于关联派生 supervisor。
	ID string
	// Host 是 master 所在主机名，便于状态命令定位进程。
	Host string
	// PID 是 master 进程 ID。
	PID int
	// Status 是写入状态或读取时由 heartbeat TTL 派生的状态。
	Status string
	// StartedAt 是 master 启动时间。
	StartedAt time.Time
	// LastHeartbeatAt 是最近一次 heartbeat 时间。
	LastHeartbeatAt time.Time
	// SupervisorCount 是该 master 按当前环境配置派生的 supervisor 数量。
	SupervisorCount int
	// Environment 是 master 使用的 Horizon 环境名，不替代完整应用 bootstrap。
	Environment string
	// GoroutineCount 是 master heartbeat 随进程自省上报的低成本协程数指标。
	GoroutineCount goprocess.Metric
	// MemoryRSSBytes 是 master heartbeat 随进程自省上报的 RSS 字节数指标。
	MemoryRSSBytes goprocess.Metric
	// MemoryPercent 是 master heartbeat 在系统总内存可得时上报的内存占比指标。
	MemoryPercent goprocess.Metric
}

type SupervisorState struct {
	// Name 是 supervisor 的稳定名称，也是 Redis key 与控制标记索引使用的标识。
	Name string
	// Host 是 supervisor 所在主机名，用于状态命令定位运行实例。
	Host string
	// PID 是 supervisor 所在进程 ID，便于命令输出贴近 Laravel Horizon。
	PID int
	// MasterID 是派生该 supervisor 的 master 逻辑 ID，用于重复实例诊断。
	MasterID string
	// Environment 是该 supervisor 所属的 Horizon 环境，用于避免不同环境的同名 supervisor 互相阻塞。
	Environment string
	// Status 是上报状态或读取时派生状态，只允许使用本包定义的 supervisor 状态常量。
	Status string
	// StartedAt 是 supervisor 启动时间，用于 supervisor-status 详情展示。
	StartedAt time.Time
	// LastHeartbeatAt 是最近 heartbeat 时间，读取/list/status 时结合 TTL 推导 stale。
	LastHeartbeatAt time.Time
	// WorkerCount 是该 supervisor 当前管理的 worker 总数。
	WorkerCount int
	// Connection 是该 supervisor 消费的队列连接名。
	Connection string
	// Queues 是该 supervisor 监听的队列列表。
	Queues []string
	// Pools 保存按 queue process pool 拆分后的当前 worker 数和目标 worker 数。
	Pools []ProcessPoolState
	// GoroutineCount 是 supervisor heartbeat 随进程自省上报的低成本协程数指标。
	GoroutineCount goprocess.Metric
	// MemoryRSSBytes 是 supervisor heartbeat 随进程自省上报的 RSS 字节数指标。
	MemoryRSSBytes goprocess.Metric
	// MemoryPercent 是 supervisor heartbeat 在系统总内存可得时上报的内存占比指标。
	MemoryPercent goprocess.Metric
}

// WorkerState 是 worker heartbeat 写入和状态快照统计的基础状态模型。
//
// 设计原因：worker 状态不再保存 per-job 明细（CurrentJob、Processed、Failed、CurrentQueue）；
// 任务量统计走 collector + flusher 的 event_metrics 聚合通道，高价值任务明细走 high_value_detail 通道。
// 该模型只承载进程身份、生命周期状态、自省指标和 heartbeat 诊断，避免队列事件热路径直接写 Store。
type WorkerState struct {
	// ID 是 worker 的稳定实例 ID，也是 Redis key 与索引使用的标识。
	ID string
	// Supervisor 是 worker 归属的 supervisor 名称，用于继承 supervisor pause 标记。
	Supervisor string
	// Environment 是 worker 所属的 Horizon environment，用于多环境/多机状态隔离。
	Environment string
	// Host 是 worker 所在主机名。
	Host string
	// PID 是 worker 所在进程 ID。
	PID int
	// Status 是上报状态或读取时派生状态，只允许使用本包定义的 worker 状态常量。
	Status string
	// StartedAt 是 worker 启动时间。
	StartedAt time.Time
	// LastHeartbeatAt 是最近 heartbeat 时间，读取/list/status 时结合 TTL 推导 stale。
	LastHeartbeatAt time.Time
	// ConfiguredQueues 是 worker 启动参数或 supervisor 配置中的队列范围，用于展示绑定 queue。
	ConfiguredQueues []string
	// GoroutineCount 是 worker heartbeat 随进程自省上报的低成本协程数指标。
	GoroutineCount goprocess.Metric
	// MemoryRSSBytes 是 worker heartbeat 随进程自省上报的 RSS 字节数指标。
	MemoryRSSBytes goprocess.Metric
	// MemoryPercent 是 worker heartbeat 在系统总内存可得时上报的内存占比指标。
	MemoryPercent goprocess.Metric
	// CollectorMemoryBytes 是当前 worker 进程内 Horizon collector 自有结构的低成本内存估算。
	CollectorMemoryBytes goprocess.Metric
	// LastHeartbeatErrorCode 是最近一次 heartbeat 写入失败的稳定错误码，不包含底层 err.Error()。
	LastHeartbeatErrorCode string
	// LastHeartbeatErrorMessage 是最近一次 heartbeat 写入失败的短诊断信息，用于 Dashboard 排障。
	LastHeartbeatErrorMessage string
	// LastHeartbeatErrorAt 是最近一次 heartbeat 写入失败发生的时间。
	LastHeartbeatErrorAt time.Time
}

// ControlState 保存 Horizon 控制命令写入的全局和 supervisor 级控制标记。
//
// 需求背景：控制命令只写 Store 标记，不直接杀 goroutine 或复用 queue restart key。
type ControlState struct {
	// GlobalPaused 表示 horizon:pause 写入的全局暂停标记。
	GlobalPaused bool
	// PausedSupervisors 保存 horizon:pause-supervisor 写入的 supervisor 级暂停标记。
	PausedSupervisors map[string]bool
	// TerminateRequestedAt 是 horizon:terminate 写入的一次性优雅退出请求时间。
	TerminateRequestedAt time.Time
	// TerminateShouldWait 表示 --wait 写入的内部终止等待策略，由长驻 master/supervisor 消费。
	TerminateShouldWait bool
}

// StatusSnapshot 是 horizon:status 输出所需的派生视图。
//
// 设计思路：全局 status 不单独持久化，而是每次从控制标记和 heartbeat 状态派生，优先级固定为
// terminating > paused > running > inactive。
type StatusSnapshot struct {
	// Status 是派生出的全局状态。
	Status string
	// GlobalPaused 透出当前全局暂停标记。
	GlobalPaused bool
	// TerminateRequested 表示是否存在未清理的 terminate 请求。
	TerminateRequested bool
	// SupervisorCount 是当前 Store 中可读取的 supervisor 记录数。
	SupervisorCount int
	// WorkerCount 是当前 Store 中可读取的 worker 记录数。
	WorkerCount int
	// StaleSupervisorCount 是读取时被 TTL 判定为 stale 的 supervisor 数量。
	StaleSupervisorCount int
	// StaleWorkerCount 是读取时被 TTL 判定为 stale 的 worker 数量。
	StaleWorkerCount int
	// QueueCount 是当前只读队列聚合视图中的 connection:queue 数量。
	QueueCount int `json:"queue_count"`
	// JobsPerMinute 是过去 1 小时 processed 数折算出的每分钟处理速率。
	JobsPerMinute *float64 `json:"jobs_per_minute,omitempty"`
	// JobsPastHour 是最近 1 小时内窗口重叠的 processed 数。
	JobsPastHour *int64 `json:"jobs_past_hour,omitempty"`
	// TotalProcessed 是默认 24h metrics 窗口内的 processed 总数。
	TotalProcessed *int64 `json:"total_processed,omitempty"`
}

// Store 定义 Horizon 控制状态和基础 heartbeat 状态的稳定访问边界。
//
// 使用方式：命令层只依赖该接口读取状态或写入控制标记；实际 memory/redis 构造由 Manager 的 StoreResolver
// 完成，避免命令包直接读取全局配置或创建 Redis client。
// QueueLengthBucket 表示单个 connection+queue 的当前队列长度采样结果。
//
// 需求背景：issue 04 要求队列长度来自 queue backend 的轮询事实源，不能混入 issue 03 的事件派生 metrics。
// 设计思路：该模型只保存定位目标和长度，不保存 queue.Envelope、payload、failed job 或 driver 内部状态，确保后续 CLI/UI 展示边界清晰。
type QueueLengthBucket struct {
	// Connection 是 Horizon supervisor 配置中声明的 queue connection 名称。
	Connection string `json:"connection"`
	// Queue 是 Horizon supervisor 配置中声明的 queue 名称。
	Queue string `json:"queue"`
	// Size 是采样时 queue adapter 返回的当前队列长度。
	Size int64 `json:"size"`
}

// QueueLengthSnapshot 保存一次 Horizon 队列长度采样的完整结果。
//
// 使用方式：horizon:snapshot 先从当前环境 supervisor 配置推导目标，再对每个目标调用 QueueConnection.Size，
// 全部成功后才通过 Store.SaveQueueLengthSnapshot 持久化该模型。
type QueueLengthSnapshot struct {
	// CapturedAt 是本次队列长度采样完成并准备保存的时间。
	CapturedAt time.Time `json:"captured_at"`
	// Queues 按 connection+queue 维度保存采样结果。
	Queues []QueueLengthBucket `json:"queues"`
}

// EventMetricWindowQuery 描述 event_metrics window 的 Store 读取条件。
//
// 用途：作为 `/metrics/current`、`/metrics/history/{kind}/{key}` 与 Store 之间的只读查询合同。
// 使用方式：HTTP 层解析 query string 后构造该结构，Memory/Redis Store 按字段执行过滤并返回稳定分页。
// 设计原因：过滤必须下推到 Store，避免 HTTP read model 先拉大页再过滤导致内存压力、分页漂移和诊断误判。
// 设计思路：该结构只表达事件窗口范围、来源维度和分页；聚合、quality、missing/stale source 诊断仍由 read model 完成。
// 需求背景：issue 44 要求 metrics API 支持时间范围和来源下钻，且 `flush_at` 只能作为诊断时间，不能参与范围归属。
type EventMetricWindowQuery struct {
	// Page 是 raw/source detail 窗口列表的分页请求；聚合读取会复用其他字段但扩大分页遍历完整过滤集合。
	Page PageRequest
	// OmitSourceDetails 表示 HTTP 调用方只需要 queue 聚合 summary，不需要来源分片输出。
	// 该字段不参与 Store 过滤，只用于 read model 控制响应体大小。
	OmitSourceDetails bool
	// From 是事件窗口范围下界；窗口需满足 WindowEnd > From 才会被纳入，空值表示无下界。
	From time.Time
	// To 是事件窗口范围上界；窗口需满足 WindowStart < To 才会被纳入，空值表示无上界。
	To time.Time
	// SourceHost 是采集事件的主机精确匹配条件；空值表示不过滤该维度。
	SourceHost string
	// SourceEnvironment 是 Horizon environment 精确匹配条件；空值表示不过滤该维度。
	SourceEnvironment string
	// SourceSupervisor 是 worker runtime supervisor 精确匹配条件；空值表示不过滤该维度。
	SourceSupervisor string
	// Connection 是队列连接精确匹配条件；空值表示不过滤该维度。
	Connection string
	// Queue 是队列名称精确匹配条件；空值表示不过滤该维度。
	Queue string
}

// HighValueDetailQuery 描述 high_value_detail 只读列表查询条件。
//
// 使用方式：HTTP 层解析 page/page_size/kind/occurred_from/occurred_to 后传入 Store；
// Store 必须先按 kind 和 OccurredAt 半开范围过滤，再对过滤结果分页。
// 设计边界：只允许 failed、poison、slow_job 三类高价值诊断摘要，不提供 recent/completed/silenced job 列表。
type HighValueDetailQuery struct {
	Page         PageRequest
	Kind         string
	OccurredFrom time.Time
	OccurredTo   time.Time
}

// QueueWaitSnapshot 表示 connection:queue 的等待时间能力与观测值。
//
// 设计思路：wait time 只能来自 queue event/envelope 中显式的 queued_at 元数据；当 driver
// 或历史数据无法提供该时间时，Status 必须返回 unknown/unsupported，不能静默返回 0。
type QueueWaitSnapshot struct {
	// Key 是 connection:queue 形式的稳定索引。
	Key string `json:"key"`
	// Connection 是队列连接名。
	Connection string `json:"connection"`
	// Queue 是队列名。
	Queue string `json:"queue"`
	// Status 表示 wait time 能力状态：known、unknown 或 unsupported。
	Status string `json:"status"`
	// WaitMS 是已知状态下观测到的最长等待毫秒数；unknown/unsupported 时保持 0。
	WaitMS int64 `json:"wait_ms"`
	// Threshold 是 waits 配置中的长等待阈值，单位毫秒。
	Threshold int64 `json:"threshold_ms"`
	// LongWait 表示 WaitMS 已达到 Threshold；unknown/unsupported 时必须为 false。
	LongWait bool `json:"long_wait"`
	// SampledAt 是本次 wait snapshot 生成时间。
	SampledAt time.Time `json:"sampled_at"`
}

// MetricsHistorySnapshot 是 Dashboard/API 读取的安全 metrics history 点。
//
// 安全边界：history 只保存聚合数值、时间戳、wait 状态和定位 key，不保存 job payload、
// raw envelope、broker 内部字段、Redis/RabbitMQ credential 或完整错误堆栈。
type MetricsHistorySnapshot struct {
	// Kind 区分 history 类型；当前只保存 queue 级 metrics history。
	Kind string `json:"kind"`
	// Key 在 queue history 中是 connection:queue。
	Key string `json:"key"`
	// Timestamp 是该 history bucket 的采样时间。
	Timestamp time.Time `json:"timestamp"`
	// Throughput 是该 bucket 的吞吐量聚合值。
	Throughput int64 `json:"throughput"`
	// RuntimeMS 是该 bucket 的运行耗时聚合值，单位毫秒。
	RuntimeMS int64 `json:"runtime_ms"`
	// Failed 是该 bucket 的失败任务数量。
	Failed int64 `json:"failed"`
	// Released 是该 bucket 的释放重试数量。
	Released int64 `json:"released"`
	// PoisonEnvelopes 是 queue history 中的坏消息数量。
	PoisonEnvelopes int64 `json:"poison_envelopes"`
	// WaitMS 是 queue history 中的等待时间观测值，单位毫秒。
	WaitMS int64 `json:"wait_ms"`
	// WaitStatus 是 wait time 能力状态。
	WaitStatus string `json:"wait_status"`
	// WindowStart 是该 history 点对应的 event_metrics 事件窗口开始时间。
	WindowStart time.Time `json:"window_start,omitempty"`
	// WindowEnd 是该 history 点对应的 event_metrics 事件窗口结束时间。
	WindowEnd time.Time `json:"window_end,omitempty"`
	// FlushAt 是 Store 写入诊断时间，不参与吞吐窗口归属。
	FlushAt time.Time `json:"flush_at,omitempty"`
	// EffectiveSampleRate 是该窗口实际采样率；0 表示不可估算。
	EffectiveSampleRate float64 `json:"effective_sample_rate,omitempty"`
	// SampleCount 是该窗口实际采样命中数。
	SampleCount int64 `json:"sample_count,omitempty"`
	// EstimatedTotal 是按窗口采样率估算后的原始事件总量。
	EstimatedTotal int64 `json:"estimated_total,omitempty"`
	// Quality 是 exact、estimated、degraded、unknown 或 partial。
	Quality string `json:"quality,omitempty"`
	// Degraded 表示该窗口存在可诊断的质量下降。
	Degraded bool `json:"degraded,omitempty"`
	// Unknown 表示该窗口存在不可量化缺口。
	Unknown bool `json:"unknown,omitempty"`
}

// BatchSummary 是 Horizon Dashboard 可展示的批次安全摘要。
//
// 需求背景：issue 13/14/15 需要 Batches 页面和只读 API 复用 Prismgo BatchEvent，
// 但不能把 batch 内部 job payload、raw envelope 或 broker 字段写入 Horizon Store。
// 设计思路：该模型只保存批次进度、状态和时间戳，Store 与 HTTP 层都以该 DTO 为边界。
type BatchSummary struct {
	// ID 是 Prismgo queue 批次 ID，用于列表搜索和详情读取。
	ID string `json:"id"`
	// Name 是业务侧创建批次时显式提供的展示名称。
	Name string `json:"name"`
	// Status 是 running、finished 或 cancelled，按 BatchStatus 派生。
	Status string `json:"status"`
	// Total 是批次内任务总数。
	Total int `json:"total"`
	// Pending 是尚未完成的任务数量。
	Pending int `json:"pending"`
	// Processed 是已经结束处理的任务数量，包含失败任务。
	Processed int `json:"processed"`
	// Failed 是失败任务数量。
	Failed int `json:"failed"`
	// Cancelled 表示批次是否已取消，保留布尔值便于 Dashboard 兼容旧展示逻辑。
	Cancelled bool `json:"cancelled"`
	// CreatedAt 是批次创建时间。
	CreatedAt time.Time `json:"created_at"`
	// FinishedAt 是批次完成时间；未完成时为空。
	FinishedAt time.Time `json:"finished_at,omitempty"`
	// CancelledAt 是批次取消时间；未取消时为空。
	CancelledAt time.Time `json:"cancelled_at,omitempty"`
	// UpdatedAt 是 Horizon 接收最近一次 BatchEvent 的时间，用于稳定排序和排障。
	UpdatedAt time.Time `json:"updated_at"`
	// WindowStart 是该 BatchEvent 归属的低频 summary window 开始时间。
	WindowStart time.Time `json:"window_start,omitempty"`
	// WindowEnd 是该 BatchEvent 归属的低频 summary window 结束时间。
	WindowEnd time.Time `json:"window_end,omitempty"`
	// FlushAt 是 flusher 写入 Store 的诊断时间，不改变事件窗口归属。
	FlushAt time.Time `json:"flush_at,omitempty"`
	// Partial 表示 shutdown best-effort flush 写入了尚未完整结束的 batch summary window。
	Partial bool `json:"partial"`
	// Quality 是 exact、degraded 或 partial，用于 Dashboard 直接展示 summary 质量。
	Quality string `json:"quality,omitempty"`
}

// OrphanProcess 表示 purge 扫描到的、不再属于 active supervisor pool 的 Horizon worker 进程。
//
// 设计思路：该模型只保存 master、PID 和首次发现时间，不保存 job payload、worker heartbeat 副本或 failed metadata。
type OrphanProcess struct {
	MasterID    string
	PID         int
	FirstSeenAt time.Time
}

type Store interface {
	// AcquireMasterLease 原子声明当前 host/environment 的 master 运行租约。
	//
	// 语义说明：返回 true 时必须已经写入首个 master heartbeat；返回 false 表示同一运行域已有
	// fresh master。该方法替代“先 list 再 heartbeat”的非原子启动检查，避免双 master 同时派生 supervisor。
	AcquireMasterLease(context.Context, MasterState) (bool, error)
	HeartbeatMaster(context.Context, MasterState) error
	Masters(context.Context, time.Time) ([]MasterState, error)
	// AcquireSupervisorLease 原子声明当前 host/environment/name 的 supervisor 运行租约。
	//
	// 语义说明：返回 true 时必须已经写入首个 supervisor heartbeat；返回 false 表示同一运行域已有
	// fresh supervisor。长期存活仍由 HeartbeatSupervisor 续约，读取路径继续按 heartbeat TTL 派生 stale。
	AcquireSupervisorLease(context.Context, SupervisorState) (bool, error)
	HeartbeatSupervisor(context.Context, SupervisorState) error
	HeartbeatWorker(context.Context, WorkerState) error
	Supervisor(context.Context, string, time.Time) (SupervisorState, bool, error)
	Supervisors(context.Context, time.Time) ([]SupervisorState, error)
	Workers(context.Context, time.Time) ([]WorkerState, error)
	Control(context.Context) (ControlState, error)
	SetGlobalPaused(context.Context, bool) error
	SetSupervisorPaused(context.Context, string, bool) error
	RequestTerminate(context.Context, time.Time, bool) error
	ClearTerminateRequest(context.Context) error
	StatusSnapshot(context.Context, time.Time) (StatusSnapshot, error)
	Trim(context.Context, time.Time) error

	ClearMetrics(context.Context) error
	AppendEventMetricWindows(context.Context, []EventMetricWindow, time.Duration) error
	EventMetricWindows(context.Context, EventMetricWindowQuery) (PageEnvelope[EventMetricWindow], error)
	// EventMetricRollupWindows 返回按 connection:queue 聚合后的 event_metrics 窗口。
	//
	// 使用边界：该方法只服务 `/metrics/current` 和 `/queues` 的 summary 聚合路径；
	// 返回值不包含 source host/environment/supervisor/jobName，不能用于来源下钻、Metric Sources
	// 或 raw windows 分页展示。Store 实现必须在 AppendEventMetricWindows 写 raw 窗口时同步维护
	// 该 rollup，避免 summary 请求为了聚合而反复扫描完整 raw window 集合。
	EventMetricRollupWindows(context.Context, EventMetricWindowQuery) ([]EventMetricWindow, error)
	SaveQueueLengthSnapshot(context.Context, QueueLengthSnapshot) error
	QueueLengthSnapshot(context.Context) (QueueLengthSnapshot, error)
	SaveHighValueDetails(context.Context, []HighValueJobDetail, time.Duration) error
	HighValueDetails(context.Context, HighValueDetailQuery) (PageEnvelope[HighValueJobDetail], error)
	HighValueDetail(context.Context, string) (HighValueJobDetail, bool, error)
	SaveObservabilityDiagnostics(context.Context, []ObservabilityDiagnostic, time.Duration) error
	ObservabilityDiagnostics(context.Context, PageRequest) (PageEnvelope[ObservabilityDiagnostic], error)
	SaveBatchSummary(context.Context, BatchSummary) error
	BatchesPage(context.Context, string, PageRequest) (PageEnvelope[BatchSummary], error)
	Batch(context.Context, string) (BatchSummary, bool, error)
	RecordOrphanProcess(context.Context, string, int, time.Time) error
	OrphanProcessesOlderThan(context.Context, string, time.Duration, time.Time) ([]OrphanProcess, error)
	ForgetOrphanProcess(context.Context, string, int) error
}

// normalizeStoreOptions 补齐 Store 默认 prefix 与 heartbeat TTL。
//
// 参数说明：options 是调用方传入的 Store 配置；返回值会保证 Prefix 和 HeartbeatTTL 非空可用。
func normalizeStoreOptions(options StoreOptions) StoreOptions {
	if options.Prefix == "" {
		options.Prefix = "prismgo_horizon"
	}
	if options.HeartbeatTTL <= 0 {
		options.HeartbeatTTL = 60 * time.Second
	}
	return options
}

// supervisorWithDerivedStatus 在读取路径上把 heartbeat 与控制标记投影为 supervisor 状态。
//
// 参数说明：state 是 Store 中保存的原始 heartbeat 状态；control 是当前控制标记；ttl 是 stale 判断窗口；
// now 由测试或调用方传入，便于稳定验证时间敏感逻辑。
// masterWithDerivedStatus 在读取路径根据 heartbeat TTL 派生 master 状态。
//
// 逻辑说明：stale 不作为写入状态保存，避免 memory 和 Redis 因清理时机不同而出现状态漂移。
func masterWithDerivedStatus(state MasterState, ttl time.Duration, now time.Time) MasterState {
	if heartbeatStale(state.LastHeartbeatAt, ttl, now) {
		state.Status = MasterStale
	}
	return state
}

func supervisorWithDerivedStatus(state SupervisorState, control ControlState, ttl time.Duration, now time.Time) SupervisorState {
	state.Queues = append([]string(nil), state.Queues...)
	state.Pools = cloneProcessPools(state.Pools)
	if heartbeatStale(state.LastHeartbeatAt, ttl, now) {
		state.Status = SupervisorStale
		return state
	}
	if !control.TerminateRequestedAt.IsZero() {
		state.Status = SupervisorTerminating
		return state
	}
	if control.GlobalPaused || control.PausedSupervisors[state.Name] {
		state.Status = SupervisorPaused
	}
	return state
}

// workerWithDerivedStatus 在读取路径上把 heartbeat 与控制标记投影为 worker 状态。
//
// 设计原因：stale 不作为写入状态，pause/terminate 也来自控制标记，因此所有 Store 实现都复用同一派生函数。
func workerWithDerivedStatus(state WorkerState, control ControlState, ttl time.Duration, now time.Time) WorkerState {
	state.ConfiguredQueues = append([]string(nil), state.ConfiguredQueues...)
	if heartbeatStale(state.LastHeartbeatAt, ttl, now) {
		state.Status = WorkerStale
		return state
	}
	if !control.TerminateRequestedAt.IsZero() {
		state.Status = WorkerTerminating
		return state
	}
	if control.GlobalPaused || control.PausedSupervisors[state.Supervisor] {
		state.Status = WorkerPaused
	}
	return state
}

// heartbeatStale 判断 heartbeat 是否已经超过 TTL。
//
// 参数说明：lastHeartbeat 是实体最后一次上报时间；ttl 是允许存活窗口；now 为空时使用当前时间，方便生产路径调用。
func heartbeatStale(lastHeartbeat time.Time, ttl time.Duration, now time.Time) bool {
	if lastHeartbeat.IsZero() {
		return true
	}
	if now.IsZero() {
		now = time.Now()
	}
	return lastHeartbeat.Add(ttl).Before(now)
}

// deriveStatusSnapshot 根据控制标记和已派生的实体状态生成 horizon:status 快照。
//
// 逻辑说明：terminate 请求优先级最高，其次是全局 pause；只有至少一个非 stale supervisor 时才视为 running。
func deriveStatusSnapshot(control ControlState, supervisors []SupervisorState, workers []WorkerState) StatusSnapshot {
	snapshot := StatusSnapshot{
		GlobalPaused:    control.GlobalPaused,
		SupervisorCount: len(supervisors),
		WorkerCount:     len(workers),
	}
	runningSupervisors := 0
	for _, supervisor := range supervisors {
		if supervisor.Status == SupervisorStale {
			snapshot.StaleSupervisorCount++
			continue
		}
		runningSupervisors++
	}
	runningWorkers := 0
	for _, worker := range workers {
		if worker.Status == WorkerStale {
			snapshot.StaleWorkerCount++
			continue
		}
		runningWorkers++
	}
	snapshot.TerminateRequested = !control.TerminateRequestedAt.IsZero() && (runningSupervisors > 0 || runningWorkers > 0)
	switch {
	case snapshot.TerminateRequested:
		snapshot.Status = GlobalTerminating
	case snapshot.GlobalPaused:
		snapshot.Status = GlobalPaused
	case runningSupervisors > 0:
		snapshot.Status = GlobalRunning
	default:
		snapshot.Status = GlobalInactive
	}
	return snapshot
}

// cloneControlState 复制控制标记，避免调用方修改 Store 内部 map。
func cloneControlState(input ControlState) ControlState {
	out := input
	out.PausedSupervisors = make(map[string]bool, len(input.PausedSupervisors))
	for name, paused := range input.PausedSupervisors {
		out.PausedSupervisors[name] = paused
	}
	return out
}

// cloneQueueLengthSnapshot 复制队列长度快照切片，避免 Store 内部状态被调用方后续修改。
func cloneQueueLengthSnapshot(input QueueLengthSnapshot) QueueLengthSnapshot {
	out := input
	out.Queues = append([]QueueLengthBucket{}, input.Queues...)
	return out
}

// cloneBatchSummaries 复制 batch 摘要切片，避免 Store 内部缓存被调用方修改。
func cloneBatchSummaries(input []BatchSummary) []BatchSummary {
	return append([]BatchSummary(nil), input...)
}

func cloneHighValueJobDetails(input []HighValueJobDetail) []HighValueJobDetail {
	return append([]HighValueJobDetail(nil), input...)
}

func cloneObservabilityDiagnostics(input []ObservabilityDiagnostic) []ObservabilityDiagnostic {
	return append([]ObservabilityDiagnostic(nil), input...)
}

func cloneEventMetricWindows(input []EventMetricWindow) []EventMetricWindow {
	return append([]EventMetricWindow(nil), input...)
}

func sortEventMetricWindows(items []EventMetricWindow) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].WindowStart.Equal(items[j].WindowStart) {
			return items[i].WindowStart.After(items[j].WindowStart)
		}
		if !items[i].FlushAt.Equal(items[j].FlushAt) {
			return items[i].FlushAt.After(items[j].FlushAt)
		}
		if items[i].Connection != items[j].Connection {
			return items[i].Connection < items[j].Connection
		}
		if items[i].Queue != items[j].Queue {
			return items[i].Queue < items[j].Queue
		}
		if items[i].SourcePrefix != items[j].SourcePrefix {
			return items[i].SourcePrefix < items[j].SourcePrefix
		}
		if items[i].SourceHost != items[j].SourceHost {
			return items[i].SourceHost < items[j].SourceHost
		}
		if items[i].SourceEnvironment != items[j].SourceEnvironment {
			return items[i].SourceEnvironment < items[j].SourceEnvironment
		}
		if items[i].SourceSupervisor != items[j].SourceSupervisor {
			return items[i].SourceSupervisor < items[j].SourceSupervisor
		}
		return items[i].JobName < items[j].JobName
	})
}

// queueEventMetricRollups 把同一批 raw event_metrics 窗口折叠为 queue 级 rollup。
//
// 聚合维度：WindowStart、WindowEnd、MetricsWindowMS、Connection、Queue。来源维度和 JobName
// 被有意丢弃，因为 summary 只需要队列级吞吐、失败、释放、排队和 runtime 估算。
// 质量语义：计数直接求和，quality 取最差级别；采样率保留非零最小值，避免把低采样窗口
// 合并后误展示成更高精度。需要来源明细时必须读取 raw windows，而不是反解 rollup。
func queueEventMetricRollups(windows []EventMetricWindow, flushAt time.Time) []EventMetricWindow {
	groups := make(map[string]*EventMetricWindow)
	for _, window := range windows {
		key := strings.Join([]string{
			window.WindowStart.Format(time.RFC3339Nano),
			window.WindowEnd.Format(time.RFC3339Nano),
			strconv.FormatInt(eventMetricMetricsWindowMS(window.MetricsWindowMS, window.WindowStart, window.WindowEnd), 10),
			strings.TrimSpace(window.Connection),
			strings.TrimSpace(window.Queue),
		}, ":")
		rollup, ok := groups[key]
		if !ok {
			item := EventMetricWindow{
				WindowStart:         window.WindowStart,
				WindowEnd:           window.WindowEnd,
				FlushAt:             flushAt,
				MetricsWindowMS:     eventMetricMetricsWindowMS(window.MetricsWindowMS, window.WindowStart, window.WindowEnd),
				Connection:          strings.TrimSpace(window.Connection),
				Queue:               strings.TrimSpace(window.Queue),
				EffectiveSampleRate: window.EffectiveSampleRate,
				Quality:             window.Quality,
			}
			groups[key] = &item
			rollup = &item
		}
		rollup.Processed += window.Processed
		rollup.Failed += window.Failed
		rollup.Released += window.Released
		rollup.Poison += window.Poison
		rollup.Queued += window.Queued
		rollup.RuntimeMS += window.RuntimeMS
		rollup.SampleCount += window.SampleCount
		rollup.RuntimeSampleCount += window.RuntimeSampleCount
		rollup.EstimatedTotal += window.EstimatedTotal
		rollup.Estimated = rollup.Estimated || window.Estimated
		rollup.Degraded = rollup.Degraded || window.Degraded
		rollup.Unknown = rollup.Unknown || window.Unknown
		rollup.Partial = rollup.Partial || window.Partial
		rollup.Quality = mergeEventMetricQuality(rollup.Quality, window.Quality)
		if rollup.EffectiveSampleRate == 0 || (window.EffectiveSampleRate > 0 && window.EffectiveSampleRate < rollup.EffectiveSampleRate) {
			rollup.EffectiveSampleRate = window.EffectiveSampleRate
		}
	}
	out := make([]EventMetricWindow, 0, len(groups))
	for _, item := range groups {
		if item.Quality == "" {
			item.Quality = eventMetricQualityForWindow(item.Estimated, item.Degraded, item.Partial, item.Unknown)
		}
		out = append(out, *item)
	}
	sortEventMetricWindows(out)
	return out
}

// normalizeEventMetricWindowQuery 规范化 Store 层可直接执行的 event_metrics 查询条件。
//
// 使用方式：Memory/Redis Store 和聚合读取路径在执行过滤前调用；它只修剪来源字段并补齐分页默认值，
// 不校验 HTTP 参数格式，避免 Store 和 HTTP 错误语义耦合。
// 设计原因：Store 查询可能来自 HTTP、命令或内部 read model；统一规范化可保证分页和精确匹配语义一致。
func normalizeEventMetricWindowQuery(query EventMetricWindowQuery) EventMetricWindowQuery {
	query.SourceHost = strings.TrimSpace(query.SourceHost)
	query.SourceEnvironment = strings.TrimSpace(query.SourceEnvironment)
	query.SourceSupervisor = strings.TrimSpace(query.SourceSupervisor)
	query.Connection = strings.TrimSpace(query.Connection)
	query.Queue = strings.TrimSpace(query.Queue)
	if query.Page.Page <= 0 {
		query.Page.Page = defaultPageNumber
	}
	if query.Page.PageSize <= 0 {
		query.Page.PageSize = defaultPageSize
	}
	return query
}

// eventMetricWindowMatchesQuery 判断单个事件窗口是否命中查询条件。
//
// 设计思路：时间范围采用半开窗口重叠语义，即 WindowEnd > From 且 WindowStart < To；
// 来源维度全部为精确匹配，空查询字段表示不过滤该维度。
// 需求背景：issue 44 明确禁止用 FlushAt 决定范围归属，因为 FlushAt 只是 Store 写入诊断时间。
func eventMetricWindowMatchesQuery(window EventMetricWindow, query EventMetricWindowQuery) bool {
	if !query.From.IsZero() && !window.WindowEnd.After(query.From) {
		return false
	}
	if !query.To.IsZero() && !window.WindowStart.Before(query.To) {
		return false
	}
	if query.SourceHost != "" && window.SourceHost != query.SourceHost {
		return false
	}
	if query.SourceEnvironment != "" && window.SourceEnvironment != query.SourceEnvironment {
		return false
	}
	if query.SourceSupervisor != "" && window.SourceSupervisor != query.SourceSupervisor {
		return false
	}
	if query.Connection != "" && window.Connection != query.Connection {
		return false
	}
	if query.Queue != "" && window.Queue != query.Queue {
		return false
	}
	return true
}

func sortMasterStates(items []MasterState) {
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
}

func sortSupervisorStates(items []SupervisorState) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].LastHeartbeatAt.Equal(items[j].LastHeartbeatAt) {
			return items[i].LastHeartbeatAt.After(items[j].LastHeartbeatAt)
		}
		if items[i].Name != items[j].Name {
			return items[i].Name < items[j].Name
		}
		if items[i].Host != items[j].Host {
			return items[i].Host < items[j].Host
		}
		if items[i].Environment != items[j].Environment {
			return items[i].Environment < items[j].Environment
		}
		return items[i].PID < items[j].PID
	})
}

func sortWorkerStates(items []WorkerState) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].LastHeartbeatAt.Equal(items[j].LastHeartbeatAt) {
			return items[i].LastHeartbeatAt.After(items[j].LastHeartbeatAt)
		}
		if items[i].ID != items[j].ID {
			return items[i].ID < items[j].ID
		}
		if items[i].Host != items[j].Host {
			return items[i].Host < items[j].Host
		}
		if items[i].Environment != items[j].Environment {
			return items[i].Environment < items[j].Environment
		}
		return items[i].Supervisor < items[j].Supervisor
	})
}

func batchMatchesQuery(summary BatchSummary, query string) bool {
	if query == "" {
		return true
	}
	return strings.Contains(strings.ToLower(summary.ID), query) ||
		strings.Contains(strings.ToLower(summary.Name), query)
}

func sortBatchSummaries(items []BatchSummary) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].CreatedAt.After(items[j].CreatedAt)
		}
		return items[i].ID < items[j].ID
	})
}

func batchSummaryRetentionTime(item BatchSummary) time.Time {
	switch {
	case !item.UpdatedAt.IsZero():
		return item.UpdatedAt
	case !item.FlushAt.IsZero():
		return item.FlushAt
	case !item.CreatedAt.IsZero():
		return item.CreatedAt
	default:
		return time.Now().UTC()
	}
}

func sortHighValueJobDetails(items []HighValueJobDetail) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].OccurredAt.Equal(items[j].OccurredAt) {
			return items[i].OccurredAt.After(items[j].OccurredAt)
		}
		return items[i].ID < items[j].ID
	})
}

func normalizeHighValueDetailQuery(query HighValueDetailQuery) HighValueDetailQuery {
	query.Page = parsePageRequest(
		strconv.Itoa(query.Page.Page),
		strconv.Itoa(query.Page.PageSize),
	)
	query.Kind = strings.TrimSpace(query.Kind)
	return query
}

func highValueDetailMatchesQuery(detail HighValueJobDetail, query HighValueDetailQuery) bool {
	if query.Kind != "" && detail.Kind != query.Kind {
		return false
	}
	if !query.OccurredFrom.IsZero() && detail.OccurredAt.Before(query.OccurredFrom) {
		return false
	}
	if !query.OccurredTo.IsZero() && !detail.OccurredAt.Before(query.OccurredTo) {
		return false
	}
	return true
}

func isAllowedHighValueDetailKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "", HighValueDetailFailed, HighValueDetailPoison, HighValueDetailSlowJob:
		return true
	default:
		return false
	}
}

func sortObservabilityDiagnostics(items []ObservabilityDiagnostic) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].ObservedAt.Equal(items[j].ObservedAt) {
			return items[i].ObservedAt.After(items[j].ObservedAt)
		}
		if items[i].Reason == items[j].Reason {
			return items[i].Description < items[j].Description
		}
		return items[i].Reason < items[j].Reason
	})
}

// cloneProcessPools 复制 supervisor process pool 状态，避免调用方修改 Store 内部切片。
func cloneProcessPools(input []ProcessPoolState) []ProcessPoolState {
	if len(input) == 0 {
		return nil
	}
	out := make([]ProcessPoolState, 0, len(input))
	for _, pool := range input {
		pool.Queues = append([]string(nil), pool.Queues...)
		out = append(out, pool)
	}
	return out
}
