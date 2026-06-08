package cmd

import (
	"context"
	"errors"
	"time"

	"github.com/prismgo/framework/console"
)

// ErrRuntimeNotConfigured 表示运行时命令缺少由父包注入的 Horizon Runtime。
var ErrRuntimeNotConfigured = errors.New("horizon: store resolver is not configured")

// ErrNoProcessesToTerminate 表示 terminate 请求已写入，但当前没有 fresh master/supervisor 可通知。
//
// 需求背景：Laravel Horizon 在没有可终止进程时输出稳定文案；Runtime 仍要保留 queue restart 与
// terminate flag 写入语义，因此用哨兵错误让命令层决定展示文本而不是回滚控制状态。
var ErrNoProcessesToTerminate = errors.New("horizon: no processes to terminate")

// MemoryStoreWarning 是运行时命令在 memory store 下统一输出的非生产提示。
const MemoryStoreWarning = "Memory Horizon store is only suitable for local/testing and is not recommended for production."

// RuntimeLoader 为运行时命令解析已注入核心依赖的 Horizon Runtime。
//
// 设计思路：cmd 包只依赖父包投影出来的窄接口，不反向 import horizon 包，避免 command 子包和核心包形成循环依赖。
type RuntimeLoader func(context.Context) (Runtime, error)

// Runtime 是 Horizon 运行时命令需要的最小行为边界。
//
// 需求背景：命令文件放在 horizon/cmd 后，不能直接访问核心包的 Manager、Store 或 QueueManager 类型；
// 该接口把状态读取、控制写入和维护操作收敛为命令语义，生产实现由 horizon 包适配。
type Runtime interface {
	UsesMemoryStore() bool
	StatusSnapshot(context.Context, time.Time) (StatusSnapshot, error)
	Masters(context.Context, time.Time) ([]MasterState, error)
	Supervisors(context.Context, time.Time) ([]SupervisorState, error)
	Supervisor(context.Context, string, time.Time) (SupervisorState, bool, error)
	Workers(context.Context, time.Time) ([]WorkerState, error)
	SetGlobalPaused(context.Context, bool) error
	SetSupervisorPaused(context.Context, string, bool) error
	RequestTerminate(context.Context, time.Time, bool) error
	MaxWorkerTimeout(string) (int, error)
	Snapshot(context.Context, time.Time) (SnapshotSummary, error)
	ClearMetrics(context.Context) error
	QueueTargets() []QueueTarget
	ClearQueue(context.Context, QueueTarget) error
	ForgetFailedJob(context.Context, string) error
	ForgetAllFailedJobs(context.Context) error
	Purge(context.Context, time.Time, string) (PurgeSummary, error)
	RunMaster(context.Context, MasterOptions) error
	RunSupervisor(context.Context, SupervisorProcessOptions) error
	RunWorker(context.Context, WorkerOptions) error
	Listen(context.Context, ListenOptions) (ListenSummary, error)
}

// StatusSnapshot 是 horizon:status 输出所需的状态投影。
type StatusSnapshot struct {
	Status               string
	GlobalPaused         bool
	TerminateRequested   bool
	SupervisorCount      int
	WorkerCount          int
	StaleSupervisorCount int
	StaleWorkerCount     int
}

// MasterState 是 horizon:stale 展示 master heartbeat 诊断时使用的只读投影。
type MasterState struct {
	ID              string
	Host            string
	PID             int
	Status          string
	StartedAt       time.Time
	LastHeartbeatAt time.Time
	SupervisorCount int
	Environment     string
}

// SupervisorState 是 supervisor 相关命令展示的运行时状态投影。
type SupervisorState struct {
	Name            string
	Host            string
	PID             int
	Status          string
	StartedAt       time.Time
	LastHeartbeatAt time.Time
	WorkerCount     int
	Connection      string
	Queues          []string
	Pools           []ProcessPoolState
}

// ProcessPoolState 是 supervisor 下 per-queue process pool 的只读状态投影。
type ProcessPoolState struct {
	Name           string
	Queue          string
	Queues         []string
	CurrentWorkers int
	TargetWorkers  int
}

// WorkerState 是 horizon:stale 展示 worker heartbeat 诊断时使用的只读投影。
//
// 设计原因（issue 50）：不再包含 CurrentJobID、CurrentJobName 字段；
// worker 当前执行明细已从状态模型中移除，任务统计走 event_metrics 聚合通道。
type WorkerState struct {
	ID              string
	Supervisor      string
	Host            string
	PID             int
	Status          string
	LastHeartbeatAt time.Time
}

// MasterOptions 是 horizon master 命令传给核心 runtime 的参数。
type MasterOptions struct {
	// Environment 是命令行指定的 Horizon 环境名，空值时由核心配置决定。
	Environment string
}

// SupervisorProcessOptions 是 horizon:supervisor 内部/调试入口的运行参数。
type SupervisorProcessOptions struct {
	Name         string
	Connection   string
	Environment  string
	MasterID     string
	ParentID     string
	Balance      string
	Backoff      string
	MaxJobs      int
	MaxTime      int
	Force        bool
	MaxProcesses int
	MinProcesses int
	Memory       int
	Nice         int
	Paused       bool
	Queue        string
	Sleep        int
	Timeout      int
	Tries        int
	WorkersName  string
}

// WorkerOptions 是 horizon:work 内部/调试入口的运行参数。
type WorkerOptions struct {
	Name        string
	Supervisor  string
	Environment string
	Prefix      string
	Connection  string
	Queue       string
	Sleep       int
	Rest        int
	Timeout     int
	Tries       int
	Backoff     string
	// RetryAfter 是底层 queue worker 的任务保留窗口，单位秒。
	//
	// 需求背景：horizon supervisor 配置已经包含 retry_after；horizon:work 命令层必须保留该参数，
	// 否则 RabbitMQ/Redis worker 会回退到 queue 默认值，导致 Horizon 展示配置和实际消费行为不一致。
	// 设计思路：命令层只保存秒数，runtime adapter 再转换为 time.Duration 并传给 queue.WorkerOptions。
	RetryAfter    int
	MaxJobs       int
	MaxTime       int
	Force         bool
	Memory        int
	Once          bool
	JSON          bool
	StopWhenEmpty bool
}

// ListenOptions 是 horizon:listen 本地开发监听命令传给 runtime 的参数。
type ListenOptions struct {
	// Environment 是被启动的 horizon master 使用的环境名。
	Environment string
	// Poll 是文件轮询间隔；Prismgo 使用标准库轮询，不强制依赖 Node/chokidar。
	Poll time.Duration
}

// ListenSummary 是 horizon:listen 退出时的本地开发监听摘要。
type ListenSummary struct {
	WatchPathCount int
	Starts         int
	Restarts       int
}

// MetricsCounters 保存 snapshot 输出摘要需要的聚合计数。
type MetricsCounters struct {
	Processed       int64
	Failed          int64
	Released        int64
	PoisonEnvelopes int64
}

const (
	// SnapshotStatusEnabled 表示能力已启用，本次 snapshot 正常执行，数量为真实采样或写入结果。
	SnapshotStatusEnabled = "enabled"
	// SnapshotStatusSkipped 表示能力被配置关闭，本次 snapshot 跳过且不覆盖旧数据。
	SnapshotStatusSkipped = "skipped"
)

// SnapshotSummary 是 horizon:snapshot 命令输出的安全摘要。
//
// 安全边界：该结构只包含数量和时间，不包含 queue payload、failed job envelope 或底层 driver 明细。
type SnapshotSummary struct {
	CapturedAt             time.Time
	QueueLengthStatus      string
	QueueLengthCount       int
	MetricsStatus          string
	BucketCount            int
	WaitsStatus            string
	BatchSummariesStatus   string
	FlushStatus            string
	FlushWindowCount       int
	FlushDetailCount       int
	FlushDiagnosticCount   int
	FlushBatchSummaryCount int
	FlushDropCount         int64
	FlushQuality           string
	FlushDegraded          bool
	FlushError             string
	Totals                 MetricsCounters
}

// PurgeSummary 是 horizon:purge 输出的 orphan process 清理摘要。
type PurgeSummary struct {
	OrphansDiscovered int
	TerminateRequests int
	OrphansForgotten  int
}

// QueueTarget 表示维护命令最终定位到的 connection+queue。
type QueueTarget struct {
	Connection string
	Queue      string
}

// runtimeCommand 是 Horizon 运行时命令的通用适配器。
//
// 需求背景：运行时命令都需要同一套 Runtime 解析、memory store 警告和错误边界；
// 用一个轻量包装减少重复，同时每个具体 command 保持在独立文件中。
type runtimeCommand struct {
	// name 是 console signature，包含命令名和参数声明。
	name string
	// description 是命令帮助文本。
	description string
	// run 是命令核心行为；Runtime 已由父包适配并解析完成。
	run func(console.CommandContext, Runtime) error
}

// NewRuntimeCommand 组装运行时命令的公共依赖解析流程。
//
// 参数说明：signature 是命令定义；description 是帮助说明；load 负责解析 Runtime；run 只处理具体命令行为。
func NewRuntimeCommand(signature string, description string, load RuntimeLoader, run func(console.CommandContext, Runtime) error) console.Command {
	return &runtimeCommand{name: signature, description: description, run: func(ctx console.CommandContext, _ Runtime) error {
		if load == nil {
			return ErrRuntimeNotConfigured
		}
		runtime, err := load(ctx.Context())
		if err != nil {
			return err
		}
		// RuntimeLoader 允许延迟接入宿主资源；返回 nil 时必须显式报配置错误，避免命令入口空指针。
		if runtime == nil {
			return ErrRuntimeNotConfigured
		}
		if runtime.UsesMemoryStore() {
			ctx.IO().Warn(MemoryStoreWarning)
		}
		return run(ctx, runtime)
	}}
}

// Definition 返回 console 命令定义。
func (c *runtimeCommand) Definition() *console.Definition {
	return console.MustDefinition(c.name, c.description)
}

// Run 执行运行时命令。
//
// 逻辑说明：真正的 Store 解析发生在 newRuntimeCommand 注入的包装函数中；nil receiver 返回明确配置错误。
func (c *runtimeCommand) Handle(ctx console.CommandContext) error {
	if c == nil || c.run == nil {
		return ErrRuntimeNotConfigured
	}
	return c.run(ctx, nil)
}

// formatTime 统一状态命令中的时间格式，零值输出为空字符串。
func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339)
}
