package queue

import (
	"context"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/facade"
	"github.com/prismgo/framework/queue/payload"
)

const serviceKey = "queue.manager"

var defaultRegistry = NewRegistry()

// DefaultRegistry 返回全局任务注册表。
func DefaultRegistry() *Registry {
	return defaultRegistry
}

// Resolve 从当前 Application 容器解析队列 Manager。
func Resolve() *Manager {
	return facade.Resolve[*Manager](serviceKey)
}

// Extend 注册包级自定义 connector。
//
// 参数 name 是 connection 配置中的 driver 名称；connector 负责按连接配置创建底层队列。
// 需求背景：业务包通常在 init() 中完成 driver 扩展，此时当前 Application 容器可能尚未
// 绑定 queue manager，因此这里只写入包级 registry，不触发 Resolve()。空名称或 nil
// connector 会被忽略；同名注册会覆盖先前 connector。
func Extend(name string, connector queuecontract.Connector) {
	registerConnector(name, connector)
}

// UseMiddleware 为全局 Manager 注册任务 middleware。
func UseMiddleware(middleware ...Middleware) {
	Resolve().UseMiddleware(middleware...)
}

// Dispatch 通过全局 Manager 投递任务。
func Dispatch(ctx context.Context, job Job, options ...DispatchOption) (string, error) {
	return Resolve().Dispatch(ctx, job, options...)
}

// Batch 创建使用默认 Manager 的批次构建器。
func Batch(jobs ...Job) *BatchBuilder {
	return Resolve().Batch(jobs...)
}

// Failed 返回全局 Manager 绑定的失败任务存储。
func Failed() FailedStore {
	return Resolve().Failed()
}

// RequestRestart 通过全局 Manager 写入 worker 重启信号。
func RequestRestart(ctx context.Context) error {
	return Resolve().RequestRestart(ctx)
}

// Close 关闭全局 Manager 持有的队列连接。
func Close() error {
	return Resolve().Close()
}

// Later 延迟投递任务。
func Later(ctx context.Context, delaySeconds int, job Job, options ...DispatchOption) (string, error) {
	options = append(options, DelaySeconds(delaySeconds))
	return Dispatch(ctx, job, options...)
}

// GetBatchStatus 读取全局 Manager 中的批次状态。
func GetBatchStatus(ctx context.Context, id string) (payload.BatchStatus, error) {
	return Resolve().BatchStatus(ctx, id)
}

// CancelBatch 通过全局 Manager 取消批次。
func CancelBatch(ctx context.Context, id string) error {
	return Resolve().CancelBatch(ctx, id)
}

// MarkBatchJob 通过全局 Manager 标记批次内任务执行结果。
func MarkBatchJob(ctx context.Context, id string, success bool) error {
	return Resolve().MarkBatchJob(ctx, id, success)
}

// DelaySeconds 以秒为单位设置延迟，便于命令和配置使用。
func DelaySeconds(seconds int) DispatchOption {
	return func(o *DispatchOptions) {
		if seconds > 0 {
			o.Delay = time.Duration(seconds) * time.Second
		}
	}
}
