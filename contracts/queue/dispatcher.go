package queue

import "context"

// DispatcherServiceKey 是队列投递 contract 在容器中的稳定服务名。
const DispatcherServiceKey = "queue.dispatcher"

// Dispatcher 是队列投递能力的完整契约。
//
// 用途：提供任务投递和 worker 信号控制。
// 事件系统等跨包组件通过此接口依赖队列能力。
//
// 使用方式：
//
//	dispatcher.DispatchJob(ctx, job, nil)
type Dispatcher interface {
	// DispatchJob 将任务投递到队列。
	//
	// 参数 job 是待执行的任务。
	// 参数 options 是可选的连接、队列、延迟和重试策略。
	// 返回任务 ID。
	DispatchJob(ctx context.Context, job Job, options DispatchOptions) (string, error)

	// RequestRestart 写入 worker 重启信号。
	//
	// 长驻 worker 在当前任务执行完毕后检查该信号并退出，
	// 常用于队列重启部署。
	RequestRestart(ctx context.Context) error

	// Close 关闭所有连接。
	Close() error
}
