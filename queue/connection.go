package queue

import (
	"context"

	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
)

// FailedStore 保存最终失败的任务，支持 queue:failed / queue:retry / queue:forget。
//
// 设计说明：失败任务记录是 queue 实现层的可观测数据，不是跨包解耦所需的 contracts 抽象；
// 因此 FailedStore 留在 queue 包，避免 contracts 反向承载具体事件/归档 payload。
type FailedStore interface {
	// Record 记录一条失败任务。
	//
	// 参数 failed 是 worker 最终失败后生成的归档记录，调用方应完整保存 Envelope 便于重试。
	Record(ctx context.Context, failed payload.FailedJob) error

	// Page 分页返回失败任务记录，按 FailedAt 升序排列。
	//
	// 设计边界：失败任务存储可能保存大量历史记录，契约不再提供全量 All 入口；
	// 命令、Dashboard 和自定义实现都应只读取当前页，避免一次请求拉取完整失败归档。
	Page(ctx context.Context, page state.PageRequest) (state.PageEnvelope[payload.FailedJob], error)

	// Find 按 ID 查找单条失败记录。
	//
	// 参数 id 是失败任务唯一 ID。
	Find(ctx context.Context, id string) (*payload.FailedJob, error)

	// Forget 按 ID 删除单条失败记录。
	//
	// 参数 id 是失败任务唯一 ID。
	Forget(ctx context.Context, id string) error

	// Flush 清空所有失败记录。
	Flush(ctx context.Context) error
}
