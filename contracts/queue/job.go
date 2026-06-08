package queue

import "context"

// Job 是可投递到队列的任务契约。
//
// 用途：所有队列任务必须实现此接口。队列 worker 通过 Handle 方法执行任务逻辑。
//
// 使用方式：
//
//	type ExportReportJob struct { ReportID uint }
//	func (j *ExportReportJob) Handle(ctx context.Context) error {
//	    return exportService.Export(ctx, j.ReportID)
//	}
//
//	queue.Dispatch(ctx, &ExportReportJob{ReportID: 123})
type Job interface {
	// Handle 执行任务的实际逻辑。
	//
	// 参数 ctx 是 worker 提供的运行上下文，包含超时和取消信号。
	// 返回非 nil error 时 worker 根据重试策略决定是否重试或标记失败。
	Handle(ctx context.Context) error
}
