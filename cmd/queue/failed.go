package queue

import (
	"time"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/queue/state"
)

// FailedCommand 列出失败队列任务，等价于 Laravel 的 queue:failed。
type FailedCommand struct{}

// NewFailedCommand 构造 queue:failed 命令。
func NewFailedCommand() *FailedCommand { return &FailedCommand{} }

// Definition 返回失败任务列表命令定义。
func (c *FailedCommand) Definition() *console.Definition {
	return console.MustDefinition("queue:failed {--page=1} {--page-size=100}", "列出失败队列任务")
}

// Run 从当前队列失败任务存储读取记录并以表格输出。
func (c *FailedCommand) Handle(ctx console.CommandContext) error {
	pageNumber := intOption(ctx, "page", 1)
	if pageNumber <= 0 {
		pageNumber = 1
	}
	pageSize := intOption(ctx, "page-size", 100)
	if pageSize <= 0 {
		pageSize = 100
	}
	manager, err := resolveManager()
	if err != nil {
		return err
	}
	page, err := manager.Failed().Page(ctx.Context(), state.PageRequest{Page: pageNumber, PageSize: pageSize})
	if err != nil {
		return err
	}
	items := page.Items
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.ID, item.Connection, item.Queue, item.JobName, item.Error, item.FailedAt.Format(time.RFC3339)})
	}
	return ctx.IO().Table([]string{"ID", "Connection", "Queue", "Job", "Error", "FailedAt"}, rows)
}
