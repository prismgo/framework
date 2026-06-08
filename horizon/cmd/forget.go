package cmd

import (
	"fmt"
	"strings"

	"github.com/prismgo/framework/console"
)

// NewForgetCommand 创建 horizon:forget 命令。
func NewForgetCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:forget {id?} {--all}", "Forget failed queue jobs", load, runForgetCommand)
}

// runForgetCommand 删除一条 failed job 记录。
//
// 安全边界：命令输出只包含 failed record id 和状态，不打印 Envelope、payload、raw error object 或完整错误堆栈。
func runForgetCommand(ctx console.CommandContext, runtime Runtime) error {
	if boolCommandOption(ctx, "all") {
		if err := runtime.ForgetAllFailedJobs(ctx.Context()); err != nil {
			return err
		}
		ctx.IO().Success("All failed jobs forgotten.")
		return nil
	}
	id := strings.TrimSpace(ctx.Input().Argument("id"))
	if id == "" {
		return fmt.Errorf("horizon: failed job id is required")
	}
	if err := runtime.ForgetFailedJob(ctx.Context(), id); err != nil {
		return err
	}
	ctx.IO().Success("Failed job forgotten: " + id)
	return nil
}
