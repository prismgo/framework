// Package routine 提供安全协程执行能力。
//
// routine 默认 recover panic，并通过全局 exception handler 上报 panic 和任务返回的 error。
// 调用方可通过链式 API 补充组件、名称、字段和回调：
//
//	routine.Task(ctx, task).
//	    Component("horizon").
//	    Name("supervisor.loop").
//	    Fields(fields).
//	    OnPanic(onPanic).
//	    Run()
package routine
