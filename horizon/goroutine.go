package horizon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prismgo/framework/routine"
)

const (
	goroutineRestartPanicThreshold = 10
	goroutineRestartShortDelay     = 10 * time.Millisecond
)

func startRecoveringGoroutine(ctx context.Context, subsystem string, fields map[string]any, run func()) {
	startRecoveringGoroutineWithPanicHandler(ctx, subsystem, fields, nil, run)
}

func startRecoveringGoroutineWithPanicHandler(ctx context.Context, subsystem string, fields map[string]any, onPanic func(error), run func()) {
	routine.Task(ctx, func(context.Context) error {
		run()
		return nil
	}).
		Component("horizon").
		Name(subsystem).
		Fields(horizonRoutineFields(subsystem, fields)).
		OnPanic(onPanic).
		Go()
}

// startRestartingTrackedGoroutineWithPanicHandler 启动一个由调用方 WaitGroup 追踪的长期 loop。
//
// 设计边界：只用于 collector/flusher 这类主循环；panic 被记录后自动重新进入 run，直到 run 正常返回
// 或 context 取消。连续 panic 达到阈值后进入阶梯冷却，避免异常持续存在时形成热重启风暴。
func startRestartingTrackedGoroutineWithPanicHandler(ctx context.Context, wg *sync.WaitGroup, subsystem string, fields map[string]any, onPanic func(error), run func()) {
	startRestartingTrackedGoroutineWithPolicy(ctx, wg, subsystem, fields, onPanic, run, defaultGoroutineRestartPolicy())
}

// startRestartingTrackedGoroutineWithPolicy 是 restart loop 的测试入口。
//
// 参数说明：policy 控制连续 panic 后的重启等待；生产路径固定使用 defaultGoroutineRestartPolicy，
// 不暴露用户配置，避免把内部自恢复保护扩大为配置面。
func startRestartingTrackedGoroutineWithPolicy(ctx context.Context, wg *sync.WaitGroup, subsystem string, fields map[string]any, onPanic func(error), run func(), policy goroutineRestartPolicy) {
	policy = policy.normalized()
	routine.Task(ctx, func(context.Context) error {
		defer wg.Done()
		consecutivePanics := 0
		for {
			panicked := runWithPanicHandler(onPanic, run)
			if !panicked {
				return nil
			}
			consecutivePanics++
			delay, _ := policy.delayAfterPanic(consecutivePanics)
			if waitRestartDelay(ctx, delay) {
				return nil
			}
		}
		return nil
	}).
		Component("horizon").
		Name(subsystem).
		Fields(horizonRoutineFields(subsystem, fields)).
		Go()
}

// goroutineRestartPolicy 描述 collector/flusher 主 loop panic 后的固定重启策略。
//
// 逻辑说明：第 1-9 次连续 panic 保持短间隔快速恢复；第 10 次开始进入 1/5/10 分钟阶梯冷却，
// 之后持续 panic 时固定 10 分钟重试一次。
type goroutineRestartPolicy struct {
	panicThreshold int
	shortDelay     time.Duration
	coolingDelays  []time.Duration
}

func defaultGoroutineRestartPolicy() goroutineRestartPolicy {
	return goroutineRestartPolicy{
		panicThreshold: goroutineRestartPanicThreshold,
		shortDelay:     goroutineRestartShortDelay,
		coolingDelays:  []time.Duration{time.Minute, 5 * time.Minute, 10 * time.Minute},
	}
}

func (p goroutineRestartPolicy) normalized() goroutineRestartPolicy {
	defaults := defaultGoroutineRestartPolicy()
	if p.panicThreshold <= 0 {
		p.panicThreshold = defaults.panicThreshold
	}
	if p.shortDelay <= 0 {
		p.shortDelay = defaults.shortDelay
	}
	if len(p.coolingDelays) == 0 {
		p.coolingDelays = defaults.coolingDelays
	}
	return p
}

// delayAfterPanic 返回指定连续 panic 次数后的等待时间，以及该等待是否属于长冷却。
func (p goroutineRestartPolicy) delayAfterPanic(consecutivePanics int) (time.Duration, bool) {
	p = p.normalized()
	if consecutivePanics < p.panicThreshold {
		return p.shortDelay, false
	}
	coolingIndex := consecutivePanics - p.panicThreshold
	if coolingIndex >= len(p.coolingDelays) {
		coolingIndex = len(p.coolingDelays) - 1
	}
	return p.coolingDelays[coolingIndex], true
}

// waitRestartDelay 等待下一轮 restart，并让 Stop/context cancel 优先打断分钟级冷却。
func waitRestartDelay(ctx context.Context, delay time.Duration) (cancelled bool) {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}

// runWithPanicHandler 执行单轮 loop，并把 panic 转换成可记录的 error。
func runWithPanicHandler(onPanic func(error), run func()) (panicked bool) {
	defer func() {
		if rec := recover(); rec != nil {
			panicked = true
			if onPanic != nil {
				onPanic(panicAsError(rec))
			}
		}
	}()
	run()
	return false
}

// panicAsError 保留 panic(error) 的原始错误；其他 panic 值转成稳定文本。
func panicAsError(rec any) error {
	if err, ok := rec.(error); ok {
		return err
	}
	return fmt.Errorf("%v", rec)
}

func horizonRoutineFields(subsystem string, fields map[string]any) map[string]any {
	out := make(map[string]any, len(fields)+2)
	for key, value := range fields {
		out[key] = value
	}
	out["subsystem"] = subsystem
	out["host"] = hostname()
	return out
}
