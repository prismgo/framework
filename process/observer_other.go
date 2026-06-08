//go:build !linux

package process

import (
	"context"
	"runtime"
	"time"
)

// defaultPlatformSampler 是非 Linux 平台的降级采样器。
// 需求背景：issue 27 要求平台能力不足时返回稳定 unsupported/unavailable，而不是伪造数值或引入未确认依赖。
type defaultPlatformSampler struct{}

// newPlatformSampler 返回当前平台的采样器实现。
// 非 Linux 平台暂不做 OS 进程资源读取，只保留字段级能力状态。
func newPlatformSampler() platformSampler {
	return defaultPlatformSampler{}
}

// observe 为非 Linux 平台返回每个 PID 的 unsupported 字段形状。
// 参数说明：sampleWindow 仍写入快照，便于 API 形状在所有平台保持一致。
func (defaultPlatformSampler) observe(_ context.Context, pids []int, sampleWindow time.Duration) (map[int]Snapshot, error) {
	out := make(map[int]Snapshot, len(pids))
	for _, pid := range pids {
		snapshot := baseSnapshot(pid, sampleWindow)
		reason := "process resource sampling is unsupported on " + runtime.GOOS
		snapshot.CPUPercent = unsupported(UnitPercent, reason)
		snapshot.MemoryRSSBytes = unsupported(UnitBytes, reason)
		snapshot.MemoryPercent = unsupported(UnitPercent, reason)
		snapshot.GoroutineCount = unsupported(UnitCount, "external goroutine count is unavailable on "+runtime.GOOS)
		out[pid] = snapshot
	}
	return out, nil
}

// selfSnapshot 返回当前 Go 进程的低成本 runtime 字段，并对 OS 资源字段显式标记 unsupported。
// 设计思路：heartbeat 仍可获得 goroutine 数；RSS、内存百分比和请求时 CPU 采样等待后续平台实现。
func (defaultPlatformSampler) selfSnapshot() Snapshot {
	snapshot := currentRuntimeSnapshot()
	reason := "rss sampling is unsupported on " + runtime.GOOS
	snapshot.MemoryRSSBytes = unsupported(UnitBytes, reason)
	snapshot.MemoryPercent = unsupported(UnitPercent, reason)
	snapshot.CPUPercent = unsupported(UnitPercent, "request-time cpu sampling is unsupported on "+runtime.GOOS)
	return snapshot
}
