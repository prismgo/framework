//go:build linux

package process

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	// readProcessFile 抽象 /proc 文件读取入口。
	// 设计思路：生产路径继续直连 os.ReadFile；测试路径可替换为受控夹具，覆盖 CPU 采样与降级分支。
	readProcessFile = os.ReadFile
)

// defaultPlatformSampler 是 Linux 平台的默认进程采样器。
// 设计思路：仅使用标准库读取 /proc，满足 issue 27 不新增第三方依赖且采样成本有界的要求。
type defaultPlatformSampler struct{}

// procStat 保存 /proc/<pid>/stat 中本次实现需要的最小字段集合。
// utime/stime 用于计算短窗口 CPU%，rss 用于计算 RSS 字节数和内存百分比。
type procStat struct {
	utime uint64
	stime uint64
	rss   int64
}

// newPlatformSampler 返回当前平台实现，供公共 Observer 在不同 GOOS 下透明切换。
func newPlatformSampler() platformSampler {
	return defaultPlatformSampler{}
}

// observe 对当前分页 PID 做两段式短窗口采样。
// 参数说明：ctx 用于取消等待；pids 是待采样 PID；sampleWindow 是 CPU 两次 /proc 读数之间的有界窗口。
// 逻辑说明：先读取一次 stat 并补齐内存字段，再等待短窗口读取第二次 stat 计算 CPU%。CPU 计算使用
// /proc/stat 的系统总 tick 变化量推导单核百分比，避免把 Linux USER_HZ 静态写死为固定值。
func (defaultPlatformSampler) observe(ctx context.Context, pids []int, sampleWindow time.Duration) (map[int]Snapshot, error) {
	out := make(map[int]Snapshot, len(pids))
	first := make(map[int]procStat, len(pids))
	totalMemoryBytes, totalMemoryErr := readTotalMemoryBytes()
	beforeTotalTicks, beforeTotalErr := readTotalCPUTicks()
	for _, pid := range pids {
		snapshot := baseSnapshot(pid, sampleWindow)
		stat, err := readProcStat(pid)
		if err != nil {
			out[pid] = unavailableProcessSnapshot(snapshot, err)
			continue
		}
		first[pid] = stat
		applyMemory(&snapshot, stat.rss, totalMemoryBytes, totalMemoryErr)
		out[pid] = snapshot
	}
	if len(first) > 0 && sampleWindow > 0 {
		timer := time.NewTimer(sampleWindow)
		select {
		case <-ctx.Done():
			timer.Stop()
			return out, nil
		case <-timer.C:
		}
		afterTotalTicks, afterTotalErr := readTotalCPUTicks()
		for pid, before := range first {
			snapshot := out[pid]
			after, err := readProcStat(pid)
			if err != nil {
				snapshot.CPUPercent = unavailable(UnitPercent, "process exited before cpu sample completed")
				out[pid] = snapshot
				continue
			}
			if beforeTotalErr != nil || afterTotalErr != nil {
				snapshot.CPUPercent = unavailable(UnitPercent, "system cpu sample unavailable")
				out[pid] = snapshot
				continue
			}
			deltaTotalTicks := afterTotalTicks - beforeTotalTicks
			if deltaTotalTicks == 0 {
				snapshot.CPUPercent = unavailable(UnitPercent, "system cpu sample window unavailable")
				out[pid] = snapshot
				continue
			}
			deltaProcessTicks := (after.utime + after.stime) - (before.utime + before.stime)
			cpuPercent := float64(deltaProcessTicks) / float64(deltaTotalTicks) * float64(runtime.NumCPU()) * 100
			snapshot.CPUPercent = available(cpuPercent, UnitPercent)
			out[pid] = snapshot
		}
	}
	return out, nil
}

// selfSnapshot 返回当前 Go 进程可低成本上报给 heartbeat 的字段。
// CPU% 仍保持 unavailable，因为 heartbeat 不应该为瞬时 CPU 指标阻塞等待短窗口。
func (defaultPlatformSampler) selfSnapshot() Snapshot {
	snapshot := currentRuntimeSnapshot()
	stat, err := readProcStat(os.Getpid())
	if err != nil {
		snapshot.MemoryRSSBytes = unavailable(UnitBytes, "current process rss unavailable")
		snapshot.MemoryPercent = unavailable(UnitPercent, "current process memory percent unavailable")
		return snapshot
	}
	totalMemoryBytes, totalMemoryErr := readTotalMemoryBytes()
	applyMemory(&snapshot, stat.rss, totalMemoryBytes, totalMemoryErr)
	snapshot.CPUPercent = unavailable(UnitPercent, "cpu percent is only sampled on request for selected pids")
	return snapshot
}

// applyMemory 将 Linux /proc rss 页数转换为 read model 使用的 RSS 字节数和内存百分比。
// totalMemoryBytes 是本轮观测共享的系统总内存；读取失败时只降级 memory_percent，不隐藏仍可用的 RSS 字节数。
func applyMemory(snapshot *Snapshot, rssPages int64, totalMemoryBytes int64, totalMemoryErr error) {
	rssBytes := rssPages * int64(os.Getpagesize())
	snapshot.MemoryRSSBytes = available(rssBytes, UnitBytes)
	if totalMemoryErr != nil || totalMemoryBytes <= 0 {
		snapshot.MemoryPercent = unavailable(UnitPercent, "system total memory unavailable")
		return
	}
	snapshot.MemoryPercent = available(float64(rssBytes)/float64(totalMemoryBytes)*100, UnitPercent)
}

// unavailableProcessSnapshot 将单个 PID 读取失败转换为字段级 unavailable。
// 设计思路：列表 API 不因为进程退出或权限不足整体失败，而是在对应字段给出稳定 reason。
func unavailableProcessSnapshot(snapshot Snapshot, err error) Snapshot {
	reason := "process unavailable: " + err.Error()
	snapshot.CPUPercent = unavailable(UnitPercent, reason)
	snapshot.MemoryRSSBytes = unavailable(UnitBytes, reason)
	snapshot.MemoryPercent = unavailable(UnitPercent, reason)
	snapshot.GoroutineCount = unavailable(UnitCount, "goroutine count is only available for the current Go process")
	return snapshot
}

// readProcStat 读取 /proc/<pid>/stat 并解析 CPU tick 与 RSS 页数。
// 复杂逻辑说明：进程名字段可能包含空格和括号，所以先定位最后一个右括号，再解析其后的固定序号字段。
func readProcStat(pid int) (procStat, error) {
	if pid <= 0 {
		return procStat{}, fmt.Errorf("pid must be positive")
	}
	data, err := readProcessFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return procStat{}, err
	}
	line := string(data)
	end := strings.LastIndex(line, ")")
	if end < 0 || end+2 >= len(line) {
		return procStat{}, fmt.Errorf("invalid proc stat")
	}
	fields := strings.Fields(line[end+2:])
	if len(fields) < 22 {
		return procStat{}, fmt.Errorf("short proc stat")
	}
	utime, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return procStat{}, err
	}
	stime, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return procStat{}, err
	}
	rss, err := strconv.ParseInt(fields[21], 10, 64)
	if err != nil {
		return procStat{}, err
	}
	return procStat{utime: utime, stime: stime, rss: rss}, nil
}

// readTotalCPUTicks 读取 /proc/stat 首行聚合 CPU tick。
// 设计思路：用系统总 tick 变化量和 CPU 核心数推导单核 CPU 百分比，避免依赖固定 USER_HZ 常量。
func readTotalCPUTicks() (uint64, error) {
	data, err := readProcessFile("/proc/stat")
	if err != nil {
		return 0, err
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) == 0 {
		return 0, fmt.Errorf("cpu line missing")
	}
	fields := strings.Fields(lines[0])
	if len(fields) < 2 || fields[0] != "cpu" {
		return 0, fmt.Errorf("invalid cpu line")
	}
	var total uint64
	for _, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return 0, err
		}
		total += value
	}
	return total, nil
}

// readTotalMemoryBytes 读取 /proc/meminfo 的 MemTotal，用于把 RSS 换算为内存百分比。
// 读取失败会由上层降级 memory_percent，避免平台信息缺失影响 RSS 展示。
func readTotalMemoryBytes() (int64, error) {
	data, err := readProcessFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("invalid MemTotal")
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("MemTotal missing")
}
