package horizon

import (
	"math"
	"sort"
	"time"
)

// QueueWorkload 表示 autoscaler 针对单个 queue 看到的积压和运行耗时事实。
//
// 需求背景：Laravel Horizon auto balance 按 queue pool 计算 workload；Prismgo 将 ready job 数量和
// Horizon metrics 提供的 runtime 显式传入，避免 scaler 自己访问 queue backend 或发明自定义公式。
type QueueWorkload struct {
	// Queue 是当前 process pool 绑定的队列名。
	Queue string
	// Ready 是 queue backend 返回的 ready job 数量。
	Ready int64
	// Runtime 是 Horizon metrics 对该 queue 的平均任务运行耗时。
	Runtime time.Duration
}

// ProcessPoolState 表示 supervisor 下一个 queue process pool 的当前和目标 worker 数。
//
// 使用方式：balance=false 只有一个 pool 且 Queues 保存完整优先级队列列表；simple/auto 每个 queue 一个 pool。
type ProcessPoolState struct {
	Name           string
	Queue          string
	Queues         []string
	CurrentWorkers int
	TargetWorkers  int
}

// ScaleState 保存上一次扩缩容决策需要的少量状态。
//
// 参数说明：CurrentWorkers 使用 queue 名作为 key；balance=false 使用空字符串 key。LastScaledAt 只约束 auto balance。
type ScaleState struct {
	CurrentWorkers map[string]int
	LastScaledAt   time.Time
}

// CalculateProcessPools 按 Laravel Horizon 的 false/simple/auto 差异计算 process pool 目标。
//
// 逻辑说明：false 保留完整队列列表的单 pool；simple 做固定 per-queue 分配；auto 根据 size/time workload
// 比例使用 ceil 分配 max_processes，并受 min/max、balance_max_shift、balance_cooldown 约束。
func CalculateProcessPools(supervisor SupervisorConfig, workloads []QueueWorkload, state ScaleState, now time.Time) []ProcessPoolState {
	queues := append([]string(nil), supervisor.Queues...)
	if len(queues) == 0 {
		return nil
	}
	switch supervisor.Balance {
	case BalanceSimple:
		return simpleProcessPools(supervisor, queues, state)
	case BalanceAuto:
		return autoProcessPools(supervisor, queues, workloadsByQueue(workloads), state, now)
	default:
		return falseProcessPool(supervisor, queues, workloads)
	}
}

func falseProcessPool(supervisor SupervisorConfig, queues []string, workloads []QueueWorkload) []ProcessPoolState {
	var ready int64
	for _, workload := range workloads {
		ready += workload.Ready
	}
	target := clampInt(int(ready), supervisor.MinProcesses, supervisor.MaxProcesses)
	return []ProcessPoolState{{
		Name:           supervisor.Name + ":all",
		Queues:         queues,
		CurrentWorkers: currentWorkers(ScaleState{}, ""),
		TargetWorkers:  target,
	}}
}

func simpleProcessPools(supervisor SupervisorConfig, queues []string, state ScaleState) []ProcessPoolState {
	target := supervisor.MaxProcesses / len(queues)
	target = clampInt(target, supervisor.MinProcesses, supervisor.MaxProcesses)
	pools := make([]ProcessPoolState, 0, len(queues))
	for _, queueName := range queues {
		pools = append(pools, ProcessPoolState{
			Name:           supervisor.Name + ":" + queueName,
			Queue:          queueName,
			Queues:         []string{queueName},
			CurrentWorkers: currentWorkers(state, queueName),
			TargetWorkers:  target,
		})
	}
	return pools
}

func autoProcessPools(supervisor SupervisorConfig, queues []string, workloads map[string]QueueWorkload, state ScaleState, now time.Time) []ProcessPoolState {
	if inCooldown(supervisor, state, now) {
		return currentProcessPools(supervisor, queues, state)
	}
	rawTargets := autoRawTargets(supervisor, queues, workloads)
	pools := make([]ProcessPoolState, 0, len(queues))
	for _, queueName := range queues {
		current := currentWorkers(state, queueName)
		if current == 0 {
			current = supervisor.MinProcesses
		}
		target := rawTargets[queueName]
		target = clampShift(current, target, supervisor.BalanceMaxShift)
		pools = append(pools, ProcessPoolState{
			Name:           supervisor.Name + ":" + queueName,
			Queue:          queueName,
			Queues:         []string{queueName},
			CurrentWorkers: current,
			TargetWorkers:  target,
		})
	}
	return pools
}

func currentProcessPools(supervisor SupervisorConfig, queues []string, state ScaleState) []ProcessPoolState {
	pools := make([]ProcessPoolState, 0, len(queues))
	for _, queueName := range queues {
		current := currentWorkers(state, queueName)
		if current == 0 {
			current = supervisor.MinProcesses
		}
		pools = append(pools, ProcessPoolState{
			Name:           supervisor.Name + ":" + queueName,
			Queue:          queueName,
			Queues:         []string{queueName},
			CurrentWorkers: current,
			TargetWorkers:  current,
		})
	}
	return pools
}

func autoRawTargets(supervisor SupervisorConfig, queues []string, workloads map[string]QueueWorkload) map[string]int {
	weights := make(map[string]float64, len(queues))
	var total float64
	for _, queueName := range queues {
		workload := workloads[queueName]
		weight := float64(workload.Ready)
		if supervisor.AutoScalingStrategy != AutoScalingStrategySize {
			weight = float64(workload.Ready) * float64(workload.Runtime)
		}
		weights[queueName] = weight
		total += weight
	}
	targets := make(map[string]int, len(queues))
	for _, queueName := range queues {
		target := supervisor.MinProcesses
		if total > 0 {
			target = int(math.Ceil(weights[queueName] / total * float64(supervisor.MaxProcesses)))
		} else if workloads[queueName].Ready > 0 {
			target = supervisor.MaxProcesses
		}
		targets[queueName] = clampPoolTarget(target, supervisor, len(queues))
	}
	return targets
}

func clampPoolTarget(value int, supervisor SupervisorConfig, queueCount int) int {
	maxForPool := supervisor.MaxProcesses - supervisor.MinProcesses*(queueCount-1)
	if maxForPool < supervisor.MinProcesses {
		maxForPool = supervisor.MinProcesses
	}
	return clampInt(value, supervisor.MinProcesses, maxForPool)
}

func inCooldown(supervisor SupervisorConfig, state ScaleState, now time.Time) bool {
	if state.LastScaledAt.IsZero() || supervisor.BalanceCooldown <= 0 {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return now.Before(state.LastScaledAt.Add(time.Duration(supervisor.BalanceCooldown) * time.Second))
}

func workloadsByQueue(workloads []QueueWorkload) map[string]QueueWorkload {
	out := make(map[string]QueueWorkload, len(workloads))
	for _, workload := range workloads {
		out[workload.Queue] = workload
	}
	return out
}

func currentWorkers(state ScaleState, queueName string) int {
	if state.CurrentWorkers == nil {
		return 0
	}
	return state.CurrentWorkers[queueName]
}

func clampShift(current int, target int, maxShift int) int {
	if maxShift <= 0 {
		maxShift = 1
	}
	if target > current+maxShift {
		return current + maxShift
	}
	if target < current-maxShift {
		return current - maxShift
	}
	return target
}

func clampInt(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if maxValue > 0 && value > maxValue {
		return maxValue
	}
	return value
}

// RuntimeForQueue 从 Horizon metrics snapshot 读取指定 queue 的平均 processed runtime。
//
// 设计原因：auto_scaling_strategy=time 必须基于 metrics 体系提供的 runtime，缺失数据时返回 0，
// 由 autoscaler 按 Laravel 的 total_time==0 分支处理，而不是回退为积压数量的变体公式。
func RuntimeForQueue(snapshot MetricsSnapshot, connection string, queueName string) time.Duration {
	buckets := append([]MetricsBucketSnapshot(nil), snapshot.Buckets...)
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Connection == buckets[j].Connection {
			return buckets[i].Queue < buckets[j].Queue
		}
		return buckets[i].Connection < buckets[j].Connection
	})
	for _, bucket := range buckets {
		if bucket.Connection == connection && bucket.Queue == queueName && bucket.ProcessedCount > 0 {
			return time.Duration(bucket.ProcessedRuntimeTotalMS/bucket.ProcessedCount) * time.Millisecond
		}
	}
	return 0
}
