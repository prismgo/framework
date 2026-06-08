package horizon

import (
	"reflect"
	"testing"
	"time"
)

func TestScalerBuildsFalseAndSimpleProcessPools(t *testing.T) {
	// 需求背景：historical scenario 07 要求 balance=false 保留单个 queue-priority pool，
	// balance=simple 则为每个 queue 建立独立 pool，并保持固定分配而不是自动均衡。
	workloads := []QueueWorkload{{Queue: "high", Ready: 3}, {Queue: "default", Ready: 2}}
	falseSupervisor := SupervisorConfig{
		Name: "fixed", Queues: []string{"high", "default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 4,
	}
	simpleSupervisor := SupervisorConfig{
		Name: "simple", Queues: []string{"high", "default"}, Balance: BalanceSimple, MinProcesses: 1, MaxProcesses: 5,
	}

	falsePools := CalculateProcessPools(falseSupervisor, workloads, ScaleState{}, time.Now())
	if len(falsePools) != 1 || falsePools[0].TargetWorkers != 4 || !reflect.DeepEqual(falsePools[0].Queues, []string{"high", "default"}) {
		t.Fatalf("balance=false should keep one priority pool capped by max processes, got %#v", falsePools)
	}

	simplePools := CalculateProcessPools(simpleSupervisor, workloads, ScaleState{}, time.Now())
	if len(simplePools) != 2 {
		t.Fatalf("balance=simple should create per-queue pools, got %#v", simplePools)
	}
	for _, pool := range simplePools {
		if pool.TargetWorkers != 2 || len(pool.Queues) != 1 {
			t.Fatalf("simple pool should get fixed max/queue_count target and one queue, got %#v", pool)
		}
	}
}

func TestAutoScalerUsesMetricsRuntimeCooldownAndMaxShift(t *testing.T) {
	// 逻辑说明：auto=time 使用 Horizon metrics 的 runtime 形成 workload，不回退到自定义总积压公式；
	// cooldown 未到时保持当前目标，cooldown 到期后再按 balance_max_shift 限制单次移动幅度。
	supervisor := SupervisorConfig{
		Name: "auto", Queues: []string{"slow", "fast"}, Balance: BalanceAuto, MinProcesses: 1, MaxProcesses: 6,
		AutoScalingStrategy: AutoScalingStrategyTime, BalanceMaxShift: 2, BalanceCooldown: 3,
	}
	workloads := []QueueWorkload{
		{Queue: "slow", Ready: 2, Runtime: 5 * time.Second},
		{Queue: "fast", Ready: 8, Runtime: 250 * time.Millisecond},
	}
	state := ScaleState{
		CurrentWorkers: map[string]int{"slow": 1, "fast": 3},
		LastScaledAt:   time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC),
	}

	cooldownPools := CalculateProcessPools(supervisor, workloads, state, state.LastScaledAt.Add(time.Second))
	if targetFor(cooldownPools, "slow") != 1 || targetFor(cooldownPools, "fast") != 3 {
		t.Fatalf("cooldown should keep current targets, got %#v", cooldownPools)
	}

	scaledPools := CalculateProcessPools(supervisor, workloads, state, state.LastScaledAt.Add(4*time.Second))
	if targetFor(scaledPools, "slow") != 3 {
		t.Fatalf("slow queue should move toward time-weighted target by max shift, got %#v", scaledPools)
	}
	if targetFor(scaledPools, "fast") < 1 {
		t.Fatalf("fast queue should keep min process floor, got %#v", scaledPools)
	}
}

func TestRuntimeForQueueUsesMetricsSnapshot(t *testing.T) {
	// 需求背景：Laravel auto=time 依赖每个 queue 的运行耗时；Prismgo 应复用 Horizon metrics bucket，
	// 而不是从 ready job 数量派生一个自定义耗时。
	snapshot := MetricsSnapshot{Buckets: []MetricsBucketSnapshot{
		{Connection: "redis", Queue: "default", ProcessedCount: 2, ProcessedRuntimeTotalMS: 1500},
	}}

	if got := RuntimeForQueue(snapshot, "redis", "default"); got != 750*time.Millisecond {
		t.Fatalf("runtime from metrics = %s", got)
	}
	if got := RuntimeForQueue(snapshot, "redis", "missing"); got != 0 {
		t.Fatalf("missing metrics runtime = %s", got)
	}
}

func TestAutoScalerCoversSizeStrategyAndZeroRuntimeFallback(t *testing.T) {
	// 测试目的：覆盖 auto=size 和 total_time==0 分支，固定 Laravel 对齐的 ceil 与 min/max 约束。
	sizeSupervisor := SupervisorConfig{
		Name: "auto-size", Queues: []string{"a", "b"}, Balance: BalanceAuto, MinProcesses: 1, MaxProcesses: 5,
		AutoScalingStrategy: AutoScalingStrategySize, BalanceMaxShift: 10,
	}
	sizePools := CalculateProcessPools(sizeSupervisor, []QueueWorkload{{Queue: "a", Ready: 1}, {Queue: "b", Ready: 4}}, ScaleState{}, time.Now())
	if targetFor(sizePools, "a") != 1 || targetFor(sizePools, "b") != 4 {
		t.Fatalf("size strategy targets = %#v", sizePools)
	}

	timeSupervisor := sizeSupervisor
	timeSupervisor.AutoScalingStrategy = AutoScalingStrategyTime
	zeroRuntimePools := CalculateProcessPools(timeSupervisor, []QueueWorkload{{Queue: "a", Ready: 1}, {Queue: "b", Ready: 0}}, ScaleState{}, time.Now())
	if targetFor(zeroRuntimePools, "a") <= targetFor(zeroRuntimePools, "b") {
		t.Fatalf("zero runtime backlog queue should receive larger target, got %#v", zeroRuntimePools)
	}

	shrinkSupervisor := sizeSupervisor
	shrinkSupervisor.BalanceMaxShift = 2
	shrinkPools := CalculateProcessPools(shrinkSupervisor, []QueueWorkload{{Queue: "a", Ready: 0}, {Queue: "b", Ready: 1}}, ScaleState{CurrentWorkers: map[string]int{"a": 5, "b": 1}}, time.Now())
	if targetFor(shrinkPools, "a") != 3 {
		t.Fatalf("shrink should be limited by max shift, got %#v", shrinkPools)
	}
}

func TestScalerCoversBoundaryBranches(t *testing.T) {
	// 测试目的：固定 historical scenario 07 autoscaler 的空队列、隐式 now 冷却和 runtime bucket 过滤边界。
	if pools := CalculateProcessPools(SupervisorConfig{Name: "empty", Balance: BalanceAuto}, nil, ScaleState{}, time.Now()); len(pools) != 0 {
		t.Fatalf("empty queues should not create pools: %#v", pools)
	}
	supervisor := SupervisorConfig{
		Name: "auto", Queues: []string{"a", "b", "c"}, Balance: BalanceAuto, MinProcesses: 2, MaxProcesses: 4,
		AutoScalingStrategy: AutoScalingStrategySize, BalanceCooldown: 30,
	}
	state := ScaleState{
		CurrentWorkers: map[string]int{"a": 3},
		LastScaledAt:   time.Now().Add(-time.Second),
	}
	pools := CalculateProcessPools(supervisor, []QueueWorkload{{Queue: "a", Ready: 10}}, state, time.Time{})
	if targetFor(pools, "a") != 3 || targetFor(pools, "b") != 2 || targetFor(pools, "c") != 2 {
		t.Fatalf("implicit now cooldown should keep current/min targets, got %#v", pools)
	}
	snapshot := MetricsSnapshot{Buckets: []MetricsBucketSnapshot{
		{Connection: "redis", Queue: "default", ProcessedCount: 0, ProcessedRuntimeTotalMS: 9000},
		{Connection: "redis", Queue: "default", ProcessedCount: 2, ProcessedRuntimeTotalMS: 1500},
	}}
	if got := RuntimeForQueue(snapshot, "redis", "default"); got != 750*time.Millisecond {
		t.Fatalf("runtime should skip zero processed bucket, got %s", got)
	}
}

func targetFor(pools []ProcessPoolState, queue string) int {
	for _, pool := range pools {
		if pool.Queue == queue {
			return pool.TargetWorkers
		}
	}
	return 0
}
