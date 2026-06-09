package horizon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	goprocess "github.com/prismgo/framework/process"
	"github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/queue/payload"
	prismredis "github.com/prismgo/framework/redis"
	"github.com/redis/go-redis/v9"

	"github.com/prismgo/framework/console"
	horizoncmd "github.com/prismgo/framework/horizon/cmd"
	_ "prismgo/config"
)

func TestStoreFactoryAndManagerErrorBranches(t *testing.T) {
	// 需求背景：运行时命令必须通过 Manager/StoreResolver 获取 Store。该测试覆盖 memory 复用、未知 store
	// 配置错误和缺失 resolver 的错误边界，避免命令层退化为 no-op 或静默 fallback。
	ctx := context.Background()
	factory := &DefaultStoreFactory{}
	store, err := factory.ResolveStore(ctx, Config{Store: "memory", Prefix: "factory", HeartbeatTTL: time.Second})
	if err != nil || store == nil {
		t.Fatalf("memory store resolve failed: %v", err)
	}
	same, err := factory.ResolveStore(ctx, Config{Store: "memory", Prefix: "factory", HeartbeatTTL: time.Second})
	if err != nil || same != store {
		t.Fatalf("memory store should be reused, got %v %v", same, err)
	}
	defaultKeyStore := factory.memoryStore(Config{Store: "memory", Prefix: " : ", HeartbeatTTL: time.Second})
	if defaultKeyStore == nil {
		t.Fatal("empty memory prefix should fall back to default key")
	}
	if _, err := factory.ResolveStore(ctx, Config{Store: "missing"}); err == nil || !strings.Contains(err.Error(), "unknown store") {
		t.Fatalf("expected unknown store error, got %v", err)
	}
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if _, err := manager.ResolveStore(ctx); !errors.Is(err, ErrStoreNotConfigured) {
		t.Fatalf("expected store resolver error, got %v", err)
	}
	if (*Manager)(nil).QueueManager() != nil || (*Manager)(nil).WorkerRunner() != nil || (*Manager)(nil).EventDispatcher() != nil || (*Manager)(nil).Collector() != nil || (*Manager)(nil).Flusher() != nil || (*Manager)(nil).StoreFactory() != nil {
		t.Fatal("nil manager accessors should return nil dependencies")
	}
	if _, ok := (*Manager)(nil).ProcessRunner().(OSProcessRunner); !ok {
		t.Fatal("nil manager should return OS process runner fallback")
	}
	if _, ok := (*Manager)(nil).ProcessInspector().(OSProcessInspector); !ok {
		t.Fatal("nil manager should return OS process inspector fallback")
	}
	if _, ok := (*Manager)(nil).ControlNotifier().(NoopControlNotifier); !ok {
		t.Fatal("nil manager should return noop control notifier fallback")
	}
}

func TestOSProcessInspectorWindowsScanFallback(t *testing.T) {
	// 测试目的：Windows 开发环境不能完整表达 POSIX process scan，生产适配器应保持可运行并返回空结果。
	if runtime.GOOS != "windows" {
		t.Skip("windows fallback only")
	}
	processes, err := OSProcessInspector{}.HorizonProcesses(context.Background())
	if err != nil {
		t.Fatalf("windows process scan fallback: %v", err)
	}
	if len(processes) != 0 {
		t.Fatalf("windows fallback should not invent process rows: %#v", processes)
	}
	if err := (OSProcessInspector{}).Terminate(context.Background(), 0, false); err == nil {
		t.Fatal("windows fallback should report unsupported/invalid interrupt target")
	}
}

// TestOSProcessInspectorTerminateUnix 覆盖 Unix 路径的 Terminate 方法，包括
// SIGTERM（force=false）和 SIGKILL（force=true）两个分支。
//
// 需求背景：Terminate 是生产环境中 horizon:terminate 命令终止工作时进程的核心方法；
// 此前仅有 Windows fallback 的间接覆盖，Unix 路径始终为 0%。
func TestOSProcessInspectorTerminateUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix terminate path only")
	}
	ctx := context.Background()
	inspector := OSProcessInspector{}

	// 分支一：无效 pid 应返回错误。
	if err := inspector.Terminate(ctx, 0, false); err == nil {
		t.Fatal("expected error for pid 0 on unix")
	}

	// 分支二：通过 SIGTERM 终止子进程；Wait 确认进程因信号退出。
	cmd := testSleepProcess(t)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep process: %v", err)
	}
	pid := cmd.Process.Pid
	if pid <= 0 {
		t.Fatal("expected positive pid")
	}
	if err := inspector.Terminate(ctx, pid, false); err != nil {
		t.Fatalf("terminate pid=%d with SIGTERM: %v", pid, err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatalf("expected process pid=%d to be terminated by signal, got clean exit", pid)
	}

	// 分支三：通过 Kill(SIGKILL) 强制终止子进程。
	cmd2 := testSleepProcess(t)
	if err := cmd2.Start(); err != nil {
		t.Fatalf("start second sleep process: %v", err)
	}
	pid2 := cmd2.Process.Pid
	if pid2 <= 0 {
		t.Fatal("expected positive pid for force test")
	}
	if err := inspector.Terminate(ctx, pid2, true); err != nil {
		t.Fatalf("terminate pid=%d with Kill(force=true): %v", pid2, err)
	}
	if err := cmd2.Wait(); err == nil {
		t.Fatalf("expected process pid=%d to be killed by signal, got clean exit", pid2)
	}
}

// TestQueueWaitsByKey 覆盖 queueWaitsByKey 的构造/索引分支。
func TestQueueWaitsByKey(t *testing.T) {
	items := []QueueWaitSnapshot{
		{Key: "redis:default", Connection: "redis", Queue: "default", Status: "known", WaitMS: 100},
		{Key: "redis:critical", Connection: "redis", Queue: "critical", Status: "unknown", WaitMS: 0},
	}
	result := queueWaitsByKey(items)
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if v, ok := result["redis:default"]; !ok || v.WaitMS != 100 {
		t.Fatalf("expected redis:default with wait 100, got %+v %v", v, ok)
	}
	if v, ok := result["redis:critical"]; !ok || v.Status != "unknown" {
		t.Fatalf("expected redis:critical status unknown, got %+v %v", v, ok)
	}
	// 空切片也应干净返回空 map。
	if result := queueWaitsByKey(nil); len(result) != 0 {
		t.Fatalf("expected empty map for nil input, got %d", len(result))
	}
}

// TestTruncateSummary 覆盖 truncateSummary 的截断/不截断分支。
func TestTruncateSummary(t *testing.T) {
	if s := truncateSummary("short"); s != "short" {
		t.Fatalf("short string unchanged: got %q", s)
	}
	long := strings.Repeat("x", maxErrorSummaryLength+10)
	if s := truncateSummary(long); len(s) != maxErrorSummaryLength {
		t.Fatalf("long string should be truncated to %d, got %d", maxErrorSummaryLength, len(s))
	}
	// 恰好等于上限不应截断。
	equal := strings.Repeat("y", maxErrorSummaryLength)
	if s := truncateSummary(equal); len(s) != maxErrorSummaryLength {
		t.Fatalf("equal string should not be truncated: got %d", len(s))
	}
}

// TestSplitQueueWaitKey 覆盖 splitQueueWaitKey 的拆分/无冒号分支。
func TestSplitQueueWaitKey(t *testing.T) {
	conn, queue := splitQueueWaitKey("redis:default")
	if conn != "redis" || queue != "default" {
		t.Fatalf("expected redis:default -> redis,default got %q,%q", conn, queue)
	}
	conn, queue = splitQueueWaitKey("nocolon")
	if conn != "nocolon" || queue != "" {
		t.Fatalf("expected nocolon -> nocolon,'' got %q,%q", conn, queue)
	}
	conn, queue = splitQueueWaitKey("")
	if conn != "" || queue != "" {
		t.Fatalf("expected empty -> '','' got %q,%q", conn, queue)
	}
}

// TestHeartbeatErrorReadModel 覆盖 heartbeatErrorReadModel 的有错误/无错误分支。
func TestHeartbeatErrorReadModel(t *testing.T) {
	now := time.Now().UTC()
	worker := WorkerState{
		LastHeartbeatErrorCode:    "E001",
		LastHeartbeatErrorMessage: "something broke",
		LastHeartbeatErrorAt:      now,
	}
	errModel := heartbeatErrorReadModel(worker)
	if errModel.Code != "E001" || errModel.Message != "something broke" || !errModel.FailedAt.Equal(now) {
		t.Fatalf("expected error model populated, got %+v", errModel)
	}

	// 无错误码则返回空。
	empty := heartbeatErrorReadModel(WorkerState{})
	if empty.Code != "" || empty.Message != "" || !empty.FailedAt.IsZero() {
		t.Fatalf("expected empty error model, got %+v", empty)
	}
}

// TestAverageRuntimeMS 覆盖 averageRuntimeMS 的正常和除零/负分支。
func TestAverageRuntimeMS(t *testing.T) {
	if v := averageRuntimeMS(100, 10); v != 10 {
		t.Fatalf("100/10 = 10, got %d", v)
	}
	if v := averageRuntimeMS(0, 10); v != 0 {
		t.Fatalf("zero total returns 0, got %d", v)
	}
	if v := averageRuntimeMS(-1, 10); v != 0 {
		t.Fatalf("negative total returns 0, got %d", v)
	}
	if v := averageRuntimeMS(100, 0); v != 0 {
		t.Fatalf("zero count returns 0, got %d", v)
	}
	if v := averageRuntimeMS(100, -1); v != 0 {
		t.Fatalf("negative count returns 0, got %d", v)
	}
}

// TestClampSampleRate 覆盖 clampSampleRate 的三条分支。
func TestClampSampleRate(t *testing.T) {
	if v := clampSampleRate(-0.5); v != 0 {
		t.Fatalf("negative clamped to 0, got %v", v)
	}
	if v := clampSampleRate(2.0); v != 1 {
		t.Fatalf("above 1 clamped to 1, got %v", v)
	}
	if v := clampSampleRate(0.5); v != 0.5 {
		t.Fatalf("in-range unchanged, got %v", v)
	}
}

// TestQueueWaitByAggregateKey 覆盖 queueWaitByAggregateKey 的命中/未命中分支。
func TestQueueWaitByAggregateKey(t *testing.T) {
	items := []QueueWaitSnapshot{
		{Connection: "redis", Queue: "default", WaitMS: 100},
		{Connection: "redis", Queue: "critical", WaitMS: 200},
	}
	v, ok := queueWaitByAggregateKey(items, queueAggregateKey{Connection: "redis", Queue: "default"})
	if !ok || v.WaitMS != 100 {
		t.Fatalf("expected redis:default found, got %+v %v", v, ok)
	}
	_, ok = queueWaitByAggregateKey(items, queueAggregateKey{Connection: "redis", Queue: "missing"})
	if ok {
		t.Fatal("expected not found for missing queue")
	}
	_, ok = queueWaitByAggregateKey(nil, queueAggregateKey{Connection: "redis", Queue: "default"})
	if ok {
		t.Fatal("expected not found for nil slice")
	}
}

// TestCapabilityState 覆盖 capabilityState 的 enabled/disabled 分支。
func TestCapabilityState(t *testing.T) {
	if s := capabilityState(true); s != "supported" {
		t.Fatalf("enabled -> supported, got %q", s)
	}
	if s := capabilityState(false); s != "disabled" {
		t.Fatalf("disabled -> disabled, got %q", s)
	}
}

// TestSortMasterStates 覆盖 sortMasterStates 的排序路径。
func TestSortMasterStates(t *testing.T) {
	items := []MasterState{
		{ID: "m3"}, {ID: "m1"}, {ID: "m2"},
	}
	sortMasterStates(items)
	for i := 1; i < len(items); i++ {
		if items[i-1].ID > items[i].ID {
			t.Fatalf("items not sorted: %#v", items)
		}
	}
}

// TestWatchSignatureEqual 覆盖 watchSignatureEqual 的真/假分支。
func TestWatchSignatureEqual(t *testing.T) {
	empty := map[string]watchFileState{}
	left := map[string]watchFileState{"a": {1, 100}}
	right := map[string]watchFileState{"a": {1, 100}}
	diff := map[string]watchFileState{"a": {1, 100}, "b": {2, 200}}
	diffVal := map[string]watchFileState{"a": {2, 100}}

	if !watchSignatureEqual(empty, empty) {
		t.Fatal("empty maps should be equal")
	}
	if !watchSignatureEqual(left, right) {
		t.Fatal("identical maps should be equal")
	}
	if watchSignatureEqual(empty, left) {
		t.Fatal("different length should not be equal")
	}
	if watchSignatureEqual(left, diff) {
		t.Fatal("different length should not be equal")
	}
	if watchSignatureEqual(left, diffVal) {
		t.Fatal("different value should not be equal")
	}
}

// TestDashboardPath 覆盖 DashboardPath 的默认/自定义路径分支。
func TestDashboardPath(t *testing.T) {
	p := Config{Path: ""}.DashboardPath()
	if p != "/horizon" {
		t.Fatalf("empty path defaults to /horizon, got %q", p)
	}
	p = Config{Path: "my-horizon"}.DashboardPath()
	if p != "/my-horizon" {
		t.Fatalf("custom path /my-horizon, got %q", p)
	}
}

// TestParseStringList 覆盖 parseStringList 的全部类型分支。
func TestParseStringList(t *testing.T) {
	if v := parseStringList(nil); v != nil {
		t.Fatalf("nil returns nil, got %v", v)
	}
	if v := parseStringList("a,b,c"); len(v) != 3 || v[0] != "a" {
		t.Fatalf("csv string splits, got %v", v)
	}
	if v := parseStringList([]string{"a", "b"}); len(v) != 2 {
		t.Fatalf("string slice passes through, got %v", v)
	}
	if v := parseStringList([]any{"a", 1}); len(v) != 2 || v[1] != "1" {
		t.Fatalf("any slice converted, got %v", v)
	}
	if v := parseStringList(42); len(v) != 1 || v[0] != "42" {
		t.Fatalf("scalar wrapped, got %v", v)
	}
}

// TestFirstNonNegativeInt 覆盖 firstNonNegativeInt 的合法/非法分支。
func TestFirstNonNegativeInt(t *testing.T) {
	if v := firstNonNegativeInt(5, 10); v != 5 {
		t.Fatalf("valid value: got %d", v)
	}
	if v := firstNonNegativeInt(-1, 10); v != 10 {
		t.Fatalf("negative falls back: got %d", v)
	}
	if v := firstNonNegativeInt(nil, 10); v != 0 {
		t.Fatalf("nil parses as 0: got %d", v)
	}
	if v := firstNonNegativeInt("", 10); v != 0 {
		t.Fatalf("empty string parses as 0: got %d", v)
	}
}

// TestFirstBool 覆盖 firstBool 的全部布尔字符串分支。
func TestFirstBool(t *testing.T) {
	if firstBool(nil, true) != true {
		t.Fatal("nil returns fallback true")
	}
	if firstBool(true, false) != true {
		t.Fatal("bool true")
	}
	if firstBool(false, true) != false {
		t.Fatal("bool false")
	}
	for _, s := range []string{"1", "true", "yes", "on"} {
		if !firstBool(s, false) {
			t.Fatalf("%q should be true", s)
		}
	}
	for _, s := range []string{"0", "false", "no", "off", ""} {
		if firstBool(s, true) {
			t.Fatalf("%q should be false", s)
		}
	}
	if firstBool("garbage", true) != true {
		t.Fatal("garbage string falls back to true")
	}
	if firstBool(1.5, false) != false {
		t.Fatal("float 1.5 via Sprint is not a truthy string")
	}
}

// TestStrictBool 覆盖 strictBool 的全部类型分支和错误路径。
func TestStrictBool(t *testing.T) {
	v, err := strictBool("test", true)
	if err != nil || v != true {
		t.Fatalf("bool true: %v %v", v, err)
	}
	v, err = strictBool("test", false)
	if err != nil || v != false {
		t.Fatalf("bool false: %v %v", v, err)
	}
	v, err = strictBool("test", "on")
	if err != nil || v != true {
		t.Fatalf("string on: %v %v", v, err)
	}
	v, err = strictBool("test", "off")
	if err != nil || v != false {
		t.Fatalf("string off: %v %v", v, err)
	}
	_, err = strictBool("test", "garbage")
	if err == nil {
		t.Fatal("garbage string should error")
	}
	_, err = strictBool("test", 1)
	if err == nil {
		t.Fatal("int should error")
	}
}

// TestParseNonNegativeInt 覆盖 parseNonNegativeInt 的剩余分支。
func TestParseNonNegativeInt(t *testing.T) {
	if n, ok := parseNonNegativeInt(int64(5)); !ok || n != 5 {
		t.Fatalf("int64: %d %v", n, ok)
	}
	if n, ok := parseNonNegativeInt(float64(3)); !ok || n != 3 {
		t.Fatalf("float64: %d %v", n, ok)
	}
	if _, ok := parseNonNegativeInt(float64(-1)); ok {
		t.Fatal("negative float should be invalid")
	}
	if n, ok := parseNonNegativeInt(" 42 "); !ok || n != 42 {
		t.Fatalf("trimmed string: %d %v", n, ok)
	}
	if _, ok := parseNonNegativeInt("not a number"); ok {
		t.Fatal("invalid string should be false")
	}
}

// TestPreferSnapshotMetric 覆盖 preferSnapshotMetric 的三条路径。
func TestPreferSnapshotMetric(t *testing.T) {
	avail := goprocess.Metric{Value: 1, Unit: "pct", Status: goprocess.StatusAvailable}
	store := goprocess.Metric{Value: 2, Unit: "pct", Status: goprocess.StatusAvailable}
	empty := goprocess.Metric{}

	// 采样优先于存储。
	if v := preferSnapshotMetric(avail, store, "pct", "reason"); v.Value != 1 {
		t.Fatalf("sample wins over stored, got %+v", v)
	}
	// 无采样则取存储值。
	if v := preferSnapshotMetric(empty, store, "pct", "reason"); v.Value != 2 {
		t.Fatalf("stored used when sample empty, got %+v", v)
	}
	// 两者皆无返回 unavailable。
	if v := preferSnapshotMetric(empty, empty, "pct", "unavailable"); v.Status != goprocess.StatusUnavailable {
		t.Fatalf("unavailable when both empty, got %+v", v)
	}
}

// TestEventMetricWindowDuration 覆盖 eventMetricWindowDuration 的三条分支。
func TestEventMetricWindowDuration(t *testing.T) {
	t0 := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(5 * time.Minute)

	// MetricsWindowMS 优先。
	w1 := EventMetricWindow{MetricsWindowMS: 300000}
	if d := eventMetricWindowDuration(w1); d != 5*time.Minute {
		t.Fatalf("metrics window ms: got %v", d)
	}
	// 无 MetricsWindowMS，由 WindowEnd-WindowStart 计算。
	w2 := EventMetricWindow{WindowStart: t0, WindowEnd: t1}
	if d := eventMetricWindowDuration(w2); d != 5*time.Minute {
		t.Fatalf("end-start: got %v", d)
	}
	// 两者都无返回 0。
	w3 := EventMetricWindow{}
	if d := eventMetricWindowDuration(w3); d != 0 {
		t.Fatalf("zero window: got %v", d)
	}
}

// TestFirstPositiveSamplingInt 覆盖 firstPositiveSamplingInt 的命中/未命中/空分支。
func TestFirstPositiveSamplingInt(t *testing.T) {
	if v := firstPositiveSamplingInt(0, 5, 0); v != 5 {
		t.Fatalf("finds first positive: got %d", v)
	}
	if v := firstPositiveSamplingInt(0, 0); v != 0 {
		t.Fatalf("all zero returns 0: got %d", v)
	}
	if v := firstPositiveSamplingInt(); v != 0 {
		t.Fatalf("empty returns 0: got %d", v)
	}
}

// TestFirstPositiveDuration 覆盖 firstPositiveDuration 的命中/未命中分支。
func TestFirstPositiveDuration(t *testing.T) {
	if v := firstPositiveDuration(0, time.Second, 0); v != time.Second {
		t.Fatalf("finds first positive: got %v", v)
	}
	if v := firstPositiveDuration(0, 0); v != 0 {
		t.Fatalf("all zero returns 0: got %v", v)
	}
	if v := firstPositiveDuration(); v != 0 {
		t.Fatalf("empty returns 0: got %v", v)
	}
}

// TestObservabilityDiagnosticID 覆盖 observabilityDiagnosticID 的零值与正常分支。
func TestObservabilityDiagnosticID(t *testing.T) {
	now := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	item := ObservabilityDiagnostic{Reason: "test", ObservedAt: now}
	id := observabilityDiagnosticID(item, 1)
	if id == "" || !strings.Contains(id, "test") {
		t.Fatalf("expected reason in id, got %q", id)
	}
	// 零值 ObservedAt 应 fallback 到当前时间。
	itemZero := ObservabilityDiagnostic{Reason: "fallback"}
	id2 := observabilityDiagnosticID(itemZero, 0)
	if !strings.Contains(id2, "fallback") {
		t.Fatalf("expected fallback reason in id, got %q", id2)
	}
}

// TestClampPoolTarget 覆盖 clampPoolTarget 的边界计算和 clamp 分支。
func TestClampPoolTarget(t *testing.T) {
	sv := SupervisorConfig{MinProcesses: 2, MaxProcesses: 10}
	// maxForPool = 10 - 2*(2-1) = 8, between min=2 and max=8, value 5 fits.
	if v := clampPoolTarget(5, sv, 2); v != 5 {
		t.Fatalf("in range: got %d", v)
	}
	// maxForPool = 10 - 2*(3-1) = 6, value 8 clamped to 6.
	if v := clampPoolTarget(8, sv, 3); v != 6 {
		t.Fatalf("clamped to maxForPool: got %d", v)
	}
	// maxForPool = 10 - 2*(6-1) = 0, clamped to min=2; value 1 clamped to 2.
	if v := clampPoolTarget(0, sv, 6); v != 2 {
		t.Fatalf("clamped to min: got %d", v)
	}
}

// TestIntAliasField 覆盖 intAliasField 的 snake/camel/fallback/error 分支。
func TestIntAliasField(t *testing.T) {
	// snake_case 命中。
	v, err := intAliasField("s1", map[string]any{"max_processes": 5}, "max_processes", "maxProcesses", 1)
	if err != nil || v != 5 {
		t.Fatalf("snake_case: %d %v", v, err)
	}
	// camelCase 回退。
	v, err = intAliasField("s1", map[string]any{"maxProcesses": 3}, "max_processes", "maxProcesses", 1)
	if err != nil || v != 3 {
		t.Fatalf("camelCase fallback: %d %v", v, err)
	}
	// 两都缺失走 fallback。
	v, err = intAliasField("s1", map[string]any{}, "max_processes", "maxProcesses", 7)
	if err != nil || v != 7 {
		t.Fatalf("fallback: %d %v", v, err)
	}
	// 非法值报错。
	_, err = intAliasField("s1", map[string]any{"max_processes": -1}, "max_processes", "maxProcesses", 1)
	if err == nil {
		t.Fatal("expected error for negative value")
	}
}

// TestSortEventMetricWindows 覆盖 sortEventMetricWindows 排序路径。
func TestSortEventMetricWindows(t *testing.T) {
	t0 := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	items := []EventMetricWindow{
		// 覆盖所有 sort 层级: WindowStart → FlushAt → Connection → Queue
		// → SourcePrefix → SourceHost → SourceEnvironment → SourceSupervisor.
		{WindowStart: t0, FlushAt: t1, Connection: "redis", Queue: "default", SourcePrefix: "pfx-a", SourceHost: "h1", SourceEnvironment: "prod", SourceSupervisor: "s1"},
		{WindowStart: t0, FlushAt: t1, Connection: "redis", Queue: "default", SourcePrefix: "pfx-a", SourceHost: "h1", SourceEnvironment: "prod", SourceSupervisor: "s2"},
		{WindowStart: t0, FlushAt: t1, Connection: "redis", Queue: "default", SourcePrefix: "pfx-a", SourceHost: "h1", SourceEnvironment: "staging", SourceSupervisor: "s1"},
		{WindowStart: t0, FlushAt: t1, Connection: "redis", Queue: "default", SourcePrefix: "pfx-a", SourceHost: "h2"},
		{WindowStart: t0, FlushAt: t1, Connection: "redis", Queue: "default", SourcePrefix: "pfx-b", SourceHost: "h1"},
		{WindowStart: t0, FlushAt: t1, Connection: "redis", Queue: "other", SourcePrefix: "pfx-a", SourceHost: "h1"},
		{WindowStart: t0, FlushAt: t1, Connection: "beanstalkd", Queue: "default", SourcePrefix: "pfx-a", SourceHost: "h1"},
		{WindowStart: t0, FlushAt: t0, Connection: "redis", Queue: "critical"},
		{WindowStart: t0, FlushAt: t0, Connection: "redis", Queue: "default"},
		{WindowStart: t0.Add(time.Hour), FlushAt: t0},
		{WindowStart: t0.Add(-time.Hour), FlushAt: t0},
	}
	sortEventMetricWindows(items)
	for i := 1; i < len(items); i++ {
		a, b := items[i-1], items[i]
		if a.WindowStart.Before(b.WindowStart) {
			t.Fatalf("not sorted: %+v after %+v", b, a)
		}
		// 同 WindowStart 验证次级排序。
		if a.WindowStart.Equal(b.WindowStart) {
			if a.FlushAt.Before(b.FlushAt) {
				t.Fatalf("FlushAt desc: %+v after %+v", b, a)
			}
			if a.FlushAt.Equal(b.FlushAt) {
				if a.Connection > b.Connection {
					t.Fatalf("Connection asc: %+v after %+v", b, a)
				}
				if a.Connection == b.Connection && a.Queue > b.Queue {
					t.Fatalf("Queue asc: %+v after %+v", b, a)
				}
			}
		}
	}
}

// TestBatchSummaryRetentionTime 覆盖 batchSummaryRetentionTime 的四条优先路径。
func TestBatchSummaryRetentionTime(t *testing.T) {
	now := time.Now().UTC()
	t0 := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	// UpdatedAt 最高优先级。
	if v := batchSummaryRetentionTime(BatchSummary{UpdatedAt: t0, FlushAt: now, CreatedAt: now}); !v.Equal(t0) {
		t.Fatalf("UpdatedAt wins: got %v", v)
	}
	// FlushAt 次优。
	if v := batchSummaryRetentionTime(BatchSummary{FlushAt: t0, CreatedAt: now}); !v.Equal(t0) {
		t.Fatalf("FlushAt wins when UpdatedAt zero: got %v", v)
	}
	// CreatedAt 第三优。
	if v := batchSummaryRetentionTime(BatchSummary{CreatedAt: t0}); !v.Equal(t0) {
		t.Fatalf("CreatedAt wins when others zero: got %v", v)
	}
	// 全部为零则返回当前时间。
	v := batchSummaryRetentionTime(BatchSummary{})
	if v.Before(now.Add(-time.Second)) || v.After(now.Add(time.Second)) {
		t.Fatalf("all zero defaults to now: got %v", v)
	}
}

// TestEventMetricWindowMatchesQuery 覆盖 eventMetricWindowMatchesQuery 的全部过滤分支。
func TestEventMetricWindowMatchesQuery(t *testing.T) {
	t0 := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	window := EventMetricWindow{
		WindowStart:       t0,
		WindowEnd:         t1,
		SourceHost:        "h1",
		SourceEnvironment: "prod",
		SourceSupervisor:  "s1",
		Connection:        "redis",
		Queue:             "default",
	}
	// 空查询全部通过。
	if !eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{}) {
		t.Fatal("empty query should match")
	}
	// 全部精确匹配。
	q := EventMetricWindowQuery{SourceHost: "h1", SourceEnvironment: "prod", SourceSupervisor: "s1", Connection: "redis", Queue: "default"}
	if !eventMetricWindowMatchesQuery(window, q) {
		t.Fatal("exact match should pass")
	}
	// 各项不匹配。
	if eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{SourceHost: "h2"}) {
		t.Fatal("host mismatch should fail")
	}
	if eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{SourceEnvironment: "staging"}) {
		t.Fatal("env mismatch should fail")
	}
	if eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{Connection: "sqs"}) {
		t.Fatal("connection mismatch should fail")
	}
	if eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{Queue: "critical"}) {
		t.Fatal("queue mismatch should fail")
	}
	// 时间范围：From 要求 WindowEnd > From。
	if eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{From: t1}) {
		t.Fatal("from after window end should fail")
	}
	if !eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{From: t0.Add(-time.Minute)}) {
		t.Fatal("from before window end should pass")
	}
	// 时间范围：To 要求 WindowStart < To。
	if eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{To: t0}) {
		t.Fatal("to before window start should fail")
	}
	if !eventMetricWindowMatchesQuery(window, EventMetricWindowQuery{To: t1.Add(time.Minute)}) {
		t.Fatal("to after window start should pass")
	}
}

// TestScalingStrategyField 覆盖 scalingStrategyField 的全部分支。
func TestScalingStrategyField(t *testing.T) {
	// 缺失回退到默认 time。
	s, err := scalingStrategyField("s1", map[string]any{})
	if err != nil || s != AutoScalingStrategyTime {
		t.Fatalf("missing defaults to time: %q %v", s, err)
	}
	// 空字符串也回退。
	s, err = scalingStrategyField("s1", map[string]any{"auto_scaling_strategy": "  "})
	if err != nil || s != AutoScalingStrategyTime {
		t.Fatalf("empty string defaults to time: %q %v", s, err)
	}
	// 显式 time。
	s, err = scalingStrategyField("s1", map[string]any{"auto_scaling_strategy": "time"})
	if err != nil || s != AutoScalingStrategyTime {
		t.Fatalf("explicit time: %q %v", s, err)
	}
	// 显式 size。
	s, err = scalingStrategyField("s1", map[string]any{"auto_scaling_strategy": "size"})
	if err != nil || s != AutoScalingStrategySize {
		t.Fatalf("explicit size: %q %v", s, err)
	}
	// 非法值报错。
	_, err = scalingStrategyField("s1", map[string]any{"auto_scaling_strategy": "invalid"})
	if err == nil {
		t.Fatal("expected error for invalid strategy")
	}
}

// TestStrictSampleRate 覆盖 strictSampleRate 的全部类型分支和验证分支。
func TestStrictSampleRate(t *testing.T) {
	v, err := strictSampleRate("test", float64(0.5))
	if err != nil || v != 0.5 {
		t.Fatalf("float64: %v %v", v, err)
	}
	v, err = strictSampleRate("test", float32(0.3))
	if err != nil || v != 0.30000001192092896 {
		t.Fatalf("float32: %v %v", v, err)
	}
	v, err = strictSampleRate("test", int(1))
	if err != nil || v != 1 {
		t.Fatalf("int: %v %v", v, err)
	}
	v, err = strictSampleRate("test", int64(0))
	if err != nil || v != 0 {
		t.Fatalf("int64: %v %v", v, err)
	}
	v, err = strictSampleRate("test", "0.75")
	if err != nil || v != 0.75 {
		t.Fatalf("string: %v %v", v, err)
	}
	// 空字符串报错。
	_, err = strictSampleRate("test", "")
	if err == nil {
		t.Fatal("empty string should error")
	}
	// 负值报错。
	_, err = strictSampleRate("test", -0.5)
	if err == nil {
		t.Fatal("negative should error")
	}
	// 超 1 报错。
	_, err = strictSampleRate("test", 1.5)
	if err == nil {
		t.Fatal("above 1 should error")
	}
	// default 类型分支（uint 等非标准数字类型走 Sprint 再 ParseFloat）。
	v, err = strictSampleRate("test", uint(0))
	if err != nil || v != 0 {
		t.Fatalf("uint via default: %v %v", v, err)
	}
}

func TestMemoryStoreBoundaryMethods(t *testing.T) {
	// 设计思路：memory store 用于本地和单元测试，因此也必须遵守与 Redis Store 一致的空标识校验、
	// pause 派生和 TTL trim 语义。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{})
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{}); err == nil {
		t.Fatal("expected empty supervisor name error")
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{}); err == nil {
		t.Fatal("expected empty worker id error")
	}
	if _, found, err := store.Worker(ctx, "missing", now); err != nil || found {
		t.Fatalf("missing worker found=%v err=%v", found, err)
	}
	if err := store.SetSupervisorPaused(ctx, "", true); err == nil {
		t.Fatal("expected empty paused supervisor error")
	}
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "s1", Status: SupervisorRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "w1", Supervisor: "s1", Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}
	worker, found, err := store.Worker(ctx, "w1", now)
	if err != nil || !found || worker.Status != WorkerIdle {
		t.Fatalf("worker read got %#v found=%v err=%v", worker, found, err)
	}
	if err := store.SetSupervisorPaused(ctx, "s1", true); err != nil {
		t.Fatalf("pause supervisor: %v", err)
	}
	worker, found, err = store.Worker(ctx, "w1", now)
	if err != nil || !found || worker.Status != WorkerPaused {
		t.Fatalf("paused worker got %#v found=%v err=%v", worker, found, err)
	}
	if err := store.Trim(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("trim: %v", err)
	}
	supervisors, _ := store.Supervisors(ctx, now)
	workers, _ := store.Workers(ctx, now)
	if len(supervisors) != 0 || len(workers) != 0 {
		t.Fatalf("expected trim to remove stale items, got supervisors=%d workers=%d", len(supervisors), len(workers))
	}
}

func TestRedisStoreReadBranchesAndFactoryClient(t *testing.T) {
	// 需求背景：Redis 是生产推荐 Store。该测试通过 miniredis 验证 NewRedisStore 构造、单条读取、
	// supervisor pause 标记、terminate 派生以及残留索引过滤，不依赖真实 Redis 服务。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 16, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	store := NewRedisStoreFromClient(redis.NewClient(&redis.Options{Addr: server.Addr()}), StoreOptions{Prefix: "redis_extra", HeartbeatTTL: time.Minute})
	t.Cleanup(func() { _ = store.client.Close() })
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{}); err == nil {
		t.Fatal("expected empty supervisor name error")
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{}); err == nil {
		t.Fatal("expected empty worker id error")
	}
	if _, found, err := store.Supervisor(ctx, "missing", now); err != nil || found {
		t.Fatalf("missing supervisor found=%v err=%v", found, err)
	}
	if _, found, err := store.Worker(ctx, "missing", now); err != nil || found {
		t.Fatalf("missing worker found=%v err=%v", found, err)
	}
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "s1", Status: SupervisorRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "w1", Supervisor: "s1", Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}
	if err := store.SetSupervisorPaused(ctx, "", true); err == nil {
		t.Fatal("expected empty pause supervisor error")
	}
	if err := store.SetSupervisorPaused(ctx, "s1", true); err != nil {
		t.Fatalf("pause supervisor: %v", err)
	}
	if err := store.SetSupervisorPaused(ctx, "s1", false); err != nil {
		t.Fatalf("continue supervisor: %v", err)
	}
	if err := store.RequestTerminate(ctx, time.Time{}, false); err != nil {
		t.Fatalf("request terminate default time: %v", err)
	}
	supervisor, found, err := store.Supervisor(ctx, "s1", now)
	if err != nil || !found || supervisor.Status != SupervisorTerminating {
		t.Fatalf("terminating supervisor got %#v found=%v err=%v", supervisor, found, err)
	}
	worker, found, err := store.Worker(ctx, "w1", now)
	if err != nil || !found || worker.Status != WorkerTerminating {
		t.Fatalf("terminating worker got %#v found=%v err=%v", worker, found, err)
	}
	if err := store.ClearTerminateRequest(ctx); err != nil {
		t.Fatalf("clear terminate: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	client.SAdd(ctx, "redis_extra:supervisors", "orphan")
	client.SAdd(ctx, "redis_extra:workers", "orphan-worker")
	if _, err := store.Supervisors(ctx, now); err != nil {
		t.Fatalf("list supervisors with orphan index: %v", err)
	}
	if _, err := store.Workers(ctx, now); err != nil {
		t.Fatalf("list workers with orphan index: %v", err)
	}
	_ = client.Close()
}

func TestRedisStoreErrorBranches(t *testing.T) {
	// 设计原因：Redis Store 不允许在 Redis 不可用或数据损坏时 fallback 到 memory。本测试固定坏 JSON、
	// 关闭连接后的错误路径，确保调用方能收到显式错误。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 16, 30, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	store := NewRedisStoreFromClient(redis.NewClient(&redis.Options{Addr: server.Addr()}), StoreOptions{Prefix: "redis_errors", HeartbeatTTL: time.Minute})
	badSupervisor := SupervisorState{Name: "bad", Host: "host-1", Environment: "local"}
	badSupervisorIdentity := supervisorInstanceID(badSupervisor)
	if err := store.client.Set(ctx, store.supervisorInstanceKey(badSupervisorIdentity), "{", 0).Err(); err != nil {
		t.Fatalf("seed bad supervisor: %v", err)
	}
	if err := store.client.SAdd(ctx, "redis_errors:supervisors", badSupervisorIdentity).Err(); err != nil {
		t.Fatalf("seed bad supervisor index: %v", err)
	}
	if _, err := store.Supervisors(ctx, now); err == nil {
		t.Fatal("expected bad supervisor json error")
	}
	badWorker := WorkerState{ID: "bad", Supervisor: "bad", Host: "host-1", Environment: "local"}
	badWorkerIdentity := workerInstanceID(badWorker)
	if err := store.client.Set(ctx, store.workerInstanceKey(badWorkerIdentity), "{", 0).Err(); err != nil {
		t.Fatalf("seed bad worker: %v", err)
	}
	if err := store.client.SAdd(ctx, "redis_errors:workers", badWorkerIdentity).Err(); err != nil {
		t.Fatalf("seed bad worker index: %v", err)
	}
	if _, err := store.Workers(ctx, now); err == nil {
		t.Fatal("expected bad worker json error")
	}
	_ = store.client.Close()
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "s1"}); err == nil {
		t.Fatal("expected heartbeat supervisor redis error")
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "w1"}); err == nil {
		t.Fatal("expected heartbeat worker redis error")
	}
	if _, err := store.StatusSnapshot(ctx, now); err == nil {
		t.Fatal("expected status snapshot redis error")
	}
	if err := store.Trim(ctx, now); err == nil {
		t.Fatal("expected trim redis error")
	}
}

func TestDefaultManagerAndLoadConfigEntryPoints(t *testing.T) {
	// 需求背景：应用注册的命令使用 defaultManager 作为入口。本测试验证 LoadConfig/defaultManager
	// 能提供完整的 Store 配置与 Store resolver。
	registry := useHorizonTestContainer(t)
	if err := reloadConfigFacadeForTest(t, registry); err != nil {
		t.Fatalf("reload config facade: %v", err)
	}
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Store == "" || cfg.Connection == "" || cfg.HeartbeatTTL <= 0 {
		t.Fatalf("unexpected loaded config: %#v", cfg)
	}
	bound, err := NewManager(Config{Store: "memory", HeartbeatTTL: time.Minute}, WithStoreFactory(defaultStoreFactory))
	if err != nil {
		t.Fatalf("new default manager: %v", err)
	}
	if err := registry.Instance(managerFacadeKey, bound); err != nil {
		t.Fatalf("bind horizon manager: %v", err)
	}
	manager, err := defaultManager()
	if err != nil {
		t.Fatalf("default manager: %v", err)
	}
	if manager.StoreFactory() == nil {
		t.Fatal("default manager should install store factory")
	}
}

func TestStoreFactoryRedisAndDefaultManagerFailureBranches(t *testing.T) {
	// 测试目的：补齐 DefaultStoreFactory 的 redis/default 分支，以及 fallback manager 在配置非法时的失败边界。
	registry := useHorizonTestContainer(t)

	server := miniredis.RunT(t)
	redisManager, err := prismredis.NewManager(prismredis.Config{
		DefaultName: "default",
		Connections: map[string]prismredis.ConnectionConfig{
			"default": {Name: "default", Addr: server.Addr()},
		},
	})
	if err != nil {
		t.Fatalf("redis NewManager error = %v", err)
	}
	if err := registry.Instance("redis", redisManager); err != nil {
		t.Fatalf("bind redis manager: %v", err)
	}
	t.Cleanup(func() { _ = redisManager.Close(context.Background()) })

	t.Setenv("REDIS_HOST", "127.0.0.1")
	t.Setenv("REDIS_PORT", server.Port())
	if err := reloadConfigFacadeForTest(t, registry); err != nil {
		t.Fatalf("reload config facade: %v", err)
	}

	store, err := (&DefaultStoreFactory{}).ResolveStore(context.Background(), Config{
		Store:        "",
		Connection:   "default",
		Prefix:       "factory_redis",
		HeartbeatTTL: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("resolve redis store: %v", err)
	}
	redisStore, ok := store.(*RedisStore)
	if !ok {
		t.Fatalf("resolved store type = %T, want *RedisStore", store)
	}
	if redisStore.client.Options().Addr != server.Addr() {
		t.Fatalf("redis addr = %q, want %q", redisStore.client.Options().Addr, server.Addr())
	}
	if redisStore.prefix() != "factory_redis" || redisStore.options.HeartbeatTTL != 3*time.Second {
		t.Fatalf("redis store options not preserved: prefix=%q ttl=%s", redisStore.prefix(), redisStore.options.HeartbeatTTL)
	}

	t.Setenv("HORIZON_MAX_PROCESSES", "-1")
	if err := reloadConfigFacadeForTest(t, registry); err != nil {
		t.Fatalf("reload invalid config facade: %v", err)
	}
	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatal("Resolve without bound manager did not panic")
			}
			if got := fmt.Sprint(recovered); got != `container "horizon.manager": container factory is not registered` {
				t.Fatalf("panic = %q, want horizon.manager not registered", got)
			}
		}()
		_ = Resolve()
	}()
	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatal("defaultManager without bound manager did not panic")
			}
			if got := fmt.Sprint(recovered); got != `container "horizon.manager": container factory is not registered` {
				t.Fatalf("panic = %q, want horizon.manager not registered", got)
			}
		}()
		_, _ = defaultManager()
	}()
}

func TestSmallHelperBranches(t *testing.T) {
	// 测试目的：补齐时间敏感 helper 的边界语义，尤其是零 heartbeat 与隐式 now 的处理。
	if got := firstNonNegativeInt("bad", 7); got != 7 {
		t.Fatalf("firstNonNegativeInt invalid = %d", got)
	}
	if !heartbeatStale(time.Time{}, time.Second, time.Now()) {
		t.Fatal("zero heartbeat should be stale")
	}
	if heartbeatStale(time.Now(), time.Second, time.Time{}) {
		t.Fatal("current heartbeat should not be stale with implicit now")
	}
}

func TestDashboardPathAndBatchSortBranches(t *testing.T) {
	// 测试目的：补齐 DashboardPath 显式路径和 batch 创建时间排序的剩余分支。
	if got := (Config{Path: "ops/horizon"}).DashboardPath(); got != "/ops/horizon" {
		t.Fatalf("explicit dashboard path = %q, want /ops/horizon", got)
	}
	items := []BatchSummary{
		{ID: "batch-b", CreatedAt: time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC)},
		{ID: "batch-c", CreatedAt: time.Date(2026, 5, 13, 11, 0, 0, 0, time.UTC)},
		{ID: "batch-a", CreatedAt: time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC)},
	}
	sortBatchSummaries(items)
	if items[0].ID != "batch-c" || items[1].ID != "batch-a" || items[2].ID != "batch-b" {
		t.Fatalf("sorted batches = %#v", items)
	}
}

func TestEventMetricWindowHelpersAndSortBranches(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	windows := []EventMetricWindow{
		{WindowStart: now, FlushAt: now, Connection: "redis", Queue: "z", JobName: "b"},
		{WindowStart: now, FlushAt: now.Add(time.Second), Connection: "redis", Queue: "a", JobName: "b"},
		{WindowStart: now, FlushAt: now.Add(time.Second), Connection: "amqp", Queue: "z", JobName: "b"},
		{WindowStart: now, FlushAt: now.Add(time.Second), Connection: "redis", Queue: "a", JobName: "a"},
		{WindowStart: now.Add(time.Minute), FlushAt: now, Connection: "redis", Queue: "a", JobName: "a"},
	}
	sortEventMetricWindows(windows)
	if !windows[0].WindowStart.Equal(now.Add(time.Minute)) || windows[1].Connection != "amqp" || windows[2].JobName != "a" {
		t.Fatalf("event metric windows not sorted by window/flush/source: %#v", windows)
	}

	exact := eventMetricWindowFromIncrement(EventMetricIncrement{
		WindowStart: now, WindowEnd: now.Add(time.Minute), Connection: "redis", Queue: "default", Processed: 2,
	})
	if exact.Quality != EventMetricQualityExact || exact.SampleCount != 2 || exact.EffectiveSampleRate != 1 {
		t.Fatalf("exact window defaults = %#v", exact)
	}
	estimated := eventMetricWindowFromIncrement(EventMetricIncrement{
		WindowStart: now, WindowEnd: now.Add(time.Minute), Connection: "redis", Queue: "default",
		Failed: 1, Samples: 1, EffectiveSampleRate: 0.25, Estimated: true,
	})
	if estimated.Quality != EventMetricQualityEstimated || estimated.EstimatedTotal != 4 {
		t.Fatalf("estimated window defaults = %#v", estimated)
	}
	if eventMetricQuality(false, false, true) != EventMetricQualityPartial ||
		eventMetricQuality(false, true, false) != EventMetricQualityDegraded {
		t.Fatal("quality priority should be partial > degraded > estimated > exact")
	}

	batch := FlushBatch{
		Increments:         []EventMetricIncrement{{Estimated: true}},
		EventMetricWindows: []EventMetricWindow{{Estimated: true}},
		HighValueDetails:   []HighValueJobDetail{{ID: "detail"}},
		Diagnostics:        []ObservabilityDiagnostic{{Reason: "drop", Count: 1}},
	}
	markFlushBatchPartial(&batch)
	if !batch.Increments[0].Partial || batch.Increments[0].Quality != EventMetricQualityPartial ||
		!batch.EventMetricWindows[0].Partial || batch.EventMetricWindows[0].Quality != EventMetricQualityPartial {
		t.Fatalf("partial batch not marked: %#v", batch)
	}

	f := newFlusher(observabilityPresetConfigOrFull(), NewMemoryStore(StoreOptions{Prefix: "prefix"}), nil, nil)
	if f.storePrefix() != "prefix" || (*flusher)(nil).storePrefix() != "" {
		t.Fatalf("store prefix fallback failed")
	}
	if f.storePrefix() == "" {
		t.Fatal("memory store prefix should be exposed")
	}
	if defaultFlusher := (&flusher{store: struct{ Store }{}}); defaultFlusher.storePrefix() != "" {
		t.Fatal("unknown store prefix should be empty")
	}
}

func TestReadModelPureBranches(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	master := masterProcessReadModel(MasterState{ID: "master-1", Host: "host", PID: 10, Status: MasterRunning, LastHeartbeatAt: now})
	if master.Kind != ProcessKindMaster || master.CurrentQueue.Status != goprocess.StatusUnavailable {
		t.Fatalf("master process read model = %#v", master)
	}
	disabled := queueDisabledMetric(goprocess.UnitCount, "disabled")
	if disabled.Status != goprocess.StatusDisabled || disabled.Value != nil || disabled.Reason != "disabled" {
		t.Fatalf("disabled queue metric = %#v", disabled)
	}

	highValue := []HighValueJobDetail{
		{ID: "b", OccurredAt: now, Kind: HighValueDetailFailed},
		{ID: "a", OccurredAt: now, Kind: HighValueDetailFailed},
		{ID: "c", OccurredAt: now.Add(time.Minute), Kind: HighValueDetailFailed},
	}
	sortHighValueJobDetails(highValue)
	if highValue[0].ID != "c" || highValue[1].ID != "a" {
		t.Fatalf("high-value detail sort = %#v", highValue)
	}
	diags := []ObservabilityDiagnostic{
		{Reason: "b", Description: "z", ObservedAt: now},
		{Reason: "a", Description: "z", ObservedAt: now},
		{Reason: "a", Description: "a", ObservedAt: now},
		{Reason: "c", Description: "z", ObservedAt: now.Add(time.Minute)},
	}
	sortObservabilityDiagnostics(diags)
	if diags[0].Reason != "c" || diags[1].Description != "a" || diags[2].Reason != "a" {
		t.Fatalf("diagnostic sort = %#v", diags)
	}
}

func TestQueueReadModelsDisabledAndUnavailableBranches(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{})
	cfg := Config{
		Observability: ObservabilityConfig{
			Preset:        ObservabilityPresetMinimal,
			ProcessHealth: true,
			QueueLengths:  false,
			EventMetrics:  false,
			Waits:         false,
		},
		Supervisors: map[string]SupervisorConfig{
			"s1": {Connection: "redis", Queues: []string{"default"}},
		},
	}
	items, err := buildQueueReadModels(ctx, cfg, store, nil)
	if err != nil {
		t.Fatalf("build disabled queue models: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected configured queue model: %#v", items)
	}
	item := items[0]
	for label, metric := range map[string]goprocess.Metric{
		"size":       item.Size,
		"avgRuntime": item.AvgRuntime,
		"throughput": item.Throughput,
		"processed":  item.Processed,
		"wait":       item.WaitTime,
	} {
		if metric.Status != goprocess.StatusDisabled {
			t.Fatalf("%s should be disabled when observability is off: %#v", label, metric)
		}
	}

	cfg.Observability = observabilityPresetConfigOrFull()
	items, err = buildQueueReadModels(ctx, cfg, store, nil)
	if err != nil {
		t.Fatalf("build unavailable queue models: %v", err)
	}
	item = items[0]
	if item.Size.Status != goprocess.StatusUnavailable ||
		item.Throughput.Status != goprocess.StatusUnavailable ||
		item.Processed.Status != goprocess.StatusUnavailable ||
		item.WaitTime.Status != goprocess.StatusUnavailable {
		t.Fatalf("enabled but missing data should be unavailable: %#v", item)
	}
}

// TestQueueReadModelRuntimeUsesSingleSampleContract 验证 queue read model 的 runtime
// 指标只使用同一批 runtime sample。
//
// 需求背景：runtime 新口径已经把平均值分母收敛为 runtime sample，若 MaxRuntime 继续混入 failed runtime
// 历史字段，会导致 AvgRuntime 与 MaxRuntime 口径不一致，前端无法正确解释该组指标。
func TestQueueReadModelRuntimeUsesSingleSampleContract(t *testing.T) {
	cfg := Config{Observability: ObservabilityConfig{EventMetrics: true}}
	key := queueAggregateKey{Connection: "redis", Queue: "default"}
	item := queueReadModelFromSource(cfg, queueAggregateSource{
		buckets: []MetricsBucketSnapshot{{
			Connection:              key.Connection,
			Queue:                   key.Queue,
			MetricsCounters:         MetricsCounters{Processed: 2, Failed: 1},
			ProcessedCount:          2,
			ProcessedRuntimeTotalMS: 400,
			ProcessedRuntimeMaxMS:   300,
			FailedRuntimeTotalMS:    900,
			FailedRuntimeMaxMS:      900,
		}},
	}, key)

	if item.AvgRuntime.Status != goprocess.StatusAvailable || item.AvgRuntime.Unit != goprocess.UnitMilliseconds {
		t.Fatalf("avg runtime metric state=%#v", item.AvgRuntime)
	}
	avgValue, ok := item.AvgRuntime.Value.(int64)
	if !ok || avgValue != 200 {
		t.Fatalf("avg runtime value=%#v want=%d", item.AvgRuntime.Value, 200)
	}
	if item.MaxRuntime.Status != goprocess.StatusAvailable || item.MaxRuntime.Unit != goprocess.UnitMilliseconds {
		t.Fatalf("max runtime metric state=%#v", item.MaxRuntime)
	}
	maxValue, ok := item.MaxRuntime.Value.(int64)
	if !ok || maxValue != 300 {
		t.Fatalf("max runtime value=%#v want=%d", item.MaxRuntime.Value, 300)
	}
}

func TestProcessInspectorAndEventMetricIncrementBranches(t *testing.T) {
	ctx := context.Background()
	if _, err := (OSProcessInspector{}).HorizonProcesses(ctx); err != nil {
		t.Fatalf("process scan should be best-effort: %v", err)
	}

	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 0.25
	store := NewMemoryStore(StoreOptions{Prefix: "branch"})
	f := newFlusher(cfg, store, nil, nil)
	f.cfg.MetricsWindow = 0
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	inc := f.eventMetricIncrementFromWindow(&eventMetricsWindow{
		connection:   "redis",
		queue:        "default",
		processed:    1,
		failed:       1,
		eventSamples: 2,
		estimated:    true,
	}, &flushSnapshot{windowStart: now.Add(-time.Minute), windowEnd: now, degraded: true}, now)
	if !inc.WindowStart.Equal(now.Add(-time.Minute)) || !inc.WindowEnd.Equal(now) ||
		inc.EstimatedTotal != 8 || inc.Quality != EventMetricQualityDegraded {
		t.Fatalf("increment branch defaults = %#v", inc)
	}
	if effectiveEventMetricsSampleRate(2, 1) != 1 || effectiveEventMetricsSampleRate(-1, -1) != 0 {
		t.Fatal("sample rate should be clamped")
	}
}

func TestRedisEventMetricWindowCorruptAndRetentionBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "event_windows", HeartbeatTTL: time.Minute})

	old := EventMetricWindow{WindowStart: now.Add(-2 * time.Hour), WindowEnd: now.Add(-2 * time.Hour), FlushAt: now.Add(-2 * time.Hour), Connection: "redis", Queue: "old", Quality: EventMetricQualityExact}
	fresh := EventMetricWindow{WindowStart: now, WindowEnd: now, FlushAt: now, Connection: "redis", Queue: "fresh", Quality: EventMetricQualityExact}
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{old, fresh}, time.Hour); err != nil {
		t.Fatalf("append windows: %v", err)
	}
	page, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read windows: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].Queue != "fresh" {
		t.Fatalf("retention should trim old windows: %#v", page)
	}

	corruptID := "corrupt"
	if err := client.Set(ctx, store.eventMetricWindowKey(corruptID), "{bad-json", 0).Err(); err != nil {
		t.Fatalf("seed corrupt window: %v", err)
	}
	if err := client.ZAdd(ctx, store.eventMetricWindowsKey(), redis.Z{Score: float64(now.Add(time.Minute).UnixNano()), Member: corruptID}).Err(); err != nil {
		t.Fatalf("seed corrupt index: %v", err)
	}
	page, err = store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read windows with corrupt member: %v", err)
	}
	if page.Total < 1 || server.Exists(store.eventMetricWindowKey(corruptID)) {
		t.Fatalf("corrupt window should be read-repaired: %#v", page)
	}
}

func TestBatchSummaryWindowStatusAndRetentionBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	cancelled := payload.BatchStatus{ID: "cancelled", Cancelled: true, CancelledAt: now.Add(-3 * time.Minute)}
	if batchStatusFromEvent(queue.EventBatchCancelled, cancelled) != BatchStatusCancelled ||
		!batchEventOccurredAt(queue.EventBatchCancelled, cancelled).Equal(cancelled.CancelledAt) {
		t.Fatal("cancelled batch event should use cancelled status and timestamp")
	}
	finished := payload.BatchStatus{ID: "finished", Total: 2, Pending: 0, FinishedAt: now.Add(-2 * time.Minute)}
	if batchStatusFromEvent(queue.EventBatchFinished, finished) != BatchStatusFinished ||
		!batchEventOccurredAt(queue.EventBatchFinished, finished).Equal(finished.FinishedAt) {
		t.Fatal("finished batch event should use finished status and timestamp")
	}
	created := payload.BatchStatus{ID: "created", Total: 3, Pending: 3, CreatedAt: now.Add(-time.Minute)}
	if batchStatusFromEvent(queue.EventBatchCreated, created) != BatchStatusRunning ||
		!batchEventOccurredAt(queue.EventBatchCreated, created).Equal(created.CreatedAt) {
		t.Fatal("created batch event should use running status and created timestamp")
	}
	if !batchEventOccurredAt(queue.EventBatchUpdated, created).IsZero() {
		t.Fatal("updated batch event without reliable timestamp should fall back to collector receive time")
	}
	if !batchSummaryRetentionTime(BatchSummary{FlushAt: now.Add(-time.Second)}).Equal(now.Add(-time.Second)) ||
		!batchSummaryRetentionTime(BatchSummary{CreatedAt: now.Add(-2 * time.Second)}).Equal(now.Add(-2*time.Second)) {
		t.Fatal("batch summary retention fallback order changed")
	}

	memory := NewMemoryStore(StoreOptions{Prefix: "batch_retention"})
	if err := memory.SaveBatchSummaries(ctx, []BatchSummary{
		{ID: "old", CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)},
		{ID: "fresh", CreatedAt: now, UpdatedAt: now},
	}, time.Hour); err != nil {
		t.Fatalf("save memory batch summaries: %v", err)
	}
	memoryItems, err := memory.Batches(ctx, "")
	if err != nil {
		t.Fatalf("read memory batches: %v", err)
	}
	if len(memoryItems) != 1 || memoryItems[0].ID != "fresh" {
		t.Fatalf("memory retention should trim old batches: %#v", memoryItems)
	}

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	redisStore := NewRedisStoreFromClient(client, StoreOptions{Prefix: "batch_retention", HeartbeatTTL: time.Minute})
	if err := redisStore.SaveBatchSummaries(ctx, []BatchSummary{
		{ID: "old", CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)},
		{ID: "fresh", CreatedAt: now, UpdatedAt: now},
	}, time.Hour); err != nil {
		t.Fatalf("save redis batch summaries: %v", err)
	}
	redisItems, err := redisStore.Batches(ctx, "")
	if err != nil {
		t.Fatalf("read redis batches: %v", err)
	}
	if len(redisItems) != 1 || redisItems[0].ID != "fresh" || server.Exists(redisStore.batchKey("old")) {
		t.Fatalf("redis retention should trim old batches: %#v", redisItems)
	}
	if err := redisStore.SaveBatchSummaries(ctx, []BatchSummary{{ID: " "}}, time.Hour); err == nil {
		t.Fatal("empty batch id should fail in redis batch summary batch writer")
	}
}

func TestRedisHighValueAndDiagnosticsCorruptAndRetentionBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "diag_branches", HeartbeatTTL: time.Minute})

	oldDetail := HighValueJobDetail{ID: "old", Kind: HighValueDetailFailed, OccurredAt: now.Add(-2 * time.Hour)}
	freshDetail := HighValueJobDetail{ID: "fresh", Kind: HighValueDetailFailed, OccurredAt: now}
	if err := store.SaveHighValueDetails(ctx, []HighValueJobDetail{oldDetail, freshDetail}, time.Hour); err != nil {
		t.Fatalf("save details: %v", err)
	}
	details, err := store.HighValueDetails(ctx, HighValueDetailQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read details: %v", err)
	}
	if details.Total != 1 || details.Items[0].ID != "fresh" || server.Exists(store.highValueDetailKey("old")) {
		t.Fatalf("detail retention failed: %#v", details)
	}
	if empty, ok, err := store.HighValueDetail(ctx, ""); err != nil || ok || empty.ID != "" {
		t.Fatalf("empty detail lookup should miss: %#v %v %v", empty, ok, err)
	}
	if err := client.Set(ctx, store.highValueDetailKey("bad"), "{bad-json", 0).Err(); err != nil {
		t.Fatalf("seed corrupt detail: %v", err)
	}
	if err := client.ZAdd(ctx, store.highValueDetailsKey(), redis.Z{Score: float64(now.Add(time.Minute).UnixNano()), Member: "bad"}).Err(); err != nil {
		t.Fatalf("seed corrupt detail index: %v", err)
	}
	if _, err := store.HighValueDetails(ctx, HighValueDetailQuery{Page: PageRequest{Page: 1, PageSize: 10}}); err != nil {
		t.Fatalf("corrupt detail should be skipped: %v", err)
	}

	if err := store.SaveObservabilityDiagnostics(ctx, []ObservabilityDiagnostic{
		{Reason: "old", Count: 1, ObservedAt: now.Add(-2 * time.Hour)},
		{Reason: MemoryDropBufferFull, Count: 2, ObservedAt: now},
	}, time.Hour); err != nil {
		t.Fatalf("save diagnostics: %v", err)
	}
	diagnostics, err := store.ObservabilityDiagnostics(ctx, PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("read diagnostics: %v", err)
	}
	if diagnostics.Total != 1 || diagnostics.Items[0].Reason != MemoryDropBufferFull {
		t.Fatalf("diagnostic retention failed: %#v", diagnostics)
	}
	badID := "bad-diagnostic"
	if err := client.Set(ctx, store.observabilityDiagnosticKey(badID), "{bad-json", 0).Err(); err != nil {
		t.Fatalf("seed corrupt diagnostic: %v", err)
	}
	if err := client.ZAdd(ctx, store.observabilityDiagnosticsKey(), redis.Z{Score: float64(now.Add(time.Minute).UnixNano()), Member: badID}).Err(); err != nil {
		t.Fatalf("seed corrupt diagnostic index: %v", err)
	}
	if _, err := store.ObservabilityDiagnostics(ctx, PageRequest{Page: 1, PageSize: 10}); err != nil {
		t.Fatalf("corrupt diagnostic should be skipped: %v", err)
	}
	if server.Exists(store.observabilityDiagnosticKey(badID)) {
		t.Fatal("corrupt diagnostic entity should be removed")
	}
}

func TestRuntimeCommandBranches(t *testing.T) {
	// 需求背景：运行时命令需要输出 memory store 非生产提示，并正确处理空 supervisor 列表与
	// supervisor 级 pause/continue 控制标记。
	nilStatusCommand := horizoncmd.NewStatusCommand(nil)
	if err := nilStatusCommand.Handle(runtimeCommandContext(nilStatusCommand, runtimeInput{}, io.Discard)); !errors.Is(err, horizoncmd.ErrRuntimeNotConfigured) {
		t.Fatalf("expected nil loader error, got %v", err)
	}
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, _ := NewManager(Config{Store: "memory", HeartbeatTTL: time.Minute}, WithStoreFactory(staticStoreResolver{store: store}))
	load := func() (*Manager, error) { return manager, nil }
	if output := runHorizonCommand(t, horizoncmd.NewSupervisorsCommand(newRuntimeLoader(load)), runtimeInput{}); !strings.Contains(output, "No supervisors are running.") {
		t.Fatalf("expected empty supervisors output, got %q", output)
	}
	runHorizonCommand(t, horizoncmd.NewPauseSupervisorCommand(newRuntimeLoader(load)), runtimeInput{args: map[string][]string{"name": {"s1"}}})
	control, _ := store.Control(context.Background())
	if !control.PausedSupervisors["s1"] {
		t.Fatalf("expected supervisor pause flag, got %#v", control)
	}
	runHorizonCommand(t, horizoncmd.NewContinueSupervisorCommand(newRuntimeLoader(load)), runtimeInput{args: map[string][]string{"name": {"s1"}}})
	control, _ = store.Control(context.Background())
	if control.PausedSupervisors["s1"] {
		t.Fatalf("expected supervisor pause flag cleared, got %#v", control)
	}
	var stderr bytes.Buffer
	statusCommand := horizoncmd.NewStatusCommand(newRuntimeLoader(load))
	ctx := console.NewCommandContext(context.Background(), statusCommand, *statusCommand.Definition(), runtimeInput{}, console.NewIO(strings.NewReader(""), io.Discard, &stderr), nil, nil)
	if err := statusCommand.Handle(ctx); err == nil || !strings.Contains(err.Error(), "status is inactive") {
		t.Fatalf("expected inactive status with memory warning, got %v", err)
	}
	if !strings.Contains(stderr.String(), horizoncmd.MemoryStoreWarning) {
		t.Fatalf("expected memory warning, got %q", stderr.String())
	}
}

// testSleepProcess 创建一个 sleep 子进程用于测试进程终止逻辑。
//
// 使用方式：测试调用 Start() 获取 pid 后通过 OSProcessInspector.Terminate 终止，
// 最后调用 Wait() 回收。
func testSleepProcess(t *testing.T) *exec.Cmd {
	t.Helper()
	return exec.Command("sleep", "60")
}

func reloadConfigFacadeForTest(t *testing.T, registry *container.Container) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte{}, 0o644); err != nil {
		return err
	}
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(path); err != nil {
		return err
	}
	return registry.Instance("config.default", cfg)
}
