//go:build linux

package process

import (
	"context"
	"errors"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestObserverComputesCPUPercentFromSystemTicks 验证 CPU 百分比使用系统总 tick 变化量计算，
// 不依赖固定 USER_HZ 常量。
func TestObserverComputesCPUPercentFromSystemTicks(t *testing.T) {
	reader := newSequenceProcessFileReader(map[string][]string{
		"/proc/meminfo": {"MemTotal:       1024 kB\n"},
		"/proc/stat": {
			"cpu  1000 0 0 0 0 0 0 0 0 0\n",
			"cpu  1400 0 0 0 0 0 0 0 0 0\n",
		},
		"/proc/123/stat": {
			procStatLine(123, 100, 50, 10),
			procStatLine(123, 150, 50, 10),
		},
	})
	restore := stubProcessFileReader(t, reader.read)
	defer restore()

	observer := NewObserver(ObserverOptions{SampleWindow: time.Millisecond})
	snapshots, err := observer.Observe(context.Background(), []int{123})
	if err != nil {
		t.Fatalf("observe pid: %v", err)
	}
	snapshot := snapshots[123]
	if snapshot.CPUPercent.Status != StatusAvailable {
		t.Fatalf("cpu metric = %#v, want available", snapshot.CPUPercent)
	}
	got, ok := snapshot.CPUPercent.Value.(float64)
	if !ok {
		t.Fatalf("cpu metric value type = %T, want float64", snapshot.CPUPercent.Value)
	}
	want := float64(50) / float64(400) * float64(runtime.NumCPU()) * 100
	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("cpu percent = %v, want %v", got, want)
	}
}

// TestObserverReadsSystemMemoryOncePerObserve 验证同一轮采样内 MemTotal 只读取一次，避免每个 PID 重复读 /proc/meminfo。
func TestObserverReadsSystemMemoryOncePerObserve(t *testing.T) {
	reader := newSequenceProcessFileReader(map[string][]string{
		"/proc/meminfo": {"MemTotal:       2048 kB\n"},
		"/proc/stat": {
			"cpu  1000 0 0 0 0 0 0 0 0 0\n",
			"cpu  1200 0 0 0 0 0 0 0 0 0\n",
		},
		"/proc/101/stat": {
			procStatLine(101, 10, 10, 5),
			procStatLine(101, 20, 10, 5),
		},
		"/proc/202/stat": {
			procStatLine(202, 30, 10, 7),
			procStatLine(202, 40, 10, 7),
		},
	})
	restore := stubProcessFileReader(t, reader.read)
	defer restore()

	observer := NewObserver(ObserverOptions{SampleWindow: time.Millisecond})
	if _, err := observer.Observe(context.Background(), []int{101, 202}); err != nil {
		t.Fatalf("observe pids: %v", err)
	}
	if got := reader.calls("/proc/meminfo"); got != 1 {
		t.Fatalf("meminfo reads = %d, want 1", got)
	}
}

// TestObserverReturnsMemoryAndCPUUnavailableWhenSystemFilesFail 验证系统总量文件缺失时，只降级对应指标，不伪造可用值。
func TestObserverReturnsMemoryAndCPUUnavailableWhenSystemFilesFail(t *testing.T) {
	restore := stubProcessFileReader(t, func(path string) ([]byte, error) {
		switch path {
		case "/proc/meminfo", "/proc/stat":
			return nil, os.ErrNotExist
		case "/proc/123/stat":
			return []byte(procStatLine(123, 10, 5, 8)), nil
		default:
			return nil, errors.New("unexpected path: " + path)
		}
	})
	defer restore()

	observer := NewObserver(ObserverOptions{SampleWindow: time.Millisecond})
	snapshots, err := observer.Observe(context.Background(), []int{123})
	if err != nil {
		t.Fatalf("observe pid: %v", err)
	}
	snapshot := snapshots[123]
	if snapshot.MemoryRSSBytes.Status != StatusAvailable {
		t.Fatalf("rss metric = %#v, want available", snapshot.MemoryRSSBytes)
	}
	if snapshot.MemoryPercent.Status != StatusUnavailable || snapshot.MemoryPercent.Reason != "system total memory unavailable" {
		t.Fatalf("memory percent = %#v, want unavailable system total memory", snapshot.MemoryPercent)
	}
	if snapshot.CPUPercent.Status != StatusUnavailable || snapshot.CPUPercent.Reason != "system cpu sample unavailable" {
		t.Fatalf("cpu percent = %#v, want unavailable system cpu sample", snapshot.CPUPercent)
	}
}

// TestObserverReturnsWithoutSecondSampleWhenContextCanceled 验证采样等待被取消时直接返回首轮结果，不进入第二次 stat 读取。
func TestObserverReturnsWithoutSecondSampleWhenContextCanceled(t *testing.T) {
	reader := newSequenceProcessFileReader(map[string][]string{
		"/proc/meminfo":  {"MemTotal:       1024 kB\n"},
		"/proc/stat":     {"cpu  1000 0 0 0 0 0 0 0 0 0\n"},
		"/proc/123/stat": {procStatLine(123, 10, 5, 9)},
	})
	restore := stubProcessFileReader(t, reader.read)
	defer restore()

	observer := NewObserver(ObserverOptions{SampleWindow: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()
	snapshots, err := observer.Observe(ctx, []int{123})
	if err != nil {
		t.Fatalf("observe pid with canceled ctx: %v", err)
	}
	snapshot := snapshots[123]
	if snapshot.CPUPercent.Status != StatusUnavailable {
		t.Fatalf("cpu metric = %#v, want unavailable when canceled before second sample", snapshot.CPUPercent)
	}
	if got := reader.calls("/proc/123/stat"); got != 1 {
		t.Fatalf("stat reads = %d, want 1 when canceled before second sample", got)
	}
}

// TestSelfSnapshotUsesSharedMemoryReader 验证当前进程自省路径复用同一套总内存读取逻辑。
func TestSelfSnapshotUsesSharedMemoryReader(t *testing.T) {
	pid := os.Getpid()
	reader := newSequenceProcessFileReader(map[string][]string{
		"/proc/meminfo":                     {"MemTotal:       4096 kB\n"},
		"/proc/" + intString(pid) + "/stat": {procStatLine(pid, 12, 8, 6)},
	})
	restore := stubProcessFileReader(t, reader.read)
	defer restore()

	snapshot := SelfSnapshot()
	if snapshot.MemoryRSSBytes.Status != StatusAvailable {
		t.Fatalf("rss metric = %#v, want available", snapshot.MemoryRSSBytes)
	}
	if snapshot.MemoryPercent.Status != StatusAvailable {
		t.Fatalf("memory percent = %#v, want available", snapshot.MemoryPercent)
	}
	if snapshot.CPUPercent.Status != StatusUnavailable || snapshot.CPUPercent.Reason == "" {
		t.Fatalf("cpu percent = %#v, want unavailable with stable reason", snapshot.CPUPercent)
	}
}

// TestSelfSnapshotReturnsUnavailableMemoryWhenCurrentProcessStatFails 验证当前进程 stat 读取失败时，heartbeat 自省字段稳定降级。
func TestSelfSnapshotReturnsUnavailableMemoryWhenCurrentProcessStatFails(t *testing.T) {
	restore := stubProcessFileReader(t, func(path string) ([]byte, error) {
		if path == "/proc/"+intString(os.Getpid())+"/stat" {
			return nil, os.ErrNotExist
		}
		return nil, errors.New("unexpected path: " + path)
	})
	defer restore()

	snapshot := SelfSnapshot()
	if snapshot.MemoryRSSBytes.Status != StatusUnavailable || snapshot.MemoryRSSBytes.Reason != "current process rss unavailable" {
		t.Fatalf("rss metric = %#v, want current process unavailable", snapshot.MemoryRSSBytes)
	}
	if snapshot.MemoryPercent.Status != StatusUnavailable || snapshot.MemoryPercent.Reason != "current process memory percent unavailable" {
		t.Fatalf("memory percent = %#v, want current process unavailable", snapshot.MemoryPercent)
	}
}

// TestReadProcStatRejectsInvalidShape 验证 /proc/<pid>/stat 形状异常时返回解析错误，而不是静默使用错误字段位。
func TestReadProcStatRejectsInvalidShape(t *testing.T) {
	restore := stubProcessFileReader(t, func(path string) ([]byte, error) {
		if path != "/proc/7/stat" {
			return nil, errors.New("unexpected path: " + path)
		}
		return []byte("7 test-proc R 1 1 1\n"), nil
	})
	defer restore()

	if _, err := readProcStat(7); err == nil {
		t.Fatalf("readProcStat should fail for invalid stat shape")
	}
}

// TestReadTotalCPUTicksRejectsInvalidLine 验证 /proc/stat 首行异常时返回错误，避免产生伪 CPU 百分比。
func TestReadTotalCPUTicksRejectsInvalidLine(t *testing.T) {
	restore := stubProcessFileReader(t, func(path string) ([]byte, error) {
		if path != "/proc/stat" {
			return nil, errors.New("unexpected path: " + path)
		}
		return []byte("intr 1 2 3\n"), nil
	})
	defer restore()

	if _, err := readTotalCPUTicks(); err == nil {
		t.Fatalf("readTotalCPUTicks should fail for invalid cpu line")
	}
}

// TestReadTotalMemoryBytesRejectsInvalidMemTotal 验证 MemTotal 字段缺失或异常时返回错误，交给上层降级 memory_percent。
func TestReadTotalMemoryBytesRejectsInvalidMemTotal(t *testing.T) {
	restore := stubProcessFileReader(t, func(path string) ([]byte, error) {
		if path != "/proc/meminfo" {
			return nil, errors.New("unexpected path: " + path)
		}
		return []byte("MemFree: 10 kB\n"), nil
	})
	defer restore()

	if _, err := readTotalMemoryBytes(); err == nil {
		t.Fatalf("readTotalMemoryBytes should fail when MemTotal is missing")
	}
}

func procStatLine(pid int, utime uint64, stime uint64, rss int64) string {
	return strings.Join([]string{
		intString(pid),
		"(test-proc)",
		"R",
		"1", "1", "1", "0", "0", "0", "0", "0", "0", "0",
		uintString(utime),
		uintString(stime),
		"0", "0", "0", "0", "1", "0", "0", "0", "0", "0", "0",
		int64String(rss),
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
	}, " ") + "\n"
}

func intString(v int) string {
	return strconvItoa(v)
}

func uintString(v uint64) string {
	return strconvFormatUint(v)
}

func int64String(v int64) string {
	return strconvFormatInt(v)
}

type sequenceProcessFileReader struct {
	mu       sync.Mutex
	content  map[string][][]byte
	counters map[string]int
	callsMap map[string]int
}

func newSequenceProcessFileReader(raw map[string][]string) *sequenceProcessFileReader {
	content := make(map[string][][]byte, len(raw))
	for path, values := range raw {
		content[path] = make([][]byte, 0, len(values))
		for _, value := range values {
			content[path] = append(content[path], []byte(value))
		}
	}
	return &sequenceProcessFileReader{
		content:  content,
		counters: make(map[string]int, len(raw)),
		callsMap: make(map[string]int, len(raw)),
	}
}

func (r *sequenceProcessFileReader) read(path string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.callsMap[path]++
	values, ok := r.content[path]
	if !ok || len(values) == 0 {
		return nil, errors.New("unexpected path: " + path)
	}
	index := r.counters[path]
	if index >= len(values) {
		index = len(values) - 1
	}
	r.counters[path]++
	buf := make([]byte, len(values[index]))
	copy(buf, values[index])
	return buf, nil
}

func (r *sequenceProcessFileReader) calls(path string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.callsMap[path]
}

func stubProcessFileReader(t *testing.T, reader func(string) ([]byte, error)) func() {
	t.Helper()
	original := readProcessFile
	readProcessFile = reader
	return func() {
		readProcessFile = original
	}
}

func strconvItoa(v int) string {
	return strconv.Itoa(v)
}

func strconvFormatUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func strconvFormatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}
