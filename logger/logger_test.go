package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/sirupsen/logrus"
)

// TestManagerDailyWritesDatedFile 验证当前多通道初始化方式能把 daily 日志落到按日期拼接的文件里。
func TestManagerDailyWritesDatedFile(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app.log")

	m, err := NewManager(Config{
		Default: "app",
		Channels: map[string]ChannelOptions{
			"app": {
				Driver: "daily",
				Path:   base,
				Level:  "info",
				Now:    func() time.Time { return time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC) },
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	m.Default().Info("daily log")

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	var found string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "app-2026-04-20") {
			found = e.Name()
			break
		}
	}
	if found == "" {
		t.Fatalf("expected dated log file, got: %v", entries)
	}
}

// TestNewChannelRejectsInvalidLevel 验证通道构造仍会拒绝非法日志级别。
func TestNewChannelRejectsInvalidLevel(t *testing.T) {
	dir := t.TempDir()
	_, err := newChannel("bad", ChannelOptions{Driver: "single", Path: filepath.Join(dir, "app.log"), Level: "not-a-level"}, nil)
	if err == nil {
		t.Fatalf("expected parse level error")
	}
}

// TestDailyDriverRotatesOnDateChange 验证跨日会切换到新日期文件。
func TestDailyDriverRotatesOnDateChange(t *testing.T) {
	dir := t.TempDir()
	clock := time.Date(2026, 4, 20, 23, 59, 0, 0, time.UTC)
	now := func() time.Time { return clock }

	d, err := newDailyDriver(ChannelOptions{Path: filepath.Join(dir, "r.log"), Now: now})
	if err != nil {
		t.Fatalf("newDailyDriver: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if _, err := d.Write([]byte("day1\n")); err != nil {
		t.Fatalf("write day1: %v", err)
	}
	clock = clock.Add(2 * time.Minute) // 进入次日
	if _, err := d.Write([]byte("day2\n")); err != nil {
		t.Fatalf("write day2: %v", err)
	}

	day1 := filepath.Join(dir, "r-2026-04-20.log")
	day2 := filepath.Join(dir, "r-2026-04-21.log")
	if b, err := os.ReadFile(day1); err != nil || !strings.Contains(string(b), "day1") {
		t.Fatalf("day1 file mismatch: %s %v", string(b), err)
	}
	if b, err := os.ReadFile(day2); err != nil || !strings.Contains(string(b), "day2") {
		t.Fatalf("day2 file mismatch: %s %v", string(b), err)
	}
}

// TestDailyDriverWriteAfterCloseReturnsClosed 验证 daily 驱动关闭后进入终态。
// 需求背景：Manager.Close 后不能因为后续误写日志而重新打开日期文件，否则进程退出阶段会泄漏新文件句柄。
func TestDailyDriverWriteAfterCloseReturnsClosed(t *testing.T) {
	dir := t.TempDir()
	clock := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	d, err := newDailyDriver(ChannelOptions{
		Path: filepath.Join(dir, "daily.log"),
		Now:  func() time.Time { return clock },
	})
	if err != nil {
		t.Fatalf("newDailyDriver: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	clock = clock.Add(24 * time.Hour)
	n, err := d.Write([]byte("late write\n"))
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed after close, got n=%d err=%v", n, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "daily-2026-04-21.log")); !os.IsNotExist(err) {
		t.Fatalf("closed daily driver reopened next-day file, stat err=%v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second close should be idempotent: %v", err)
	}
}

// TestSingleDriverAppends 验证 single 驱动把多次写入追加到同一文件。
func TestSingleDriverAppends(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "single.log")
	d, err := newSingleDriver(ChannelOptions{Path: path})
	if err != nil {
		t.Fatalf("newSingleDriver: %v", err)
	}
	if _, err := d.Write([]byte("a\n")); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if _, err := d.Write([]byte("b\n")); err != nil {
		t.Fatalf("write b: %v", err)
	}
	_ = d.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := string(b); !strings.Contains(got, "a\n") || !strings.Contains(got, "b\n") {
		t.Fatalf("single append mismatch: %q", got)
	}
}

// TestSingleDriverWriteAfterCloseReturnsClosed 验证 single 驱动关闭后写入返回标准关闭错误。
// 设计思路：关闭后继续写属于生命周期错误，调用方可用 errors.Is(err, os.ErrClosed) 统一判断。
func TestSingleDriverWriteAfterCloseReturnsClosed(t *testing.T) {
	dir := t.TempDir()
	d, err := newSingleDriver(ChannelOptions{Path: filepath.Join(dir, "single.log")})
	if err != nil {
		t.Fatalf("newSingleDriver: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	n, err := d.Write([]byte("late write\n"))
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed after close, got n=%d err=%v", n, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second close should be idempotent: %v", err)
	}
}

func TestRelativeLogPathResolvesFromProjectRoot(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "prismgo", "foundation")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested package dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.work"), []byte("go 1.26.2\n"), 0o644); err != nil {
		t.Fatalf("write go.work marker: %v", err)
	}
	t.Chdir(nested)

	m, err := NewManager(Config{
		Default: "app",
		Channels: map[string]ChannelOptions{
			"app": {Driver: "single", Level: "info", Path: "storage/logs/app.log"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	m.Default().Info("root relative log")

	rootLog := filepath.Join(root, "storage", "logs", "app.log")
	if b, err := os.ReadFile(rootLog); err != nil || !strings.Contains(string(b), "root relative log") {
		t.Fatalf("expected root log at %s, got %q err=%v", rootLog, string(b), err)
	}
	nestedLog := filepath.Join(nested, "storage", "logs", "app.log")
	if _, err := os.Stat(nestedLog); err == nil {
		t.Fatalf("relative log path leaked into package cwd: %s", nestedLog)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat nested log: %v", err)
	}
}

// TestNullDriverDiscardsAll 验证 null 驱动不写任何内容。
func TestNullDriverDiscardsAll(t *testing.T) {
	d := newNullDriver()
	n, err := d.Write([]byte("whatever"))
	if err != nil || n != len("whatever") {
		t.Fatalf("null driver should accept all bytes silently, got %d %v", n, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("null close: %v", err)
	}
}

// TestManagerClosePreventsLateChannelRecreation 验证同一个 Manager 关闭后不会惰性复活文件通道。
// 需求背景：服务退出时 Close 之后仍可能有异步日志误写，必须丢弃而不是重新创建文件句柄。
func TestManagerClosePreventsLateChannelRecreation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	m, err := NewManager(Config{
		Default: "app",
		Channels: map[string]ChannelOptions{
			"app": {Driver: "single", Level: "info", Path: path},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.Default().Info("before close")
	if err := m.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove closed log file: %v", err)
	}

	m.Default().Info("after close")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("closed manager recreated log file, stat err=%v", err)
	}
}

// TestManagerChannelAndDefault 验证 Channel/Default 能路由到正确的通道，未知通道回退 default。
func TestManagerChannelAndDefault(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(Config{
		Default: "app",
		Channels: map[string]ChannelOptions{
			"app":   {Driver: "single", Level: "info", Path: filepath.Join(dir, "app.log")},
			"error": {Driver: "single", Level: "warn", Path: filepath.Join(dir, "error.log")},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	m.Default().Info("hello-default")
	m.Channel("error").Error("bye-error")
	m.Channel("unknown").Info("falls-back") // 应写到 app

	appBytes, _ := os.ReadFile(filepath.Join(dir, "app.log"))
	errBytes, _ := os.ReadFile(filepath.Join(dir, "error.log"))
	if !strings.Contains(string(appBytes), "hello-default") {
		t.Fatalf("default channel missing message: %s", appBytes)
	}
	if !strings.Contains(string(appBytes), "falls-back") {
		t.Fatalf("unknown channel did not fall back to default: %s", appBytes)
	}
	if !strings.Contains(string(errBytes), "bye-error") {
		t.Fatalf("error channel missing message: %s", errBytes)
	}
}

// TestStackFanOut 验证 stack 通道把日志扇出到所有子通道。
func TestStackFanOut(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(Config{
		Default: "stack",
		Channels: map[string]ChannelOptions{
			"a":     {Driver: "single", Level: "info", Path: filepath.Join(dir, "a.log")},
			"b":     {Driver: "single", Level: "info", Path: filepath.Join(dir, "b.log")},
			"stack": {Driver: "stack", Channels: []string{"a", "b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	m.Default().Info("stacked")
	for _, name := range []string{"a.log", "b.log"} {
		b, _ := os.ReadFile(filepath.Join(dir, name))
		if !strings.Contains(string(b), "stacked") {
			t.Fatalf("%s missing message: %s", name, b)
		}
	}
}

// TestWithFieldAndWithError 验证字段和错误会出现在序列化输出里。
func TestWithFieldAndWithError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ctx.log")
	m, err := NewManager(Config{
		Default: "x",
		Channels: map[string]ChannelOptions{
			"x": {Driver: "single", Level: "info", Path: path},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	m.Default().WithField("uid", 42).WithError(errors.New("bad")).Error("boom")
	b, _ := os.ReadFile(path)
	got := string(b)
	if !strings.Contains(got, `{"error":"bad","uid":42}`) && !strings.Contains(got, `{"uid":42,"error":"bad"}`) {
		t.Fatalf("field or error message not rendered: %s", got)
	}
	if !strings.Contains(got, "[stacktrace]") || !strings.Contains(got, "TestWithFieldAndWithError") {
		t.Fatalf("error stacktrace not rendered: %s", got)
	}
}

// TestJSONFormatter 验证 json formatter 输出合法 JSON 且包含字段。
func TestJSONFormatter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "json.log")
	m, err := NewManager(Config{
		Default: "j",
		Channels: map[string]ChannelOptions{
			"j": {Driver: "single", Formatter: "json", Level: "info", Path: path},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	m.Default().WithField("tenant", 7).Info("hi")
	b, _ := os.ReadFile(path)
	// 每条 JSON 一行，取第一行解析。
	line := strings.SplitN(strings.TrimSpace(string(b)), "\n", 2)[0]
	var row map[string]any
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		t.Fatalf("not valid json: %s", line)
	}
	if row["msg"] != "hi" {
		t.Fatalf("msg missing: %v", row)
	}
	if v, ok := row["tenant"].(float64); !ok || v != 7 {
		t.Fatalf("field missing: %v", row)
	}
}

// TestWithContextExtractsFieldsToJSON 验证 Manager 级 ContextExtractor 会把 ctx 中的白名单字段写入结构化日志。
// 需求背景：业务请求链路通常需要 request_id/trace_id 等关联字段，但不能默认把整个 ctx 隐式写入日志。
func TestWithContextExtractsFieldsToJSON(t *testing.T) {
	type contextKey string

	dir := t.TempDir()
	path := filepath.Join(dir, "context.log")
	m, err := NewManager(Config{
		Default: "app",
		ContextExtractor: func(ctx context.Context) map[string]any {
			return map[string]any{"request_id": ctx.Value(contextKey("request_id"))}
		},
		Channels: map[string]ChannelOptions{
			"app": {Driver: "single", Formatter: "json", Level: "info", Path: path},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.WithValue(context.Background(), contextKey("request_id"), "req-123")
	m.Default().WithContext(ctx).Info("context log")

	row := readFirstJSONLogRow(t, path)
	if row["request_id"] != "req-123" {
		t.Fatalf("context field missing: %v", row)
	}
}

// TestEntryWithContextPreservesExistingFields 验证 entry 已附加的字段不会被 WithContext 派生过程覆盖。
// 设计思路：WithContext 只补充 ctx 关联字段，应与 WithField/WithError 属于同一条结构化日志上下文链。
func TestEntryWithContextPreservesExistingFields(t *testing.T) {
	type contextKey string

	dir := t.TempDir()
	path := filepath.Join(dir, "entry-context.log")
	m, err := NewManager(Config{
		Default: "app",
		ContextExtractor: func(ctx context.Context) map[string]any {
			return map[string]any{"request_id": ctx.Value(contextKey("request_id"))}
		},
		Channels: map[string]ChannelOptions{
			"app": {Driver: "single", Formatter: "json", Level: "info", Path: path},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.WithValue(context.Background(), contextKey("request_id"), "req-456")
	m.Default().WithField("tenant_id", 7).WithContext(ctx).Info("merged context")

	row := readFirstJSONLogRow(t, path)
	if row["request_id"] != "req-456" {
		t.Fatalf("context field missing: %v", row)
	}
	if v, ok := row["tenant_id"].(float64); !ok || v != 7 {
		t.Fatalf("existing field missing: %v", row)
	}
}

// TestStackWithContextFanOut 验证 stack 通道会把 ctx 字段扇出到所有子通道。
// 设计思路：stack 子通道各自拥有独立 formatter/level/driver，因此必须逐个派生 WithContext，而不是共享 writer。
func TestStackWithContextFanOut(t *testing.T) {
	type contextKey string

	dir := t.TempDir()
	m, err := NewManager(Config{
		Default: "stack",
		ContextExtractor: func(ctx context.Context) map[string]any {
			return map[string]any{"trace_id": ctx.Value(contextKey("trace_id"))}
		},
		Channels: map[string]ChannelOptions{
			"a":     {Driver: "single", Formatter: "json", Level: "info", Path: filepath.Join(dir, "a.log")},
			"b":     {Driver: "single", Formatter: "json", Level: "info", Path: filepath.Join(dir, "b.log")},
			"stack": {Driver: "stack", Channels: []string{"a", "b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.WithValue(context.Background(), contextKey("trace_id"), "trace-789")
	m.Default().WithContext(ctx).Info("stack context")

	for _, name := range []string{"a.log", "b.log"} {
		row := readFirstJSONLogRow(t, filepath.Join(dir, name))
		if row["trace_id"] != "trace-789" {
			t.Fatalf("%s context field missing: %v", name, row)
		}
	}
}

// TestChannelContextExtractorOverridesManagerDefault 验证通道级 extractor 优先级高于 Manager 默认 extractor。
// 需求背景：不同通道可能有不同日志字段安全策略，例如审计通道和普通业务通道允许记录的字段不同。
func TestChannelContextExtractorOverridesManagerDefault(t *testing.T) {
	type contextKey string

	dir := t.TempDir()
	path := filepath.Join(dir, "channel-context.log")
	m, err := NewManager(Config{
		Default: "app",
		ContextExtractor: func(context.Context) map[string]any {
			return map[string]any{"request_id": "manager"}
		},
		Channels: map[string]ChannelOptions{
			"app": {
				Driver:    "single",
				Formatter: "json",
				Level:     "info",
				Path:      path,
				ContextExtractor: func(ctx context.Context) map[string]any {
					return map[string]any{"request_id": ctx.Value(contextKey("request_id"))}
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.WithValue(context.Background(), contextKey("request_id"), "channel")
	m.Default().WithContext(ctx).Info("channel context")

	row := readFirstJSONLogRow(t, path)
	if row["request_id"] != "channel" {
		t.Fatalf("channel extractor did not override manager default: %v", row)
	}
}

// TestWithContextNilAndEmptyExtractorAreNoops 验证 nil ctx 与空提取结果都是安全 no-op。
// 设计思路：WithContext 是链式日志增强能力，调用方不应因为未配置 extractor 或 ctx 为空而触发 panic。
func TestWithContextNilAndEmptyExtractorAreNoops(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty-context.log")
	m, err := NewManager(Config{
		Default: "app",
		ContextExtractor: func(context.Context) map[string]any {
			return nil
		},
		Channels: map[string]ChannelOptions{
			"app": {Driver: "single", Formatter: "json", Level: "info", Path: path},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	m.Default().WithContext(context.TODO()).Info("nil context")

	row := readFirstJSONLogRow(t, path)
	if _, ok := row["request_id"]; ok {
		t.Fatalf("unexpected context field: %v", row)
	}
	if row["msg"] != "nil context" {
		t.Fatalf("message missing: %v", row)
	}
}

// TestFacadeWithContextRequiresManager 验证 facade 便捷入口在没有当前 Manager 时保持 strict panic。
// 需求背景：logger facade 依赖应用容器完成装配，缺失 Manager 属于启动配置错误，测试应匹配 strict facade 契约。
func TestFacadeWithContextRequiresManager(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("WithContext without manager did not panic")
		}
	}()

	WithContext(context.Background()).Info("strict context")
}

// TestUseSyncsGlobalLogrus 验证 Use 会把默认通道配置同步回全局 logrus，
// 保证尚未迁移的 logrus.Info 调用也能落到默认通道文件。
func TestUseSyncsGlobalLogrus(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	dir := t.TempDir()
	path := filepath.Join(dir, "global.log")
	m, err := NewManager(Config{
		Default: "g",
		Channels: map[string]ChannelOptions{
			"g": {Driver: "single", Level: "info", Path: path},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	// 触发构造以便 syncLogrusStandard 拿到 *channel
	_ = m.Default()
	bindLoggerManagerForTest(t, registry, m)
	t.Cleanup(func() { _ = m.Close() })

	logrus.Info("via-global-logrus")
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "via-global-logrus") {
		t.Fatalf("global logrus did not write to default channel: %s", b)
	}
}

// TestUseSyncsGlobalLogrusStackHonorsChildLevels 验证全局 logrus 走 stack 默认通道时仍由子通道各自过滤级别。
// 设计思路：全局 logrus 只负责把 entry 分发回默认 Logger，不能用 MultiWriter 绕过 Channel 的 level 判断。
func TestUseSyncsGlobalLogrusStackHonorsChildLevels(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	dir := t.TempDir()
	singlePath := filepath.Join(dir, "single.log")
	errorPath := filepath.Join(dir, "error.log")
	m, err := NewManager(Config{
		Default: "stack",
		Channels: map[string]ChannelOptions{
			"single": {Driver: "single", Level: "info", Path: singlePath},
			"error":  {Driver: "single", Level: "warn", Path: errorPath},
			"stack":  {Driver: "stack", Channels: []string{"single", "error"}},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	bindLoggerManagerForTest(t, registry, m)
	t.Cleanup(func() {
		_ = m.Close()
		syncLogrusStandard(nil)
	})

	logrus.Info("global-info")
	logrus.Warn("global-warn")

	singleBytes, _ := os.ReadFile(singlePath)
	errorBytes, _ := os.ReadFile(errorPath)
	if !strings.Contains(string(singleBytes), "global-info") || !strings.Contains(string(singleBytes), "global-warn") {
		t.Fatalf("single channel should receive info and warn: %s", singleBytes)
	}
	if strings.Contains(string(errorBytes), "global-info") {
		t.Fatalf("warn channel should not receive info through stack: %s", errorBytes)
	}
	if !strings.Contains(string(errorBytes), "global-warn") {
		t.Fatalf("warn channel missing warn through stack: %s", errorBytes)
	}
}

func readFirstJSONLogRow(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	line := strings.SplitN(strings.TrimSpace(string(b)), "\n", 2)[0]
	var row map[string]any
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		t.Fatalf("not valid json: %s", line)
	}
	return row
}

// mockConfigReader 模拟 config facade 用于 buildConfig 测试。
type mockConfigReader struct {
	getString    func(path string, defaultValue ...any) string
	getStringMap func(path string) map[string]any
}

func (m *mockConfigReader) GetString(path string, defaultValue ...any) string {
	if m.getString != nil {
		return m.getString(path, defaultValue...)
	}
	return ""
}

func (m *mockConfigReader) GetStringMap(path string) map[string]any {
	if m.getStringMap != nil {
		return m.getStringMap(path)
	}
	return nil
}

// TestBuildConfigRejectsNonMapChannel 验证 logging.channels.X 不是 map 时 buildConfig 返回错误。
func TestBuildConfigRejectsNonMapChannel(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if err := registry.Singleton("config.default", func(_ containercontract.Resolver) (any, error) {
		return &mockConfigReader{
			getString: func(path string, defaultValue ...any) string {
				return "app"
			},
			getStringMap: func(path string) map[string]any {
				return map[string]any{
					"app": "not-a-map-string",
				}
			},
		}, nil
	}); err != nil {
		t.Fatalf("register mock config: %v", err)
	}

	_, err := buildConfig()
	if err == nil {
		t.Fatal("expected error for non-map channel config, got nil")
	}
}

// TestNewManagerRejectsMissingDefault 验证 default 未在 Channels 中声明时会拒绝。
func TestNewManagerRejectsMissingDefault(t *testing.T) {
	_, err := NewManager(Config{
		Default:  "nope",
		Channels: map[string]ChannelOptions{"x": {Driver: "null", Level: "info"}},
	})
	if err == nil {
		t.Fatalf("expected error for missing default channel")
	}
}

// TestStderrDriverWrites 验证 stderr 驱动把字节写到 stderr（通过重定向 os.Stderr 捕获）。
func TestStderrDriverWrites(t *testing.T) {
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = old })

	d := newStderrDriver()
	if _, err := d.Write([]byte("hello-stderr\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = w.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "hello-stderr") {
		t.Fatalf("stderr content mismatch: %s", buf.String())
	}
}
