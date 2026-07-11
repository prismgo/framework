package timer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cachepkg "github.com/prismgo/framework/cache"
	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
)

func useTimerRuntimeFacades(t *testing.T) {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	useDebugConfig(t, registry, false)
	if err := registry.Instance("exception.handler", goexception.New(goexception.WithPanicStack(false)), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
}

// TestScheduleCallRunsAtInterval 验证 Schedule.Call 注册的任务按间隔周期性执行。
// 启动后立即执行一次，之后按 interval 重复，因此短暂等待后应至少执行 2 次。
// 示例：s.Call(fn).Every(50 * time.Millisecond)
func TestScheduleCallRunsAtInterval(t *testing.T) {
	useTimerRuntimeFacades(t)
	var count atomic.Int32
	s := NewSchedule()
	s.Call(func(_ context.Context) error {
		count.Add(1)
		return nil
	}).Every(50 * time.Millisecond).Name("tick")

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer s.Stop()
	defer cancel()

	waitForCount(t, &count, 2, time.Second)
	cancel()
	s.Stop()

	got := count.Load()
	if got < 2 {
		t.Errorf("expected task to run at least 2 times, got %d", got)
	}
}

// TestScheduleStopGracefully 验证调度器能正常停止且不 panic。
// 示例：s.Call(fn).Hourly()
func TestScheduleStopGracefully(t *testing.T) {
	useTimerRuntimeFacades(t)
	var count atomic.Int32
	s := NewSchedule()
	s.Call(func(_ context.Context) error {
		count.Add(1)
		return nil
	}).Hourly().Name("once")

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	cancel()
	s.Stop()

	got := count.Load()
	if got < 1 {
		t.Errorf("expected task to run at least 1 time, got %d", got)
	}
}

// TestScheduleErrorContinues 验证任务返回错误时调度器不崩溃，继续执行下一轮。
// 示例：s.Call(fn).Every(50 * time.Millisecond)
func TestScheduleErrorContinues(t *testing.T) {
	useTimerRuntimeFacades(t)
	var count atomic.Int32
	s := NewSchedule()
	s.Call(func(_ context.Context) error {
		count.Add(1)
		return errors.New("simulated failure")
	}).Every(50 * time.Millisecond).Name("fail")

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer s.Stop()
	defer cancel()

	waitForCount(t, &count, 2, time.Second)
	cancel()
	s.Stop()

	got := count.Load()
	if got < 2 {
		t.Errorf("expected task to run at least 2 times despite errors, got %d", got)
	}
}

// TestScheduleDoneLogRequiresAppDebug 验证成功任务完成日志只在 app.debug=true 时输出。
func TestScheduleDoneLogRequiresAppDebug(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() {
		container.SetProvider(nil)
	})

	debug := false
	configpkg.Add("app", func() map[string]any {
		return map[string]any{"debug": debug}
	})
	t.Cleanup(func() {
		container.SetProvider(nil)
	})

	logPath := filepath.Join(t.TempDir(), "schedule.log")
	manager, err := logger.NewManager(logger.Config{
		Default: "app",
		Channels: map[string]logger.ChannelOptions{
			"app": {Driver: "single", Path: logPath, Level: "info"},
		},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		// 显式关闭 logger manager，避免 Windows 上文件句柄未释放导致 TempDir 清理失败
		if err := manager.Close(); err != nil {
			t.Logf("close logger manager: %v", err)
		}
	})

	s := NewSchedule()
	task := newScheduledTask(func(context.Context) error { return nil })
	task.name = "debug-gated"

	useDebugConfig(t, registry, debug)
	s.executeOnce(context.Background(), task)
	assertFileDoesNotContain(t, logPath, "[schedule] task debug-gated done")

	debug = true
	useDebugConfig(t, registry, debug)
	s.executeOnce(context.Background(), task)
	assertFileContains(t, logPath, "[schedule] task debug-gated done")
}

// TestScheduleWithoutOverlappingSkipsConcurrentRun 验证同一任务仍在运行时，本次触发会跳过。
// 示例：s.Call(fn).EveryMinute().WithoutOverlapping()
func TestScheduleWithoutOverlappingSkipsConcurrentRun(t *testing.T) {
	useTimerMemoryCache(t)

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var entered atomic.Int32

	task := NewSchedule().Call(func(_ context.Context) error {
		entered.Add(1)
		started <- struct{}{}
		<-release
		return nil
	}).WithoutOverlapping()

	s := NewSchedule()
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			s.executeOnce(context.Background(), task)
		}()
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected first task run to start")
	}
	time.Sleep(30 * time.Millisecond)
	if got := entered.Load(); got != 1 {
		t.Fatalf("expected overlapping run to be skipped, got %d entries", got)
	}
	close(release)
	wg.Wait()
}

// TestScheduleWithoutOverlappingUsesTaskNameAsLockKey 验证同名的不同任务实例共享防重叠锁。
func TestScheduleWithoutOverlappingUsesTaskNameAsLockKey(t *testing.T) {
	useTimerMemoryCache(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var firstEntered atomic.Int32
	var secondEntered atomic.Int32

	first := NewSchedule().Call(func(_ context.Context) error {
		firstEntered.Add(1)
		started <- struct{}{}
		<-release
		return nil
	}).Name("shared-name").WithoutOverlapping()
	second := NewSchedule().Call(func(_ context.Context) error {
		secondEntered.Add(1)
		return nil
	}).Name("shared-name").WithoutOverlapping()

	s := NewSchedule()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.executeOnce(context.Background(), first)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected first task run to start")
	}
	s.executeOnce(context.Background(), second)
	close(release)
	<-done

	if got := firstEntered.Load(); got != 1 {
		t.Fatalf("expected first task to run once, got %d", got)
	}
	if got := secondEntered.Load(); got != 0 {
		t.Fatalf("expected same-name overlapping task to be skipped, got %d entries", got)
	}
}

// TestScheduleWithoutOverlappingReleasesLock 验证任务结束后锁会释放，后续触发可再次执行。
// 示例：s.Call(fn).Name("sync").WithoutOverlapping()
func TestScheduleWithoutOverlappingReleasesLock(t *testing.T) {
	useTimerMemoryCache(t)

	var entered atomic.Int32
	task := NewSchedule().Call(func(_ context.Context) error {
		entered.Add(1)
		return nil
	}).Name("overlap-release").WithoutOverlapping()

	s := NewSchedule()
	s.executeOnce(context.Background(), task)
	s.executeOnce(context.Background(), task)

	if got := entered.Load(); got != 2 {
		t.Fatalf("expected lock to be released between runs, got %d entries", got)
	}
}

// TestScheduleWithoutOverlappingConfiguresExpiration 验证默认和自定义锁过期分钟配置。
func TestScheduleWithoutOverlappingConfiguresExpiration(t *testing.T) {
	noop := func(_ context.Context) error { return nil }

	defaultTask := NewSchedule().Call(noop).WithoutOverlapping()
	if !defaultTask.withoutOverlapping {
		t.Fatal("expected withoutOverlapping to be enabled")
	}
	if defaultTask.withoutOverlappingExpires != 1440*time.Minute {
		t.Fatalf("default expiration = %v, want 1440m", defaultTask.withoutOverlappingExpires)
	}

	customTask := NewSchedule().Call(noop).WithoutOverlapping(10)
	if customTask.withoutOverlappingExpires != 10*time.Minute {
		t.Fatalf("custom expiration = %v, want 10m", customTask.withoutOverlappingExpires)
	}
}

// TestScheduleWithoutOverlappingPanicsOnInvalidArguments 验证非法过期分钟参数会启动期 panic。
func TestScheduleWithoutOverlappingPanicsOnInvalidArguments(t *testing.T) {
	cases := []struct {
		name string
		fn   func()
	}{
		{"Zero", func() { NewSchedule().Call(func(_ context.Context) error { return nil }).WithoutOverlapping(0) }},
		{"Negative", func() { NewSchedule().Call(func(_ context.Context) error { return nil }).WithoutOverlapping(-1) }},
		{"TooMany", func() { NewSchedule().Call(func(_ context.Context) error { return nil }).WithoutOverlapping(10, 20) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for %s", tc.name)
				}
			}()
			tc.fn()
		})
	}
}

// TestScheduleCommandRegistersTask 验证 Command 方法通过 resolver 注册任务并正确执行。
// 示例：s.Command("test:cmd --flag=42").Every(50 * time.Millisecond)
func TestScheduleCommandRegistersTask(t *testing.T) {
	useTimerRuntimeFacades(t)
	var called atomic.Int32
	var gotArgs []string

	s := NewSchedule()
	s.SetResolver(func(name string, args []string) (ResolvedCommand, error) {
		if name != "test:cmd" {
			t.Errorf("expected name test:cmd, got %s", name)
		}
		gotArgs = args
		return ResolvedCommand{
			Fn: func(_ context.Context) error {
				called.Add(1)
				return nil
			},
			Description: "测试命令",
		}, nil
	})

	task := s.Command("test:cmd --flag=42").Every(50 * time.Millisecond)

	if len(gotArgs) != 1 || gotArgs[0] != "--flag=42" {
		t.Errorf("expected args [--flag=42], got %v", gotArgs)
	}
	if task.name != "test:cmd" {
		t.Errorf("expected task name test:cmd, got %s", task.name)
	}
	if task.description != "测试命令" {
		t.Errorf("expected description 测试命令, got %s", task.description)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer s.Stop()
	defer cancel()

	waitForCount(t, &called, 1, time.Second)
	cancel()
	s.Stop()

	if called.Load() < 1 {
		t.Error("expected command task to run at least once")
	}
}

// TestScheduleCommandNoArgs 验证 Command 方法在无额外参数时正确解析命令名称。
// 示例：s.Command("simple:task")
func TestScheduleCommandNoArgs(t *testing.T) {
	var gotName string
	var gotArgs []string

	s := NewSchedule()
	s.SetResolver(func(name string, args []string) (ResolvedCommand, error) {
		gotName = name
		gotArgs = args
		return ResolvedCommand{
			Fn:          func(_ context.Context) error { return nil },
			Description: "无参命令",
		}, nil
	})

	task := s.Command("simple:task")

	if gotName != "simple:task" {
		t.Errorf("expected name simple:task, got %s", gotName)
	}
	if len(gotArgs) != 0 {
		t.Errorf("expected empty args, got %v", gotArgs)
	}
	if task.name != "simple:task" {
		t.Errorf("expected task name simple:task, got %s", task.name)
	}
	if task.description != "无参命令" {
		t.Errorf("expected description from resolver, got %s", task.description)
	}
}

// TestScheduleCommandMultipleArgs 验证 Command 方法正确解析多个参数。
// 示例：s.Command("my:cmd --take=5 --verbose --mode=strict")
func TestScheduleCommandMultipleArgs(t *testing.T) {
	var gotArgs []string

	s := NewSchedule()
	s.SetResolver(func(_ string, args []string) (ResolvedCommand, error) {
		gotArgs = args
		return ResolvedCommand{
			Fn: func(_ context.Context) error { return nil },
		}, nil
	})

	s.Command("my:cmd --take=5 --verbose --mode=strict")

	expected := []string{"--take=5", "--verbose", "--mode=strict"}
	if len(gotArgs) != len(expected) {
		t.Fatalf("expected %d args, got %d: %v", len(expected), len(gotArgs), gotArgs)
	}
	for i, want := range expected {
		if gotArgs[i] != want {
			t.Errorf("arg[%d]: expected %s, got %s", i, want, gotArgs[i])
		}
	}
}

// TestScheduleCommandMixedArgs 验证 Command 同时携带位置参数和选项时正确解析。
// 示例：s.Command("test-task arg1 arg2 --opt1 --opt2=v1")
func TestScheduleCommandMixedArgs(t *testing.T) {
	var gotName string
	var gotArgs []string

	s := NewSchedule()
	s.SetResolver(func(name string, args []string) (ResolvedCommand, error) {
		gotName = name
		gotArgs = args
		return ResolvedCommand{
			Fn:          func(_ context.Context) error { return nil },
			Description: "混合参数命令",
		}, nil
	})

	s.Command("test-task arv1 arv2 --opt1 --opt2=v1")

	if gotName != "test-task" {
		t.Errorf("expected name test-task, got %s", gotName)
	}

	expected := []string{"arv1", "arv2", "--opt1", "--opt2=v1"}
	if len(gotArgs) != len(expected) {
		t.Fatalf("expected %d args, got %d: %v", len(expected), len(gotArgs), gotArgs)
	}
	for i, want := range expected {
		if gotArgs[i] != want {
			t.Errorf("arg[%d]: expected %s, got %s", i, want, gotArgs[i])
		}
	}
}

// TestScheduleCommandPanicsOnUnknown 验证 Command 在 resolver 返回错误时 panic。
// 示例：s.Command("not:exist")
func TestScheduleCommandPanicsOnUnknown(t *testing.T) {
	s := NewSchedule()
	s.SetResolver(func(name string, _ []string) (ResolvedCommand, error) {
		return ResolvedCommand{}, errors.New("command " + name + " not registered")
	})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for unknown command, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !contains(msg, "not:exist") {
			t.Errorf("panic message should contain command name, got: %s", msg)
		}
	}()

	s.Command("not:exist")
}

// TestScheduleCommandInSummary 验证通过 Command 注册的任务出现在 Summary 输出中。
// 示例：summary := s.Summary()
func TestScheduleCommandInSummary(t *testing.T) {
	s := NewSchedule()
	s.SetResolver(func(_ string, _ []string) (ResolvedCommand, error) {
		return ResolvedCommand{
			Fn:          func(_ context.Context) error { return nil },
			Description: "超时检测",
		}, nil
	})

	s.Command("overtime:detect --take=10").EveryFiveMinutes()

	summary := s.Summary()
	for _, want := range []string{"overtime:detect", "超时检测"} {
		if !contains(summary, want) {
			t.Errorf("summary missing %q:\n%s", want, summary)
		}
	}
}

// TestScheduleCommandErrorContinues 验证通过 Command 注册的任务返回错误时调度器继续运行。
// 示例：s.Command("fail:cmd").Every(50 * time.Millisecond)
func TestScheduleCommandErrorContinues(t *testing.T) {
	useTimerRuntimeFacades(t)
	var count atomic.Int32

	s := NewSchedule()
	s.SetResolver(func(_ string, _ []string) (ResolvedCommand, error) {
		return ResolvedCommand{
			Fn: func(_ context.Context) error {
				count.Add(1)
				return errors.New("command failed")
			},
		}, nil
	})

	s.Command("fail:cmd").Every(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer s.Stop()
	defer cancel()

	waitForCount(t, &count, 2, time.Second)
	cancel()
	s.Stop()

	if count.Load() < 2 {
		t.Errorf("expected command task to run at least 2 times despite errors, got %d", count.Load())
	}
}

// TestScheduleFrequencyMethods 验证固定间隔频率方法设置的 interval 值正确。
// 示例：s.Call(noop).EverySecond()
func TestScheduleFrequencyMethods(t *testing.T) {
	s := NewSchedule()
	noop := func(_ context.Context) error { return nil }

	cases := []struct {
		name     string
		task     *ScheduledTask
		expected time.Duration
		mode     scheduleMode
	}{
		{"EverySecond", s.Call(noop).EverySecond(), time.Second, scheduleModeCalendar},
		{"EveryTwoSeconds", s.Call(noop).EveryTwoSeconds(), 2 * time.Second, scheduleModeCalendar},
		{"EveryFiveSeconds", s.Call(noop).EveryFiveSeconds(), 5 * time.Second, scheduleModeCalendar},
		{"EveryTenSeconds", s.Call(noop).EveryTenSeconds(), 10 * time.Second, scheduleModeCalendar},
		{"EveryFifteenSeconds", s.Call(noop).EveryFifteenSeconds(), 15 * time.Second, scheduleModeCalendar},
		{"EveryTwentySeconds", s.Call(noop).EveryTwentySeconds(), 20 * time.Second, scheduleModeCalendar},
		{"EveryThirtySeconds", s.Call(noop).EveryThirtySeconds(), 30 * time.Second, scheduleModeCalendar},
		{"EveryMinute", s.Call(noop).EveryMinute(), time.Minute, scheduleModeCalendar},
		{"EveryTwoMinutes", s.Call(noop).EveryTwoMinutes(), 2 * time.Minute, scheduleModeCalendar},
		{"EveryThreeMinutes", s.Call(noop).EveryThreeMinutes(), 3 * time.Minute, scheduleModeCalendar},
		{"EveryFourMinutes", s.Call(noop).EveryFourMinutes(), 4 * time.Minute, scheduleModeCalendar},
		{"EveryFiveMinutes", s.Call(noop).EveryFiveMinutes(), 5 * time.Minute, scheduleModeCalendar},
		{"EveryTenMinutes", s.Call(noop).EveryTenMinutes(), 10 * time.Minute, scheduleModeCalendar},
		{"EveryFifteenMinutes", s.Call(noop).EveryFifteenMinutes(), 15 * time.Minute, scheduleModeCalendar},
		{"EveryThirtyMinutes", s.Call(noop).EveryThirtyMinutes(), 30 * time.Minute, scheduleModeCalendar},
		{"Hourly", s.Call(noop).Hourly(), time.Hour, scheduleModeCalendar},
		{"Daily", s.Call(noop).Daily(), 24 * time.Hour, scheduleModeCalendar},
		{"Every(3s)", s.Call(noop).Every(3 * time.Second), 3 * time.Second, scheduleModeInterval},
		{"Weekly", s.Call(noop).Weekly(), 7 * 24 * time.Hour, scheduleModeCalendar},
		{"Monthly", s.Call(noop).Monthly(), 31 * 24 * time.Hour, scheduleModeCalendar},
		{"Quarterly", s.Call(noop).Quarterly(), 90 * 24 * time.Hour, scheduleModeCalendar},
		{"Yearly", s.Call(noop).Yearly(), 365 * 24 * time.Hour, scheduleModeCalendar},
	}
	for _, tc := range cases {
		if tc.task.interval != tc.expected {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expected, tc.task.interval)
		}
		if tc.task.mode != tc.mode {
			t.Errorf("%s: expected mode %v, got %v", tc.name, tc.mode, tc.task.mode)
		}
	}
}

// TestScheduleRepeatEveryPanicsOnInvalidValue 验证 repeatEvery 对非法秒值进行保护。
// 示例：s.Call(noop).repeatEvery(60)
func TestScheduleRepeatEveryPanicsOnInvalidValue(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for invalid repeatEvery seconds")
		}
	}()

	NewSchedule().Call(func(_ context.Context) error { return nil }).repeatEvery(60)
}

// TestScheduleCalendarMethodsConfigureExpectedFields 验证日历型接口写入预期约束。
// 示例：s.Command("report:weekly").WeeklyOn(time.Monday, "09:30")
func TestScheduleCalendarMethodsConfigureExpectedFields(t *testing.T) {
	s := NewSchedule()
	noop := func(_ context.Context) error { return nil }

	weekly := s.Call(noop).WeeklyOn(time.Monday, "09:30")
	if len(weekly.calendar.weekdays) != 1 || weekly.calendar.weekdays[0] != time.Monday {
		t.Fatalf("expected monday weekday, got %v", weekly.calendar.weekdays)
	}
	if weekly.calendar.hours[0] != 9 || weekly.calendar.minutes[0] != 30 || weekly.calendar.seconds[0] != 0 {
		t.Fatalf("unexpected weekly time config: %+v", weekly.calendar)
	}

	monthly := s.Call(noop).MonthlyOn(15, "08:45")
	if len(monthly.calendar.daysOfMonth) != 1 || monthly.calendar.daysOfMonth[0] != 15 {
		t.Fatalf("expected day 15, got %v", monthly.calendar.daysOfMonth)
	}
	if monthly.calendar.hours[0] != 8 || monthly.calendar.minutes[0] != 45 {
		t.Fatalf("unexpected monthly time config: %+v", monthly.calendar)
	}

	yearly := s.Call(noop).YearlyOn(12, 31, "23:59")
	if len(yearly.calendar.months) != 1 || yearly.calendar.months[0] != time.December {
		t.Fatalf("expected december, got %v", yearly.calendar.months)
	}
	if len(yearly.calendar.daysOfMonth) != 1 || yearly.calendar.daysOfMonth[0] != 31 {
		t.Fatalf("expected day 31, got %v", yearly.calendar.daysOfMonth)
	}
}

// TestScheduledTaskNextRunAfter 验证日历型任务能计算下一次命中时间。
// 示例：next := task.nextRunAfter(now)
func TestScheduledTaskNextRunAfter(t *testing.T) {
	base := time.Date(2026, 4, 27, 9, 10, 20, 0, time.Local)

	daily := NewSchedule().Call(func(_ context.Context) error { return nil }).DailyAt("09:30")
	nextDaily := daily.nextRunAfter(base)
	wantDaily := time.Date(2026, 4, 27, 9, 30, 0, 0, time.Local)
	if !nextDaily.Equal(wantDaily) {
		t.Fatalf("dailyAt next run: expected %v, got %v", wantDaily, nextDaily)
	}

	hourly := NewSchedule().Call(func(_ context.Context) error { return nil }).HourlyAt(15)
	nextHourly := hourly.nextRunAfter(base)
	wantHourly := time.Date(2026, 4, 27, 9, 15, 0, 0, time.Local)
	if !nextHourly.Equal(wantHourly) {
		t.Fatalf("hourlyAt next run: expected %v, got %v", wantHourly, nextHourly)
	}

	weekly := NewSchedule().Call(func(_ context.Context) error { return nil }).WeeklyOn(time.Monday, "09:30")
	nextWeekly := weekly.nextRunAfter(base)
	wantWeekly := time.Date(2026, 4, 27, 9, 30, 0, 0, time.Local)
	if !nextWeekly.Equal(wantWeekly) {
		t.Fatalf("weeklyOn next run: expected %v, got %v", wantWeekly, nextWeekly)
	}

	monthly := NewSchedule().Call(func(_ context.Context) error { return nil }).MonthlyOn(1, "08:00")
	nextMonthly := monthly.nextRunAfter(base)
	wantMonthly := time.Date(2026, 5, 1, 8, 0, 0, 0, time.Local)
	if !nextMonthly.Equal(wantMonthly) {
		t.Fatalf("monthlyOn next run: expected %v, got %v", wantMonthly, nextMonthly)
	}

	last := NewSchedule().Call(func(_ context.Context) error { return nil }).LastDayOfMonth("23:00")
	nextLast := last.nextRunAfter(base)
	wantLast := time.Date(2026, 4, 30, 23, 0, 0, 0, time.Local)
	if !nextLast.Equal(wantLast) {
		t.Fatalf("lastDayOfMonth next run: expected %v, got %v", wantLast, nextLast)
	}

	quarterly := NewSchedule().Call(func(_ context.Context) error { return nil }).QuarterlyOn(45, "10:00")
	nextQuarterly := quarterly.nextRunAfter(base)
	wantQuarterly := time.Date(2026, 5, 15, 10, 0, 0, 0, time.Local)
	if !nextQuarterly.Equal(wantQuarterly) {
		t.Fatalf("quarterlyOn next run: expected %v, got %v", wantQuarterly, nextQuarterly)
	}

	yearly := NewSchedule().Call(func(_ context.Context) error { return nil }).YearlyOn(12, 31, "23:59")
	nextYearly := yearly.nextRunAfter(base)
	wantYearly := time.Date(2026, 12, 31, 23, 59, 0, 0, time.Local)
	if !nextYearly.Equal(wantYearly) {
		t.Fatalf("yearlyOn next run: expected %v, got %v", wantYearly, nextYearly)
	}
}

// TestScheduleSummary 验证 Summary 输出包含已注册任务的名称和描述。
// 示例：summary := s.Summary()
func TestScheduleParsingHelpers(t *testing.T) {
	hour, minute, second := mustParseClock("18:30:45")
	if hour != 18 || minute != 30 || second != 45 {
		t.Fatalf("unexpected clock parse result: %d:%d:%d", hour, minute, second)
	}

	minutes := mustParseIntValues("minutes", "0,15,30-31", 0, 59)
	expectedMinutes := []int{0, 15, 30, 31}
	for i, want := range expectedMinutes {
		if minutes[i] != want {
			t.Fatalf("minutes[%d]: expected %d, got %d", i, want, minutes[i])
		}
	}

	weekdays := mustParseWeekdays("mon-wed,fri")
	expectedWeekdays := []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Friday}
	for i, want := range expectedWeekdays {
		if weekdays[i] != want {
			t.Fatalf("weekdays[%d]: expected %v, got %v", i, want, weekdays[i])
		}
	}
}

func TestScheduleCalendarHelpersAndPredicates(t *testing.T) {
	janLast := time.Date(2026, 1, 31, 12, 0, 0, 0, time.Local)
	if lastDayOfMonth(janLast) != 31 {
		t.Fatalf("expected last day 31")
	}

	quarterSample := time.Date(2026, 5, 15, 0, 0, 0, 0, time.Local)
	if quarterDay(quarterSample) != 45 {
		t.Fatalf("expected quarter day 45, got %d", quarterDay(quarterSample))
	}

	if !containsInt([]int{1, 3, 5}, 3) {
		t.Fatal("expected containsInt to return true")
	}
	if containsWeekday([]time.Weekday{time.Monday, time.Wednesday}, time.Friday) {
		t.Fatal("expected containsWeekday to return false")
	}
	if !containsWeekday([]time.Weekday{time.Monday, time.Wednesday}, time.Wednesday) {
		t.Fatal("expected containsWeekday to return true")
	}
	if !containsMonth([]time.Month{time.January, time.December}, time.December) {
		t.Fatal("expected containsMonth to return true")
	}

	task := NewSchedule().Call(func(_ context.Context) error { return nil }).YearlyOn(12, "last", "23:00")
	day := time.Date(2026, 12, 31, 0, 0, 0, 0, time.Local)
	if !task.matchesCalendarDay(day) {
		t.Fatal("expected yearly last-day task to match")
	}
}

func TestScheduleInvalidInputsPanics(t *testing.T) {
	panicCases := []struct {
		name string
		fn   func()
	}{
		{"EveryZero", func() { NewSchedule().Call(func(_ context.Context) error { return nil }).Every(0) }},
		{"DailyAtInvalid", func() { NewSchedule().Call(func(_ context.Context) error { return nil }).DailyAt("25:00") }},
		{"HourlyAtInvalid", func() { NewSchedule().Call(func(_ context.Context) error { return nil }).HourlyAt(60) }},
		{"DaysOfMonthEmpty", func() { NewSchedule().Call(func(_ context.Context) error { return nil }).DaysOfMonth() }},
		{"QuarterlyOnInvalid", func() { NewSchedule().Call(func(_ context.Context) error { return nil }).QuarterlyOn(93, "10:00") }},
		{"WeekdayInvalid", func() { mustParseWeekdays("noday") }},
		{"IntArgInvalidType", func() { mustIntArg("hour", true, 0, 23) }},
		{"CommandEmpty", func() {
			s := NewSchedule()
			s.SetResolver(func(name string, args []string) (ResolvedCommand, error) { return ResolvedCommand{}, nil })
			s.Command("")
		}},
		{"CommandNilResolver", func() { NewSchedule().Command("demo:run") }},
	}

	for _, tc := range panicCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for %s", tc.name)
				}
			}()
			tc.fn()
		})
	}
}

func TestScheduleWrapperMethods(t *testing.T) {
	s := NewSchedule()
	noop := func(_ context.Context) error { return nil }

	odd := s.Call(noop).EveryOddHour(20)
	if odd.interval != 2*time.Hour || odd.calendar.minutes[0] != 20 || odd.calendar.hours[0] != 1 {
		t.Fatalf("unexpected odd-hour config: %+v", odd.calendar)
	}

	twoHours := s.Call(noop).EveryTwoHours()
	if twoHours.calendar.hours[1] != 2 {
		t.Fatalf("expected everyTwoHours schedule, got %+v", twoHours.calendar.hours)
	}

	threeHours := s.Call(noop).EveryThreeHours(30)
	if threeHours.calendar.minutes[0] != 30 || threeHours.calendar.hours[1] != 3 {
		t.Fatalf("unexpected everyThreeHours config: %+v", threeHours.calendar)
	}

	fourHours := s.Call(noop).EveryFourHours(45)
	if fourHours.calendar.minutes[0] != 45 || fourHours.calendar.hours[1] != 4 {
		t.Fatalf("unexpected everyFourHours config: %+v", fourHours.calendar)
	}

	sixHours := s.Call(noop).EverySixHours(5)
	if sixHours.calendar.minutes[0] != 5 || sixHours.calendar.hours[1] != 6 {
		t.Fatalf("unexpected everySixHours config: %+v", sixHours.calendar)
	}

	twiceDaily := s.Call(noop).TwiceDaily(9, 18)
	if twiceDaily.calendar.hours[0] != 9 || twiceDaily.calendar.hours[1] != 18 {
		t.Fatalf("unexpected twiceDaily config: %+v", twiceDaily.calendar)
	}

	twiceDailyAt := s.Call(noop).TwiceDailyAt(8, 20, 15)
	if twiceDailyAt.calendar.minutes[0] != 15 || twiceDailyAt.calendar.hours[0] != 8 || twiceDailyAt.calendar.hours[1] != 20 {
		t.Fatalf("unexpected twiceDailyAt config: %+v", twiceDailyAt.calendar)
	}

	weekdays := s.Call(noop).Weekdays()
	if len(weekdays.calendar.weekdays) != 5 || weekdays.calendar.weekdays[0] != time.Monday {
		t.Fatalf("unexpected weekdays config: %+v", weekdays.calendar.weekdays)
	}

	weekends := s.Call(noop).Weekends()
	if len(weekends.calendar.weekdays) != 2 || !containsWeekday(weekends.calendar.weekdays, time.Saturday) || !containsWeekday(weekends.calendar.weekdays, time.Sunday) {
		t.Fatalf("unexpected weekends config: %+v", weekends.calendar.weekdays)
	}

	if s.Call(noop).Mondays().calendar.weekdays[0] != time.Monday {
		t.Fatal("expected monday wrapper")
	}
	if s.Call(noop).Tuesdays().calendar.weekdays[0] != time.Tuesday {
		t.Fatal("expected tuesday wrapper")
	}
	if s.Call(noop).Wednesdays().calendar.weekdays[0] != time.Wednesday {
		t.Fatal("expected wednesday wrapper")
	}
	if s.Call(noop).Thursdays().calendar.weekdays[0] != time.Thursday {
		t.Fatal("expected thursday wrapper")
	}
	if s.Call(noop).Fridays().calendar.weekdays[0] != time.Friday {
		t.Fatal("expected friday wrapper")
	}
	if s.Call(noop).Saturdays().calendar.weekdays[0] != time.Saturday {
		t.Fatal("expected saturday wrapper")
	}
	if s.Call(noop).Sundays().calendar.weekdays[0] != time.Sunday {
		t.Fatal("expected sunday wrapper")
	}

	days := s.Call(noop).Days([]int{1, 3, 5})
	if len(days.calendar.weekdays) != 3 || days.calendar.weekdays[2] != time.Friday {
		t.Fatalf("unexpected days config: %+v", days.calendar.weekdays)
	}

	twiceMonthly := s.Call(noop).TwiceMonthly(1, 16, "10:00")
	if len(twiceMonthly.calendar.daysOfMonth) != 2 || twiceMonthly.calendar.daysOfMonth[1] != 16 {
		t.Fatalf("unexpected twiceMonthly config: %+v", twiceMonthly.calendar.daysOfMonth)
	}

	daysOfMonth := s.Call(noop).DaysOfMonth(1, 15, 28)
	if len(daysOfMonth.calendar.daysOfMonth) != 3 || daysOfMonth.calendar.daysOfMonth[2] != 28 {
		t.Fatalf("unexpected daysOfMonth config: %+v", daysOfMonth.calendar.daysOfMonth)
	}
}

func TestScheduleHelperVariants(t *testing.T) {
	minutesFromInt := mustParseIntValues("minutes", 5, 0, 59)
	if len(minutesFromInt) != 1 || minutesFromInt[0] != 5 {
		t.Fatalf("unexpected minutesFromInt: %v", minutesFromInt)
	}

	minutesFromSlice := mustParseIntValues("minutes", []int{10, 20, 20}, 0, 59)
	if len(minutesFromSlice) != 2 || minutesFromSlice[1] != 20 {
		t.Fatalf("unexpected minutesFromSlice: %v", minutesFromSlice)
	}

	minutesFromStrings := mustParseIntValues("minutes", []string{"1", "2", "3"}, 0, 59)
	if len(minutesFromStrings) != 3 || minutesFromStrings[2] != 3 {
		t.Fatalf("unexpected minutesFromStrings: %v", minutesFromStrings)
	}

	if got := mustParseWeekdays(time.Monday); len(got) != 1 || got[0] != time.Monday {
		t.Fatalf("unexpected weekday parse from time.Weekday: %v", got)
	}
	if got := mustParseWeekdays([]time.Weekday{time.Monday, time.Wednesday}); len(got) != 2 || got[1] != time.Wednesday {
		t.Fatalf("unexpected weekday parse from []time.Weekday: %v", got)
	}
	if got := mustParseWeekdays(2); len(got) != 1 || got[0] != time.Tuesday {
		t.Fatalf("unexpected weekday parse from int: %v", got)
	}
	if got := mustParseWeekdays([]int{0, 6}); len(got) != 2 || got[1] != time.Saturday {
		t.Fatalf("unexpected weekday parse from []int: %v", got)
	}
	if got := mustParseWeekdays([]string{"mon", "fri"}); len(got) != 2 || got[1] != time.Friday {
		t.Fatalf("unexpected weekday parse from []string: %v", got)
	}

	if mustWeekdayValue("sun") != 0 || mustWeekdayValue("thursday") != 4 {
		t.Fatal("unexpected weekday numeric mapping")
	}

	if mustIntArg("hour", 8, 0, 23) != 8 || mustIntArg("hour", "9", 0, 23) != 9 {
		t.Fatal("unexpected mustIntArg result")
	}

	if len(allMinutes()) != 60 {
		t.Fatalf("expected 60 minutes, got %d", len(allMinutes()))
	}
	if firstAny([]any{"x"}, "fallback") != "x" || firstAny(nil, "fallback") != "fallback" {
		t.Fatal("unexpected firstAny result")
	}
	if firstString([]string{"a"}, "fallback") != "a" || firstString(nil, "fallback") != "fallback" {
		t.Fatal("unexpected firstString result")
	}

	task := &ScheduledTask{}
	if len(task.allowedSeconds()) != 1 || task.allowedSeconds()[0] != 0 {
		t.Fatal("unexpected default allowedSeconds")
	}
	if len(task.allowedMinutes()) != 60 || len(task.allowedHours()) != 24 {
		t.Fatal("unexpected default allowedMinutes/allowedHours")
	}
}

func TestScheduleCalendarNextRunAcrossBoundary(t *testing.T) {
	base := time.Date(2026, 4, 27, 23, 59, 58, 0, time.Local)
	task := NewSchedule().Call(func(_ context.Context) error { return nil }).EveryThirtySeconds()
	next := task.nextRunAfter(base)
	want := time.Date(2026, 4, 28, 0, 0, 0, 0, time.Local)
	if !next.Equal(want) {
		t.Fatalf("expected boundary next run %v, got %v", want, next)
	}
}

// TestScheduleSummary 验证 Summary 输出包含已注册任务的名称和描述。
// 示例：summary := s.Summary()
func TestScheduleSummary(t *testing.T) {
	s := NewSchedule()
	noop := func(_ context.Context) error { return nil }
	s.Call(noop).EveryFiveMinutes().Name("job_a").Description("does A")
	s.Call(noop).Hourly().Name("job_b").Description("does B")

	summary := s.Summary()
	if summary == "" {
		t.Fatal("expected non-empty summary")
	}
	for _, want := range []string{"job_a", "does A", "job_b", "does B"} {
		if !contains(summary, want) {
			t.Errorf("summary missing %q:\n%s", want, summary)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func useDebugConfig(t *testing.T, registry *container.Container, debug bool) {
	t.Helper()
	_ = debug

	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte{}, 0o644); err != nil {
		t.Fatalf("write env file: %v", err)
	}
	cfg, err := configpkg.NewFromFile(path)
	if err != nil {
		t.Fatalf("new config: %v", err)
	}
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
}

func assertFileContains(t *testing.T, path, want string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !contains(string(data), want) {
		t.Fatalf("expected log file to contain %q, got:\n%s", want, string(data))
	}
}

func assertFileDoesNotContain(t *testing.T, path, want string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read log file: %v", err)
	}
	if contains(string(data), want) {
		t.Fatalf("expected log file not to contain %q, got:\n%s", want, string(data))
	}
}

func waitForCount(t *testing.T, count *atomic.Int32, min int32, timeout time.Duration) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		if got := count.Load(); got >= min {
			return
		}

		select {
		case <-deadline.C:
			t.Fatalf("expected task to run at least %d times, got %d", min, count.Load())
		case <-ticker.C:
		}
	}
}

func useTimerMemoryCache(t *testing.T) {
	t.Helper()

	manager, err := cachepkg.NewManager(cachepkg.Config{
		Default:  "memory",
		Encoding: "json",
		Stores: map[string]cachepkg.StoreConfig{
			"memory": {
				Driver:          "memory",
				CleanupInterval: time.Millisecond,
			},
		},
		Lock: cachepkg.LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	useDebugConfig(t, registry, false)
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}
	if err := registry.Instance("exception.handler", goexception.New(goexception.WithPanicStack(false)), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	logManager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", logManager, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
		container.SetProvider(nil)
	})
}
