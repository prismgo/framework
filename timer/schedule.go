// Package timer 提供 Laravel 风格的定时任务调度基础设施。
//
// 使用方式类似 Laravel 的 Kernel::schedule()，通过链式调用注册任务：
//
//	s := timer.NewSchedule()
//	s.Command("test:task").EveryFiveMinutes()
//	s.Start(ctx)
//
// 调度器支持两类执行模式：
// 1. 固定间隔模式：基于 time.Duration，启动后立即执行一次，之后按固定间隔重复；
// 2. 日历调度模式：基于秒、分、时、周、月等约束计算下一次执行时间，按命中时刻触发。
package timer

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prismgo/framework/cache"
	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/logger"
	"github.com/prismgo/framework/routine"
)

const defaultWithoutOverlappingExpires = 1440 * time.Minute

var scheduledTaskID atomic.Uint64

// ResolvedCommand 包含命令解析后的执行函数和描述信息。
type ResolvedCommand struct {
	Fn          func(ctx context.Context) error
	Description string
}

// CommandResolver 根据命令名称和参数返回可执行函数，由外部（如 Kernel）注入。
type CommandResolver func(name string, args []string) (ResolvedCommand, error)

// Schedule 定时任务调度器，管理多个 ScheduledTask 的注册与执行。
// 对应 Laravel 的 Illuminate\Console\Scheduling\Schedule。
type Schedule struct {
	tasks    []*ScheduledTask
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	resolver CommandResolver
}

// NewSchedule 创建调度器实例。
// 示例：s := timer.NewSchedule()
func NewSchedule() *Schedule {
	return &Schedule{}
}

// SetResolver 注入命令解析器，使 Command 方法可用。
// 示例：s.SetResolver(resolveCommand)
func (s *Schedule) SetResolver(r CommandResolver) {
	s.resolver = r
}

// Command 通过命令签名注册定时任务，支持传递参数（如 "overtime:detect --take=10"）。
// 对应 Laravel 的 $schedule->command('overtime:detect --take=10')。
// 命令必须已通过 Kernel.Register 注册，否则 panic。
// 示例：s.Command("overtime:detect --take=100").EveryFiveMinutes()
func (s *Schedule) Command(signature string) *ScheduledTask {
	parts := strings.Fields(signature)
	if len(parts) == 0 {
		panic("schedule command: empty signature")
	}
	if s.resolver == nil {
		panic("schedule command: resolver is nil")
	}

	name := parts[0]
	var args []string
	if len(parts) > 1 {
		args = parts[1:]
	}

	resolved, err := s.resolver(name, args)
	if err != nil {
		panic(fmt.Sprintf("schedule command %q: %v", name, err))
	}

	task := newScheduledTask(resolved.Fn)
	task.name = name
	task.description = resolved.Description
	s.tasks = append(s.tasks, task)
	return task
}

// Call 注册一个定时任务回调，返回 ScheduledTask 供链式配置频率和元信息。
// 对应 Laravel 的 $schedule->call(callable)。
// 示例：s.Call(func(ctx context.Context) error { return nil }).Daily()
func (s *Schedule) Call(fn func(ctx context.Context) error) *ScheduledTask {
	task := newScheduledTask(fn)
	s.tasks = append(s.tasks, task)
	return task
}

// Start 启动所有已注册任务的定时循环。
// 每个任务在独立 goroutine 中运行，ctx 取消或调用 Stop 时全部停止。
// 示例：s.Start(ctx)
func (s *Schedule) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	for _, t := range s.tasks {
		s.wg.Add(1)
		task := t
		routine.Task(ctx, func(context.Context) error {
			s.runTask(ctx, task)
			return nil
		}).
			Component("cron").
			Name("schedule.task").
			Fields(map[string]any{"task": task.name}).
			Go()
	}
}

// Stop 通知所有任务停止并等待退出。
// 示例：s.Stop()
func (s *Schedule) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// Summary 返回所有已注册任务的摘要信息，用于启动时日志输出。
// 示例：logger.Infof("tasks:\n%s", s.Summary())
func (s *Schedule) Summary() string {
	var b strings.Builder
	for _, t := range s.tasks {
		fmt.Fprintf(&b, "  %-24s %s\n", t.name, t.description)
	}
	return b.String()
}

// runTask 在独立 goroutine 中执行单个任务。
// 固定间隔任务在启动后立即执行一次；日历任务等待下一次命中时刻后执行。
func (s *Schedule) runTask(ctx context.Context, t *ScheduledTask) {
	defer s.wg.Done()

	if t.usesCalendarSchedule() {
		s.runCalendarTask(ctx, t)
		return
	}

	s.runIntervalTask(ctx, t)
}

// runIntervalTask 按固定 time.Duration 周期执行任务。
func (s *Schedule) runIntervalTask(ctx context.Context, t *ScheduledTask) {
	s.executeOnce(ctx, t)

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if config.GetBool("app.debug", false) {
				logger.Infof("[schedule] task %s stopped", t.name)
			}
			return
		case <-ticker.C:
			s.executeOnce(ctx, t)
		}
	}
}

// runCalendarTask 按日历命中时刻执行任务。
func (s *Schedule) runCalendarTask(ctx context.Context, t *ScheduledTask) {
	if t.runOnStart {
		s.executeOnce(ctx, t)
	}

	for {
		next := t.nextRunAfter(time.Now())
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if config.GetBool("app.debug", false) {
				logger.Infof("[schedule] task %s stopped", t.name)
			}
			return
		case <-timer.C:
		}

		s.executeOnce(ctx, t)
	}
}

// executeOnce 执行一次任务并记录耗时和错误。
func (s *Schedule) executeOnce(ctx context.Context, t *ScheduledTask) {
	start := time.Now()
	fields := map[string]any{
		"task":      t.name,
		"status":    500,
		"component": "cron",
	}
	done := make(chan struct{})
	var doneOnce sync.Once
	closeDone := func() { doneOnce.Do(func() { close(done) }) }
	succeeded := false
	routine.Task(ctx, func(ctx context.Context) error {
		defer func() {
			fields["duration_ms"] = time.Since(start).Milliseconds()
		}()
		if t.withoutOverlapping {
			lock := cache.Lock(t.overlapLockKey(), t.withoutOverlappingExpires)
			ok, err := lock.Get(ctx)
			if err != nil || !ok {
				closeDone()
				return err
			}
			defer func() { _ = lock.Release(context.Background()) }()
		}
		if err := t.fn(ctx); err != nil {
			return err
		}
		succeeded = true
		closeDone()
		return nil
	}).
		Component("cron").
		Name("schedule.execute").
		Fields(fields).
		OnError(func(error) {
			closeDone()
		}).
		OnPanic(func(error) {
			closeDone()
		}).
		Go()
	<-done
	if succeeded && config.GetBool("app.debug", false) {
		logger.Infof("[schedule] task %s done (%v)", t.name, time.Since(start))
	}
}

// ScheduledTask 表示单个定时任务的调度配置。
// 设计思路：
// 1. interval 保留原有固定间隔能力，兼容现有 Every(d) 用法；
// 2. calendar 保存 Laravel 风格的日历调度约束；
// 3. 链式方法按需切换调度模式，尽量保持调用直观、显式。
type ScheduledTask struct {
	fn          func(ctx context.Context) error
	interval    time.Duration
	name        string
	description string
	mode        scheduleMode
	runOnStart  bool
	calendar    calendarSpec

	id                        uint64
	withoutOverlapping        bool
	withoutOverlappingExpires time.Duration
}

type scheduleMode uint8

const (
	scheduleModeInterval scheduleMode = iota
	scheduleModeCalendar
)

type calendarSpec struct {
	seconds        []int
	minutes        []int
	hours          []int
	weekdays       []time.Weekday
	daysOfMonth    []int
	months         []time.Month
	lastDayOfMonth bool
	dayOfQuarter   int
	location       *time.Location
}

func newScheduledTask(fn func(ctx context.Context) error) *ScheduledTask {
	return &ScheduledTask{
		fn:                        fn,
		interval:                  time.Minute,
		mode:                      scheduleModeInterval,
		runOnStart:                true,
		id:                        scheduledTaskID.Add(1),
		withoutOverlappingExpires: defaultWithoutOverlappingExpires,
	}
}

// EverySecond 将任务安排为每秒执行一次，按整秒边界触发。
// 示例：s.Command("heartbeat:check").EverySecond()
func (t *ScheduledTask) EverySecond() *ScheduledTask {
	return t.repeatEvery(1)
}

// EveryTwoSeconds 将任务安排为每两秒执行一次，按秒边界触发。
// 示例：s.Command("queue:poll").EveryTwoSeconds()
func (t *ScheduledTask) EveryTwoSeconds() *ScheduledTask {
	return t.repeatEvery(2)
}

// EveryFiveSeconds 将任务安排为每五秒执行一次，按秒边界触发。
// 示例：s.Command("cache:warm").EveryFiveSeconds()
func (t *ScheduledTask) EveryFiveSeconds() *ScheduledTask {
	return t.repeatEvery(5)
}

// EveryTenSeconds 将任务安排为每十秒执行一次，按秒边界触发。
// 示例：s.Command("metrics:flush").EveryTenSeconds()
func (t *ScheduledTask) EveryTenSeconds() *ScheduledTask {
	return t.repeatEvery(10)
}

// EveryFifteenSeconds 将任务安排为每十五秒执行一次，按秒边界触发。
// 示例：s.Command("watcher:scan").EveryFifteenSeconds()
func (t *ScheduledTask) EveryFifteenSeconds() *ScheduledTask {
	return t.repeatEvery(15)
}

// EveryTwentySeconds 将任务安排为每二十秒执行一次，按秒边界触发。
// 示例：s.Command("stats:sync").EveryTwentySeconds()
func (t *ScheduledTask) EveryTwentySeconds() *ScheduledTask {
	return t.repeatEvery(20)
}

// EveryThirtySeconds 将任务安排为每三十秒执行一次，按秒边界触发。
// 示例：s.Command("snapshot:save").EveryThirtySeconds()
func (t *ScheduledTask) EveryThirtySeconds() *ScheduledTask {
	return t.repeatEvery(30)
}

// repeatEvery 将任务安排为每分钟内按固定秒步长重复执行。
// seconds 仅允许 1 到 59，非法值会 panic。
// 示例：task.repeatEvery(10)
func (t *ScheduledTask) repeatEvery(seconds int) *ScheduledTask {
	if seconds < 1 || seconds > 59 {
		panic(fmt.Sprintf("schedule repeatEvery: invalid seconds %d", seconds))
	}

	t.interval = time.Duration(seconds) * time.Second
	t.useCalendar()
	t.runOnStart = false
	t.calendar.seconds = stepValues(0, seconds, 59)
	return t
}

// EveryMinute 将任务安排为每分钟执行一次，在每分钟的第 0 秒触发。
// 为兼容现有 prismgo/timer 行为，调度器启动后会先立即执行一次。
// 示例：s.Command("area:sync").EveryMinute()
func (t *ScheduledTask) EveryMinute() *ScheduledTask {
	t.everyMinutes(1)
	t.runOnStart = true
	return t
}

// EveryTwoMinutes 将任务安排为每两分钟执行一次，在分钟边界触发。
// 为兼容现有 prismgo/timer 行为，调度器启动后会先立即执行一次。
// 示例：s.Command("report:refresh").EveryTwoMinutes()
func (t *ScheduledTask) EveryTwoMinutes() *ScheduledTask {
	t.everyMinutes(2)
	t.runOnStart = true
	return t
}

// EveryThreeMinutes 将任务安排为每三分钟执行一次，在分钟边界触发。
// 示例：s.Command("report:refresh").EveryThreeMinutes()
func (t *ScheduledTask) EveryThreeMinutes() *ScheduledTask {
	return t.everyMinutes(3)
}

// EveryFourMinutes 将任务安排为每四分钟执行一次，在分钟边界触发。
// 示例：s.Command("report:refresh").EveryFourMinutes()
func (t *ScheduledTask) EveryFourMinutes() *ScheduledTask {
	return t.everyMinutes(4)
}

// EveryFiveMinutes 将任务安排为每五分钟执行一次，在分钟边界触发。
// 为兼容现有 prismgo/timer 行为，调度器启动后会先立即执行一次。
// 示例：s.Command("overtime:detect").EveryFiveMinutes()
func (t *ScheduledTask) EveryFiveMinutes() *ScheduledTask {
	t.everyMinutes(5)
	t.runOnStart = true
	return t
}

// EveryTenMinutes 将任务安排为每十分钟执行一次，在分钟边界触发。
// 为兼容现有 prismgo/timer 行为，调度器启动后会先立即执行一次。
// 示例：s.Command("followup:generate").EveryTenMinutes()
func (t *ScheduledTask) EveryTenMinutes() *ScheduledTask {
	t.everyMinutes(10)
	t.runOnStart = true
	return t
}

// EveryFifteenMinutes 将任务安排为每十五分钟执行一次，在分钟边界触发。
// 为兼容现有 prismgo/timer 行为，调度器启动后会先立即执行一次。
// 示例：s.Command("followup:remind").EveryFifteenMinutes()
func (t *ScheduledTask) EveryFifteenMinutes() *ScheduledTask {
	t.everyMinutes(15)
	t.runOnStart = true
	return t
}

// EveryThirtyMinutes 将任务安排为每三十分钟执行一次，在分钟边界触发。
// 为兼容现有 prismgo/timer 行为，调度器启动后会先立即执行一次。
// 示例：s.Command("followup:remind").EveryThirtyMinutes()
func (t *ScheduledTask) EveryThirtyMinutes() *ScheduledTask {
	t.everyMinutes(30)
	t.runOnStart = true
	return t
}

func (t *ScheduledTask) everyMinutes(step int) *ScheduledTask {
	t.interval = time.Duration(step) * time.Minute
	t.useCalendar()
	t.runOnStart = false
	t.calendar.seconds = []int{0}
	t.calendar.minutes = stepValues(0, step, 59)
	t.calendar.hours = allHours()
	return t
}

// Hourly 将任务安排为每小时执行一次，在整点触发。
// 为兼容现有 prismgo/timer 行为，调度器启动后会先立即执行一次。
// 示例：s.Command("stats:archive").Hourly()
func (t *ScheduledTask) Hourly() *ScheduledTask {
	t.interval = time.Hour
	t.hourBasedSchedule(0, allHours())
	t.runOnStart = true
	return t
}

// HourlyAt 将任务安排为每小时在给定分钟执行一次。
// offset 支持 int、string、[]int、[]string。
// 示例：s.Command("stats:archive").HourlyAt(15)
func (t *ScheduledTask) HourlyAt(offset any) *ScheduledTask {
	return t.hourBasedSchedule(offset, allHours())
}

// EveryOddHour 将任务安排为每个奇数小时在给定分钟执行一次。
// 不传 offset 时默认在第 0 分钟触发。
// 示例：s.Command("cache:cleanup").EveryOddHour(20)
func (t *ScheduledTask) EveryOddHour(offset ...any) *ScheduledTask {
	t.interval = 2 * time.Hour
	return t.hourBasedSchedule(firstAny(offset, 0), []int{1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23})
}

// EveryTwoHours 将任务安排为每两小时在给定分钟执行一次。
// 不传 offset 时默认在第 0 分钟触发。
// 示例：s.Command("cache:cleanup").EveryTwoHours(10)
func (t *ScheduledTask) EveryTwoHours(offset ...any) *ScheduledTask {
	t.interval = 2 * time.Hour
	return t.hourBasedSchedule(firstAny(offset, 0), []int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22})
}

// EveryThreeHours 将任务安排为每三小时在给定分钟执行一次。
// 不传 offset 时默认在第 0 分钟触发。
// 示例：s.Command("sync:tenant").EveryThreeHours(30)
func (t *ScheduledTask) EveryThreeHours(offset ...any) *ScheduledTask {
	t.interval = 3 * time.Hour
	return t.hourBasedSchedule(firstAny(offset, 0), stepValues(0, 3, 23))
}

// EveryFourHours 将任务安排为每四小时在给定分钟执行一次。
// 不传 offset 时默认在第 0 分钟触发。
// 示例：s.Command("sync:tenant").EveryFourHours(45)
func (t *ScheduledTask) EveryFourHours(offset ...any) *ScheduledTask {
	t.interval = 4 * time.Hour
	return t.hourBasedSchedule(firstAny(offset, 0), stepValues(0, 4, 23))
}

// EverySixHours 将任务安排为每六小时在给定分钟执行一次。
// 不传 offset 时默认在第 0 分钟触发。
// 示例：s.Command("backup:compact").EverySixHours(5)
func (t *ScheduledTask) EverySixHours(offset ...any) *ScheduledTask {
	t.interval = 6 * time.Hour
	return t.hourBasedSchedule(firstAny(offset, 0), stepValues(0, 6, 23))
}

// Daily 将任务安排为每天 00:00 执行一次。
// 为兼容现有 prismgo/timer 行为，调度器启动后会先立即执行一次。
// 示例：s.Command("billing:close").Daily()
func (t *ScheduledTask) Daily() *ScheduledTask {
	t.interval = 24 * time.Hour
	t.hourBasedSchedule(0, 0)
	t.runOnStart = true
	return t
}

// At 将任务安排为在给定时间执行。
// 该方法只设置时间部分，不会清除已设置的星期、月份、季度等约束。
// 示例：s.Command("billing:close").Weekdays().At("18:30")
func (t *ScheduledTask) At(timeValue string) *ScheduledTask {
	hour, minute, second := mustParseClock(timeValue)
	t.useCalendar()
	t.runOnStart = false
	t.calendar.hours = []int{hour}
	t.calendar.minutes = []int{minute}
	t.calendar.seconds = []int{second}
	return t
}

// DailyAt 将任务安排为每天在给定时间执行。
// timeValue 支持 HH:MM 或 HH:MM:SS。
// 示例：s.Command("billing:close").DailyAt("18:30")
func (t *ScheduledTask) DailyAt(timeValue string) *ScheduledTask {
	t.interval = 24 * time.Hour
	return t.At(timeValue)
}

// TwiceDaily 将任务安排为每天执行两次。
// 不传参数时默认在 01:00 与 13:00 触发。
// 示例：s.Command("summary:send").TwiceDaily(9, 18)
func (t *ScheduledTask) TwiceDaily(hours ...int) *ScheduledTask {
	first, second := 1, 13
	if len(hours) > 0 {
		first = hours[0]
	}
	if len(hours) > 1 {
		second = hours[1]
	}
	return t.TwiceDailyAt(first, second, 0)
}

// TwiceDailyAt 将任务安排为每天在两个小时点的给定分钟执行。
// 不传参数时默认在 01:00 与 13:00 触发；offset 为分钟偏移。
// 示例：s.Command("summary:send").TwiceDailyAt(9, 18, 15)
func (t *ScheduledTask) TwiceDailyAt(values ...int) *ScheduledTask {
	first, second, offset := 1, 13, 0
	if len(values) > 0 {
		first = values[0]
	}
	if len(values) > 1 {
		second = values[1]
	}
	if len(values) > 2 {
		offset = values[2]
	}
	mustValidateRange("first hour", first, 0, 23)
	mustValidateRange("second hour", second, 0, 23)
	mustValidateRange("offset minute", offset, 0, 59)

	t.interval = 12 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.hours = normalizeInts([]int{first, second})
	t.calendar.minutes = []int{offset}
	t.calendar.seconds = []int{0}
	return t
}

// hourBasedSchedule 将任务安排为在给定分钟与小时组合上执行。
// minutes 和 hours 支持 int、string、[]int、[]string。
// 示例：task.hourBasedSchedule([]int{0, 30}, []int{9, 18})
func (t *ScheduledTask) hourBasedSchedule(minutes any, hours any) *ScheduledTask {
	parsedMinutes := mustParseIntValues("minutes", minutes, 0, 59)
	parsedHours := mustParseIntValues("hours", hours, 0, 23)
	t.useCalendar()
	t.runOnStart = false
	t.calendar.seconds = []int{0}
	t.calendar.minutes = parsedMinutes
	t.calendar.hours = parsedHours
	return t
}

// Weekdays 将任务限制为仅在工作日执行。
// 若未设置具体时间，默认表示工作日的每分钟第 0 秒触发。
// 示例：s.Command("notify:pending").Weekdays()
func (t *ScheduledTask) Weekdays() *ScheduledTask {
	return t.Days([]time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday})
}

// Weekends 将任务限制为仅在周末执行。
// 若未设置具体时间，默认表示周末的每分钟第 0 秒触发。
// 示例：s.Command("report:weekend").Weekends()
func (t *ScheduledTask) Weekends() *ScheduledTask {
	return t.Days([]time.Weekday{time.Saturday, time.Sunday})
}

// Mondays 将任务限制为仅在周一执行。
// 示例：s.Command("report:weekly").Mondays().At("09:00")
func (t *ScheduledTask) Mondays() *ScheduledTask {
	return t.Days(time.Monday)
}

// Tuesdays 将任务限制为仅在周二执行。
// 示例：s.Command("report:weekly").Tuesdays().At("09:00")
func (t *ScheduledTask) Tuesdays() *ScheduledTask {
	return t.Days(time.Tuesday)
}

// Wednesdays 将任务限制为仅在周三执行。
// 示例：s.Command("report:weekly").Wednesdays().At("09:00")
func (t *ScheduledTask) Wednesdays() *ScheduledTask {
	return t.Days(time.Wednesday)
}

// Thursdays 将任务限制为仅在周四执行。
// 示例：s.Command("report:weekly").Thursdays().At("09:00")
func (t *ScheduledTask) Thursdays() *ScheduledTask {
	return t.Days(time.Thursday)
}

// Fridays 将任务限制为仅在周五执行。
// 示例：s.Command("report:weekly").Fridays().At("09:00")
func (t *ScheduledTask) Fridays() *ScheduledTask {
	return t.Days(time.Friday)
}

// Saturdays 将任务限制为仅在周六执行。
// 示例：s.Command("report:weekly").Saturdays().At("09:00")
func (t *ScheduledTask) Saturdays() *ScheduledTask {
	return t.Days(time.Saturday)
}

// Sundays 将任务限制为仅在周日执行。
// 示例：s.Command("report:weekly").Sundays().At("09:00")
func (t *ScheduledTask) Sundays() *ScheduledTask {
	return t.Days(time.Sunday)
}

// Weekly 将任务安排为每周日 00:00 执行一次。
// 示例：s.Command("billing:weekly-close").Weekly()
func (t *ScheduledTask) Weekly() *ScheduledTask {
	t.interval = 7 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.weekdays = []time.Weekday{time.Sunday}
	t.calendar.hours = []int{0}
	t.calendar.minutes = []int{0}
	t.calendar.seconds = []int{0}
	return t
}

// WeeklyOn 将任务安排为每周在给定星期与时间执行。
// dayOfWeek 支持 int、time.Weekday、string、[]int、[]string、[]time.Weekday。
// 示例：s.Command("report:weekly").WeeklyOn(time.Monday, "09:30")
func (t *ScheduledTask) WeeklyOn(dayOfWeek any, timeValue ...string) *ScheduledTask {
	at := firstString(timeValue, "0:0")
	weekdays := mustParseWeekdays(dayOfWeek)
	t.useCalendar()
	t.runOnStart = false
	t.calendar.weekdays = weekdays
	return t.At(at)
}

// Monthly 将任务安排为每月 1 日 00:00 执行一次。
// 示例：s.Command("statement:close").Monthly()
func (t *ScheduledTask) Monthly() *ScheduledTask {
	t.interval = 31 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.daysOfMonth = []int{1}
	t.calendar.lastDayOfMonth = false
	return t.At("0:0")
}

// MonthlyOn 将任务安排为每月给定日期与时间执行。
// 不传 timeValue 时默认在 00:00 触发。
// 示例：s.Command("statement:close").MonthlyOn(15, "08:30")
func (t *ScheduledTask) MonthlyOn(dayOfMonth int, timeValue ...string) *ScheduledTask {
	mustValidateRange("day of month", dayOfMonth, 1, 31)
	at := firstString(timeValue, "0:0")
	t.interval = 31 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.daysOfMonth = []int{dayOfMonth}
	t.calendar.lastDayOfMonth = false
	t.calendar.dayOfQuarter = 0
	return t.At(at)
}

// TwiceMonthly 将任务安排为每月两个日期在给定时间执行。
// 不传参数时默认在每月 1 日和 16 日 00:00 触发。
// 示例：s.Command("invoice:remind").TwiceMonthly(1, 16, "10:00")
func (t *ScheduledTask) TwiceMonthly(args ...any) *ScheduledTask {
	first, second, at := 1, 16, "0:0"
	if len(args) > 0 {
		first = mustIntArg("first day", args[0], 1, 31)
	}
	if len(args) > 1 {
		second = mustIntArg("second day", args[1], 1, 31)
	}
	if len(args) > 2 {
		value, ok := args[2].(string)
		if !ok {
			panic("schedule twiceMonthly: time must be string")
		}
		at = value
	}

	t.interval = 15 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.daysOfMonth = normalizeInts([]int{first, second})
	t.calendar.lastDayOfMonth = false
	t.calendar.dayOfQuarter = 0
	return t.At(at)
}

// LastDayOfMonth 将任务安排为每月最后一天在给定时间执行。
// 不传 timeValue 时默认在 00:00 触发。
// 示例：s.Command("statement:close").LastDayOfMonth("23:55")
func (t *ScheduledTask) LastDayOfMonth(timeValue ...string) *ScheduledTask {
	at := firstString(timeValue, "0:0")
	t.interval = 31 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.daysOfMonth = nil
	t.calendar.lastDayOfMonth = true
	t.calendar.dayOfQuarter = 0
	return t.At(at)
}

// DaysOfMonth 将任务限制在每月指定日期执行。
// 若未设置具体时间，默认表示这些日期的每分钟第 0 秒触发。
// 示例：s.Command("invoice:remind").DaysOfMonth(1, 15, 28)
func (t *ScheduledTask) DaysOfMonth(days ...int) *ScheduledTask {
	if len(days) == 0 {
		panic("schedule daysOfMonth: at least one day is required")
	}
	for _, day := range days {
		mustValidateRange("day of month", day, 1, 31)
	}
	return t.applyDaysOfMonth(days)
}

// Quarterly 将任务安排为每个季度的第一天 00:00 执行一次。
// 示例：s.Command("finance:quarterly-close").Quarterly()
func (t *ScheduledTask) Quarterly() *ScheduledTask {
	t.interval = 90 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.dayOfQuarter = 1
	t.calendar.daysOfMonth = nil
	t.calendar.lastDayOfMonth = false
	return t.At("0:0")
}

// QuarterlyOn 将任务安排为每个季度的第 N 天在给定时间执行。
// 不传参数时默认在季度第 1 天 00:00 触发。
// 示例：s.Command("finance:quarterly-close").QuarterlyOn(45, "10:00")
func (t *ScheduledTask) QuarterlyOn(args ...any) *ScheduledTask {
	dayOfQuarter, at := 1, "0:0"
	if len(args) > 0 {
		dayOfQuarter = mustIntArg("day of quarter", args[0], 1, 92)
	}
	if len(args) > 1 {
		value, ok := args[1].(string)
		if !ok {
			panic("schedule quarterlyOn: time must be string")
		}
		at = value
	}

	t.interval = 90 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.dayOfQuarter = dayOfQuarter
	t.calendar.daysOfMonth = nil
	t.calendar.lastDayOfMonth = false
	return t.At(at)
}

// Yearly 将任务安排为每年 1 月 1 日 00:00 执行一次。
// 示例：s.Command("stats:yearly-reset").Yearly()
func (t *ScheduledTask) Yearly() *ScheduledTask {
	t.interval = 365 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.months = []time.Month{time.January}
	t.calendar.daysOfMonth = []int{1}
	t.calendar.lastDayOfMonth = false
	t.calendar.dayOfQuarter = 0
	return t.At("0:0")
}

// YearlyOn 将任务安排为每年指定月、日、时间执行。
// 不传参数时默认在每年 1 月 1 日 00:00 触发。
// dayOfMonth 支持 int、string("L"/"last")。
// 示例：s.Command("stats:yearly-reset").YearlyOn(12, 31, "23:59")
func (t *ScheduledTask) YearlyOn(args ...any) *ScheduledTask {
	month := 1
	dayOfMonth := any(1)
	at := "0:0"
	if len(args) > 0 {
		month = mustIntArg("month", args[0], 1, 12)
	}
	if len(args) > 1 {
		dayOfMonth = args[1]
	}
	if len(args) > 2 {
		value, ok := args[2].(string)
		if !ok {
			panic("schedule yearlyOn: time must be string")
		}
		at = value
	}

	t.interval = 365 * 24 * time.Hour
	t.useCalendar()
	t.runOnStart = false
	t.calendar.months = []time.Month{time.Month(month)}
	t.calendar.dayOfQuarter = 0

	if isLastDayToken(dayOfMonth) {
		t.calendar.daysOfMonth = nil
		t.calendar.lastDayOfMonth = true
		return t.At(at)
	}

	day := mustIntArg("day of month", dayOfMonth, 1, 31)
	t.calendar.daysOfMonth = []int{day}
	t.calendar.lastDayOfMonth = false
	return t.At(at)
}

// Days 设置任务仅在指定星期执行。
// days 支持 int、time.Weekday、string、[]int、[]string、[]time.Weekday。
// 若未设置具体时间，默认表示这些星期的每分钟第 0 秒触发。
// 示例：s.Command("report:weekly").Days([]int{1, 3, 5}).At("09:00")
func (t *ScheduledTask) Days(days any) *ScheduledTask {
	weekdays := mustParseWeekdays(days)
	t.useCalendar()
	t.runOnStart = false
	t.calendar.weekdays = weekdays
	return t
}

// Every 设置任务按自定义固定间隔执行。
// 该模式在调度器启动后立即执行一次，随后按固定间隔重复。
// 示例：s.Call(fn).Every(30 * time.Second)
func (t *ScheduledTask) Every(d time.Duration) *ScheduledTask {
	if d <= 0 {
		panic(fmt.Sprintf("schedule every: invalid interval %v", d))
	}
	t.interval = d
	t.mode = scheduleModeInterval
	t.calendar = calendarSpec{}
	return t
}

// WithoutOverlapping 防止同一任务上一次执行尚未结束时再次运行。
// expiresAt 可选，单位为分钟；默认 1440 分钟。
// 示例：s.Command("emails:send").EveryMinute().WithoutOverlapping(10)
func (t *ScheduledTask) WithoutOverlapping(expiresAt ...int) *ScheduledTask {
	if len(expiresAt) > 1 {
		panic("schedule withoutOverlapping: too many arguments")
	}
	expires := 1440
	if len(expiresAt) == 1 {
		expires = expiresAt[0]
	}
	if expires <= 0 {
		panic(fmt.Sprintf("schedule withoutOverlapping: invalid expiresAt %d", expires))
	}
	t.withoutOverlapping = true
	t.withoutOverlappingExpires = time.Duration(expires) * time.Minute
	return t
}

// Name 设置任务名称，用于日志标识。
// 示例：s.Call(fn).EveryMinute().Name("tenant_sync")
func (t *ScheduledTask) Name(name string) *ScheduledTask {
	t.name = name
	return t
}

// Description 设置任务描述。
// 示例：s.Call(fn).EveryMinute().Description("同步租户缓存")
func (t *ScheduledTask) Description(desc string) *ScheduledTask {
	t.description = desc
	return t
}

func (t *ScheduledTask) useCalendar() {
	t.mode = scheduleModeCalendar
	if t.calendar.location == nil {
		t.calendar.location = time.Local
	}
}

func (t *ScheduledTask) usesCalendarSchedule() bool {
	return t.mode == scheduleModeCalendar
}

func (t *ScheduledTask) overlapLockKey() string {
	name := strings.TrimSpace(t.name)
	if name != "" {
		return "schedule:" + name
	}
	return fmt.Sprintf("schedule:task:%d", t.id)
}

func (t *ScheduledTask) nextRunAfter(now time.Time) time.Time {
	location := t.calendar.location
	if location == nil {
		location = time.Local
	}

	candidate := now.In(location).Truncate(time.Second).Add(time.Second)
	for i := 0; i < 366*6; i++ {
		dayStart := time.Date(candidate.Year(), candidate.Month(), candidate.Day(), 0, 0, 0, 0, location)
		if !t.matchesCalendarDay(dayStart) {
			candidate = dayStart.Add(24 * time.Hour)
			continue
		}

		next := t.nextTimeOnDay(candidate)
		if !next.IsZero() {
			return next
		}

		candidate = dayStart.Add(24 * time.Hour)
	}

	panic("schedule nextRunAfter: unable to find next execution time within search window")
}

func (t *ScheduledTask) nextTimeOnDay(candidate time.Time) time.Time {
	location := candidate.Location()
	year, month, day := candidate.Date()
	allowedHours := t.allowedHours()
	allowedMinutes := t.allowedMinutes()
	allowedSeconds := t.allowedSeconds()

	for _, hour := range allowedHours {
		if hour < candidate.Hour() {
			continue
		}

		for _, minute := range allowedMinutes {
			if hour == candidate.Hour() && minute < candidate.Minute() {
				continue
			}

			for _, second := range allowedSeconds {
				if hour == candidate.Hour() && minute == candidate.Minute() && second < candidate.Second() {
					continue
				}
				return time.Date(year, month, day, hour, minute, second, 0, location)
			}
		}
	}

	return time.Time{}
}

func (t *ScheduledTask) matchesCalendarDay(day time.Time) bool {
	if len(t.calendar.months) > 0 && !containsMonth(t.calendar.months, day.Month()) {
		return false
	}
	if len(t.calendar.weekdays) > 0 && !containsWeekday(t.calendar.weekdays, day.Weekday()) {
		return false
	}
	if t.calendar.dayOfQuarter > 0 && quarterDay(day) != t.calendar.dayOfQuarter {
		return false
	}
	if t.calendar.lastDayOfMonth {
		return day.Day() == lastDayOfMonth(day)
	}
	if len(t.calendar.daysOfMonth) > 0 {
		return containsInt(t.calendar.daysOfMonth, day.Day())
	}
	return true
}

func (t *ScheduledTask) applyDaysOfMonth(days []int) *ScheduledTask {
	t.useCalendar()
	t.runOnStart = false
	t.calendar.daysOfMonth = normalizeInts(days)
	t.calendar.lastDayOfMonth = false
	t.calendar.dayOfQuarter = 0
	return t
}

func (t *ScheduledTask) allowedSeconds() []int {
	if len(t.calendar.seconds) == 0 {
		return []int{0}
	}
	return t.calendar.seconds
}

func (t *ScheduledTask) allowedMinutes() []int {
	if len(t.calendar.minutes) == 0 {
		return allMinutes()
	}
	return t.calendar.minutes
}

func (t *ScheduledTask) allowedHours() []int {
	if len(t.calendar.hours) == 0 {
		return allHours()
	}
	return t.calendar.hours
}

func mustParseClock(value string) (int, int, int) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 && len(parts) != 3 {
		panic(fmt.Sprintf("schedule time: invalid value %q", value))
	}

	hour := mustAtoiInRange("hour", parts[0], 0, 23)
	minute := mustAtoiInRange("minute", parts[1], 0, 59)
	second := 0
	if len(parts) == 3 {
		second = mustAtoiInRange("second", parts[2], 0, 59)
	}
	return hour, minute, second
}

func mustParseIntValues(field string, value any, min int, max int) []int {
	switch v := value.(type) {
	case int:
		mustValidateRange(field, v, min, max)
		return []int{v}
	case string:
		return mustParseIntListString(field, v, min, max)
	case []int:
		if len(v) == 0 {
			panic(fmt.Sprintf("schedule %s: empty values", field))
		}
		for _, item := range v {
			mustValidateRange(field, item, min, max)
		}
		return normalizeInts(v)
	case []string:
		if len(v) == 0 {
			panic(fmt.Sprintf("schedule %s: empty values", field))
		}
		joined := strings.Join(v, ",")
		return mustParseIntListString(field, joined, min, max)
	default:
		panic(fmt.Sprintf("schedule %s: unsupported value type %T", field, value))
	}
}

func mustParseIntListString(field string, value string, min int, max int) []int {
	parts := strings.Split(value, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				panic(fmt.Sprintf("schedule %s: invalid range %q", field, part))
			}
			start := mustAtoiInRange(field, rangeParts[0], min, max)
			end := mustAtoiInRange(field, rangeParts[1], min, max)
			if start > end {
				panic(fmt.Sprintf("schedule %s: invalid range %q", field, part))
			}
			for item := start; item <= end; item++ {
				values = append(values, item)
			}
			continue
		}
		values = append(values, mustAtoiInRange(field, part, min, max))
	}
	if len(values) == 0 {
		panic(fmt.Sprintf("schedule %s: empty values", field))
	}
	return normalizeInts(values)
}

func mustParseWeekdays(value any) []time.Weekday {
	switch v := value.(type) {
	case time.Weekday:
		return []time.Weekday{v}
	case []time.Weekday:
		if len(v) == 0 {
			panic("schedule weekdays: empty values")
		}
		return normalizeWeekdays(v)
	case int:
		mustValidateRange("weekday", v, 0, 6)
		return []time.Weekday{time.Weekday(v)}
	case []int:
		if len(v) == 0 {
			panic("schedule weekdays: empty values")
		}
		weekdays := make([]time.Weekday, 0, len(v))
		for _, item := range v {
			mustValidateRange("weekday", item, 0, 6)
			weekdays = append(weekdays, time.Weekday(item))
		}
		return normalizeWeekdays(weekdays)
	case string:
		return mustParseWeekdayString(v)
	case []string:
		if len(v) == 0 {
			panic("schedule weekdays: empty values")
		}
		return mustParseWeekdayString(strings.Join(v, ","))
	default:
		panic(fmt.Sprintf("schedule weekdays: unsupported value type %T", value))
	}
}

func mustParseWeekdayString(value string) []time.Weekday {
	parts := strings.Split(value, ",")
	weekdays := make([]time.Weekday, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(strings.ToLower(part))
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				panic(fmt.Sprintf("schedule weekdays: invalid range %q", part))
			}
			start := mustWeekdayValue(rangeParts[0])
			end := mustWeekdayValue(rangeParts[1])
			if start > end {
				panic(fmt.Sprintf("schedule weekdays: invalid range %q", part))
			}
			for item := start; item <= end; item++ {
				weekdays = append(weekdays, time.Weekday(item))
			}
			continue
		}
		weekdays = append(weekdays, time.Weekday(mustWeekdayValue(part)))
	}
	if len(weekdays) == 0 {
		panic("schedule weekdays: empty values")
	}
	return normalizeWeekdays(weekdays)
}

func mustWeekdayValue(value string) int {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "0", "sun", "sunday":
		return 0
	case "1", "mon", "monday":
		return 1
	case "2", "tue", "tues", "tuesday":
		return 2
	case "3", "wed", "wednesday":
		return 3
	case "4", "thu", "thur", "thurs", "thursday":
		return 4
	case "5", "fri", "friday":
		return 5
	case "6", "sat", "saturday":
		return 6
	default:
		panic(fmt.Sprintf("schedule weekday: invalid value %q", value))
	}
}

func mustAtoiInRange(field string, value string, min int, max int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		panic(fmt.Sprintf("schedule %s: invalid integer %q", field, value))
	}
	mustValidateRange(field, parsed, min, max)
	return parsed
}

func mustValidateRange(field string, value int, min int, max int) {
	if value < min || value > max {
		panic(fmt.Sprintf("schedule %s: value %d out of range [%d,%d]", field, value, min, max))
	}
}

func mustIntArg(field string, value any, min int, max int) int {
	switch v := value.(type) {
	case int:
		mustValidateRange(field, v, min, max)
		return v
	case string:
		return mustAtoiInRange(field, v, min, max)
	default:
		panic(fmt.Sprintf("schedule %s: unsupported value type %T", field, value))
	}
}

func isLastDayToken(value any) bool {
	text, ok := value.(string)
	if !ok {
		return false
	}
	text = strings.TrimSpace(strings.ToLower(text))
	return text == "l" || text == "last"
}

func normalizeInts(values []int) []int {
	cloned := append([]int(nil), values...)
	sort.Ints(cloned)
	result := cloned[:0]
	for _, value := range cloned {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func normalizeWeekdays(values []time.Weekday) []time.Weekday {
	ints := make([]int, 0, len(values))
	for _, value := range values {
		ints = append(ints, int(value))
	}
	ints = normalizeInts(ints)
	result := make([]time.Weekday, 0, len(ints))
	for _, value := range ints {
		result = append(result, time.Weekday(value))
	}
	return result
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsWeekday(values []time.Weekday, target time.Weekday) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsMonth(values []time.Month, target time.Month) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func stepValues(start int, step int, max int) []int {
	values := make([]int, 0, (max-start)/step+1)
	for value := start; value <= max; value += step {
		values = append(values, value)
	}
	return values
}

func allHours() []int {
	return stepValues(0, 1, 23)
}

func allMinutes() []int {
	return stepValues(0, 1, 59)
}

func lastDayOfMonth(day time.Time) int {
	return time.Date(day.Year(), day.Month()+1, 0, 0, 0, 0, 0, day.Location()).Day()
}

func quarterDay(day time.Time) int {
	quarterStartMonth := ((int(day.Month())-1)/3)*3 + 1
	quarterStart := time.Date(day.Year(), time.Month(quarterStartMonth), 1, 0, 0, 0, 0, day.Location())
	currentDay := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, day.Location())
	return int(currentDay.Sub(quarterStart).Hours()/24) + 1
}

func firstAny(values []any, fallback any) any {
	if len(values) == 0 {
		return fallback
	}
	return values[0]
}

func firstString(values []string, fallback string) string {
	if len(values) == 0 {
		return fallback
	}
	return values[0]
}
