// Package timer Cron 层异常处理集成测试。
//
// 测试范围：
//   - 任务返回 error 时通过 prismgo/exception 上报
//   - 任务 panic 时恢复、上报、循环继续
//   - 成功任务不触发上报
package timer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
)

func bindTimerExceptionHandler(t *testing.T, handler *goexception.Handler) {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("exception.handler", handler, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	if err := registry.Instance("config.default", config.New()); err != nil {
		t.Fatalf("bind config: %v", err)
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

// TestScheduleTaskErrorIsReported 验证任务返回 error 时通过 exception.Report 上报。
func TestScheduleTaskErrorIsReported(t *testing.T) {
	var reported struct {
		err    error
		fields map[string]any
	}
	handler := goexception.New(
		goexception.WithReporter(func(_ any, err error, fields map[string]any) {
			reported.err = err
			reported.fields = fields
		}),
	)
	bindTimerExceptionHandler(t, handler)

	var count atomic.Int32
	taskErr := errors.New("task failed")
	s := NewSchedule()
	s.Call(func(_ context.Context) error {
		count.Add(1)
		return taskErr
	}).Every(50 * time.Millisecond).Name("error-task")

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer cancel()
	defer s.Stop()

	waitForCount(t, &count, 1, time.Second)
	cancel()
	s.Stop()

	if reported.err == nil {
		t.Fatal("expected reporter to be called, but it wasn't")
	}
	if reported.fields["task"] != "error-task" {
		t.Errorf("task = %q, want error-task", reported.fields["task"])
	}
	if reported.fields["status"] != 500 {
		t.Errorf("status = %v, want 500", reported.fields["status"])
	}
	if reported.fields["component"] != "cron" {
		t.Errorf("component = %q, want cron", reported.fields["component"])
	}
	if _, ok := reported.fields["duration_ms"]; !ok {
		t.Error("fields missing duration_ms")
	}
}

// TestScheduleTaskPanicIsRecoveredAndReported 验证任务 panic 时恢复、上报且循环继续。
func TestScheduleTaskPanicIsRecoveredAndReported(t *testing.T) {
	var reported struct {
		err    error
		fields map[string]any
	}
	handler := goexception.New(
		goexception.WithReporter(func(_ any, err error, fields map[string]any) {
			reported.err = err
			reported.fields = fields
		}),
	)
	bindTimerExceptionHandler(t, handler)

	var count atomic.Int32
	s := NewSchedule()
	s.Call(func(_ context.Context) error {
		count.Add(1)
		panic("task panic")
	}).Every(50 * time.Millisecond).Name("panic-task")

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer cancel()
	defer s.Stop()

	// 等待任务至少执行 2 次（验证 panic 后循环未中断）
	waitForCount(t, &count, 2, time.Second)
	cancel()
	s.Stop()

	if reported.err == nil {
		t.Fatal("expected reporter to be called for panic, but it wasn't")
	}
	if reported.fields["task"] != "panic-task" {
		t.Errorf("task = %q, want panic-task", reported.fields["task"])
	}
	if reported.fields["status"] != 500 {
		t.Errorf("status = %v, want 500", reported.fields["status"])
	}
	if reported.fields["component"] != "cron" {
		t.Errorf("component = %q, want cron", reported.fields["component"])
	}

	got := count.Load()
	if got < 2 {
		t.Errorf("task should run at least 2 times, got %d", got)
	}
}

// TestSuccessfulScheduleTaskIsNotReported 验证成功任务不触发 Reporter。
func TestSuccessfulScheduleTaskIsNotReported(t *testing.T) {
	reported := false
	handler := goexception.New(
		goexception.WithReporter(func(_ any, _ error, _ map[string]any) {
			reported = true
		}),
	)
	bindTimerExceptionHandler(t, handler)

	var count atomic.Int32
	s := NewSchedule()
	s.Call(func(_ context.Context) error {
		count.Add(1)
		return nil
	}).Every(50 * time.Millisecond).Name("ok-task")

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer cancel()
	defer s.Stop()

	waitForCount(t, &count, 1, time.Second)
	cancel()
	s.Stop()

	if reported {
		t.Error("reporter should NOT be called for successful task")
	}
}
