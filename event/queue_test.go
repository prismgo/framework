package event

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"

	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
	"github.com/prismgo/framework/queue"
	prismredis "github.com/prismgo/framework/redis"
)

type queuedEvent struct {
	Value string `json:"value"`
}

func (queuedEvent) Name() string { return "event.queue_test" }

type namedQueuedEvent struct {
	Value string `json:"value"`
}

func (*namedQueuedEvent) Name() string { return "event.named_queue_test" }

type badValueEvent struct{}

func (badValueEvent) Name() string { return "event.bad_value" }

type testQueuedListener struct {
	count *atomic.Int32
}

func (l testQueuedListener) Handle(_ context.Context, ev Event) error {
	if typed, ok := ev.(*queuedEvent); ok && typed.Value == "ok" {
		l.count.Add(1)
	}
	return nil
}

func (testQueuedListener) ShouldQueue() bool { return true }

func TestDispatcherShouldQueueListenerRunsThroughQueueWorker(t *testing.T) {
	useIsolatedFacadeRegistry(t)

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	containerRegistry := container.NewContainer()
	container.SetProvider(func() *container.Container { return containerRegistry })
	redisManager, err := prismredis.NewManager(prismredis.Config{
		DefaultName: "default",
		Connections: map[string]prismredis.ConnectionConfig{
			"default": {Name: "default", Addr: srv.Addr()},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager: %v", err)
	}
	if err := containerRegistry.Instance("redis", redisManager); err != nil {
		t.Fatalf("use redis manager: %v", err)
	}
	defer func() {
		_ = redisManager.Close(context.Background())
		container.SetProvider(nil)
		srv.Close()
	}()

	manager, err := queue.NewManager(queue.Config{
		Default: "redis",
		Connections: map[string]queue.ConnectionConfig{
			"redis": {
				Driver: "redis",
				Options: map[string]any{
					"connection": "default",
					"prefix":     "event_queue_test",
				},
			},
		},
	}, queue.DefaultRegistry())
	if err != nil {
		t.Fatalf("new queue manager: %v", err)
	}
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close queue manager: %v", err)
		}
	}()
	UseQueuedDispatcher(queue.NewDispatcher(manager))
	t.Cleanup(func() { UseQueuedDispatcher(nil) })
	RegisterEvent[*queuedEvent]()

	var count atomic.Int32
	bus := New()
	bus.Listen("event.queue_test", testQueuedListener{count: &count})
	bus.Dispatch(context.Background(), queuedEvent{Value: "ok"})
	if count.Load() != 0 {
		t.Fatal("queued listener should not run inside Dispatch")
	}

	if err := queue.NewWorker(manager).Work(context.Background(), queue.WorkerOptions{Once: true}); err != nil {
		t.Fatalf("work queued listener: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("queued listener count = %d, want 1", count.Load())
	}
}

func TestRegisterEventUsesEventNameFromType(t *testing.T) {
	useIsolatedFacadeRegistry(t)

	RegisterEvent[*namedQueuedEvent]()

	ev, err := restoreEvent("event.named_queue_test", []byte(`{"value":"ok"}`))
	if err != nil {
		t.Fatalf("restore event: %v", err)
	}
	if _, ok := ev.(*namedQueuedEvent); !ok {
		t.Fatalf("restored event type = %T, want *namedQueuedEvent", ev)
	}
}

func TestRegisterEventPanicsForValueEventType(t *testing.T) {
	useIsolatedFacadeRegistry(t)

	defer func() {
		if recover() == nil {
			t.Fatal("expected RegisterEvent to panic for non-pointer event type")
		}
	}()

	RegisterEvent[badValueEvent]()
}

func TestQueuedListenerDispatchErrorIsReported(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)
	UseQueuedDispatcher(nil)

	var reported error
	var fields map[string]any
	if err := registry.Instance("exception.handler", exception.New(
		exception.WithPanicStack(false),
		exception.WithReporter(func(_ any, err error, got map[string]any) {
			reported = err
			fields = got
		}),
	)); err != nil {
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

	bus := New()
	bus.Listen("bad.json", queueOptionsListener{})
	bus.Dispatch(context.Background(), badJSONEvent{Ch: make(chan int)})

	if reported == nil || !strings.Contains(reported.Error(), "unsupported type: chan int") {
		t.Fatalf("reported error = %v, want JSON marshal error", reported)
	}
	if fields["component"] != "event" ||
		fields["subsystem"] != "queued_listener" ||
		fields["event"] != "bad.json" ||
		fields["listener"] != "queued" ||
		fields["operation"] != "dispatch" ||
		fields["status"] != 500 {
		t.Fatalf("reported fields = %#v", fields)
	}
}

func TestQueuedListenerMissingDispatcherIsReported(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)
	UseQueuedDispatcher(nil)

	var reported error
	if err := registry.Instance("exception.handler", exception.New(
		exception.WithPanicStack(false),
		exception.WithReporter(func(_ any, err error, _ map[string]any) {
			reported = err
		}),
	)); err != nil {
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

	var count atomic.Int32
	bus := New()
	bus.Listen("event.queue_test", testQueuedListener{count: &count})
	bus.Dispatch(context.Background(), queuedEvent{Value: "ok"})

	if count.Load() != 0 {
		t.Fatal("queued listener should not run without dispatcher")
	}
	if reported == nil || !strings.Contains(reported.Error(), "dispatcher is not configured") {
		t.Fatalf("reported error = %v, want dispatcher configuration error", reported)
	}
}

// TestQueuedListenerJobPanicsAndRecovers 验证 queued listener 执行时 panic 会被恢复并返回错误
func TestQueuedListenerJobPanicsAndRecovers(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	var reported error
	var fields map[string]any
	if err := registry.Instance("exception.handler", exception.New(
		exception.WithPanicStack(false),
		exception.WithReporter(func(_ any, err error, got map[string]any) {
			reported = err
			fields = got
		}),
	)); err != nil {
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

	// 注册一个会 panic 的监听器
	id := nextQueuedListenerID()
	panicListener := ListenerFunc(func(ctx context.Context, ev Event) error {
		panic("queued listener panic test")
	})
	rememberQueuedListener(id, panicListener)

	// 创建 job 并执行
	job := &queuedListenerJob{
		ListenerID: id,
		EventName:  "test.panic",
		Payload:    []byte(`{}`),
	}

	// 应该返回错误而不是 panic
	err = job.Handle(context.Background())
	if err == nil {
		t.Fatal("expected error from panicked listener, got nil")
	}
	if !strings.Contains(err.Error(), "queued listener panic") {
		t.Fatalf("expected panic error message, got: %v", err)
	}

	// 验证异常被报告
	if reported == nil || !strings.Contains(reported.Error(), "queued listener panic") {
		t.Fatalf("expected reported error, got: %v", reported)
	}
	if fields["component"] != "event" ||
		fields["subsystem"] != "queued_listener" ||
		fields["event"] != "test.panic" ||
		fields["listener_id"] != id ||
		fields["operation"] != "handle" {
		t.Fatalf("reported fields = %#v", fields)
	}
	if fields["stack"] == "" {
		t.Fatal("expected stack trace in reported fields")
	}
}
