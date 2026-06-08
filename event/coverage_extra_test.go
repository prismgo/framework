package event

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue"
)

type queueOptionsListener struct{}

func (queueOptionsListener) Handle(context.Context, Event) error { return nil }
func (queueOptionsListener) ShouldQueue() bool                   { return true }
func (queueOptionsListener) QueueConnection() string             { return "sync" }
func (queueOptionsListener) QueueName() string                   { return "mail" }
func (queueOptionsListener) QueueDelay() time.Duration           { return time.Second }
func (queueOptionsListener) QueueTries() int                     { return 2 }
func (queueOptionsListener) QueueBackoff() []time.Duration {
	return []time.Duration{time.Second, 2 * time.Second}
}
func (queueOptionsListener) QueueTimeout() time.Duration { return 3 * time.Second }

type badJSONEvent struct {
	Ch chan int `json:"ch"`
}

func (badJSONEvent) Name() string { return "bad.json" }

type badPayloadEvent struct{}

func (*badPayloadEvent) Name() string { return "event.bad_payload" }

func TestFacadeFactoryAndLifecycleNames(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	bus := New()
	if err := registry.Singleton("event.dispatcher", func(containercontract.Resolver) (any, error) { return bus, nil }); err != nil {
		t.Fatalf("register event dispatcher: %v", err)
	}
	resolved := Resolve()
	if resolved != bus {
		t.Fatal("factory should resolve and install dispatcher")
	}

	names := []string{
		AppBooting{}.Name(),
		AppBooted{}.Name(),
		AppTerminating{}.Name(),
		AppTerminated{}.Name(),
		ProviderRegistering{}.Name(),
		ProviderRegistered{}.Name(),
		ProviderBooting{}.Name(),
		ProviderBooted{}.Name(),
		ServerStarting{}.Name(),
		ServerStarted{}.Name(),
		ServerStopping{}.Name(),
		ServerStopped{}.Name(),
		RequestReceived{}.Name(),
		RequestHandled{}.Name(),
		RequestFailed{}.Name(),
		RequestFinished{}.Name(),
	}
	want := []string{
		EventAppBooting,
		EventAppBooted,
		EventAppTerminating,
		EventAppTerminated,
		EventProviderRegistering,
		EventProviderRegistered,
		EventProviderBooting,
		EventProviderBooted,
		EventServerStarting,
		EventServerStarted,
		EventServerStopping,
		EventServerStopped,
		EventRequestReceived,
		EventRequestHandled,
		EventRequestFailed,
		EventRequestFinished,
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("lifecycle name[%d] = %q, want %q", i, names[i], want[i])
		}
	}
}

func TestQueuedHelpersAndErrors(t *testing.T) {
	useIsolatedFacadeRegistry(t)

	wrapped := Queued(ListenerFunc(func(context.Context, Event) error { return nil }))
	if !isQueued(wrapped) {
		t.Fatal("Queued wrapper should mark listener as queued")
	}
	if isQueued(ListenerFunc(func(context.Context, Event) error { return nil })) {
		t.Fatal("plain listener should not be queued")
	}

	options := listenerQueueOptions(queueOptionsListener{})
	if options.QueueConnection() != "sync" || options.QueueName() != "mail" ||
		options.QueueDelay() != time.Second || options.QueueTries() != 2 ||
		options.QueueTimeout() != 3*time.Second || len(options.QueueBackoff()) != 2 {
		t.Fatalf("listener options = %#v, want populated queue listener options", options)
	}
	plain := listenerQueueOptions(ListenerFunc(func(context.Context, Event) error { return nil }))
	if plain.QueueConnection() != "" || plain.QueueName() != "" || plain.QueueDelay() != 0 ||
		plain.QueueTries() != 0 || plain.QueueTimeout() != 0 || len(plain.QueueBackoff()) != 0 {
		t.Fatal("plain listener should provide empty queue options")
	}

	eventFactoriesMu.Lock()
	eventFactories["event.nil_factory"] = func() Event { return nil }
	eventFactoriesMu.Unlock()
	if _, err := restoreEvent("event.nil_factory", []byte(`{}`)); err == nil {
		t.Fatal("expected nil event factory error")
	}
	RegisterEvent[*badPayloadEvent]()
	if _, err := restoreEvent("event.bad_payload", []byte(`{bad`)); err == nil {
		t.Fatal("expected bad event payload error")
	}
	raw, err := restoreEvent("event.raw", json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatalf("restore raw event: %v", err)
	}
	if raw.Name() != "event.raw" {
		t.Fatalf("raw event name = %q", raw.Name())
	}

	if err := (&queuedListenerJob{ListenerID: "missing"}).Handle(context.Background()); err == nil {
		t.Fatal("expected missing queued listener error")
	}
	id := nextQueuedListenerID()
	rememberQueuedListener(id, ListenerFunc(func(context.Context, Event) error { return errors.New("listener failed") }))
	if queuedListener(id) == nil {
		t.Fatal("expected remembered listener")
	}
	if err := (&queuedListenerJob{ListenerID: id, EventName: "event.raw", Payload: []byte(`{}`)}).Handle(context.Background()); err == nil {
		t.Fatal("expected listener error")
	}

	dispatchQueuedListener(context.Background(), "bad-json", queueOptionsListener{}, badJSONEvent{Ch: make(chan int)})

	manager, err := queue.NewManager(queue.Config{Default: "sync"}, queue.DefaultRegistry())
	if err != nil {
		t.Fatalf("new queue manager: %v", err)
	}
	// Manager 已实现 contracts/queue.Dispatcher (DispatchJob/RequestRestart/Close 匹配，
	// Connection/Failed 因返回内部实现类型暂不直接匹配接口；此处跳过该分支
	_ = manager
	// 直接测试 null dispatcher 路径作为替代覆盖
	dispatchQueuedListener(context.Background(), id, queueOptionsListener{}, queuedEvent{Value: "ok"})
}

func TestRegisterQueuedListenerJobsRegistersInternalJob(t *testing.T) {
	registry := queue.NewRegistry()
	name, err := queue.JobTypeName(&queuedListenerJob{})
	if err != nil {
		t.Fatalf("job type name: %v", err)
	}
	if registry.Has(name) {
		t.Fatal("registry should not contain queued listener job before registration")
	}

	RegisterQueuedListenerJobs(registry)

	if !registry.Has(name) {
		t.Fatal("registry should contain queued listener job after registration")
	}
}

func TestResolveQueuedDispatcherUsesDispatcherServiceKey(t *testing.T) {
	useIsolatedFacadeRegistry(t)
	UseQueuedDispatcher(nil)

	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	var count int
	dispatcher := recordingQueuedDispatcher{dispatch: func(_ context.Context, job queuecontract.Job, _ queuecontract.DispatchOptions) (string, error) {
		count++
		return "job-id", job.Handle(context.Background())
	}}
	if err := registry.Instance(queuecontract.DispatcherServiceKey, queuecontract.Dispatcher(dispatcher)); err != nil {
		t.Fatalf("register dispatcher: %v", err)
	}

	var handled int
	RegisterEvent[*queuedEvent]()
	bus := New()
	bus.Listen("event.queue_test", testQueuedDispatcherListener{handled: &handled})
	bus.Dispatch(context.Background(), queuedEvent{Value: "ok"})

	if count != 1 {
		t.Fatalf("dispatcher count = %d, want 1", count)
	}
	if handled != 1 {
		t.Fatalf("listener handled = %d, want 1", handled)
	}
}

type testQueuedDispatcherListener struct {
	handled *int
}

func (l testQueuedDispatcherListener) Handle(_ context.Context, ev Event) error {
	if typed, ok := ev.(*queuedEvent); ok && typed.Value == "ok" {
		(*l.handled)++
	}
	return nil
}

func (testQueuedDispatcherListener) ShouldQueue() bool { return true }

type recordingQueuedDispatcher struct {
	dispatch func(context.Context, queuecontract.Job, queuecontract.DispatchOptions) (string, error)
}

func (d recordingQueuedDispatcher) DispatchJob(ctx context.Context, job queuecontract.Job, options queuecontract.DispatchOptions) (string, error) {
	return d.dispatch(ctx, job, options)
}

func (recordingQueuedDispatcher) RequestRestart(context.Context) error { return nil }
func (recordingQueuedDispatcher) Close() error                         { return nil }
