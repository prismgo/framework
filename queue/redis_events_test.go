package queue

import (
	"context"
	"encoding/base64"
	"errors"
	redisqueue "github.com/prismgo/framework/queue/redis"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
	"github.com/redis/go-redis/v9"
)

func captureQueueEvents(t *testing.T) *[]Event {
	t.Helper()
	events := []Event{}
	UseEventSink(func(_ context.Context, ev Event) {
		events = append(events, ev)
	})
	t.Cleanup(func() { UseEventSink(nil) })
	return &events
}

func TestRedisQueueConstructionEmitsNamedLifecycleEvents(t *testing.T) {
	events := captureQueueEvents(t)
	server := miniredis.RunT(t)
	useRedisLifecycleManager(t, server.Addr())

	conn, err := redisqueue.NewRedisQueue(redisqueue.RedisOptions{Name: "redis_direct"})
	if err != nil {
		t.Fatalf("new redis queue: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if len(*events) != 2 {
		t.Fatalf("events = %d, want connecting and connected", len(*events))
	}
	connecting, ok := (*events)[0].(InfrastructureEvent)
	if !ok {
		t.Fatalf("first event type = %T, want InfrastructureEvent", (*events)[0])
	}
	connected, ok := (*events)[1].(InfrastructureEvent)
	if !ok {
		t.Fatalf("second event type = %T, want InfrastructureEvent", (*events)[1])
	}
	for _, ev := range []InfrastructureEvent{connecting, connected} {
		if ev.Driver != "redis" || ev.Connection != "redis_direct" || ev.Queue != "" || ev.Error != "" || ev.Timestamp.IsZero() {
			t.Fatalf("event payload = %#v", ev)
		}
	}
	if connecting.Name() != EventConnectionConnecting {
		t.Fatalf("first event name = %q, want %q", connecting.Name(), EventConnectionConnecting)
	}
	if connected.Name() != EventConnectionConnected {
		t.Fatalf("second event name = %q, want %q", connected.Name(), EventConnectionConnected)
	}
}

func TestRedisQueueConstructionFailureEmitsDisconnectedEventAndReturnsError(t *testing.T) {
	events := captureQueueEvents(t)
	if _, err := redisqueue.NewRedisQueue(redisqueue.RedisOptions{Name: "redis_broken", Connection: "missing"}); err == nil {
		t.Fatal("expected redis queue construction error")
	}
	if len(*events) != 2 {
		t.Fatalf("events = %d, want connecting and disconnected", len(*events))
	}
	connecting, ok := (*events)[0].(InfrastructureEvent)
	if !ok {
		t.Fatalf("first event type = %T, want InfrastructureEvent", (*events)[0])
	}
	disconnected, ok := (*events)[1].(InfrastructureEvent)
	if !ok {
		t.Fatalf("second event type = %T, want InfrastructureEvent", (*events)[1])
	}
	if connecting.Name() != EventConnectionConnecting {
		t.Fatalf("first event name = %q, want %q", connecting.Name(), EventConnectionConnecting)
	}
	if disconnected.Name() != EventConnectionDisconnected {
		t.Fatalf("second event name = %q, want %q", disconnected.Name(), EventConnectionDisconnected)
	}
	if disconnected.Connection != "redis_broken" || disconnected.Driver != "redis" || disconnected.Error == "" {
		t.Fatalf("disconnected event = %#v", disconnected)
	}
}

func TestRedisManagerConnectionUsesConfiguredNameInLifecycleEvents(t *testing.T) {
	events := captureQueueEvents(t)
	server := miniredis.RunT(t)
	useRedisLifecycleManager(t, server.Addr())

	manager, err := NewManager(Config{
		Default: "redis_high",
		Connections: map[string]ConnectionConfig{
			"redis_high": {
				Driver:     "redis",
				RetryAfter: time.Second,
				Options:    map[string]any{"connection": "default", "prefix": "redis_event_name"},
			},
		},
	}, newTestRegistry())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	if len(*events) != 2 {
		t.Fatalf("events = %d, want connecting and connected", len(*events))
	}
	for _, item := range *events {
		ev, ok := item.(InfrastructureEvent)
		if !ok {
			t.Fatalf("event type = %T, want InfrastructureEvent", item)
		}
		if ev.Driver != "redis" || ev.Connection != "redis_high" {
			t.Fatalf("event payload = %#v", ev)
		}
	}
}

func TestRedisCloseEmitsDisconnectedEventWithCloseResult(t *testing.T) {
	events := captureQueueEvents(t)
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_close", Prefix: "redis_close"})
	*events = nil

	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	srv.Close()

	if len(*events) != 1 {
		t.Fatalf("events = %d, want disconnected", len(*events))
	}
	disconnected, ok := (*events)[0].(InfrastructureEvent)
	if !ok {
		t.Fatalf("event type = %T, want InfrastructureEvent", (*events)[0])
	}
	if disconnected.Name() != EventConnectionDisconnected || disconnected.Driver != "redis" || disconnected.Connection != "redis_close" || disconnected.Error != "" {
		t.Fatalf("event payload = %#v", disconnected)
	}
}

func TestRedisPushFailuresEmitPublishFailed(t *testing.T) {
	ctx := context.Background()
	events := captureQueueEvents(t)
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_publish", Prefix: "redis_publish", Codec: encodingpkg.JSON()})
	t.Cleanup(func() { _ = conn.Close() })

	*events = nil
	srv.Close()
	valid := &payload.Envelope{ID: "ok", Name: testJobName(), Queue: "ready", Payload: []byte(`{"key":"ok"}`)}
	body, err := conn.Codec().Marshal(valid)
	if err != nil {
		t.Fatalf("marshal valid envelope: %v", err)
	}
	if err := conn.Push(ctx, "ready", queuecontract.Payload(body)); err == nil {
		t.Fatal("expected ready publish redis error")
	}
	assertRedisPublishFailedEvent(t, *events, "redis_publish", "ready")

	*events = nil
	if err := conn.Later(ctx, "delayed", queuecontract.Payload(body), time.Second); err == nil {
		t.Fatal("expected delayed publish redis error")
	}
	assertRedisPublishFailedEvent(t, *events, "redis_publish", "delayed")
}

func TestRedisCommandErrorsDoNotEmitDisconnectedEvent(t *testing.T) {
	events := captureQueueEvents(t)
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_io", Prefix: "redis_io"})
	t.Cleanup(func() { _ = conn.Close() })
	*events = nil

	srv.Close()
	if _, err := conn.Size(context.Background(), "default"); err == nil {
		t.Fatal("expected redis command error")
	}
	for _, item := range *events {
		if item.Name() == EventConnectionDisconnected {
			t.Fatalf("unexpected disconnected event after command I/O error: %#v", item)
		}
	}
}

func TestRedisReleasePublishesFailureOnlyAfterDeleteSucceeds(t *testing.T) {
	ctx := context.Background()
	events := captureQueueEvents(t)
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	failures := map[string]error{}
	client.AddHook(redisCommandErrorHook{fail: failures})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_release", Prefix: "redis_release", Codec: encodingpkg.JSON()})
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Close()
	})
	*events = nil

	releaseEnv := &payload.Envelope{
		ID:      "release",
		Name:    testJobName(),
		Queue:   "default",
		Payload: []byte(`{"key":"release"}`),
	}
	reservedBody, err := conn.Codec().Marshal(releaseEnv)
	if err != nil {
		t.Fatalf("marshal release envelope: %v", err)
	}
	if err := conn.Push(ctx, "default", queuecontract.Payload(reservedBody)); err != nil {
		t.Fatalf("push release envelope: %v", err)
	}
	reserved, err := conn.Pop(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("pop release envelope: %v", err)
	}
	failures["rpush"] = errors.New("forced rpush failure")
	err = reserved.Release(ctx, 0)
	if err == nil {
		t.Fatal("expected release publish error")
	}
	assertRedisPublishFailedEvent(t, *events, "redis_release", "default")
}

func TestRedisReleaseDeleteFailureDoesNotEmitPublishFailed(t *testing.T) {
	ctx := context.Background()
	events := captureQueueEvents(t)
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	failures := map[string]error{}
	client.AddHook(redisCommandErrorHook{fail: failures})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_release_delete", Prefix: "redis_release_delete", Codec: encodingpkg.JSON()})
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Close()
	})
	*events = nil

	releaseEnv := &payload.Envelope{
		ID:      "release",
		Name:    testJobName(),
		Queue:   "default",
		Payload: []byte(`{"key":"release"}`),
	}
	reservedBody, err := conn.Codec().Marshal(releaseEnv)
	if err != nil {
		t.Fatalf("marshal release envelope: %v", err)
	}
	if err := conn.Push(ctx, "default", queuecontract.Payload(reservedBody)); err != nil {
		t.Fatalf("push release envelope: %v", err)
	}
	reserved, err := conn.Pop(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("pop release envelope: %v", err)
	}
	failures["zrem"] = errors.New("forced zrem failure")
	err = reserved.Release(ctx, 0)
	if err == nil {
		t.Fatal("expected delete error")
	}
	for _, item := range *events {
		if item.Name() == EventPublishFailed {
			t.Fatalf("unexpected publish_failed event after delete failure: %#v", item)
		}
	}
}

func TestRedisPopPoisonEnvelopeEmitsDiscardEvent(t *testing.T) {
	ctx := context.Background()
	events := captureQueueEvents(t)
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_poison", Prefix: "redis_poison"})
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Close()
	})
	*events = nil

	const badBody = "{bad envelope json"
	if err := client.RPush(ctx, conn.ReadyKey("default"), badBody).Err(); err != nil {
		t.Fatalf("push bad body: %v", err)
	}
	if _, err := conn.Pop(ctx, []string{"default"}); !errors.Is(err, ErrPoisonEnvelope) {
		t.Fatalf("pop err = %v, want ErrPoisonEnvelope", err)
	}

	if ready, err := client.LLen(ctx, conn.ReadyKey("default")).Result(); err != nil || ready != 0 {
		t.Fatalf("ready len = %d, %v; want discarded bad body", ready, err)
	}
	if len(*events) != 1 {
		t.Fatalf("events = %d, want poison envelope", len(*events))
	}
	poison, ok := (*events)[0].(PoisonEnvelope)
	if !ok {
		t.Fatalf("event type = %T, want PoisonEnvelope", (*events)[0])
	}
	if poison.Name() != EventPoisonEnvelope || poison.Connection != "redis_poison" || poison.Driver != "redis" || poison.Queue != "default" || poison.Action != PoisonEnvelopeActionDiscard {
		t.Fatalf("poison event payload = %#v", poison)
	}
	if poison.BodyEncoding != "base64" || poison.BodyBase64 != base64.StdEncoding.EncodeToString([]byte(badBody)) || poison.BodySize != len(badBody) || poison.BodyTruncated || poison.Error == "" || poison.Timestamp.IsZero() {
		t.Fatalf("poison event body fields = %#v", poison)
	}
}

func TestRedisPoisonEnvelopeDoesNotBlockNextReadyMessage(t *testing.T) {
	ctx := context.Background()
	events := captureQueueEvents(t)
	manager, _ := newRedisManager(t)
	conn := mustQueueEnvelope(t, manager, "redis")
	*events = nil

	if err := conn.Client().RPush(ctx, conn.ReadyKey("default"), "{bad").Err(); err != nil {
		t.Fatalf("push bad body: %v", err)
	}
	valid := &payload.Envelope{ID: "next", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"next"}`)}
	body, err := conn.Codec().Marshal(valid)
	if err != nil {
		t.Fatalf("marshal valid body: %v", err)
	}
	if err := conn.Push(ctx, "default", queuecontract.Payload(body)); err != nil {
		t.Fatalf("push valid body: %v", err)
	}

	if _, err := conn.Pop(ctx, []string{"default"}); !errors.Is(err, ErrPoisonEnvelope) {
		t.Fatalf("first pop err = %v, want ErrPoisonEnvelope", err)
	}
	reserved, err := conn.Pop(ctx, []string{"default"})
	next := reservedEnvelope(reserved)
	if err != nil {
		t.Fatalf("second pop: %v", err)
	}
	if next == nil || next.ID != "next" {
		t.Fatalf("next envelope id = %q, want next", next.ID)
	}
	if len(*events) != 1 || (*events)[0].Name() != EventPoisonEnvelope {
		t.Fatalf("events after poison then normal pop = %#v", *events)
	}
}

func TestRedisWorkerTreatsPoisonEnvelopeAsNoSuccessfulJob(t *testing.T) {
	ctx := context.Background()
	resetTestLog()
	events := captureQueueEvents(t)
	manager, _ := newRedisManager(t)
	conn := mustQueueEnvelope(t, manager, "redis")
	*events = nil

	if err := conn.Client().RPush(ctx, conn.ReadyKey("default"), "{bad").Err(); err != nil {
		t.Fatalf("push bad body: %v", err)
	}
	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: "after-poison"}); err != nil {
		t.Fatalf("dispatch valid job: %v", err)
	}

	if err := NewWorker(manager).Work(ctx, WorkerOptions{MaxJobs: 1, Sleep: time.Millisecond}); err != nil {
		t.Fatalf("work: %v", err)
	}
	page, err := manager.Failed().Page(ctx, state.PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("failed page: %v", err)
	}
	failed := page.Items
	if len(failed) != 0 {
		t.Fatalf("failed jobs = %#v, want none", failed)
	}
	testLog.Lock()
	items := append([]string(nil), testLog.items...)
	testLog.Unlock()
	if !containsString(items, "after-poison") {
		t.Fatalf("worker did not continue to valid job after poison: %v", items)
	}
	for _, ev := range *events {
		if ev.Name() == EventJobFailed {
			t.Fatalf("unexpected job_failed event for poison envelope: %#v", ev)
		}
	}
}

func TestRedisWorkerEmitsConsumerLifecycleEventsPerQueue(t *testing.T) {
	events := captureQueueEvents(t)
	manager, _ := newRedisManager(t)
	*events = nil

	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{
		Queues:        []string{"low", "high"},
		StopWhenEmpty: true,
	}); err != nil {
		t.Fatalf("work: %v", err)
	}

	got := eventNamesAndQueues(*events)
	want := []string{
		EventConsumerStarted + ":low",
		EventConsumerStarted + ":high",
		EventConsumerStopped + ":low",
		EventConsumerStopped + ":high",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestRedisWorkerLifecycleEventsOnExitPaths(t *testing.T) {
	cases := []struct {
		name string
		run  func(context.Context, *Manager) error
	}{
		{
			name: "once",
			run: func(ctx context.Context, manager *Manager) error {
				return NewWorker(manager).Work(ctx, WorkerOptions{Once: true})
			},
		},
		{
			name: "stop when empty",
			run: func(ctx context.Context, manager *Manager) error {
				return NewWorker(manager).Work(ctx, WorkerOptions{StopWhenEmpty: true})
			},
		},
		{
			name: "max jobs",
			run: func(ctx context.Context, manager *Manager) error {
				if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: "max-jobs"}); err != nil {
					return err
				}
				return NewWorker(manager).Work(ctx, WorkerOptions{MaxJobs: 1})
			},
		},
		{
			name: "max time",
			run: func(ctx context.Context, manager *Manager) error {
				return NewWorker(manager).Work(ctx, WorkerOptions{MaxTime: time.Millisecond, Sleep: time.Millisecond})
			},
		},
		{
			name: "context canceled",
			run: func(_ context.Context, manager *Manager) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return NewWorker(manager).Work(ctx, WorkerOptions{})
			},
		},
		{
			name: "queue restart",
			run: func(ctx context.Context, manager *Manager) error {
				done := make(chan error, 1)
				go func() {
					done <- NewWorker(manager).Work(ctx, WorkerOptions{Sleep: time.Millisecond})
				}()
				time.Sleep(5 * time.Millisecond)
				if err := manager.RequestRestart(ctx); err != nil {
					return err
				}
				select {
				case err := <-done:
					return err
				case <-time.After(time.Second):
					return errors.New("worker did not stop after restart")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := captureQueueEvents(t)
			manager, _ := newRedisManager(t)
			*events = nil

			if err := tc.run(context.Background(), manager); err != nil {
				t.Fatalf("work: %v", err)
			}
			got := eventNamesAndQueues(*events)
			if !containsString(got, EventConsumerStarted+":default") || !containsString(got, EventConsumerStopped+":default") {
				t.Fatalf("lifecycle events = %v, want started/stopped for default", got)
			}
		})
	}
}

func eventNamesAndQueues(events []Event) []string {
	out := make([]string, 0, len(events))
	for _, item := range events {
		ev, ok := item.(InfrastructureEvent)
		if !ok {
			continue
		}
		out = append(out, ev.Name()+":"+ev.Queue)
	}
	return out
}

type redisCommandErrorHook struct {
	fail map[string]error
}

func (h redisCommandErrorHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h redisCommandErrorHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err := h.fail[strings.ToLower(cmd.Name())]; err != nil {
			return err
		}
		return next(ctx, cmd)
	}
}

func (h redisCommandErrorHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if err := h.fail[strings.ToLower(cmd.Name())]; err != nil {
				return err
			}
		}
		return next(ctx, cmds)
	}
}

func assertRedisPublishFailedEvent(t *testing.T, events []Event, connection, queue string) {
	t.Helper()
	if len(events) != 1 {
		t.Fatalf("events = %d, want publish_failed", len(events))
	}
	ev, ok := events[0].(InfrastructureEvent)
	if !ok {
		t.Fatalf("event type = %T, want InfrastructureEvent", events[0])
	}
	if ev.Name() != EventPublishFailed || ev.Driver != "redis" || ev.Connection != connection || ev.Queue != queue || ev.Error == "" || ev.Timestamp.IsZero() {
		t.Fatalf("event payload = %#v", ev)
	}
}
