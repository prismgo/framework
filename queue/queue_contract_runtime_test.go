package queue

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

type contractOnlyQueue struct {
	mu       sync.Mutex
	items    []*contractOnlyReservedJob
	deleted  int
	released []time.Duration
	popCalls []contractOnlyPopCall
}

type contractOnlyPopCall struct {
	queues []string
	wait   queuecontract.PopWaitMode
}

func (q *contractOnlyQueue) Push(_ context.Context, queue string, body queuecontract.Payload) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, &contractOnlyReservedJob{owner: q, queue: queue, body: append([]byte(nil), body...)})
	return nil
}

func (q *contractOnlyQueue) Later(ctx context.Context, queue string, body queuecontract.Payload, _ time.Duration) error {
	return q.Push(ctx, queue, body)
}

func (q *contractOnlyQueue) Bulk(ctx context.Context, queue string, bodies []queuecontract.Payload) (queuecontract.BulkResult, error) {
	accepted := 0
	for _, body := range bodies {
		if err := q.Push(ctx, queue, body); err != nil {
			return queuecontract.BulkResult{Accepted: accepted}, err
		}
		accepted++
	}
	return queuecontract.BulkResult{Accepted: accepted}, nil
}

func (q *contractOnlyQueue) Pop(_ context.Context, queues []string, wait ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	mode := queuecontract.PopWaitAvailable
	if len(wait) > 0 {
		mode = wait[0]
	}
	q.popCalls = append(q.popCalls, contractOnlyPopCall{queues: append([]string(nil), queues...), wait: mode})
	if len(q.items) == 0 {
		return nil, ErrEmpty
	}
	item := q.items[0]
	q.items = q.items[1:]
	item.attempts++
	return item, nil
}

func TestWorkerPassesQueuesAndWaitModeToQueueConnection(t *testing.T) {
	ctx := context.Background()
	manager, err := NewManager(Config{Default: "sync"}, NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	q := &contractOnlyQueue{}
	manager.connectionSpecs["contract"] = ConnectionConfig{Driver: "contract", Queue: "default"}
	manager.queues["contract"] = q

	if err := NewWorker(manager).Work(ctx, WorkerOptions{Connection: "contract", Queues: []string{"default", "low"}, StopWhenEmpty: true}); err != nil {
		t.Fatal(err)
	}
	want := []contractOnlyPopCall{
		{queues: []string{"default", "low"}, wait: queuecontract.PopNoWait},
		{queues: []string{"default", "low"}, wait: queuecontract.PopWaitAvailable},
	}
	if len(q.popCalls) != len(want) {
		t.Fatalf("pop calls = %v, want %v", q.popCalls, want)
	}
	for i := range want {
		if !reflect.DeepEqual(q.popCalls[i], want[i]) {
			t.Fatalf("pop call %d = %#v, want %#v", i, q.popCalls[i], want[i])
		}
	}
}

func (q *contractOnlyQueue) Size(context.Context, string) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.items)), nil
}

func (q *contractOnlyQueue) Clear(context.Context, string) error {
	q.mu.Lock()
	q.items = nil
	q.mu.Unlock()
	return nil
}

func (q *contractOnlyQueue) Close() error { return nil }

type contractOnlyReservedJob struct {
	owner    *contractOnlyQueue
	queue    string
	body     queuecontract.Payload
	attempts int
}

func (j *contractOnlyReservedJob) ID() string   { return "" }
func (j *contractOnlyReservedJob) Name() string { return "" }
func (j *contractOnlyReservedJob) Payload() queuecontract.Payload {
	return append(queuecontract.Payload(nil), j.body...)
}
func (j *contractOnlyReservedJob) Attempts() int { return j.attempts }
func (j *contractOnlyReservedJob) Delete(context.Context) error {
	j.owner.mu.Lock()
	j.owner.deleted++
	j.owner.mu.Unlock()
	return nil
}
func (j *contractOnlyReservedJob) Release(_ context.Context, delay time.Duration) error {
	j.owner.mu.Lock()
	j.owner.released = append(j.owner.released, delay)
	j.owner.items = append(j.owner.items, j)
	j.owner.mu.Unlock()
	return nil
}

func TestWorkerConsumesContractsQueueWithoutLegacyConnection(t *testing.T) {
	ctx := context.Background()
	manager, err := NewManager(Config{Default: "sync"}, NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	RegisterTypeTo[*testJob](manager.Registry())
	q := &contractOnlyQueue{}
	manager.connectionSpecs["contract"] = ConnectionConfig{Driver: "contract", Queue: "default"}
	manager.queues["contract"] = q

	testLog.Lock()
	testLog.hits = map[string]int{}
	testLog.items = nil
	testLog.Unlock()

	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: "contract-main"}, OnConnection("contract")); err != nil {
		t.Fatal(err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Connection: "contract", Once: true}); err != nil {
		t.Fatal(err)
	}

	testLog.Lock()
	defer testLog.Unlock()
	if got := testLog.hits["contract-main"]; got != 1 {
		t.Fatalf("job executions = %d, want 1", got)
	}
	if q.deleted != 1 {
		t.Fatalf("reserved deletes = %d, want 1", q.deleted)
	}
}

func TestWorkerReleasesContractsReservedJobOnRetry(t *testing.T) {
	ctx := context.Background()
	manager, err := NewManager(Config{Default: "sync"}, NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	RegisterTypeTo[*testJob](manager.Registry())
	q := &contractOnlyQueue{}
	manager.connectionSpecs["contract"] = ConnectionConfig{Driver: "contract", Queue: "default"}
	manager.queues["contract"] = q

	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: "contract-retry", FailTill: 1}, OnConnection("contract"), Tries(2), Backoff(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Connection: "contract", Once: true, Tries: 2}); err != nil {
		t.Fatal(err)
	}
	if len(q.released) != 1 || q.released[0] != time.Second {
		t.Fatalf("release delays = %v, want [1s]", q.released)
	}
	if q.deleted != 0 {
		t.Fatalf("reserved deletes = %d, want 0 before retry succeeds", q.deleted)
	}
}

func TestRequestRestartUsesIndependentRestartStore(t *testing.T) {
	manager, err := NewManager(Config{Default: "sync"}, NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	q := &contractOnlyQueue{}
	manager.connectionSpecs["contract"] = ConnectionConfig{Driver: "contract", Queue: "default"}
	manager.queues["contract"] = q
	manager.defaultConnection = "contract"

	started := time.Now().Add(-time.Second)
	if manager.restartRequested(context.Background(), started) {
		t.Fatal("restart should be empty before RequestRestart")
	}
	if err := manager.RequestRestart(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !manager.restartRequested(context.Background(), started) {
		t.Fatal("restart should come from independent restart store")
	}
	if _, err := manager.Queue("contract"); err != nil {
		t.Fatalf("contract queue should remain available: %v", err)
	}
}

func TestRequestRestartUsesConfiguredCacheStore(t *testing.T) {
	useTestCache(t, "memory")
	manager, err := NewManager(Config{
		Default: "sync",
		Restart: RestartConfig{Cache: "memory", Key: "queue:restart:test"},
	}, NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewManager(Config{
		Default: "sync",
		Restart: RestartConfig{Cache: "memory", Key: "queue:restart:test"},
	}, NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().Add(-time.Second)
	if other.restartRequested(context.Background(), started) {
		t.Fatal("restart should be empty before cache write")
	}
	if err := manager.RequestRestart(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !other.restartRequested(context.Background(), started) {
		t.Fatal("restart should be visible through configured cache store")
	}
}
