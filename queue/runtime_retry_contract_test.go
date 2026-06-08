package queue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
)

type retryRuntimeSingleReservedQueue struct {
	bareQueue
	reserved queuecontract.ReservedJob
}

func (q *retryRuntimeSingleReservedQueue) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	if q.reserved == nil {
		return nil, ErrEmpty
	}
	reserved := q.reserved
	q.reserved = nil
	return reserved, nil
}

func TestBatchRetryDoesNotMarkFailedUntilFinalFailure(t *testing.T) {
	resetTestLog()
	registry := newTestRegistry()
	body, err := registry.Marshal(&testJob{Key: "batch-retry", FailTill: 1})
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	batch := NewMemoryBatchStore()
	if err := batch.CreateBatch(context.Background(), payload.BatchStatus{ID: "batch-retry", Total: 1, Pending: 1, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	reserved := &testReservedJob{
		env: &payload.Envelope{
			ID:       "batch-retry-job",
			Name:     testJobName(),
			Queue:    "default",
			Payload:  body,
			Attempts: 1,
			BatchID:  "batch-retry",
		},
	}
	queueConn := &retryRuntimeSingleReservedQueue{reserved: reserved}
	manager := newRuntimeBackedManagerForTest("retry_runtime", "default", map[string]queuecontract.Queue{"retry_runtime": queueConn}, NewMemoryFailedStore(), batch, registry)

	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Connection: "retry_runtime", Once: true, Tries: 2}); err != nil {
		t.Fatalf("work: %v", err)
	}
	status, err := manager.BatchStatus(context.Background(), "batch-retry")
	if err != nil {
		t.Fatalf("batch status: %v", err)
	}
	if status.Pending != 1 || status.Processed != 0 || status.Failed != 0 {
		t.Fatalf("batch status after retryable failure = %#v, want pending unchanged", status)
	}
}

func TestWorkerReturnsDeleteErrorsForSuccessSkipAndBatchCancel(t *testing.T) {
	cases := []struct {
		name       string
		env        payload.Envelope
		configure  func(*Manager)
		wantDelete bool
	}{
		{
			name: "success",
			env:  payload.Envelope{ID: "success", Name: testJobName(), Queue: "default"},
		},
		{
			name: "skip",
			env:  payload.Envelope{ID: "skip", Name: testJobName(), Queue: "default"},
			configure: func(m *Manager) {
				_ = m
			},
		},
		{
			name: "batch-cancel",
			env:  payload.Envelope{ID: "cancel", Name: testJobName(), Queue: "default", BatchID: "cancelled"},
			configure: func(m *Manager) {
				store := NewMemoryBatchStore()
				_ = store.CreateBatch(context.Background(), payload.BatchStatus{ID: "cancelled", Total: 1, Pending: 1, Cancelled: true})
				m.runtime.batch = store
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetTestLog()
			registry := newTestRegistry()
			job := &testJob{Key: "delete-" + tc.name}
			if tc.name == "skip" {
				job.Skip = true
			}
			body, err := registry.Marshal(job)
			if err != nil {
				t.Fatalf("marshal job: %v", err)
			}
			tc.env.Payload = body
			reserved := &testReservedJob{env: &tc.env, deleteErr: errors.New("delete failed")}
			queueConn := &retryRuntimeSingleReservedQueue{reserved: reserved}
			manager := newRuntimeBackedManagerForTest("retry_runtime", "default", map[string]queuecontract.Queue{"retry_runtime": queueConn}, NewMemoryFailedStore(), nil, registry)
			if tc.configure != nil {
				tc.configure(manager)
			}
			err = NewWorker(manager).Work(context.Background(), WorkerOptions{Connection: "retry_runtime", Once: true})
			if err == nil || !strings.Contains(err.Error(), "delete failed") {
				t.Fatalf("expected delete error, got %v", err)
			}
		})
	}
}

func TestSyncDispatchProcessesOnlyCurrentJobOnce(t *testing.T) {
	resetTestLog()
	manager := newSyncManager()
	queueConn, err := manager.Queue("sync")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	syncConn := queueConn.(*SyncConnection)
	oldEnv, err := newDispatcherPayloadFactory(manager.runtime).MakeEnvelope(&testJob{Key: "old-sync"}, payload.EnvelopeOptions{Queue: "default"})
	if err != nil {
		t.Fatalf("old envelope: %v", err)
	}
	oldBody, err := encodeQueueEnvelope(manager.runtime, oldEnv)
	if err != nil {
		t.Fatalf("old body: %v", err)
	}
	syncConn.mu.Lock()
	syncConn.queues["default"] = append(syncConn.queues["default"], oldBody)
	syncConn.mu.Unlock()

	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "current-sync"}); err != nil {
		t.Fatalf("dispatch current: %v", err)
	}
	testLog.Lock()
	oldHits := testLog.hits["old-sync"]
	currentHits := testLog.hits["current-sync"]
	testLog.Unlock()
	if oldHits != 0 || currentHits != 1 {
		t.Fatalf("executions old/current = %d/%d, want 0/1", oldHits, currentHits)
	}
	if size, err := syncConn.Size(context.Background(), "default"); err != nil || size != 1 {
		t.Fatalf("sync queue size = %d, %v; want old job still queued", size, err)
	}
}

func TestSyncDispatchFailureReturnsWithoutRetryOrFailedStore(t *testing.T) {
	resetTestLog()
	manager := newSyncManager()
	_, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "sync-fails-once", FailTill: 1}, Tries(3), Backoff(time.Millisecond))
	if err == nil || !strings.Contains(err.Error(), "fail sync-fails-once") {
		t.Fatalf("expected dispatch failure, got %v", err)
	}
	testLog.Lock()
	hits := testLog.hits["sync-fails-once"]
	testLog.Unlock()
	if hits != 1 {
		t.Fatalf("sync failing job executions = %d, want 1", hits)
	}
	if items := mustFailedItems(t, manager.Failed()); len(items) != 0 {
		t.Fatalf("sync dispatch should not record failed jobs, got %#v", items)
	}
}
