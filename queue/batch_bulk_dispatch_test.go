package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
)

type bulkBatchProbeQueue struct {
	bareQueue
	pushes       []queuecontract.Payload
	laters       []queuecontract.Payload
	bulks        [][]queuecontract.Payload
	bulkAccepted int
	failSingleAt int
	err          error
}

type bulkBatchDelayedJob struct {
	testJob
}

func (j *bulkBatchDelayedJob) QueueDelay() time.Duration {
	return time.Millisecond
}

type bulkBatchUniqueOnlyJob struct {
	testJob
}

func (j *bulkBatchUniqueOnlyJob) UniqueID() string {
	return "batch_bulk-unique:" + j.Key
}

func (j *bulkBatchUniqueOnlyJob) UniqueFor() time.Duration {
	return time.Minute
}

func (q *bulkBatchProbeQueue) Push(_ context.Context, _ string, body queuecontract.Payload) error {
	q.pushes = append(q.pushes, append(queuecontract.Payload(nil), body...))
	if q.failSingleAt > 0 && len(q.pushes)+len(q.laters) >= q.failSingleAt {
		return q.err
	}
	if q.failSingleAt > 0 {
		return nil
	}
	return q.err
}

func (q *bulkBatchProbeQueue) Later(_ context.Context, _ string, body queuecontract.Payload, _ time.Duration) error {
	q.laters = append(q.laters, append(queuecontract.Payload(nil), body...))
	if q.failSingleAt > 0 && len(q.pushes)+len(q.laters) >= q.failSingleAt {
		return q.err
	}
	if q.failSingleAt > 0 {
		return nil
	}
	return q.err
}

func (q *bulkBatchProbeQueue) Bulk(_ context.Context, _ string, bodies []queuecontract.Payload) (queuecontract.BulkResult, error) {
	copied := make([]queuecontract.Payload, 0, len(bodies))
	for _, body := range bodies {
		copied = append(copied, append(queuecontract.Payload(nil), body...))
	}
	q.bulks = append(q.bulks, copied)
	accepted := q.bulkAccepted
	if q.err == nil && accepted == 0 {
		accepted = len(bodies)
	}
	return queuecontract.BulkResult{Accepted: accepted}, q.err
}

func TestBatchDispatchUsesBulkForReadyJobsAndKeepsEventsAfterTransport(t *testing.T) {
	probe := &bulkBatchProbeQueue{}
	manager := newRuntimeBackedManagerForTest("bulk", "default", map[string]queuecontract.Queue{"bulk": probe}, NewMemoryFailedStore(), NewMemoryBatchStore(), newTestRegistry())
	events := []string{}
	UseEventSink(func(_ context.Context, ev Event) {
		events = append(events, ev.Name())
	})
	defer UseEventSink(nil)

	status, err := manager.Batch(&testJob{Key: "a"}, &testJob{Key: "b"}).
		Name("batch_bulk").
		Options(OnQueue("critical"), Tags("batch-bulk")).
		Dispatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch batch: %v", err)
	}
	if status.Total != 2 || status.Pending != 2 {
		t.Fatalf("status = %#v, want two pending jobs", status)
	}
	if len(probe.bulks) != 1 || len(probe.bulks[0]) != 2 {
		t.Fatalf("bulk calls = %#v, want one bulk call with two payloads", probe.bulks)
	}
	if len(probe.pushes) != 0 || len(probe.laters) != 0 {
		t.Fatalf("pushes=%d laters=%d, ready batch should not use single dispatch", len(probe.pushes), len(probe.laters))
	}
	if len(events) != 3 || events[0] != EventBatchCreated || events[1] != EventJobQueued || events[2] != EventJobQueued {
		t.Fatalf("events = %v, want batch_created then two job_queued after bulk succeeds", events)
	}
}

func TestBatchDispatchRejectsEmptyOrNilWithoutMetadata(t *testing.T) {
	store := NewMemoryBatchStore()
	manager := newRuntimeBackedManagerForTest("bulk", "default", map[string]queuecontract.Queue{"bulk": &bulkBatchProbeQueue{}}, NewMemoryFailedStore(), store, newTestRegistry())
	events := []string{}
	UseEventSink(func(_ context.Context, ev Event) { events = append(events, ev.Name()) })
	defer UseEventSink(nil)

	if _, err := manager.Batch().Dispatch(context.Background()); err == nil {
		t.Fatal("empty batch should return an error")
	}
	if _, err := manager.Batch(nil).Dispatch(context.Background()); err == nil {
		t.Fatal("nil job batch should return an error")
	}
	if len(events) != 0 {
		t.Fatalf("events = %v, want no metadata event for invalid batch", events)
	}
}

func TestBatchDispatchDeletesMetadataWhenTransportFails(t *testing.T) {
	store := NewMemoryBatchStore()
	probe := &bulkBatchProbeQueue{err: errors.New("bulk failed")}
	manager := newRuntimeBackedManagerForTest("bulk", "default", map[string]queuecontract.Queue{"bulk": probe}, NewMemoryFailedStore(), store, newTestRegistry())

	status, err := manager.Batch(&testJob{Key: "a"}, &testJob{Key: "b"}).Dispatch(context.Background())
	if err == nil {
		t.Fatal("transport failure should be returned")
	}
	if _, findErr := store.GetBatch(context.Background(), status.ID); !errors.Is(findErr, ErrEmpty) {
		t.Fatalf("batch metadata should be deleted after transport failure, got %v", findErr)
	}
}

func TestBatchDispatchKeepsMetadataWhenBulkPartiallyAccepted(t *testing.T) {
	store := NewMemoryBatchStore()
	probe := &bulkBatchProbeQueue{bulkAccepted: 1, err: errors.New("bulk failed after one accept")}
	manager := newRuntimeBackedManagerForTest("bulk", "default", map[string]queuecontract.Queue{"bulk": probe}, NewMemoryFailedStore(), store, newTestRegistry())
	events := []string{}
	UseEventSink(func(_ context.Context, ev Event) { events = append(events, ev.Name()) })
	defer UseEventSink(nil)

	status, err := manager.Batch(&testJob{Key: "a"}, &testJob{Key: "b"}).Dispatch(context.Background())
	if err == nil {
		t.Fatal("partial bulk failure should be returned")
	}
	latest, findErr := store.GetBatch(context.Background(), status.ID)
	if findErr != nil {
		t.Fatalf("batch metadata should be retained after partial bulk failure: %v", findErr)
	}
	if latest.Total != 2 || latest.Pending != 1 || latest.Processed != 1 || latest.Failed != 1 {
		t.Fatalf("batch status = %#v, want total=2 pending=1 processed=1 failed=1", latest)
	}
	queued := 0
	for _, event := range events {
		if event == EventJobQueued {
			queued++
		}
	}
	if queued != 1 {
		t.Fatalf("job_queued events = %d, want only accepted job queued", queued)
	}
}

func TestBatchDispatchTreatsNilErrorPartialBulkAcceptedAsFailure(t *testing.T) {
	store := NewMemoryBatchStore()
	probe := &bulkBatchProbeQueue{bulkAccepted: 1}
	manager := newRuntimeBackedManagerForTest("bulk", "default", map[string]queuecontract.Queue{"bulk": probe}, NewMemoryFailedStore(), store, newTestRegistry())
	events := []string{}
	UseEventSink(func(_ context.Context, ev Event) { events = append(events, ev.Name()) })
	defer UseEventSink(nil)

	status, err := manager.Batch(&testJob{Key: "a"}, &testJob{Key: "b"}).Dispatch(context.Background())
	if err == nil {
		t.Fatal("nil-error partial bulk accepted should be returned as dispatch failure")
	}
	latest, findErr := store.GetBatch(context.Background(), status.ID)
	if findErr != nil {
		t.Fatalf("batch metadata should be retained after nil-error partial bulk accepted: %v", findErr)
	}
	if latest.Total != 2 || latest.Pending != 1 || latest.Processed != 1 || latest.Failed != 1 {
		t.Fatalf("batch status = %#v, want total=2 pending=1 processed=1 failed=1", latest)
	}
	queued := 0
	for _, event := range events {
		if event == EventJobQueued {
			queued++
		}
	}
	if queued != 1 {
		t.Fatalf("job_queued events = %d, want only accepted job queued", queued)
	}
}

func TestBatchDispatchKeepsMetadataWhenSingleAcceptedBeforeFailure(t *testing.T) {
	store := NewMemoryBatchStore()
	probe := &bulkBatchProbeQueue{failSingleAt: 2, err: errors.New("second single failed")}
	manager := newRuntimeBackedManagerForTest("bulk", "default", map[string]queuecontract.Queue{"bulk": probe}, NewMemoryFailedStore(), store, newTestRegistry())

	status, err := manager.Batch(&bulkBatchDelayedJob{testJob: testJob{Key: "accepted"}}, &bulkBatchDelayedJob{testJob: testJob{Key: "failed"}}).Dispatch(context.Background())
	if err == nil {
		t.Fatal("single failure after one accepted job should be returned")
	}
	latest, findErr := store.GetBatch(context.Background(), status.ID)
	if findErr != nil {
		t.Fatalf("batch metadata should be retained after partial single failure: %v", findErr)
	}
	if latest.Total != 2 || latest.Pending != 1 || latest.Processed != 1 || latest.Failed != 1 {
		t.Fatalf("batch status = %#v, want total=2 pending=1 processed=1 failed=1", latest)
	}
}

func TestBatchDispatchSplitsReadyDelayAndUniqueJobs(t *testing.T) {
	useTestCache(t, "memory")
	probe := &bulkBatchProbeQueue{}
	registry := newTestRegistry()
	RegisterTypeTo[*bulkBatchUniqueOnlyJob](registry)
	RegisterTypeTo[*bulkBatchDelayedJob](registry)
	manager := newRuntimeBackedManagerForTest("bulk", "default", map[string]queuecontract.Queue{"bulk": probe}, NewMemoryFailedStore(), NewMemoryBatchStore(), registry)

	_, err := manager.Batch(&testJob{Key: "ready"}, &bulkBatchUniqueOnlyJob{testJob: testJob{Key: "unique"}}, &bulkBatchDelayedJob{testJob: testJob{Key: "delay"}}).
		Options(OnQueue("default")).
		Dispatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch mixed batch: %v", err)
	}
	if len(probe.bulks) != 1 || len(probe.bulks[0]) != 1 {
		t.Fatalf("bulk calls = %#v, want one ready job bulk", probe.bulks)
	}
	if len(probe.laters) != 1 || len(probe.pushes) != 1 {
		t.Fatalf("laters=%d pushes=%d, want delay via Later and unique via single Push", len(probe.laters), len(probe.pushes))
	}
}

func TestMemoryBatchStoreDeleteMissingIsIdempotent(t *testing.T) {
	store := NewMemoryBatchStore()
	if err := store.DeleteBatch(context.Background(), "missing"); err != nil {
		t.Fatalf("delete missing batch: %v", err)
	}
	if err := store.CreateBatch(context.Background(), payload.BatchStatus{ID: "exists"}); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	if err := store.DeleteBatch(context.Background(), "exists"); err != nil {
		t.Fatalf("delete batch: %v", err)
	}
	if _, err := store.GetBatch(context.Background(), "exists"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("deleted batch err = %v, want ErrEmpty", err)
	}
}
