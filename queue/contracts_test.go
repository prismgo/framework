package queue

import (
	"context"
	"testing"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

type contractTestJob struct{}

func (contractTestJob) Handle(context.Context) error { return nil }

func init() {
	var _ queuecontract.Job = contractTestJob{}
	var _ queuecontract.Dispatcher = (*Dispatcher)(nil)
	var _ = queuecontract.ConsumerIntentLeaser(nil)
	var _ = ConsumerIntentLeaser(nil)
}

func TestDispatchJobRejectsNilContractJob(t *testing.T) {
	manager, err := NewManager(Config{Default: "sync"}, NewRegistry())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	var job queuecontract.Job
	if _, err := NewDispatcher(manager).DispatchJob(context.Background(), job, nil); err == nil {
		t.Fatal("expected nil contract job error")
	}
}
