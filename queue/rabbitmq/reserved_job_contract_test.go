package rabbitmq

import (
	"context"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
)

func TestRabbitMQReservedJobMetadataAndCopies(t *testing.T) {
	body := queuecontract.Payload("encoded")
	reserved := &RabbitMQReservedJob{env: &payload.Envelope{ID: "rabbit-2", Name: "job", Attempts: 4}, body: body}
	if reserved.ID() != "rabbit-2" || reserved.Name() != "job" || reserved.Attempts() != 4 {
		t.Fatalf("reserved metadata = id:%q name:%q attempts:%d", reserved.ID(), reserved.Name(), reserved.Attempts())
	}
	copyBody := reserved.Payload()
	copyBody[0] = 'X'
	if string(reserved.Payload()) != "encoded" {
		t.Fatal("RabbitMQ reserved payload should be returned as a defensive copy")
	}
	if err := reserved.Delete(context.Background()); err != nil {
		t.Fatalf("nil queue delete should be no-op: %v", err)
	}
	if err := reserved.Release(context.Background(), time.Second); err != nil {
		t.Fatalf("nil queue release should be no-op: %v", err)
	}
}

func TestRabbitMQReservedJobPayloadCopy(t *testing.T) {
	now := time.Now().Unix()
	env := &payload.Envelope{
		ID:            "rabbit-1",
		Name:          "job",
		Queue:         "emails",
		Payload:       []byte(`{"key":"rabbit"}`),
		Attempts:      2,
		MaxTries:      3,
		TimeoutSec:    10,
		FailOnTimeout: true,
		BackoffSec:    []int{1, 2},
		Tags:          []string{"mail"},
		CreatedAt:     now,
		AvailableAt:   now,
	}
	reserved := &RabbitMQReservedJob{env: env, body: queuecontract.Payload(`{"encoded":true}`)}
	copied := reserved.Payload()
	copied[0] = 'X'
	if string(reserved.Payload()) != `{"encoded":true}` {
		t.Fatal("reserved payload should be returned as a defensive copy")
	}
}
