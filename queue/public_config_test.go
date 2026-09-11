package queue_test

import (
	"errors"
	"testing"
	"time"

	"github.com/prismgo/framework/queue"
)

func TestRabbitMQRejectsRetryAfterFromPublicConnectionConfig(t *testing.T) {
	manager, err := queue.NewManager(queue.Config{
		Default: "rabbitmq",
		Connections: map[string]queue.ConnectionConfig{
			"rabbitmq": {Driver: "rabbitmq", RetryAfter: time.Second},
		},
	}, queue.NewRegistry())
	if manager != nil {
		t.Cleanup(func() { _ = manager.Close() })
	}
	if err != nil {
		t.Fatalf("NewManager() error = %v, want lazy construction", err)
	}
	_, err = manager.Queue("")
	if !errors.Is(err, queue.ErrUnsupportedRetryAfter) {
		t.Fatalf("Queue() error = %v, want %v", err, queue.ErrUnsupportedRetryAfter)
	}
}
