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
	if !errors.Is(err, queue.ErrUnsupportedRetryAfter) {
		t.Fatalf("NewManager() error = %v, want %v", err, queue.ErrUnsupportedRetryAfter)
	}
}
