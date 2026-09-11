package queue_test

import (
	"strings"
	"testing"

	"github.com/prismgo/framework/queue"
)

func TestRabbitMQRequiresAnExtensionConnector(t *testing.T) {
	manager, err := queue.NewManager(queue.Config{
		Default: "rabbitmq",
		Connections: map[string]queue.ConnectionConfig{
			"rabbitmq": {Driver: "rabbitmq"},
		},
	}, queue.NewRegistry())
	if err != nil {
		t.Fatalf("NewManager() error = %v, want lazy construction", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	_, err = manager.Queue("")
	if err == nil || !strings.Contains(err.Error(), `unknown driver "rabbitmq"`) {
		t.Fatalf("Queue() error = %v, want unknown rabbitmq driver", err)
	}
}
