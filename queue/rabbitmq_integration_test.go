package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const rabbitMQIntegrationEnv = "PRISMGO_RABBITMQ_TEST_URL"

func TestRabbitMQIntegrationDispatchConsume(t *testing.T) {
	ctx := context.Background()
	fixture := newRabbitMQIntegrationFixture(t, "dispatch")
	manager := fixture.newManager(t)

	resetQueueTestLog()
	jobKey := fixture.name + "-job"
	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: jobKey}); err != nil {
		t.Fatalf("dispatch rabbitmq job: %v", err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{
		Connection: "rabbitmq",
		Queues:     []string{fixture.queue},
		Once:       true,
	}); err != nil {
		t.Fatalf("work rabbitmq job: %v", err)
	}
	if got := queueTestHits(jobKey); got != 1 {
		t.Fatalf("job hits = %d, want 1", got)
	}
}

func TestRabbitMQIntegrationDelayRelease(t *testing.T) {
	ctx := context.Background()
	fixture := newRabbitMQIntegrationFixture(t, "release")
	manager := fixture.newManager(t)
	conn, err := manager.Queue("rabbitmq")
	if err != nil {
		t.Fatalf("rabbitmq connection: %v", err)
	}

	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: fixture.name + "-release"}); err != nil {
		t.Fatalf("dispatch rabbitmq job: %v", err)
	}
	reserved, err := conn.Pop(ctx, []string{fixture.queue})
	if err != nil {
		t.Fatalf("pop rabbitmq job: %v", err)
	}
	if err := reserved.Release(ctx, 200*time.Millisecond); err != nil {
		t.Fatalf("release rabbitmq job: %v", err)
	}
	if _, err := conn.Pop(ctx, []string{fixture.queue}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("immediate pop after delayed release err = %v, want ErrEmpty", err)
	}

	released, err := conn.Pop(ctx, []string{fixture.queue})
	if err != nil {
		t.Fatalf("pop released rabbitmq job: %v", err)
	}
	if released.Attempts() < 2 {
		t.Fatalf("released attempts = %d, want at least 2", released.Attempts())
	}
	if err := released.Delete(ctx); err != nil {
		t.Fatalf("delete released rabbitmq job: %v", err)
	}
}

type rabbitMQIntegrationFixture struct {
	url          string
	name         string
	exchange     string
	queue        string
	restartQueue string
}

func newRabbitMQIntegrationFixture(t *testing.T, slug string) rabbitMQIntegrationFixture {
	t.Helper()
	url := strings.TrimSpace(os.Getenv(rabbitMQIntegrationEnv))
	if url == "" {
		t.Skipf("%s is not set; skipping real RabbitMQ integration test", rabbitMQIntegrationEnv)
	}
	name := fmt.Sprintf("prismgo.integration.%s.%d", slug, time.Now().UnixNano())
	fixture := rabbitMQIntegrationFixture{
		url:          url,
		name:         name,
		exchange:     name + ".exchange",
		queue:        name + ".queue",
		restartQueue: name + ".restart",
	}
	t.Cleanup(func() { fixture.cleanup(t) })
	return fixture
}

func (f rabbitMQIntegrationFixture) newManager(t *testing.T) *Manager {
	t.Helper()
	registry := NewRegistry()
	RegisterTypeTo[*testJob](registry)
	manager, err := NewManager(Config{
		Default: "rabbitmq",
		Connections: map[string]ConnectionConfig{
			"rabbitmq": {
				Driver:   "rabbitmq",
				Queue:    f.queue,
				BlockFor: 2 * time.Second,
				Options: map[string]any{
					"url":                f.url,
					"exchange":           f.exchange,
					"exchange_type":      "direct",
					"declare":            true,
					"exchange_durable":   false,
					"queue_durable":      false,
					"message_persistent": false,
					"confirm":            true,
					"delay_mode":         "ttl_dlx",
					"delay_buckets":      []time.Duration{200 * time.Millisecond},
					"prefetch":           1,
					"publish_timeout":    2 * time.Second,
					"restart_queue":      f.restartQueue,
					"restart_enabled":    true,
				},
			},
		},
	}, registry)
	if err != nil {
		t.Fatalf("new rabbitmq manager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Logf("close rabbitmq manager: %v", err)
		}
	})
	return manager
}

func (f rabbitMQIntegrationFixture) cleanup(t *testing.T) {
	t.Helper()
	conn, err := amqp.Dial(f.url)
	if err != nil {
		t.Logf("dial rabbitmq for cleanup: %v", err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close rabbitmq connection: %v", err)
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		t.Logf("open rabbitmq cleanup channel: %v", err)
		return
	}
	defer func() {
		if err := ch.Close(); err != nil {
			t.Errorf("close rabbitmq channel: %v", err)
		}
	}()

	_, _ = ch.QueueDelete(f.queue, false, false, false)
	_, _ = ch.QueueDelete(f.exchange+"."+f.queue+".delay.0s", false, false, false)
	_, _ = ch.QueueDelete(f.restartQueue, false, false, false)
	_ = ch.ExchangeDelete(f.exchange, false, false)
}

func resetQueueTestLog() {
	testLog.Lock()
	defer testLog.Unlock()
	testLog.items = nil
	testLog.hits = map[string]int{}
}

func queueTestHits(key string) int {
	testLog.Lock()
	defer testLog.Unlock()
	return testLog.hits[key]
}
