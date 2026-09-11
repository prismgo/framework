package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

func TestManagerConcurrentFirstQueueResolvesConnectorOnce(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "external",
		Connections: map[string]ConnectionConfig{
			"external": {Driver: "adapter"},
		},
	}, NewRegistry())
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	var resolverCalls atomic.Int32
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	release := make(chan struct{})
	manager.Extend("adapter", func() (queuecontract.Connector, error) {
		if resolverCalls.Add(1) == 1 {
			close(firstEntered)
			<-release
		} else {
			close(secondEntered)
		}
		return &publicConfigConnector{queue: &contractOnlyQueue{}}, nil
	})

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			if _, queueErr := manager.Queue(""); queueErr != nil {
				t.Errorf("Queue failed: %v", queueErr)
			}
		}()
		if resolverCalls.Load() == 0 {
			<-firstEntered
		}
	}

	select {
	case <-secondEntered:
		t.Error("connector resolver ran more than once during one in-flight connection build")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if got := resolverCalls.Load(); got != 1 {
		t.Fatalf("connector resolver calls = %d, want 1", got)
	}
}

func TestManagerExtendPassesPublicConnectorConfigOnFirstQueue(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "external",
		Connections: map[string]ConnectionConfig{
			"external": {
				Driver:     "adapter",
				Queue:      "emails",
				Prefix:     "jobs",
				RetryAfter: 7 * time.Second,
				BlockFor:   time.Second,
				Options:    map[string]any{"endpoint": "local"},
			},
		},
	}, NewRegistry())
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	connector := &publicConfigConnector{queue: &contractOnlyQueue{}}
	resolverCalls := 0
	manager.Extend(" ADAPTER ", func() (queuecontract.Connector, error) {
		resolverCalls++
		return connector, nil
	})

	got, err := manager.Queue("")
	if err != nil {
		t.Fatalf("Queue failed: %v", err)
	}
	if got != connector.queue || resolverCalls != 1 {
		t.Fatalf("Queue result/calls = (%v, %d), want (%v, 1)", got, resolverCalls, connector.queue)
	}
	if connector.name != "external" {
		t.Fatalf("connection name = %q, want %q", connector.name, "external")
	}
	if connector.config.Driver != "adapter" || connector.config.Queue != "emails" || connector.config.Prefix != "jobs" {
		t.Fatalf("connector config = %#v, want normalized public fields", connector.config)
	}
	if connector.config.RetryAfter != 7*time.Second || connector.config.BlockFor != time.Second {
		t.Fatalf("connector durations = (%v, %v), want (7s, 1s)", connector.config.RetryAfter, connector.config.BlockFor)
	}
	if connector.receivedEndpoint != "local" || connector.config.Codec == nil {
		t.Fatalf("connector endpoint/codec = (%q, %v), want local and codec", connector.receivedEndpoint, connector.config.Codec)
	}
	if manager.connectionSpecs["external"].Options["endpoint"] != "local" {
		t.Fatal("connector mutated the manager's connection options")
	}
}

func TestManagerRejectsNilQueueFromConnector(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "external",
		Connections: map[string]ConnectionConfig{
			"external": {Driver: "adapter"},
		},
	}, NewRegistry())
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	manager.Extend("adapter", connectorResolver(&publicConfigConnector{}))

	got, err := manager.Queue("")
	if got != nil || err == nil {
		t.Fatalf("Queue() = (%v, %v), want nil and connector contract error", got, err)
	}
}

func TestManagerConnectorResolversAreApplicationLocal(t *testing.T) {
	newManager := func(t *testing.T) *Manager {
		t.Helper()
		manager, err := NewManager(Config{
			Default: "custom",
			Connections: map[string]ConnectionConfig{
				"custom": {Driver: "adapter"},
			},
		}, NewRegistry())
		if err != nil {
			t.Fatalf("NewManager failed: %v", err)
		}
		return manager
	}
	first := newManager(t)
	second := newManager(t)
	firstQueue := &contractOnlyQueue{}
	secondQueue := &contractOnlyQueue{}
	first.Extend("adapter", connectorResolver(&publicConfigConnector{queue: firstQueue}))
	second.Extend("adapter", connectorResolver(&publicConfigConnector{queue: secondQueue}))

	gotFirst, err := first.Queue("")
	if err != nil {
		t.Fatalf("first Queue failed: %v", err)
	}
	gotSecond, err := second.Queue("")
	if err != nil {
		t.Fatalf("second Queue failed: %v", err)
	}
	if gotFirst != firstQueue || gotSecond != secondQueue {
		t.Fatalf("application queues = (%v, %v), want (%v, %v)", gotFirst, gotSecond, firstQueue, secondQueue)
	}
}

type publicConfigConnector struct {
	queue            queuecontract.Queue
	name             string
	config           queuecontract.ConnectorConfig
	receivedEndpoint string
}

func (c *publicConfigConnector) Connect(_ context.Context, name string, config queuecontract.ConnectorConfig) (queuecontract.Queue, error) {
	c.name = name
	c.config = config
	c.receivedEndpoint, _ = config.Options["endpoint"].(string)
	if c.config.Options != nil {
		c.config.Options["endpoint"] = "changed"
	}
	return c.queue, nil
}
