package driver

import (
	"context"
	"encoding/base64"
	"sync"
	"time"
)

const (
	EventConnectionConnecting      = "queue.connection_connecting"
	EventConnectionConnected       = "queue.connection_connected"
	EventConnectionDisconnected    = "queue.connection_disconnected"
	EventConnectionReconnecting    = "queue.connection_reconnecting"
	EventConnectionReconnected     = "queue.connection_reconnected"
	EventConnectionReconnectFailed = "queue.connection_reconnect_failed"
	EventTopologyDeclared          = "queue.topology_declared"
	EventTopologyDeclareFailed     = "queue.topology_declare_failed"
	EventConsumerStarted           = "queue.consumer_started"
	EventConsumerStopped           = "queue.consumer_stopped"
	EventConsumerStopFailed        = "queue.consumer_stop_failed"
	EventPublishFailed             = "queue.publish_failed"
	EventPoisonEnvelope            = "queue.poison_envelope"
	EventReleaseRepublishFailed    = "queue.release_republish_failed"

	PoisonEnvelopeActionReject       = "reject"
	PoisonEnvelopeActionRejectFailed = "reject_failed"
	PoisonEnvelopeActionDiscard      = "discard"

	// DefaultPoisonBodyLimit limits raw message data exposed through poison events.
	DefaultPoisonBodyLimit = 4096
)

// Event is the minimum event contract emitted by queue adapters.
type Event interface {
	Name() string
}

// InfrastructureEvent describes a sanitized queue adapter lifecycle event.
type InfrastructureEvent struct {
	EventName  string
	Connection string
	Driver     string
	Queue      string
	Exchange   string
	Attempt    int
	Error      string
	Timestamp  time.Time
}

// Name returns the stable event name.
func (e InfrastructureEvent) Name() string { return e.EventName }

// InfrastructureFacts contains safe inputs for an InfrastructureEvent.
type InfrastructureFacts struct {
	EventName  string
	Connection string
	Driver     string
	Queue      string
	Exchange   string
	Attempt    int
	Err        error
	Now        time.Time
}

// NewInfrastructureEvent builds an event without retaining the raw error value.
func NewInfrastructureEvent(facts InfrastructureFacts) InfrastructureEvent {
	now := facts.Now
	if now.IsZero() {
		now = time.Now()
	}
	errText := ""
	if facts.Err != nil {
		errText = facts.Err.Error()
	}
	return InfrastructureEvent{
		EventName: facts.EventName, Connection: facts.Connection, Driver: facts.Driver,
		Queue: facts.Queue, Exchange: facts.Exchange, Attempt: facts.Attempt,
		Error: errText, Timestamp: now,
	}
}

// PoisonEnvelope describes a message that could not be decoded by an adapter.
type PoisonEnvelope struct {
	Connection    string
	Driver        string
	Queue         string
	Action        string
	Error         string
	Encoding      string
	BodyBase64    string
	BodyEncoding  string
	BodySize      int
	BodyTruncated bool
	Timestamp     time.Time
}

// Name returns EventPoisonEnvelope.
func (PoisonEnvelope) Name() string { return EventPoisonEnvelope }

// PoisonEnvelopeFacts contains safe inputs for a poison-envelope event.
type PoisonEnvelopeFacts struct {
	Connection string
	Driver     string
	Queue      string
	Action     string
	Encoding   string
	Body       []byte
	BodyLimit  int
	Err        error
	Now        time.Time
}

// NewPoisonEnvelope builds a size-limited, base64-encoded poison event.
func NewPoisonEnvelope(facts PoisonEnvelopeFacts) PoisonEnvelope {
	now := facts.Now
	if now.IsZero() {
		now = time.Now()
	}
	limit := facts.BodyLimit
	if limit <= 0 {
		limit = DefaultPoisonBodyLimit
	}
	body := facts.Body
	truncated := len(body) > limit
	if truncated {
		body = body[:limit]
	}
	errText := ""
	if facts.Err != nil {
		errText = facts.Err.Error()
	}
	return PoisonEnvelope{
		Connection: facts.Connection, Driver: facts.Driver, Queue: facts.Queue,
		Action: facts.Action, Error: errText, Encoding: facts.Encoding,
		BodyBase64: base64.StdEncoding.EncodeToString(body), BodyEncoding: "base64",
		BodySize: len(facts.Body), BodyTruncated: truncated, Timestamp: now,
	}
}

// EventSink receives events emitted by queue adapters.
type EventSink func(context.Context, Event)

var eventSink struct {
	sync.RWMutex
	fn EventSink
}

// UseEventSink replaces the process event outlet used by queue adapters.
func UseEventSink(sink EventSink) {
	eventSink.Lock()
	eventSink.fn = sink
	eventSink.Unlock()
}

// Emit publishes an event when an event sink is installed.
func Emit(ctx context.Context, event Event) {
	eventSink.RLock()
	sink := eventSink.fn
	eventSink.RUnlock()
	if sink != nil && event != nil {
		sink(ctx, event)
	}
}
