package events

import (
	"context"

	queuedriver "github.com/prismgo/framework/queue/driver"
)

const (
	EventConnectionConnecting      = queuedriver.EventConnectionConnecting
	EventConnectionConnected       = queuedriver.EventConnectionConnected
	EventConnectionDisconnected    = queuedriver.EventConnectionDisconnected
	EventConnectionReconnecting    = queuedriver.EventConnectionReconnecting
	EventConnectionReconnected     = queuedriver.EventConnectionReconnected
	EventConnectionReconnectFailed = queuedriver.EventConnectionReconnectFailed
	EventTopologyDeclared          = queuedriver.EventTopologyDeclared
	EventTopologyDeclareFailed     = queuedriver.EventTopologyDeclareFailed
	EventConsumerStarted           = queuedriver.EventConsumerStarted
	EventConsumerStopped           = queuedriver.EventConsumerStopped
	EventConsumerStopFailed        = queuedriver.EventConsumerStopFailed
	EventPublishFailed             = queuedriver.EventPublishFailed
	EventPoisonEnvelope            = queuedriver.EventPoisonEnvelope
	EventReleaseRepublishFailed    = queuedriver.EventReleaseRepublishFailed

	PoisonEnvelopeActionReject       = queuedriver.PoisonEnvelopeActionReject
	PoisonEnvelopeActionRejectFailed = queuedriver.PoisonEnvelopeActionRejectFailed
	PoisonEnvelopeActionDiscard      = queuedriver.PoisonEnvelopeActionDiscard
)

type Event = queuedriver.Event
type InfrastructureEvent = queuedriver.InfrastructureEvent
type PoisonEnvelope = queuedriver.PoisonEnvelope
type Sink = queuedriver.EventSink

func UseSink(sink Sink) { queuedriver.UseEventSink(sink) }

func Fire(ctx context.Context, event Event) { queuedriver.Emit(ctx, event) }
