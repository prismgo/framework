package driver

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestEmitPublishesSanitizedInfrastructureEvent(t *testing.T) {
	wantTime := time.Unix(123, 0).UTC()
	var captured Event
	UseEventSink(func(_ context.Context, event Event) {
		captured = event
	})
	t.Cleanup(func() { UseEventSink(nil) })

	Emit(context.Background(), NewInfrastructureEvent(InfrastructureFacts{
		EventName:  EventConnectionConnected,
		Connection: "primary",
		Driver:     "adapter",
		Queue:      "emails",
		Err:        errors.New("redacted transport failure"),
		Now:        wantTime,
	}))

	event, ok := captured.(InfrastructureEvent)
	if !ok {
		t.Fatalf("captured event = %T, want InfrastructureEvent", captured)
	}
	if event.Name() != EventConnectionConnected || event.Connection != "primary" || event.Driver != "adapter" {
		t.Fatalf("captured event = %#v, want public infrastructure facts", event)
	}
	if event.Error != "redacted transport failure" || !event.Timestamp.Equal(wantTime) {
		t.Fatalf("captured error/time = (%q, %v), want sanitized text/%v", event.Error, event.Timestamp, wantTime)
	}
}

func TestNewPoisonEnvelopeSanitizesBody(t *testing.T) {
	wantTime := time.Unix(456, 0).UTC()
	tests := []struct {
		name          string
		facts         PoisonEnvelopeFacts
		wantBody      string
		wantError     string
		wantSize      int
		wantTruncated bool
		wantTime      time.Time
	}{
		{
			name: "explicit body limit",
			facts: PoisonEnvelopeFacts{
				Connection: "primary",
				Driver:     "adapter",
				Queue:      "emails",
				Action:     PoisonEnvelopeActionReject,
				Encoding:   "json",
				Body:       []byte("secret"),
				BodyLimit:  3,
				Err:        errors.New("decode failed"),
				Now:        wantTime,
			},
			wantBody:      base64.StdEncoding.EncodeToString([]byte("sec")),
			wantError:     "decode failed",
			wantSize:      6,
			wantTruncated: true,
			wantTime:      wantTime,
		},
		{
			name: "defaults",
			facts: PoisonEnvelopeFacts{
				Body: []byte(strings.Repeat("x", DefaultPoisonBodyLimit+1)),
			},
			wantBody:      base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", DefaultPoisonBodyLimit))),
			wantSize:      DefaultPoisonBodyLimit + 1,
			wantTruncated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := NewPoisonEnvelope(tt.facts)
			if event.Name() != EventPoisonEnvelope {
				t.Fatalf("event.Name() = %q, want %q", event.Name(), EventPoisonEnvelope)
			}
			if event.BodyBase64 != tt.wantBody || event.BodyEncoding != "base64" {
				t.Fatalf("body = (%q, %q), want (%q, %q)", event.BodyBase64, event.BodyEncoding, tt.wantBody, "base64")
			}
			if event.Error != tt.wantError || event.BodySize != tt.wantSize || event.BodyTruncated != tt.wantTruncated {
				t.Fatalf("sanitized facts = (%q, %d, %t), want (%q, %d, %t)", event.Error, event.BodySize, event.BodyTruncated, tt.wantError, tt.wantSize, tt.wantTruncated)
			}
			if tt.wantTime.IsZero() {
				if event.Timestamp.IsZero() {
					t.Fatal("event.Timestamp is zero, want generated timestamp")
				}
			} else if !event.Timestamp.Equal(tt.wantTime) {
				t.Fatalf("event.Timestamp = %v, want %v", event.Timestamp, tt.wantTime)
			}
		})
	}
}
