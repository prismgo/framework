package payload

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestEnvelopeJSONCodecPreservesRawPayload(t *testing.T) {
	env := Envelope{
		ID:          "job-1",
		Name:        "MailJob",
		Queue:       "emails",
		Payload:     Payload(`{"to":"ops@example.test"}`),
		CreatedAt:   10,
		AvailableAt: 20,
		Chain: []PendingJob{{
			Name:    "FollowupJob",
			Payload: Payload(`{"after":true}`),
			Queue:   "emails",
		}},
	}

	encoded, err := MarshalEnvelope(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if bytes.Contains(encoded, []byte(`eyJ0byI6`)) {
		t.Fatalf("payload must stay raw JSON, got base64 in %s", encoded)
	}
	if !bytes.Contains(encoded, []byte(`"payload":{"to":"ops@example.test"}`)) {
		t.Fatalf("payload was not embedded as raw JSON: %s", encoded)
	}

	decoded, err := UnmarshalEnvelope(encoded)
	if err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if string(decoded.Payload) != `{"to":"ops@example.test"}` {
		t.Fatalf("payload mismatch: %s", decoded.Payload)
	}
	if len(decoded.Chain) != 1 || string(decoded.Chain[0].Payload) != `{"after":true}` {
		t.Fatalf("chain payload mismatch: %#v", decoded.Chain)
	}
}

func TestEnvelopeJSONCodecRejectsInvalidNestedPayload(t *testing.T) {
	_, err := MarshalEnvelope(Envelope{
		ID:          "job-1",
		Name:        "BrokenJob",
		Queue:       "emails",
		Payload:     Payload(`{broken`),
		CreatedAt:   10,
		AvailableAt: 20,
	})
	if err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("expected root payload validation error, got %v", err)
	}

	_, err = MarshalEnvelope(Envelope{
		ID:          "job-1",
		Name:        "MailJob",
		Queue:       "emails",
		Payload:     Payload(`{"ok":true}`),
		CreatedAt:   10,
		AvailableAt: 20,
		Chain: []PendingJob{{
			Name:    "BrokenJob",
			Payload: Payload(`{broken`),
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "chain[0].payload") {
		t.Fatalf("expected chain payload validation error, got %v", err)
	}
}

func TestPayloadJSONHelpersHandleEmptyInvalidAndNilReceiver(t *testing.T) {
	encoded, err := Payload(nil).MarshalJSON()
	if err != nil {
		t.Fatalf("marshal empty payload: %v", err)
	}
	if string(encoded) != "null" {
		t.Fatalf("empty payload should marshal as null, got %s", encoded)
	}
	if _, err := Payload(`{broken`).MarshalJSON(); err == nil {
		t.Fatal("expected invalid raw JSON payload error")
	}
	var nilPayload *Payload
	if err := nilPayload.UnmarshalJSON([]byte(`{"ignored":true}`)); err != nil {
		t.Fatalf("nil payload unmarshal should no-op: %v", err)
	}
	var decoded Payload
	if err := decoded.UnmarshalJSON([]byte(`{"ok":true}`)); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if string(decoded) != `{"ok":true}` {
		t.Fatalf("decoded payload mismatch: %s", decoded)
	}
}

func TestFailedJobJSONCodecRoundTripsEnvelope(t *testing.T) {
	failedAt := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	failed := FailedJob{
		ID:         "failed-1",
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-1",
		JobName:    "MailJob",
		Envelope: Envelope{
			ID:          "job-1",
			Name:        "MailJob",
			Queue:       "default",
			Payload:     Payload(`{"id":123}`),
			CreatedAt:   10,
			AvailableAt: 10,
		},
		Error:    "boom",
		FailedAt: failedAt,
		Tags:     []string{"tenant:1"},
	}

	encoded, err := MarshalFailedJob(failed)
	if err != nil {
		t.Fatalf("marshal failed job: %v", err)
	}
	decoded, err := UnmarshalFailedJob(encoded)
	if err != nil {
		t.Fatalf("unmarshal failed job: %v", err)
	}
	if decoded.ID != failed.ID || decoded.Envelope.ID != failed.Envelope.ID || string(decoded.Envelope.Payload) != `{"id":123}` {
		t.Fatalf("decoded failed job mismatch: %#v", decoded)
	}
	if !decoded.FailedAt.Equal(failedAt) {
		t.Fatalf("failed_at mismatch: %s", decoded.FailedAt)
	}
}

func TestFailedJobJSONCodecRejectsInvalidNestedPayload(t *testing.T) {
	_, err := MarshalFailedJob(FailedJob{
		ID:         "failed-1",
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-1",
		JobName:    "BrokenJob",
		Envelope: Envelope{
			ID:          "job-1",
			Name:        "BrokenJob",
			Queue:       "default",
			Payload:     Payload(`{broken`),
			CreatedAt:   10,
			AvailableAt: 10,
		},
		Error: "boom",
	})
	if err == nil || !strings.Contains(err.Error(), "envelope.payload") {
		t.Fatalf("expected nested payload validation error, got %v", err)
	}
}

func TestPayloadDecodersReturnJSONErrors(t *testing.T) {
	if _, err := UnmarshalEnvelope([]byte(`{`)); err == nil {
		t.Fatal("expected envelope decode error")
	}
	if _, err := UnmarshalFailedJob([]byte(`{`)); err == nil {
		t.Fatal("expected failed job decode error")
	}
}
