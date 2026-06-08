package observability

import (
	"encoding/base64"
	"time"

	queueevents "github.com/prismgo/framework/queue/internal/events"
)

const DefaultPoisonBodyLimit = 4096

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

func InfrastructureEvent(facts InfrastructureFacts) queueevents.InfrastructureEvent {
	now := facts.Now
	if now.IsZero() {
		now = time.Now()
	}
	errText := ""
	if facts.Err != nil {
		errText = facts.Err.Error()
	}
	return queueevents.InfrastructureEvent{
		EventName:  facts.EventName,
		Connection: facts.Connection,
		Driver:     facts.Driver,
		Queue:      facts.Queue,
		Exchange:   facts.Exchange,
		Attempt:    facts.Attempt,
		Error:      errText,
		Timestamp:  now,
	}
}

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

func PoisonEnvelope(facts PoisonEnvelopeFacts) queueevents.PoisonEnvelope {
	now := facts.Now
	if now.IsZero() {
		now = time.Now()
	}
	limit := facts.BodyLimit
	if limit <= 0 {
		limit = DefaultPoisonBodyLimit
	}
	bodyPart := facts.Body
	truncated := false
	if len(bodyPart) > limit {
		bodyPart = bodyPart[:limit]
		truncated = true
	}
	errText := ""
	if facts.Err != nil {
		errText = facts.Err.Error()
	}
	return queueevents.PoisonEnvelope{
		Connection:    facts.Connection,
		Driver:        facts.Driver,
		Queue:         facts.Queue,
		Action:        facts.Action,
		Error:         errText,
		Encoding:      facts.Encoding,
		BodyBase64:    base64.StdEncoding.EncodeToString(bodyPart),
		BodyEncoding:  "base64",
		BodySize:      len(facts.Body),
		BodyTruncated: truncated,
		Timestamp:     now,
	}
}
