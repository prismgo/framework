package observability

import (
	"time"

	queuedriver "github.com/prismgo/framework/queue/driver"
	queueevents "github.com/prismgo/framework/queue/internal/events"
)

const DefaultPoisonBodyLimit = queuedriver.DefaultPoisonBodyLimit

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
	return queuedriver.NewInfrastructureEvent(queuedriver.InfrastructureFacts(facts))
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
	return queuedriver.NewPoisonEnvelope(queuedriver.PoisonEnvelopeFacts(facts))
}
