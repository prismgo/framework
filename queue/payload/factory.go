package payload

import (
	"time"

	"github.com/google/uuid"
	queuecontract "github.com/prismgo/framework/contracts/queue"
)

type JobRegistry interface {
	TypeName(queuecontract.Job) (string, error)
	Register(queuecontract.Job)
	Marshal(queuecontract.Job) (Payload, error)
}

type EncryptFunc func(Payload) (Payload, error)

type Factory struct {
	registry  JobRegistry
	encryptFn EncryptFunc
	now       func() time.Time
}

func NewFactory(registry JobRegistry, encrypt EncryptFunc) *Factory {
	return &Factory{registry: registry, encryptFn: encrypt, now: time.Now}
}

type EnvelopeOptions struct {
	Queue         string
	Delay         time.Duration
	Tries         int
	MaxExceptions int
	Timeout       time.Duration
	FailOnTimeout bool
	Encrypted     bool
	Backoff       []time.Duration
	RetryUntil    time.Time
	Chain         []PendingJob
	BatchID       string
	UniqueKey     string
	UniqueFor     time.Duration
	UniqueVia     string
	UniqueUntil   bool
	DebounceKey   string
	DebounceFor   time.Duration
	DebounceVia   string
	Tags          []string
	Silenced      bool
}

func (f *Factory) MakeEnvelope(job queuecontract.Job, opts EnvelopeOptions) (*Envelope, error) {
	name, err := f.registry.TypeName(job)
	if err != nil {
		return nil, err
	}
	f.registry.Register(job)
	body, err := f.registry.Marshal(job)
	if err != nil {
		return nil, err
	}
	if opts.Encrypted {
		body, err = f.encrypt(body)
		if err != nil {
			return nil, err
		}
	}
	now := f.now()
	return &Envelope{
		ID:             uuid.NewString(),
		Name:           name,
		Queue:          opts.Queue,
		Payload:        body,
		MaxTries:       opts.Tries,
		MaxExceptions:  opts.MaxExceptions,
		TimeoutSec:     seconds(opts.Timeout),
		FailOnTimeout:  opts.FailOnTimeout,
		Encrypted:      opts.Encrypted,
		BackoffSec:     secondsList(opts.Backoff),
		RetryUntil:     unixSeconds(opts.RetryUntil),
		Chain:          append([]PendingJob(nil), opts.Chain...),
		BatchID:        opts.BatchID,
		UniqueKey:      opts.UniqueKey,
		UniqueForSec:   seconds(opts.UniqueFor),
		UniqueVia:      opts.UniqueVia,
		UniqueUntil:    opts.UniqueUntil,
		DebounceKey:    opts.DebounceKey,
		DebounceForSec: seconds(opts.DebounceFor),
		DebounceVia:    opts.DebounceVia,
		Tags:           append([]string(nil), opts.Tags...),
		Silenced:       opts.Silenced,
		CreatedAt:      now.Unix(),
		AvailableAt:    now.Add(opts.Delay).Unix(),
	}, nil
}

func (f *Factory) encrypt(body Payload) (Payload, error) {
	if f.encryptFn == nil {
		return nil, ErrCipherMissing
	}
	return f.encryptFn(body)
}

func seconds(value time.Duration) int {
	if value <= 0 {
		return 0
	}
	return int(value / time.Second)
}

func unixSeconds(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.Unix()
}

func secondsList(values []time.Duration) []int {
	if len(values) == 0 {
		return nil
	}
	out := make([]int, 0, len(values))
	for _, value := range values {
		out = append(out, seconds(value))
	}
	return out
}
