package driver

import "errors"

var (
	// ErrEmpty indicates that a queue transport has no available job.
	ErrEmpty = errors.New("queue: empty")
	// ErrPoisonEnvelope indicates that a transport message cannot be decoded as an envelope.
	ErrPoisonEnvelope = errors.New("queue: poison envelope")
)
