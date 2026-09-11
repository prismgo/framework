package queue

import (
	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
)

// ConnectorResolver creates a connector when a connection is first resolved.
type ConnectorResolver func() (queuecontract.Connector, error)

func connectorConfig(spec ConnectionConfig, codec encodingcontract.Codec) queuecontract.ConnectorConfig {
	cloned := cloneConnectionConfig(spec)
	return queuecontract.ConnectorConfig{
		Driver:     normalizeDriverName(cloned.Driver),
		Queue:      cloned.Queue,
		Prefix:     cloned.Prefix,
		RetryAfter: cloned.RetryAfter,
		BlockFor:   cloned.BlockFor,
		Options:    cloned.Options,
		Codec:      codec,
	}
}
