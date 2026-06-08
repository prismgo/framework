package queue

import (
	"context"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
)

// SyncConnector 构造进程内 sync queue transport。
type SyncConnector struct {
	codec encodingcontract.Codec
}

func (c SyncConnector) Connect(_ context.Context, name string, config map[string]any) (queuecontract.Queue, error) {
	if _, err := connectorSpec(name, config); err != nil {
		return nil, err
	}
	return NewSyncConnection(c.codec), nil
}
