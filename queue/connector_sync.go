package queue

import (
	"context"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

// SyncConnector 构造进程内 sync queue transport。
type SyncConnector struct{}

func (SyncConnector) Connect(_ context.Context, _ string, config queuecontract.ConnectorConfig) (queuecontract.Queue, error) {
	return NewSyncConnection(config.Codec), nil
}
