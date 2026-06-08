package queue

import (
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
)

func newRuntimeBackedManagerForTest(connection string, queueName string, queues map[string]queuecontract.Queue, failed FailedStore, batch BatchStore, registry *Registry) *Manager {
	if connection == "" {
		connection = "sync"
	}
	if queueName == "" {
		queueName = "default"
	}
	if failed == nil {
		failed = NewMemoryFailedStore()
	}
	if batch == nil {
		batch = NewMemoryBatchStore()
	}
	if registry == nil {
		registry = NewRegistry()
	}
	return &Manager{
		defaultConnection: connection,
		queues:            queues,
		runtime: &Runtime{
			defaultConnection: connection,
			defaultQueue:      queueName,
			failed:            failed,
			batch:             batch,
			registry:          registry,
			codec:             encodingpkg.JSON(),
		},
	}
}
