// Package driver provides stable helpers and event contracts for queue adapter authors.
package driver

import (
	"strings"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

// NormalizeQueues trims and deduplicates queue names while preserving their order.
func NormalizeQueues(queues []string, defaultQueue string) []string {
	defaultQueue = strings.TrimSpace(defaultQueue)
	if defaultQueue == "" {
		defaultQueue = "default"
	}
	normalized := make([]string, 0, len(queues))
	seen := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		queue = strings.TrimSpace(queue)
		if queue == "" {
			continue
		}
		if _, ok := seen[queue]; ok {
			continue
		}
		seen[queue] = struct{}{}
		normalized = append(normalized, queue)
	}
	if len(normalized) == 0 {
		return []string{defaultQueue}
	}
	return normalized
}

// NormalizePopWaitMode returns the first explicit mode or the blocking default.
func NormalizePopWaitMode(wait []queuecontract.PopWaitMode) queuecontract.PopWaitMode {
	if len(wait) == 0 {
		return queuecontract.PopWaitAvailable
	}
	return wait[0]
}
