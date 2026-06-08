package cmd

import (
	"fmt"
	"sort"
	"strings"
)

// resolveQueueTarget 根据命令参数和 Runtime 提供的 supervisor 队列目标定位唯一维护目标。
//
// 参数说明：connection 来自 --connection，queueName 来自可选位置参数；二者为空时必须只有一个候选目标。
func resolveQueueTarget(targets []QueueTarget, connection string, queueName string) (QueueTarget, error) {
	connection = strings.TrimSpace(connection)
	queueName = strings.TrimSpace(queueName)
	if len(targets) == 0 {
		return QueueTarget{}, fmt.Errorf("horizon: no monitored queue targets configured")
	}
	filtered := filterQueueTargets(targets, connection, queueName)
	if len(filtered) == 1 {
		return filtered[0], nil
	}
	if len(filtered) > 1 {
		return QueueTarget{}, fmt.Errorf("horizon: queue target is ambiguous; candidates: %s", formatQueueTargets(filtered))
	}
	if connection != "" && !hasConnectionTarget(targets, connection) {
		return QueueTarget{}, fmt.Errorf("horizon: connection %q is not monitored; candidates: %s", connection, formatQueueTargets(targets))
	}
	if queueName != "" {
		return QueueTarget{}, fmt.Errorf("horizon: queue %q is not monitored; candidates: %s", queueName, formatQueueTargets(targets))
	}
	return QueueTarget{}, fmt.Errorf("horizon: queue target is ambiguous; candidates: %s", formatQueueTargets(targets))
}

// filterQueueTargets 按传入 connection 和 queue 过滤候选目标。
func filterQueueTargets(targets []QueueTarget, connection string, queueName string) []QueueTarget {
	filtered := make([]QueueTarget, 0, len(targets))
	for _, target := range targets {
		if connection != "" && target.Connection != connection {
			continue
		}
		if queueName != "" && target.Queue != queueName {
			continue
		}
		filtered = append(filtered, target)
	}
	return filtered
}

// hasConnectionTarget 判断 connection 是否存在于当前 supervisor 目标集合中。
func hasConnectionTarget(targets []QueueTarget, connection string) bool {
	for _, target := range targets {
		if target.Connection == connection {
			return true
		}
	}
	return false
}

// formatQueueTargets 生成稳定候选列表，供错误消息提示调用方补齐参数。
func formatQueueTargets(targets []QueueTarget) string {
	parts := make([]string, 0, len(targets))
	for _, target := range targets {
		parts = append(parts, target.Connection+":"+target.Queue)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
