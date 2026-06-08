package helper

import (
	"strings"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

// NormalizeQueues 统一清理 worker 和 driver 的队列输入。
//
// 需求背景：队列名可能来自 CLI、Horizon 或直接 contract 调用；不同入口如果各自处理空白、
// 重复队列，会让 consumer intent、事件和 Pop 实际监听范围不一致。
//
// 参数说明：queues 是调用方传入的原始队列列表；defaultQueue 是清理后为空时使用的默认队列。
// 返回值：去除空白名称、去重并保留首次出现顺序后的队列列表；全空时返回默认队列。
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

// NormalizePopWaitMode 统一 Pop wait mode 默认值。
//
// 需求背景：Pop(ctx, queues) 省略 wait 参数时应等价于 PopWaitAvailable，各 driver 不能各自
// 复制默认值逻辑，否则后续新增 wait mode 容易出现行为漂移。
//
// 参数说明：wait 是可变参数展开后的 wait mode 列表；当前 contract 只读取第一个显式值。
func NormalizePopWaitMode(wait []queuecontract.PopWaitMode) queuecontract.PopWaitMode {
	if len(wait) == 0 {
		return queuecontract.PopWaitAvailable
	}
	return wait[0]
}
