package rabbitmq

import "time"

// 本文件只管理当前 AMQP connection 内的 RabbitMQ Topology Usage Cache 淘汰元数据。
// 淘汰删除的是本地 map 命中记录，不会对 broker 执行 QueueDelete、ExchangeDelete 或 QueueUnbind。

func (c *Connection) topologyCacheEnabled() bool {
	return c != nil && (c.options.TopologyCacheTTL > 0 || c.options.TopologyCacheMaxEntries > 0)
}

func (c *Connection) topologyCacheNow() time.Time {
	if c != nil && c.topologyNow != nil {
		return c.topologyNow()
	}
	return time.Now()
}

func rabbitMQDeclaredQueueCacheKey(queue string) rabbitMQTopologyCacheKey {
	return rabbitMQTopologyCacheKey{Kind: rabbitMQTopologyCacheDeclaredQueue, Name: queue, Queue: queue}
}

func rabbitMQPluginDelayCacheKey(queue string) rabbitMQTopologyCacheKey {
	return rabbitMQTopologyCacheKey{Kind: rabbitMQTopologyCachePluginDelay, Name: queue, Queue: queue}
}

func rabbitMQTTLDLXDelayCacheKey(delayQueue, queue string) rabbitMQTopologyCacheKey {
	return rabbitMQTopologyCacheKey{Kind: rabbitMQTopologyCacheTTLDLXDelay, Name: delayQueue, Queue: queue}
}

func rabbitMQVerifiedTopologyCacheKey(key rabbitMQTopologyVerificationKey) rabbitMQTopologyCacheKey {
	return rabbitMQTopologyCacheKey{Kind: rabbitMQTopologyCacheVerified, Name: key.Name, Queue: key.Queue, Verification: key}
}

func rabbitMQRestartQueueCacheKey(queue string) rabbitMQTopologyCacheKey {
	return rabbitMQTopologyCacheKey{Kind: rabbitMQTopologyCacheRestartQueue, Name: queue, Queue: queue}
}

func (c *Connection) pruneExpiredTopologyCache() {
	if c == nil || c.options.TopologyCacheTTL <= 0 {
		return
	}
	now := c.topologyCacheNow()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.topologyUsage) == 0 {
		return
	}
	for key, entry := range c.topologyUsage {
		if entry.LastUsed.IsZero() || now.Sub(entry.LastUsed) <= c.options.TopologyCacheTTL {
			continue
		}
		if c.isTopologyUsageProtectedLocked(key) {
			continue
		}
		c.deleteTopologyUsageLocked(key)
	}
}

func (c *Connection) pruneTopologyCacheCapacity() {
	if c == nil || c.options.TopologyCacheMaxEntries <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneTopologyCacheCapacityLocked()
}

// pruneTopologyCacheCapacityLocked 通过 LRU 链表尾部淘汰最久未使用的条目。
//
// 设计原因：原实现在每次超容量时扫描全部条目寻找 LRU 候选，时间复杂度 O(n*k)（n 为
// 缓存大小，k 为需淘汰数量）。改用 container/list 后，淘汰操作降至 O(1) 每条目。
// 受保护的条目（仍有活跃 consumer intent 的队列）会从链表中移除后重新插入到合适位置，
// 避免它们被错误淘汰。
func (c *Connection) pruneTopologyCacheCapacityLocked() {
	maxEntries := c.options.TopologyCacheMaxEntries
	if maxEntries <= 0 || c.topologyLRU == nil {
		return
	}
	for len(c.topologyUsage) > maxEntries && c.topologyLRU.Len() > 0 {
		removed := false
		checks := c.topologyLRU.Len()
		for i := 0; i < checks && len(c.topologyUsage) > maxEntries; i++ {
			// 从链表尾部（最久未使用）开始寻找可淘汰的候选。
			// 最多扫描当前链表一轮；如果全部条目都受保护，则保留超容量状态并退出，避免死循环。
			back := c.topologyLRU.Back()
			if back == nil {
				return
			}
			entry, ok := back.Value.(*rabbitMQTopologyUsageEntry)
			if !ok {
				// 防御：链表节点值类型异常时移除并继续
				c.topologyLRU.Remove(back)
				removed = true
				continue
			}
			if c.isTopologyUsageProtectedLocked(entry.key) {
				c.topologyLRU.MoveToFront(back)
				continue
			}
			c.deleteTopologyUsageLocked(entry.key)
			removed = true
		}
		if !removed {
			return
		}
	}
}

// markTopologyUsageLocked 记录或覆盖一条拓扑缓存，并将其置于 LRU 链表前端。
//
// 逻辑说明：如果 key 已经存在，先删除旧节点再重新插入前端，保持"最近写入=最前"语义。
func (c *Connection) markTopologyUsageLocked(key rabbitMQTopologyCacheKey, now time.Time) {
	if !c.topologyCacheEnabled() {
		return
	}
	if c.topologyUsage == nil {
		c.topologyUsage = make(map[rabbitMQTopologyCacheKey]rabbitMQTopologyUsageEntry)
	}
	// 已存在时先移除旧节点，避免链表中出现重复
	if existing, ok := c.topologyUsage[key]; ok && existing.element != nil && c.topologyLRU != nil {
		c.topologyLRU.Remove(existing.element)
	}
	entry := rabbitMQTopologyUsageEntry{LastUsed: now, key: key}
	if c.topologyLRU != nil {
		entry.element = c.topologyLRU.PushFront(&entry)
	}
	c.topologyUsage[key] = entry
}

// touchTopologyUsageLocked 更新已有条目的时间戳并将其移到 LRU 链表前端。
func (c *Connection) touchTopologyUsageLocked(key rabbitMQTopologyCacheKey, now time.Time) {
	if !c.topologyCacheEnabled() || c.topologyUsage == nil {
		return
	}
	if existing, ok := c.topologyUsage[key]; ok {
		existing.LastUsed = now
		if existing.element != nil && c.topologyLRU != nil {
			c.topologyLRU.MoveToFront(existing.element)
		}
		c.topologyUsage[key] = existing
	}
}

// deleteTopologyUsageLocked 从 map 和 LRU 链表中同时移除指定条目。
func (c *Connection) deleteTopologyUsageLocked(key rabbitMQTopologyCacheKey) {
	if entry, ok := c.topologyUsage[key]; ok {
		if entry.element != nil && c.topologyLRU != nil {
			c.topologyLRU.Remove(entry.element)
		}
		delete(c.topologyUsage, key)
	}
	switch key.Kind {
	case rabbitMQTopologyCacheDeclaredQueue:
		delete(c.declaredQueues, key.Name)
	case rabbitMQTopologyCachePluginDelay:
		delete(c.delayedQueues, key.Name)
	case rabbitMQTopologyCacheTTLDLXDelay:
		delete(c.ttlDelayQueues, key.Name)
	case rabbitMQTopologyCacheVerified:
		delete(c.verifiedTopology, key.Verification)
	case rabbitMQTopologyCacheRestartQueue:
		delete(c.restartQueues, key.Name)
	}
}

func (c *Connection) isTopologyUsageProtectedLocked(key rabbitMQTopologyCacheKey) bool {
	switch key.Kind {
	case rabbitMQTopologyCacheDeclaredQueue, rabbitMQTopologyCachePluginDelay, rabbitMQTopologyCacheTTLDLXDelay:
		return c.hasLiveConsumerIntentLocked(key.Queue)
	case rabbitMQTopologyCacheVerified:
		switch key.Verification.Kind {
		case rabbitMQVerifiedQueue, rabbitMQVerifiedDelayQueue:
			return c.hasLiveConsumerIntentLocked(key.Verification.Queue)
		case rabbitMQVerifiedExchange, rabbitMQVerifiedDelayExchange:
			return len(c.activeConsumers) > 0 || len(c.consumerRefs) > 0
		}
	}
	return false
}

func (c *Connection) hasLiveConsumerIntentLocked(queue string) bool {
	if queue == "" {
		return false
	}
	if refs := c.consumerRefs[queue]; refs > 0 {
		return true
	}
	_, ok := c.activeConsumers[queue]
	return ok
}
