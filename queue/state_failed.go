package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	"github.com/prismgo/framework/queue/payload"
	redisqueue "github.com/prismgo/framework/queue/redis"
	"github.com/prismgo/framework/queue/state"
)

// MemoryFailedStore 是 sync 驱动和测试使用的失败任务存储。
type MemoryFailedStore struct {
	mu    sync.RWMutex
	items map[string]payload.FailedJob
	order []string
}

// NewMemoryFailedStore 创建进程内失败任务存储。
func NewMemoryFailedStore() *MemoryFailedStore {
	return &MemoryFailedStore{items: make(map[string]payload.FailedJob)}
}

func (s *MemoryFailedStore) Record(_ context.Context, failed payload.FailedJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if failed.ID == "" {
		failed.ID = failed.JobID
	}
	if failed.FailedAt.IsZero() {
		failed.FailedAt = time.Now()
	}
	if _, exists := s.items[failed.ID]; !exists {
		// 二分查找插入位置，保持 order 按 FailedAt 升序
		insertPos := sort.Search(len(s.order), func(i int) bool {
			return s.items[s.order[i]].FailedAt.After(failed.FailedAt)
		})
		// 在 insertPos 位置插入
		s.order = append(s.order, "")
		copy(s.order[insertPos+1:], s.order[insertPos:])
		s.order[insertPos] = failed.ID
	}
	s.items[failed.ID] = cloneFailed(failed)
	return nil
}

func (s *MemoryFailedStore) Page(_ context.Context, page state.PageRequest) (state.PageEnvelope[payload.FailedJob], error) {
	page = normalizeQueuePage(page)
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]payload.FailedJob, 0, len(s.order))
	for _, id := range s.order {
		if item, ok := s.items[id]; ok {
			result = append(result, cloneFailed(item))
		}
	}
	return state.PageEnvelope[payload.FailedJob]{
		Items:    queuePageSlice(result, page),
		Total:    len(result),
		Page:     page.Page,
		PageSize: page.PageSize,
	}, nil
}

func (s *MemoryFailedStore) Find(_ context.Context, id string) (*payload.FailedJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.items[id]
	if !ok {
		return nil, ErrEmpty
	}
	cloned := cloneFailed(item)
	return &cloned, nil
}

func (s *MemoryFailedStore) Forget(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, id)
	s.order = removeString(s.order, id)
	return nil
}

func (s *MemoryFailedStore) Flush(_ context.Context) error {
	s.mu.Lock()
	s.items = make(map[string]payload.FailedJob)
	s.order = nil
	s.mu.Unlock()
	return nil
}

// cloneFailed 深拷贝 FailedJob，避免调用方修改影响存储。
//
// 逻辑说明：使用字段级拷贝替代 JSON 序列化/反序列化，避免高频场景下的性能开销。
// Envelope 是值类型，直接拷贝其切片字段即可实现深拷贝。
func cloneFailed(in payload.FailedJob) payload.FailedJob {
	// Envelope 是值类型，深拷贝其中的切片字段避免共享底层数组
	in.Envelope.Payload = append([]byte(nil), in.Envelope.Payload...)
	in.Envelope.BackoffSec = append([]int(nil), in.Envelope.BackoffSec...)
	in.Envelope.Chain = append([]payload.PendingJob(nil), in.Envelope.Chain...)
	in.Envelope.Tags = append([]string(nil), in.Envelope.Tags...)
	// Tags 是 FailedJob 自身的切片字段
	in.Tags = append([]string(nil), in.Tags...)
	return in
}

func removeString(values []string, target string) []string {
	result := values[:0]
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}

// normalizeQueuePage 归一化分页参数，与 state.MaxPageSize 保持一致的上限约束。
func normalizeQueuePage(page state.PageRequest) state.PageRequest {
	if page.Page <= 0 {
		page.Page = 1
	}
	if page.PageSize <= 0 {
		page.PageSize = 50
	}
	if page.PageSize > state.MaxPageSize {
		page.PageSize = state.MaxPageSize
	}
	return page
}

func queuePageSlice[T any](items []T, page state.PageRequest) []T {
	start := 0
	if page.Page > 1 {
		start = (page.Page - 1) * page.PageSize
	}
	if start >= len(items) {
		return []T{}
	}
	end := start + page.PageSize
	if end > len(items) {
		end = len(items)
	}
	return append([]T(nil), items[start:end]...)
}

func buildFailedStore(cfg Config, codec encodingcontract.Codec) (FailedStore, error) {
	switch normalizeDriverName(cfg.Failed.Driver) {
	case "", "memory":
		return NewMemoryFailedStore(), nil
	case "redis":
		options := redisqueue.RedisOptions{
			Connection: firstNonEmpty(cfg.Failed.Store, "default"),
			Prefix:     firstNonEmpty(cfg.Failed.Prefix, "prismgo_queue"),
			FailedTTL:  cfg.Failed.TTL,
			Codec:      codec,
		}
		client, err := redisqueue.ResolveQueueClient(options)
		if err != nil {
			return nil, err
		}
		return redisqueue.NewRedisFailedStoreFromClient(client, options), nil
	default:
		return nil, fmt.Errorf("queue: unknown failed store driver %q", cfg.Failed.Driver)
	}
}
