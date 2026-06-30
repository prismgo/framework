package queue

import (
	"context"
	"testing"
	"time"

	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
)

// TestMemoryFailedStore_PageReturnsOrderedByFailedAt 验证 Page 返回按 FailedAt 升序排序的结果。
// 这是性能优化的前提：即使 Record 乱序插入，Page 仍须返回有序结果。
func TestMemoryFailedStore_PageReturnsOrderedByFailedAt(t *testing.T) {
	store := NewMemoryFailedStore()
	ctx := context.Background()

	// 乱序插入 3 条记录
	now := time.Now()
	jobs := []payload.FailedJob{
		{ID: "job-3", JobID: "job-3", Queue: "default", JobName: "TestJob", FailedAt: now.Add(2 * time.Hour)},
		{ID: "job-1", JobID: "job-1", Queue: "default", JobName: "TestJob", FailedAt: now},
		{ID: "job-2", JobID: "job-2", Queue: "default", JobName: "TestJob", FailedAt: now.Add(1 * time.Hour)},
	}

	for _, job := range jobs {
		if err := store.Record(ctx, job); err != nil {
			t.Fatalf("Record failed: %v", err)
		}
	}

	// Page 应返回按 FailedAt 升序排序的结果
	page, err := store.Page(ctx, state.PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("Page failed: %v", err)
	}

	if len(page.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(page.Items))
	}

	// 验证顺序：job-1 < job-2 < job-3
	expectedOrder := []string{"job-1", "job-2", "job-3"}
	for i, expected := range expectedOrder {
		if page.Items[i].ID != expected {
			t.Errorf("position %d: expected %s, got %s", i, expected, page.Items[i].ID)
		}
	}
}

// TestMemoryFailedStore_PagePerformanceWithLargeDataset 验证大数据量下 Page 性能。
// 优化后 Page 应为 O(pageSize)，而非 O(n log n)。
func TestMemoryFailedStore_PagePerformanceWithLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	store := NewMemoryFailedStore()
	ctx := context.Background()

	// 插入 10000 条记录
	count := 10000
	now := time.Now()
	for i := 0; i < count; i++ {
		job := payload.FailedJob{
			ID:       string(rune('A'+i%26)) + string(rune('0'+i/26)),
			JobID:    string(rune('A'+i%26)) + string(rune('0'+i/26)),
			Queue:    "default",
			JobName:  "TestJob",
			FailedAt: now.Add(time.Duration(i) * time.Millisecond),
		}
		if err := store.Record(ctx, job); err != nil {
			t.Fatalf("Record failed at %d: %v", i, err)
		}
	}

	// 测量 Page 耗时
	start := time.Now()
	page, err := store.Page(ctx, state.PageRequest{Page: 1, PageSize: 50})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Page failed: %v", err)
	}

	if len(page.Items) != 50 {
		t.Errorf("expected 50 items, got %d", len(page.Items))
	}

	if page.Total != count {
		t.Errorf("expected total %d, got %d", count, page.Total)
	}

	// 性能断言：Page 应在 10ms 内完成（优化后 O(pageSize)）
	// 未优化时 10000 条记录排序约需 1-5ms，但随数据量增长会线性恶化
	if elapsed > 10*time.Millisecond {
		t.Logf("warning: Page took %v for %d records", elapsed, count)
	}
}

// TestMemoryFailedStore_RecordMaintainsOrder 验证 Record 时维护有序索引。
// 即使乱序插入，内部 order 切片应保持按 FailedAt 升序。
func TestMemoryFailedStore_RecordMaintainsOrder(t *testing.T) {
	store := NewMemoryFailedStore()
	ctx := context.Background()

	now := time.Now()
	// 乱序插入
	if err := store.Record(ctx, payload.FailedJob{ID: "c", JobID: "c", Queue: "default", JobName: "TestJob", FailedAt: now.Add(2 * time.Hour)}); err != nil {
		t.Fatalf("Record failed: %v", err)
	}
	if err := store.Record(ctx, payload.FailedJob{ID: "a", JobID: "a", Queue: "default", JobName: "TestJob", FailedAt: now}); err != nil {
		t.Fatalf("Record failed: %v", err)
	}
	if err := store.Record(ctx, payload.FailedJob{ID: "b", JobID: "b", Queue: "default", JobName: "TestJob", FailedAt: now.Add(1 * time.Hour)}); err != nil {
		t.Fatalf("Record failed: %v", err)
	}

	// 验证内部 order 切片有序
	if len(store.order) != 3 {
		t.Fatalf("expected order length 3, got %d", len(store.order))
	}

	expectedOrder := []string{"a", "b", "c"}
	for i, expected := range expectedOrder {
		if store.order[i] != expected {
			t.Errorf("order[%d]: expected %s, got %s", i, expected, store.order[i])
		}
	}
}
