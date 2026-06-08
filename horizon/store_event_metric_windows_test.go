package horizon

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStoresQueryEventMetricWindowsByRangeSourceAndStablePage(t *testing.T) {
	// 需求背景：event metric window contract 要求 Memory Store 和 Redis Store 共享按事件窗口范围、来源维度和稳定分页的查询合同。
	ctx := context.Background()
	base := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	newStores := map[string]func(t *testing.T) Store{
		"memory": func(t *testing.T) Store {
			return NewMemoryStore(StoreOptions{})
		},
		"redis": func(t *testing.T) Store {
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			return NewRedisStoreFromClient(client, StoreOptions{Prefix: "event_metric_windows", HeartbeatTTL: time.Minute})
		},
	}
	for name, newStore := range newStores {
		t.Run(name, func(t *testing.T) {
			store := newStore(t)
			windows := []EventMetricWindow{
				eventMetricWindowFixture(base.Add(time.Minute), base.Add(2*time.Minute), base.Add(4*time.Minute), "host-b", "production", "supervisor-b", "redis", "default", 11),
				eventMetricWindowFixture(base.Add(time.Minute), base.Add(2*time.Minute), base.Add(4*time.Minute), "host-a", "production", "supervisor-a", "redis", "default", 7),
				eventMetricWindowFixture(base.Add(time.Minute), base.Add(2*time.Minute), base.Add(3*time.Minute), "host-a", "production", "supervisor-a", "redis", "critical", 13),
				eventMetricWindowFixture(base.Add(-5*time.Minute), base.Add(-4*time.Minute), base.Add(10*time.Minute), "host-a", "production", "supervisor-a", "redis", "default", 3),
			}
			if err := store.AppendEventMetricWindows(ctx, windows, 0); err != nil {
				t.Fatalf("append event metric windows: %v", err)
			}

			page, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{
				Page:              PageRequest{Page: 1, PageSize: 1},
				From:              base.Add(30 * time.Second),
				To:                base.Add(90 * time.Second),
				SourceHost:        "host-a",
				SourceEnvironment: "production",
				SourceSupervisor:  "supervisor-a",
				Connection:        "redis",
			})
			if err != nil {
				t.Fatalf("query event metric windows: %v", err)
			}
			if page.Total != 2 || len(page.Items) != 1 {
				t.Fatalf("expected filtered stable page, got %#v", page)
			}
			if page.Items[0].Queue != "default" || page.Items[0].Processed != 7 {
				t.Fatalf("first page should use stable sort by window_start, flush_at and source dimensions, got %#v", page.Items[0])
			}

			page, err = store.EventMetricWindows(ctx, EventMetricWindowQuery{
				Page:              PageRequest{Page: 2, PageSize: 1},
				From:              base.Add(30 * time.Second),
				To:                base.Add(90 * time.Second),
				SourceHost:        "host-a",
				SourceEnvironment: "production",
				SourceSupervisor:  "supervisor-a",
				Connection:        "redis",
			})
			if err != nil {
				t.Fatalf("query second page: %v", err)
			}
			if page.Total != 2 || len(page.Items) != 1 || page.Items[0].Queue != "critical" {
				t.Fatalf("second page should be stable and filtered, got %#v", page)
			}
		})
	}
}
