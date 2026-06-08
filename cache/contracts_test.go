package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
)

var (
	_ cachecontract.Factory          = (*Manager)(nil)
	_ cachecontract.Repository       = (*Repository)(nil)
	_ cachecontract.TaggedRepository = (*TaggedRepository)(nil)
	_ cachecontract.MemoRepository   = (*MemoRepository)(nil)
	_ cachecontract.FunnelLimiter    = (*FunnelLimiter)(nil)
	_ cachecontract.Lock             = (*DistributedLock)(nil)

	_ TouchStore                     = cachecontract.TouchStore(nil)
	_ cachecontract.TouchStore       = TouchStore(nil)
	_ CloseStore                     = cachecontract.CloseStore(nil)
	_ cachecontract.CloseStore       = CloseStore(nil)
	_ AtomicStore                    = cachecontract.AtomicStore(nil)
	_ cachecontract.AtomicStore      = AtomicStore(nil)
	_ BulkStore                      = cachecontract.BulkStore(nil)
	_ cachecontract.BulkStore        = BulkStore(nil)
	_ PrefixFlushStore               = cachecontract.PrefixFlushStore(nil)
	_ cachecontract.PrefixFlushStore = PrefixFlushStore(nil)
	_ TagStore                       = cachecontract.TagStore(nil)
	_ cachecontract.TagStore         = TagStore(nil)
	_ LockProvider                   = cachecontract.LockProvider(nil)
	_ cachecontract.LockProvider     = LockProvider(nil)
	_ LockFlushStore                 = cachecontract.LockFlushStore(nil)
	_ cachecontract.LockFlushStore   = LockFlushStore(nil)
)

func TestRepositoryGetStoreReturnsCacheContractStore(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "memory",
		Stores: map[string]StoreConfig{
			"memory": {Driver: "memory"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	store := manager.defaultRepository().GetStore()
	if store == nil {
		t.Fatal("GetStore() = nil, want cache contract store")
	}
}

func TestRepositoryGetStoreDelegatesStoreContractOperations(t *testing.T) {
	manager, err := NewManager(Config{
		Default: "memory",
		Prefix:  "contract",
		Stores: map[string]StoreConfig{
			"memory": {Driver: "memory"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	ctx := context.Background()
	store := manager.defaultRepository().GetStore()
	if store == nil {
		t.Fatal("GetStore() = nil, want cache contract store")
	}
	if store.Prefix() != "contract" {
		t.Fatalf("store prefix = %q, want contract", store.Prefix())
	}
	if err := store.Put(ctx, "name", "alice", time.Minute); err != nil {
		t.Fatalf("store Put error = %v", err)
	}
	if value, err := store.Get(ctx, "name"); err != nil || value != "alice" {
		t.Fatalf("store Get value=%#v err=%v, want alice", value, err)
	}
	if err := store.PutMany(ctx, map[string]any{"a": "A", "b": "B"}, time.Minute); err != nil {
		t.Fatalf("store PutMany error = %v", err)
	}
	values, err := store.Many(ctx, []string{"a", "b", "missing"})
	if err != nil || values["a"] != "A" || values["b"] != "B" {
		t.Fatalf("store Many values=%#v err=%v", values, err)
	}
	if _, ok := values["missing"]; !ok || values["missing"] != nil {
		t.Fatalf("store Many missing value=%#v present=%v, want nil present", values["missing"], ok)
	}
	if ok, err := store.Add(ctx, "once", 1, time.Minute); err != nil || !ok {
		t.Fatalf("store Add ok=%v err=%v, want true nil", ok, err)
	}
	if count, err := store.Increment(ctx, "counter", 3); err != nil || count != 3 {
		t.Fatalf("store Increment count=%d err=%v, want 3 nil", count, err)
	}
	if count, err := store.Decrement(ctx, "counter", 2); err != nil || count != 1 {
		t.Fatalf("store Decrement count=%d err=%v, want 1 nil", count, err)
	}
	if err := store.Forever(ctx, "forever", "value"); err != nil {
		t.Fatalf("store Forever error = %v", err)
	}
	if err := store.Forget(ctx, "forever"); err != nil {
		t.Fatalf("store Forget error = %v", err)
	}
	if err := store.ForgetMany(ctx, []string{"a", "b"}); err != nil {
		t.Fatalf("store ForgetMany error = %v", err)
	}
	if err := store.Flush(ctx); err != nil {
		t.Fatalf("store Flush error = %v", err)
	}
}

func TestCacheErrorsAliasContractErrors(t *testing.T) {
	if !errors.Is(ErrCacheMiss, cachecontract.ErrCacheMiss) {
		t.Fatalf("ErrCacheMiss is not contract ErrCacheMiss")
	}
	if !errors.Is(ErrStoreNotFound, cachecontract.ErrStoreNotFound) {
		t.Fatalf("ErrStoreNotFound is not contract ErrStoreNotFound")
	}
}
