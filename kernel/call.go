package kernel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prismgo/framework/cache"
	"github.com/prismgo/framework/console"
)

const commandIsolationLockTTL = 24 * time.Hour

// acquireIsolationLock 为 Isolatable 命令获取缓存锁。
//
// 用途：避免同一命令重复执行。跨进程效果取决于 prismgo/cache 默认 store 是否为共享后端。
func acquireIsolationLock(isolatable console.Isolatable, commandCtx console.CommandContext) (func(), error) {
	key := strings.TrimSpace(isolatable.IsolationKey(commandCtx))
	if key == "" {
		key = strings.TrimSpace(commandCtx.CommandName())
	}
	if key == "" {
		return func() {}, nil
	}
	lock := cache.Lock("prismgo-command-"+sanitizeLockKey(key), commandIsolationLockTTL)
	ok, err := lock.Get(commandCtx.Context())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("command %q is already running", commandCtx.CommandName())
	}
	return func() {
		_ = lock.Release(context.Background())
	}, nil
}

func sanitizeLockKey(value string) string {
	replacer := strings.NewReplacer("/", "-", "\\", "-", ":", "-", " ", "-")
	return replacer.Replace(value)
}
