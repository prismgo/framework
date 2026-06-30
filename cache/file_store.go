package cache

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	libstore "github.com/eko/gocache/lib/v4/store"
	"github.com/prismgo/framework/support"
)

const (
	fileType             = "file"
	defaultFileCachePath = "storage/framework/cache/data"
	defaultFileLockPath  = "storage/framework/cache/locks"
	fileMutationLockTTL  = 30 * time.Second
	fileMutationSleep    = 5 * time.Millisecond
	fileLockGuardTTL     = 30 * time.Second
	fileMutationMaxWait  = 5 * time.Minute
)

// fileStore 使用本地文件保存缓存值，并为 Add、计数器和锁提供原子文件锁。
type fileStore struct {
	root     string
	lockRoot string
	options  *libstore.Options
}

// fileCacheEntry 是 file driver 写入磁盘的缓存文件结构。
type fileCacheEntry struct {
	ExpiresAt int64  `json:"expires_at"`
	Value     []byte `json:"value"`
}

// fileLockEntry 是 file driver 写入磁盘的锁文件结构。
type fileLockEntry struct {
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
}

// newFileStore 创建 Laravel file driver 风格的本地文件缓存。
//
// 参数说明：prefix 是当前 Repository 的缓存 key 前缀，lockPrefix 是锁 key 前缀。
// 需求背景：多个 Repository 可以共用同一个物理 Path/LockPath，因此 file driver
// 必须把不同前缀映射到不同子目录，避免 Flush 或 FlushLocks 清理到其他仓库。
func newFileStore(cfg FileConfig, defaultTTL time.Duration, prefix, lockPrefix string) *fileStore {
	root := strings.TrimSpace(cfg.Path)
	if root == "" {
		root = defaultFileCachePath
	}
	lockRoot := strings.TrimSpace(cfg.LockPath)
	if lockRoot == "" {
		if root == defaultFileCachePath {
			lockRoot = defaultFileLockPath
		} else {
			lockRoot = filepath.Join(filepath.Dir(root), "locks")
		}
	}
	root = fileScopedRoot(support.StoragePath(root), prefix)
	lockRoot = fileScopedRoot(support.StoragePath(lockRoot), lockPrefix)
	return &fileStore{
		root:     root,
		lockRoot: lockRoot,
		options:  &libstore.Options{Expiration: defaultTTL},
	}
}

// Get 从文件缓存中读取 key，未命中或过期时返回 gocache NotFound。
func (s *fileStore) Get(ctx context.Context, key any) (any, error) {
	_ = ctx
	k, err := fileStringKey(key)
	if err != nil {
		return nil, err
	}
	entry, err := s.readCacheEntry(k)
	if err != nil {
		return nil, err
	}
	if entry.expired(time.Now()) {
		_ = os.Remove(s.cachePath(k))
		return nil, fileCacheMiss()
	}
	return []byte(entry.Value), nil
}

// GetWithTTL 返回缓存值和剩余 TTL；永久 key 的 TTL 返回 -1。
// 优化：单次读取文件，避免重复 I/O 和可能的值不一致。
func (s *fileStore) GetWithTTL(ctx context.Context, key any) (any, time.Duration, error) {
	_ = ctx
	k, err := fileStringKey(key)
	if err != nil {
		return nil, 0, err
	}
	entry, err := s.readCacheEntry(k)
	if err != nil {
		return nil, 0, err
	}
	if entry.expired(time.Now()) {
		_ = os.Remove(s.cachePath(k))
		return nil, 0, fileCacheMiss()
	}
	if entry.ExpiresAt == 0 {
		return []byte(entry.Value), -1, nil
	}
	return []byte(entry.Value), time.Until(time.Unix(0, entry.ExpiresAt)), nil
}

// Set 写入缓存文件，并按调用参数或默认配置计算过期时间。
func (s *fileStore) Set(ctx context.Context, key any, value any, options ...libstore.Option) error {
	k, err := fileStringKey(key)
	if err != nil {
		return err
	}
	opts := libstore.ApplyOptionsWithDefault(s.options, options...)
	data, err := rawBytes(value)
	if err != nil {
		return err
	}
	return s.withMutationLock(ctx, k, func() error {
		return s.writeCacheEntry(k, newFileCacheEntry(data, opts.Expiration))
	})
}

// Add 仅当 key 不存在或已过期时写入，判断和写入由文件锁串行化。
func (s *fileStore) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	var added bool
	err := s.withMutationLock(ctx, key, func() error {
		entry, err := s.readCacheEntry(key)
		if err == nil && !entry.expired(time.Now()) {
			return nil
		}
		if err != nil && !isFileMiss(err) {
			return err
		}
		if err == nil {
			_ = os.Remove(s.cachePath(key))
		}
		added = true
		return s.writeCacheEntry(key, newFileCacheEntry(value, ttl))
	})
	return added, err
}

// Increment 原子递增整数计数器；已有 key 的过期时间保持不变。
func (s *fileStore) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	var next int64
	err := s.withMutationLock(ctx, key, func() error {
		entry, exists, err := s.currentCacheEntry(key)
		if err != nil {
			return err
		}
		var current int64
		if exists {
			current, err = decodeCounter([]byte(entry.Value))
			if err != nil {
				return err
			}
		}
		next = current + delta
		entry.Value = []byte(strconv.FormatInt(next, 10))
		return s.writeCacheEntry(key, entry)
	})
	return next, err
}

// Pull 原子读取并删除指定 key。
func (s *fileStore) Pull(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	err := s.withMutationLock(ctx, key, func() error {
		entry, err := s.readCacheEntry(key)
		if err != nil {
			return err
		}
		_ = os.Remove(s.cachePath(key))
		if entry.expired(time.Now()) {
			return fileCacheMiss()
		}
		data = append([]byte(nil), entry.Value...)
		return nil
	})
	return data, err
}

// GetMany 批量读取文件缓存，未命中的 key 不会出现在返回值中。
// 优化：在单次锁操作内完成所有读取，避免 N+1 文件 I/O 问题。
func (s *fileStore) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}

	out := make(map[string][]byte, len(keys))
	now := time.Now()

	// 使用单次锁操作批量读取所有 key
	err := s.withMutationLock(ctx, "__batch_read__", func() error {
		for _, key := range keys {
			entry, err := s.readCacheEntry(key)
			if err != nil {
				if isFileMiss(err) {
					continue
				}
				return err
			}

			if entry.expired(now) {
				_ = os.Remove(s.cachePath(key))
				continue
			}

			out[key] = append([]byte(nil), entry.Value...)
		}
		return nil
	})

	return out, err
}

// PutMany 批量写入文件缓存。
func (s *fileStore) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	for key, value := range values {
		if err := s.Set(ctx, key, value, expirationOptions(ttl)...); err != nil {
			return err
		}
	}
	return nil
}

// ForgetMany 批量删除文件缓存。
func (s *fileStore) ForgetMany(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := s.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

// Delete 删除指定 key；key 不存在时视为成功。
func (s *fileStore) Delete(ctx context.Context, key any) error {
	_ = ctx
	k, err := fileStringKey(key)
	if err != nil {
		return err
	}
	err = os.Remove(s.cachePath(k))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Invalidate 预留 gocache 标签失效接口；Laravel file driver 不支持 tags。
func (s *fileStore) Invalidate(ctx context.Context, options ...libstore.InvalidateOption) error {
	_ = ctx
	_ = options
	return nil
}

// Clear 清空当前 file store 的缓存数据目录，不清理锁目录。
func (s *fileStore) Clear(ctx context.Context) error {
	_ = ctx
	if err := os.RemoveAll(s.root); err != nil {
		return err
	}
	return os.MkdirAll(s.root, 0o755)
}

// GetType 返回 store 类型名称。
func (s *fileStore) GetType() string {
	return fileType
}

// Touch 只更新已有 key 的过期时间，不改变缓存值。
func (s *fileStore) Touch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	var touched bool
	err := s.withMutationLock(ctx, key, func() error {
		entry, err := s.readCacheEntry(key)
		if err != nil {
			if isFileMiss(err) {
				return nil
			}
			return err
		}
		if entry.expired(time.Now()) {
			_ = os.Remove(s.cachePath(key))
			return nil
		}
		entry.ExpiresAt = expiresAt(ttl)
		touched = true
		return s.writeCacheEntry(key, entry)
	})
	return touched, err
}

// Flush 按当前 Repository 前缀清理 file store；文件驱动目录已按 store 隔离，直接清空数据目录。
func (s *fileStore) Flush(ctx context.Context, prefix string) error {
	_ = prefix
	return s.Clear(ctx)
}

// Acquire 通过原子创建锁文件获取跨进程文件锁。
//
// 设计说明：过期检查、删除旧锁、创建新锁必须与 Release 的 token 校验互斥，
// 否则旧 owner 在锁过期后延迟 Release 时，可能删除新 owner 刚创建的锁。
func (s *fileStore) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	var acquired bool
	err := s.withLockGuard(ctx, key, func() error {
		path := s.lockPath(key)
		entry := fileLockEntry{Token: token, ExpiresAt: time.Now().Add(ttl).UnixNano()}
		for {
			if err := s.createLockFile(path, entry); err == nil {
				acquired = true
				return nil
			} else if !os.IsExist(err) {
				return err
			}
			current, err := s.readLockEntry(path)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return err
				}
				continue
			}
			if current.expired(time.Now()) {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return err
				}
				continue
			}
			return nil
		}
	})
	return acquired, err
}

// Release 校验 owner token 后释放文件锁。
//
// 设计说明：Release 与 Acquire 共享独立 guard 文件串行化，确保读取到的 token
// 和即将删除的锁文件属于同一个 owner，不会误删刚被其他调用方获取的新锁。
func (s *fileStore) Release(ctx context.Context, key, token string) (bool, error) {
	var released bool
	err := s.withLockGuard(ctx, key, func() error {
		path := s.lockPath(key)
		entry, err := s.readLockEntry(path)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Token != token {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		released = true
		return nil
	})
	return released, err
}

// ForceRelease 不校验 owner token，直接删除文件锁。
func (s *fileStore) ForceRelease(ctx context.Context, key string) error {
	_ = ctx
	err := os.Remove(s.lockPath(key))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// FlushLocks 清理当前 file store 的锁目录。
func (s *fileStore) FlushLocks(ctx context.Context, lockPrefix string) error {
	_ = ctx
	_ = lockPrefix
	if err := os.RemoveAll(s.lockRoot); err != nil {
		return err
	}
	return os.MkdirAll(s.lockRoot, 0o755)
}

// fileScopedRoot 将逻辑前缀映射为稳定子目录。
//
// 设计说明：前缀可能包含冒号、租户 ID 等业务字符，直接用于目录名会暴露业务信息
// 且存在跨平台字符兼容问题，因此使用 SHA1 摘要作为目录名。
func fileScopedRoot(root, prefix string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		return root
	}
	return filepath.Join(root, "__prefix", hashString(prefix))
}

// currentCacheEntry 返回未过期 entry；不存在或过期时返回空 entry。
func (s *fileStore) currentCacheEntry(key string) (fileCacheEntry, bool, error) {
	entry, err := s.readCacheEntry(key)
	if err != nil {
		if isFileMiss(err) {
			return fileCacheEntry{}, false, nil
		}
		return fileCacheEntry{}, false, err
	}
	if entry.expired(time.Now()) {
		_ = os.Remove(s.cachePath(key))
		return fileCacheEntry{}, false, nil
	}
	return entry, true, nil
}

// withMutationLock 使用内部文件锁串行化同一 key 的复合读写操作。
// 为防止 goroutine 泄漏，如果调用方未设置 deadline，则使用默认最大等待时间。
func (s *fileStore) withMutationLock(ctx context.Context, key string, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// 如果调用方未设置 deadline，使用默认最大等待时间防止 goroutine 泄漏
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, fileMutationMaxWait)
		defer cancel()
	}
	lockKey := "__cache_mutation:" + key
	token := randomToken()
	for {
		// 先检查 context 是否已取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		ok, err := s.Acquire(ctx, lockKey, token, fileMutationLockTTL)
		if err != nil {
			return err
		}
		if ok {
			defer func() { _, _ = s.Release(context.Background(), lockKey, token) }()
			return fn()
		}
		if err := sleepWithContext(ctx, fileMutationSleep); err != nil {
			return err
		}
	}
}

// withLockGuard 使用独立 guard 文件串行化同一业务锁的 Acquire/Release 临界区。
// 为防止 goroutine 泄漏，如果调用方未设置 deadline，则使用默认最大等待时间。
func (s *fileStore) withLockGuard(ctx context.Context, key string, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// 如果调用方未设置 deadline，使用默认最大等待时间防止 goroutine 泄漏
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, fileMutationMaxWait)
		defer cancel()
	}
	path := s.lockGuardPath(key)
	token := randomToken()
	for {
		// 先检查 context 是否已取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		ok, err := s.acquireGuardFile(path, token)
		if err != nil {
			return err
		}
		if ok {
			defer s.releaseGuardFile(path, token)
			return fn()
		}
		if err := sleepWithContext(ctx, fileMutationSleep); err != nil {
			return err
		}
	}
}

// acquireGuardFile 获取内部 guard 文件；过期 guard 会被清理后重试。
func (s *fileStore) acquireGuardFile(path, token string) (bool, error) {
	entry := fileLockEntry{Token: token, ExpiresAt: time.Now().Add(fileLockGuardTTL).UnixNano()}
	if err := s.createLockFile(path, entry); err == nil {
		return true, nil
	} else if !os.IsExist(err) {
		return false, err
	}
	current, err := s.readLockEntry(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current.expired(time.Now()) {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}

// releaseGuardFile 只释放自己持有的 guard，避免破坏其他进程刚获取的 guard。
func (s *fileStore) releaseGuardFile(path, token string) {
	entry, err := s.readLockEntry(path)
	if err != nil || entry.Token != token {
		return
	}
	_ = os.Remove(path)
}

// readCacheEntry 从磁盘读取并解析缓存文件。
func (s *fileStore) readCacheEntry(key string) (fileCacheEntry, error) {
	data, err := os.ReadFile(s.cachePath(key))
	if err != nil {
		if os.IsNotExist(err) {
			return fileCacheEntry{}, fileCacheMiss()
		}
		return fileCacheEntry{}, err
	}
	var entry fileCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return fileCacheEntry{}, err
	}
	return entry, nil
}

// writeCacheEntry 通过临时文件加 rename 原子替换缓存文件。
func (s *fileStore) writeCacheEntry(key string, entry fileCacheEntry) error {
	path := s.cachePath(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	temp := path + "." + randomToken() + ".tmp"
	if err := os.WriteFile(temp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		_ = os.Remove(temp)
		return err
	}
	return nil
}

// createLockFile 使用 O_EXCL 原子创建锁文件。
func (s *fileStore) createLockFile(path string, entry fileLockEntry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// readLockEntry 从磁盘读取并解析锁文件。
func (s *fileStore) readLockEntry(path string) (fileLockEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fileLockEntry{}, err
	}
	var entry fileLockEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return fileLockEntry{}, err
	}
	return entry, nil
}

// cachePath 根据 key 的 SHA1 摘要生成分层缓存文件路径。
func (s *fileStore) cachePath(key string) string {
	return hashedPath(s.root, key)
}

// lockPath 根据 key 的 SHA1 摘要生成分层锁文件路径。
func (s *fileStore) lockPath(key string) string {
	return hashedPath(s.lockRoot, key)
}

// lockGuardPath 返回业务锁对应的内部 guard 文件路径。
func (s *fileStore) lockGuardPath(key string) string {
	return hashedPath(filepath.Join(s.lockRoot, "__guards"), key)
}

// hashedPath 使用两层目录分散文件，避免单目录文件过多。
func hashedPath(root, key string) string {
	sum := sha1.Sum([]byte(key))
	hash := hex.EncodeToString(sum[:])
	return filepath.Join(root, hash[:2], hash[2:4], hash)
}

// newFileCacheEntry 根据 TTL 构造缓存文件 entry。
func newFileCacheEntry(value []byte, ttl time.Duration) fileCacheEntry {
	return fileCacheEntry{ExpiresAt: expiresAt(ttl), Value: append([]byte(nil), value...)}
}

// expiresAt 把 TTL 转成 UnixNano 时间戳，0 表示不过期。
func expiresAt(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return time.Now().Add(ttl).UnixNano()
}

// expired 判断缓存文件是否已经过期。
func (e fileCacheEntry) expired(now time.Time) bool {
	return e.ExpiresAt > 0 && !now.Before(time.Unix(0, e.ExpiresAt))
}

// expired 判断锁文件是否已经过期。
func (e fileLockEntry) expired(now time.Time) bool {
	return e.ExpiresAt > 0 && !now.Before(time.Unix(0, e.ExpiresAt))
}

// fileStringKey 校验 gocache 传入的 key 类型。
func fileStringKey(key any) (string, error) {
	k, ok := key.(string)
	if !ok {
		return "", errors.New("cache: file key must be string")
	}
	return k, nil
}

// fileCacheMiss 返回与 gocache StoreInterface 兼容的未命中错误。
func fileCacheMiss() error {
	return libstore.NotFoundWithCause(ErrCacheMiss)
}

// isFileMiss 判断 file store 内部是否遇到缓存未命中。
func isFileMiss(err error) bool {
	return isMiss(err)
}
