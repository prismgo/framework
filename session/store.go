package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"math"
	"net/http"
	"regexp"
	"sync"
	"time"
)

var sessionIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{32,128}$`)

// Store 表示单次请求内可读写的服务端 session 状态。
//
// 需求背景：Laravel 风格 session API 需要把 get/put/flash/regenerate 等操作集中在一个请求级对象上，
// 避免 handler 直接接触底层 driver、cookie 或持久化 payload。
// 设计思路：Store 只维护内存中的业务键值、flash 元数据和当前 ID；真正的读写、锁和 cookie 输出交给 Manager。
// response 仅用于 Save 阶段写出 session ID cookie，不保存 session payload，避免把服务端状态泄露到客户端。
type Store struct {
	id            string
	payload       Payload
	manager       *Manager
	response      http.ResponseWriter
	dirty         bool
	saveMu        sync.Mutex
	lockMu        sync.Mutex
	requestLock   Lock
	requestLockID string
}

// newStore 根据 driver 读出的 Payload 或空 Payload 构造请求级 Store。
//
// 参数 manager 提供时间、配置和持久化能力；payload 是待恢复的服务端状态；response 用于可选写出 cookie。
// 当 payload 缺少 ID、Values 或创建时间时，会补齐安全默认值，保证后续公开 API 不需要处理半初始化状态。
func newStore(manager *Manager, payload Payload, response http.ResponseWriter) *Store {
	now := manager.now()
	if payload.ID == "" {
		payload.ID = newSessionID()
	}
	if payload.Values == nil {
		payload.Values = make(map[string]any)
	}
	if payload.CreatedAt.IsZero() {
		payload.CreatedAt = now
	}
	payload.LastActivity = now
	return &Store{id: payload.ID, payload: payload, manager: manager, response: response}
}

// ID 返回写入浏览器 cookie 的不透明 session 标识。
func (s *Store) ID() string { return s.id }

// Get 读取指定 key，缺失时返回第一个默认值；未传默认值时返回 nil。
//
// 参数 key 是业务 session 键；def 是 Laravel 风格可选默认值。Get 不会修改 dirty 状态。
func (s *Store) Get(key string, def ...any) any {
	if value, ok := s.payload.Values[key]; ok {
		return value
	}
	if len(def) > 0 {
		return def[0]
	}
	return nil
}

// Put 写入业务 session 值，并标记当前 Store 需要保存。
//
// 参数 key 是业务键名；value 是可序列化值。序列化校验留给 driver 写入阶段统一处理。
func (s *Store) Put(key string, value any) {
	s.payload.Values[key] = value
	s.markDirty()
}

// Has 判断 key 是否存在且值不为 nil，对齐 Laravel 的 has 语义。
func (s *Store) Has(key string) bool {
	value, ok := s.payload.Values[key]
	return ok && value != nil
}

// Exists 只判断 key 是否存在，即使值为 nil 也返回 true。
func (s *Store) Exists(key string) bool {
	_, ok := s.payload.Values[key]
	return ok
}

// Missing 是 Exists 的反向语义，用于表达“键不存在”。
func (s *Store) Missing(key string) bool { return !s.Exists(key) }

// All 返回所有业务 session 值的浅拷贝。
//
// 设计原因：调用方可以安全增删返回 map 的顶层键，不会绕过 Store 的 dirty 标记直接修改内部状态。
func (s *Store) All() map[string]any { return cloneMap(s.payload.Values) }

// Only 返回指定 key 中实际存在的键值浅拷贝。
func (s *Store) Only(keys ...string) map[string]any {
	out := make(map[string]any)
	for _, key := range keys {
		if value, ok := s.payload.Values[key]; ok {
			out[key] = value
		}
	}
	return out
}

// Except 返回排除指定 key 后的所有键值浅拷贝。
func (s *Store) Except(keys ...string) map[string]any {
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[key] = struct{}{}
	}
	out := make(map[string]any, len(s.payload.Values))
	for key, value := range s.payload.Values {
		if _, skip := blocked[key]; !skip {
			out[key] = value
		}
	}
	return out
}

// Pull 读取指定 key 后立即删除它。
//
// 如果 key 也是 flash 数据，会同步移除 flash 元数据，避免保存阶段重新保留一个已经被业务删除的键。
func (s *Store) Pull(key string, def ...any) any {
	value := s.Get(key, def...)
	if s.Exists(key) {
		delete(s.payload.Values, key)
		s.removeFlashKey(key)
		s.markDirty()
	}
	return value
}

// Forget 删除一个或多个业务 session 键，并同步清理对应 flash 标记。
func (s *Store) Forget(keys ...string) {
	for _, key := range keys {
		delete(s.payload.Values, key)
		s.removeFlashKey(key)
	}
	s.markDirty()
}

// Flush 清空当前 session 的业务数据和 flash 生命周期元数据。
func (s *Store) Flush() {
	s.payload.Values = make(map[string]any)
	s.payload.OldFlash = nil
	s.payload.NewFlash = nil
	s.markDirty()
}

// Increment 按 amount 递增数值型 session 值，未传 amount 时默认加 1。
//
// 参数 key 是计数器键；amount 是可选增量。非整数值会返回 ErrInvalidConfig，避免隐式类型转换造成业务歧义。
func (s *Store) Increment(key string, amount ...int64) (int64, error) {
	delta := int64(1)
	if len(amount) > 0 {
		delta = amount[0]
	}
	current, err := numericValue(s.Get(key, int64(0)))
	if err != nil {
		return 0, err
	}
	next := current + delta
	s.Put(key, next)
	return next, nil
}

// Decrement 按 amount 递减数值型 session 值，未传 amount 时默认减 1。
func (s *Store) Decrement(key string, amount ...int64) (int64, error) {
	delta := int64(1)
	if len(amount) > 0 {
		delta = amount[0]
	}
	return s.Increment(key, -delta)
}

// Regenerate 生成新的 session ID，同时保留当前业务数据。
//
// 参数 ctx 控制旧 ID 销毁操作的取消和超时。旧 ID 会立即交给 driver 销毁，防止会话固定攻击继续复用旧标识。
func (s *Store) Regenerate(ctx context.Context) error {
	oldID := s.id
	s.id = newSessionID()
	s.payload.ID = s.id
	s.markDirty()
	if s.manager == nil || s.manager.driver == nil {
		return nil
	}
	if s.ownsRequestLock(oldID) {
		err := s.manager.driver.Destroy(ctx, oldID)
		releaseErr := s.releaseRequestLock(ctx)
		if err != nil {
			return err
		}
		return releaseErr
	}
	return s.manager.withLock(ctx, oldID, func() error {
		return s.manager.driver.Destroy(ctx, oldID)
	})
}

// Invalidate 清空数据、销毁旧 ID，并创建新的空 session。
func (s *Store) Invalidate(ctx context.Context) error {
	s.Flush()
	return s.Regenerate(ctx)
}

// Save 通过所属 Manager 持久化当前 session，并在有响应对象时写出 ID cookie。
func (s *Store) Save(ctx context.Context) error {
	if s == nil || s.manager == nil {
		return ErrInvalidConfig
	}
	return s.manager.Save(ctx, s)
}

func (s *Store) markDirty() { s.dirty = true }

func (s *Store) attachRequestLock(id string, lock Lock) {
	if s == nil || lock == nil {
		return
	}
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.requestLockID = id
	s.requestLock = lock
}

func (s *Store) ownsRequestLock(id string) bool {
	if s == nil {
		return false
	}
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	return s.requestLock != nil && s.requestLockID == id
}

func (s *Store) releaseRequestLock(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.lockMu.Lock()
	lock := s.requestLock
	s.requestLock = nil
	s.requestLockID = ""
	s.lockMu.Unlock()
	if lock == nil {
		return nil
	}
	return lock.Release(ctx)
}

// ReleaseRequestLock releases the lock held by this request's session store.
func (s *Store) ReleaseRequestLock(ctx context.Context) error {
	return s.releaseRequestLock(ctx)
}

// advanceFlash 推进 flash 生命周期。
//
// 复杂逻辑说明：OldFlash 表示本次请求结束后应删除的 key，NewFlash 表示要留到下一次请求的 key。
// Save 前先删除未被 keep/reflash 的旧 flash，再把 NewFlash 提升为下一轮 OldFlash，从而实现“当前可读、下一次可读、再下一次删除”。
func (s *Store) advanceFlash() {
	for _, key := range s.payload.OldFlash {
		if !containsString(s.payload.NewFlash, key) {
			delete(s.payload.Values, key)
		}
	}
	s.payload.OldFlash = uniqueStrings(s.payload.NewFlash)
	s.payload.NewFlash = nil
	s.payload.LastActivity = s.manager.now()
}

func (s *Store) removeFlashKey(key string) {
	s.payload.OldFlash = removeString(s.payload.OldFlash, key)
	s.payload.NewFlash = removeString(s.payload.NewFlash, key)
}

// cloneMap 返回 map 顶层浅拷贝，防止公开读取 API 暴露内部 map。
func cloneMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

// numericValue 将常见整数类型归一化为 int64。
//
// 设计原因：session payload 可能来自 JSON、测试 driver 或业务代码，Increment/Decrement 需要显式拒绝非整数值。
func numericValue(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return unsignedToInt64(uint64(v))
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return unsignedToInt64(v)
	case float64:
		if math.Trunc(v) == v {
			return int64(v), nil
		}
	}
	return 0, ErrInvalidConfig
}

func unsignedToInt64(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, ErrInvalidConfig
	}
	return int64(value), nil
}

// validSessionID 校验客户端提交的 session ID 是否只包含安全 URL 字符。
func validSessionID(id string) bool {
	return sessionIDPattern.MatchString(id)
}

// newSessionID 生成 URL 安全的不透明随机 ID。
//
// 正常路径使用 crypto/rand；极端失败时退化为时间编码值，仍保持格式合法，让调用方后续保存流程可以显式返回 driver 错误。
func newSessionID() string {
	var data [32]byte
	if _, err := rand.Read(data[:]); err != nil {
		return base64.RawURLEncoding.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))
	}
	return base64.RawURLEncoding.EncodeToString(data[:])
}
