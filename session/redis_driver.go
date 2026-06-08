package session

import (
	"context"
	"errors"
	"strings"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	encodingpkg "github.com/prismgo/framework/encoding"
	redisfacade "github.com/prismgo/framework/redis"
	"github.com/redis/go-redis/v9"
)

const redisDriverName = "redis"

var redisReleaseLockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

// RedisDriver 使用 Redis 持久化服务端 session payload。
//
// 需求背景：多实例部署时 file driver 会产生本地文件亲和，Redis driver 让不同实例可以共享
// 同一份服务端 session 状态，同时浏览器 cookie 仍然只携带不透明 session ID。
// 设计思路：driver 直接使用 go-redis 控制 key、TTL、session Payload Encoding 和锁释放语义，
// 不经过 prismgo/cache，避免 session 存储被 cache store、cache prefix 或 cache flush 影响。
type RedisDriver struct {
	client    *redis.Client
	prefix    string
	encrypt   bool
	encryptor Encryptor
	codec     encodingcontract.Codec
}

// NewRedisDriver 根据 session 配置创建 Redis-backed session driver。
//
// 参数说明：cfg.Redis.Connection 是 prismgo/redis 中的连接名称；Prefix、Encoding、Encrypt
// 控制 session 自身的 key schema 和 payload。
func NewRedisDriver(cfg Config) (*RedisDriver, error) {
	cfg = normalizeConfig(cfg)
	client, err := redisDriverClient(cfg.Redis)
	if err != nil {
		return nil, err
	}
	return newRedisDriver(client, cfg)
}

// NewRedisDriverFromClient 使用外部传入的 Redis client 创建 driver。
//
// 参数 client 由测试或高级集成方持有生命周期；参数 cfg 仅使用 session 级配置，例如 Prefix、
// Encoding、Encrypt 和 Encryptor，不再读取 host、port、database 等连接字段。
func NewRedisDriverFromClient(client *redis.Client, cfg Config) (*RedisDriver, error) {
	return newRedisDriver(client, cfg)
}

func newRedisDriver(client *redis.Client, cfg Config) (*RedisDriver, error) {
	if client == nil {
		return nil, ErrInvalidConfig
	}
	cfg = normalizeConfig(cfg)
	codec, err := encodingpkg.ResolveWithDefault(encodingpkg.NameMsgpack, cfg.Encoding)
	if err != nil {
		return nil, err
	}
	return &RedisDriver{
		client:    client,
		prefix:    cfg.Redis.Prefix,
		encrypt:   cfg.Encrypt,
		encryptor: cfg.Encryptor,
		codec:     codec,
	}, nil
}

func redisDriverClient(cfg RedisConfig) (*redis.Client, error) {
	connection := strings.TrimSpace(cfg.Connection)
	client, err := redisfacade.Client(connection)
	if err == nil {
		typed, ok := client.(*redis.Client)
		if !ok {
			return nil, ErrInvalidConfig
		}
		return typed, nil
	}
	return nil, err
}

// Read 读取并解码一个 Redis session payload。
//
// 参数 ctx 贯穿 Redis 请求取消；id 必须是合法 session ID。Redis key 缺失映射为
// ErrSessionNotFound；payload 损坏、解密失败、ID 不匹配或逻辑过期会返回可恢复错误，交给
// Manager 统一降级为新 session。
func (d *RedisDriver) Read(ctx context.Context, id string) (Payload, error) {
	if !validSessionID(id) {
		return Payload{}, ErrInvalidSessionID
	}
	raw, err := d.client.Get(ctx, d.payloadKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Payload{}, ErrSessionNotFound
	}
	if err != nil {
		return Payload{}, err
	}
	payload, err := d.decode(ctx, raw)
	if err != nil {
		return Payload{}, err
	}
	if payload.ID != id {
		return Payload{}, ErrPayloadMalformed
	}
	if payload.ExpiresAt != nil && !payload.ExpiresAt.After(time.Now()) {
		_ = d.Destroy(ctx, id)
		return Payload{}, ErrSessionExpired
	}
	return payload, nil
}

// Write 写入 Redis session payload 并刷新 Redis TTL。
//
// 参数 expiresAt 来自 Manager.Save 的生命周期计算；有值时转换为 Redis TTL，没有值时写入
// 无自动过期 key。写入内容使用 session Payload Encoding，开启 Encrypt 时先编码再加密完整字节。
func (d *RedisDriver) Write(ctx context.Context, id string, payload Payload, expiresAt *time.Time) error {
	if !validSessionID(id) || payload.ID != id {
		return ErrInvalidSessionID
	}
	payload.ExpiresAt = expiresAt
	data, err := d.encode(ctx, payload)
	if err != nil {
		return err
	}
	return d.client.Set(ctx, d.payloadKey(id), data, redisTTL(expiresAt)).Err()
}

// Destroy 删除指定 session ID 的 Redis payload key。
//
// 参数 id 必须合法；Redis DEL 对不存在 key 返回成功，因此恢复路径可以重复调用而不需要额外判断。
func (d *RedisDriver) Destroy(ctx context.Context, id string) error {
	if !validSessionID(id) {
		return ErrInvalidSessionID
	}
	return d.client.Del(ctx, d.payloadKey(id)).Err()
}

// GC 在 Redis driver 中为空操作。
//
// 设计原因：Redis TTL 自带过期清理能力，主动 SCAN/KEYS 删除 session key 会增加生产风险，
// 因此该方法只满足 Driver 接口并返回 nil。
func (d *RedisDriver) GC(context.Context, time.Time) error {
	return nil
}

// Lock 获取同 session ID 的 Redis 独占锁。
//
// 参数 ttl 控制锁最大持有时间；wait 控制最多等待多久。实现使用 SET NX PX 获取锁，
// 并复用包内 10ms 轮询间隔；等待超时返回 ErrLockTimeout，不能绕过锁继续读写。
func (d *RedisDriver) Lock(ctx context.Context, id string, ttl time.Duration, wait time.Duration) (Lock, error) {
	if !validSessionID(id) {
		return nil, ErrInvalidSessionID
	}
	if ttl <= 0 {
		ttl = time.Duration(DefaultLockSeconds) * time.Second
	}
	if wait <= 0 {
		wait = time.Duration(DefaultLockWaitSeconds) * time.Second
	}
	deadline := time.Now().Add(wait)
	key := d.lockKey(id)
	token := newSessionID()

	for {
		ok, err := d.client.SetNX(ctx, key, token, ttl).Result()
		if err != nil {
			return nil, err
		}
		if ok {
			return &redisLock{client: d.client, key: key, token: token, held: true}, nil
		}
		if time.Now().After(deadline) {
			return nil, ErrLockTimeout
		}
		if err := sleepLockPoll(ctx, deadline); err != nil {
			return nil, err
		}
	}
}

// Close intentionally leaves Redis clients open.
//
// Redis client lifecycle is owned by prismgo/redis.Manager.Close(ctx) for
// shared connections, or by the caller for legacy/direct clients.
func (d *RedisDriver) Close() error {
	return nil
}

func (d *RedisDriver) encode(ctx context.Context, payload Payload) ([]byte, error) {
	raw, err := d.codec.Marshal(payload)
	if err != nil {
		return nil, safeError("serialize payload", ErrPayloadSerialize)
	}
	if !d.encrypt {
		return raw, nil
	}
	return encryptPayload(ctx, d.encryptor, raw)
}

func (d *RedisDriver) decode(ctx context.Context, raw []byte) (Payload, error) {
	if d.encrypt {
		decrypted, err := decryptPayload(ctx, d.encryptor, raw)
		if err != nil {
			return Payload{}, err
		}
		raw = decrypted
	}
	var payload Payload
	if err := d.codec.Unmarshal(raw, &payload); err != nil {
		return Payload{}, safeError("deserialize payload", ErrPayloadDeserialize)
	}
	if payload.Values == nil {
		payload.Values = make(map[string]any)
	}
	return payload, nil
}

func (d *RedisDriver) payloadKey(id string) string {
	return d.prefix + ":sessions:" + id
}

func (d *RedisDriver) lockKey(id string) string {
	return d.prefix + ":locks:" + id
}

func redisTTL(expiresAt *time.Time) time.Duration {
	if expiresAt == nil {
		return 0
	}
	ttl := time.Until(*expiresAt)
	if ttl <= 0 {
		return time.Nanosecond
	}
	return ttl
}

func normalizeRedisPrefix(value string) string {
	value = strings.Trim(strings.TrimSpace(value), " :")
	if value == "" {
		return DefaultRedisPrefix
	}
	return value
}

type redisLock struct {
	client *redis.Client
	key    string
	token  string
	held   bool
}

// Release 只在 Redis 中保存的 token 与当前锁 token 一致时删除锁。
//
// 设计原因：锁 TTL 过期后可能被另一个请求重新获取，释放时必须校验 token，避免旧持有者误删
// 新持有者的锁。
func (l *redisLock) Release(ctx context.Context) error {
	if l == nil || !l.held {
		return ErrLockNotHeld
	}
	result, err := redisReleaseLockScript.Run(ctx, l.client, []string{l.key}, l.token).Int()
	l.held = false
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrLockNotHeld
	}
	return nil
}

func init() {
	Extend(redisDriverName, func(cfg Config) (Driver, error) {
		return NewRedisDriver(cfg)
	})
}
