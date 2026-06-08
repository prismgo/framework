// Package session 定义会话系统的公共契约。
//
// 本包声明会话持久化驱动和锁接口；会话 payload 加密统一使用
// contracts/encryption.Encrypter。具体的 file、redis 驱动、Manager 和 Store 实现由
// prismgo/session 实现包提供。
package session

import (
	"context"
	"time"
)

// Driver 是会话持久化驱动的完整契约。
//
// 用途：自定义会话存储（如数据库、Redis、文件）必须实现此接口。
// Manager 通过此接口与具体存储解耦。
//
// 使用方式：
//
//	session.Extend("database", func(cfg session.Config) (session.Driver, error) {
//	    return NewDatabaseDriver(cfg)
//	})
type Driver interface {
	// Read 根据 session ID 读取持久化的会话数据。
	//
	// 参数 id 是客户端 Cookie 中的 session ID。
	// 记录不存在时返回 ErrSessionNotFound。
	Read(ctx context.Context, id string) (Payload, error)

	// Write 将会话数据持久化。
	//
	// 参数 id 是 session ID。
	// 参数 payload 是待写入的会话数据。
	// 参数 expiresAt 是服务端过期时间，为 nil 时不过期。
	Write(ctx context.Context, id string, payload Payload, expiresAt *time.Time) error

	// Destroy 删除指定 session 的持久化记录。
	//
	// 通常用于显式注销或 session 重新生成时清理旧 ID。
	Destroy(ctx context.Context, id string) error

	// GC 清理 before 时间之前过期的所有会话记录。
	GC(ctx context.Context, before time.Time) error
}

// Locker 是会话锁能力的可选契约。
//
// 用途：Driver 可选实现此接口以提供同 session ID 的串行化访问保护。
// 未实现时 Manager 跳过多请求并发保护。
//
// 使用方式：在 Driver 实现中添加 Lock 方法。
//
//	func (d *RedisDriver) Lock(ctx context.Context, id string, ttl, wait time.Duration) (session.Lock, error) {
//	    return redisLock(ctx, d.client, id, ttl, wait)
//	}
type Locker interface {
	// Lock 获取指定 session ID 的独占锁。
	//
	// 参数 ttl 是锁的最大持有时间。
	// 参数 wait 是获取锁的最大等待时间。
	Lock(ctx context.Context, id string, ttl time.Duration, wait time.Duration) (Lock, error)
}

// Lock 是已获取的会话锁契约。
//
// 用途：表示一次成功获取的会话独占锁，使用完毕后通过 Release 释放。
type Lock interface {
	// Release 释放当前持有的锁。
	Release(ctx context.Context) error
}

// Payload 是会话持久化的数据结构。
//
// 注意：Payload 是数据传输对象而非接口；因 Driver.Read 和 Driver.Write 均以此类型
// 作为参数和返回值，作为公共 API 边界的一部分随 Driver 接口一同声明在此。
type Payload struct {
	ID           string
	Values       map[string]any
	OldFlash     []string
	NewFlash     []string
	CreatedAt    time.Time
	LastActivity time.Time
	ExpiresAt    *time.Time
}
