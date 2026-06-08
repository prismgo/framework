package filesystem

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Driver 是所有底层存储驱动必须实现的最小能力集合。
//
// 约束：
// 1. Repository 只依赖这一层，不感知具体 SDK；
// 2. local 与 oss 都需要把自身行为适配到统一语义；
// 3. 用户自定义 driver 也必须实现完整接口，才能复用统一 Repository 能力；
// 4. URL 与 TemporaryURL 由驱动自己决定是否支持。
type Driver interface {
	Close() error
	Write(ctx context.Context, key string, reader io.Reader, opts PutOptions) error
	ReadAll(ctx context.Context, key string) ([]byte, error)
	Open(ctx context.Context, key string) (io.ReadCloser, FileInfo, error)
	Exists(ctx context.Context, key string) (bool, error)
	Delete(ctx context.Context, key string) error
	Copy(ctx context.Context, src, dst string) error
	Move(ctx context.Context, src, dst string) error
	Stat(ctx context.Context, key string) (FileInfo, error)
	List(ctx context.Context, prefix string, recursive bool) ([]FileInfo, error)
	MakeDirectory(ctx context.Context, dir string) error
	DeleteDirectory(ctx context.Context, dir string) error
	Path(key string) string
	URL(key string) (string, error)
	TemporaryURL(ctx context.Context, key string, expiry time.Time) (string, error)
	SetVisibility(ctx context.Context, key, visibility string) error
	GetVisibility(ctx context.Context, key string) (string, error)
}

type driver = Driver

type directoryExistser interface {
	DirectoryExists(ctx context.Context, dir string) (bool, error)
}

type temporaryURLProvider interface {
	ProvidesTemporaryURLs() bool
}

type temporaryUploadURLDriver interface {
	ProvidesTemporaryUploadURLs() bool
	TemporaryUploadURL(ctx context.Context, key string, expiry time.Time, opts ...TemporaryUploadURLOptions) (TemporaryUploadURLResult, error)
}

// DriverFactoryContext 是自定义 filesystem driver 的构造上下文。
//
// 它把磁盘配置名、标准化后的 driver 名称和磁盘配置副本传给扩展工厂，
// 业务侧可以像 Laravel Storage::extend 一样按配置创建自定义存储后端。
type DriverFactoryContext struct {
	// Name 是当前 disk 的配置名称，例如 public、local 或业务自定义名称。
	Name string
	// Driver 是标准化后的 driver 名称。
	Driver string
	// Config 是当前 disk 的完整配置副本，包含 Options 原始参数。
	Config DiskConfig
}

// DriverFactory 是用户自定义 filesystem driver 的工厂函数。
type DriverFactory func(DriverFactoryContext) (Driver, error)

var (
	driverFactoryMu sync.RWMutex
	driverFactories = map[string]DriverFactory{}
)

// Extend 注册一个自定义 filesystem driver 工厂。
//
// 注册后可在 filesystem.disks.*.driver 中使用该 driver 名称。空名称或 nil 工厂会被忽略，
// 同名注册会覆盖先前工厂，保持和 Laravel Storage::extend 一致的后注册生效语义。
func Extend(name string, factory DriverFactory) {
	registerDriverFactory(name, factory)
}

func registerDriverFactory(name string, factory DriverFactory) {
	name = normalizeDriverName(name)
	if name == "" || factory == nil {
		return
	}
	driverFactoryMu.Lock()
	driverFactories[name] = factory
	driverFactoryMu.Unlock()
}

func lookupDriverFactory(name string) (DriverFactory, bool) {
	name = normalizeDriverName(name)
	driverFactoryMu.RLock()
	factory, ok := driverFactories[name]
	driverFactoryMu.RUnlock()
	return factory, ok
}

func normalizeDriverName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func normalizeCustomDriver(name string, drv Driver) (Driver, error) {
	if drv == nil {
		return nil, fmt.Errorf("filesystem: custom driver %q is nil", name)
	}
	return drv, nil
}
