package session

import (
	"fmt"
	"sync"

	sessioncontract "github.com/prismgo/framework/contracts/session"
)

// Payload 是 session driver 之间共享的持久化数据结构。
type Payload = sessioncontract.Payload

// Driver 定义 session 持久化 driver 的最小能力。
type Driver = sessioncontract.Driver

// Locker 由支持同 session ID 串行化访问的 driver 实现。
type Locker = sessioncontract.Locker

// Lock 表示已经获取成功的独占 session 锁。
type Lock = sessioncontract.Lock

// DriverFactory 根据 Config 构建具体 session driver。
//
// 参数 Config 来自 session 配置解析结果，包含 driver、文件目录、锁和 cookie 等配置。
type DriverFactory func(Config) (Driver, error)

var driverRegistry = struct {
	sync.RWMutex
	factories map[string]DriverFactory
}{factories: make(map[string]DriverFactory)}

// Extend 按名称注册 session driver 工厂。
//
// 参数 name 是配置中的 driver 名称，例如 file；参数 factory 是构建 driver 的工厂函数。
// 空名称或 nil 工厂会被忽略；同名注册会覆盖先前工厂，保持和 Laravel manager
// extend 方法一致的后注册生效语义。
func Extend(name string, factory DriverFactory) {
	if name == "" || factory == nil {
		return
	}
	driverRegistry.Lock()
	defer driverRegistry.Unlock()
	driverRegistry.factories[name] = factory
}

// ResolveDriver 根据名称构建已注册的 session driver。
//
// 参数 name 为空时回退到 cfg.Driver；参数 cfg 是传给 DriverFactory 的完整配置。函数会
// 检查未知 driver 和 nil driver，保证 manager 后续拿到的一定是可调用实现。
func ResolveDriver(name string, cfg Config) (Driver, error) {
	if name == "" {
		name = cfg.Driver
	}
	driverRegistry.RLock()
	factory, ok := driverRegistry.factories[name]
	driverRegistry.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDriverNotFound, name)
	}
	driver, err := factory(cfg)
	if err != nil {
		return nil, err
	}
	if driver == nil {
		return nil, fmt.Errorf("%w: %s", ErrDriverNotFound, name)
	}
	return driver, nil
}
