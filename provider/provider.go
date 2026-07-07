// Package provider 声明 Prismgo 框架默认 service providers。
//
// 需求背景：这些 provider 属于框架默认能力，不属于业务 bootstrap/provider.go；
// foundation.Builder 会把它们自动放到业务 providers 之前进入同一个生命周期。
package provider

import (
	"fmt"
	"reflect"

	cachepkg "github.com/prismgo/framework/cache"
	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	contractprovider "github.com/prismgo/framework/contracts/provider"
	cookiepkg "github.com/prismgo/framework/cookie"
	databasepkg "github.com/prismgo/framework/database"
	schemapkg "github.com/prismgo/framework/database/schema"
	filesystempkg "github.com/prismgo/framework/filesystem"
	"github.com/prismgo/framework/kernel"
	queuepkg "github.com/prismgo/framework/queue"
	ratelimitpkg "github.com/prismgo/framework/ratelimit"
	redispkg "github.com/prismgo/framework/redis"
	routepkg "github.com/prismgo/framework/route"
	sessionpkg "github.com/prismgo/framework/session"
)

// ServiceProvider 兼容导出 provider contract 的服务提供者类型。
type ServiceProvider = contractprovider.ServiceProvider

// NamedProvider 兼容导出 provider contract 的稳定 identity 接口。
type NamedProvider = contractprovider.NamedProvider

// DeferrableProvider 兼容导出 provider contract 的延迟加载接口。
type DeferrableProvider = contractprovider.DeferrableProvider

// TerminableProvider 兼容导出 provider contract 的关闭钩子接口。
type TerminableProvider = contractprovider.TerminableProvider

// DefaultProviders 返回框架 default provider 清单。
//
// 设计说明：顺序对应基础设施依赖关系，redis -> cache -> queue -> cookie -> session ->
// filesystem -> database -> schema -> ratelimit。返回值每次都是新切片，调用方可以安全追加。
func DefaultProviders() []ServiceProvider {
	return []ServiceProvider{
		redispkg.ServiceProvider{},
		cachepkg.ServiceProvider{},
		queuepkg.ServiceProvider{},
		cookiepkg.ServiceProvider{},
		sessionpkg.ServiceProvider{},
		filesystempkg.ServiceProvider{},
		databasepkg.ServiceProvider{},
		schemapkg.ServiceProvider{},
		ratelimitpkg.ServiceProvider{},
		routepkg.ServiceProvider{},
	}
}

// Commands 声明当前 provider 需要挂载的 console commands。
//
// 需求背景：issue 10 要求 Prismgo provider 对齐 Laravel 13 的 commands([...]) 语义，
// provider 只声明命令，真正挂载发生在 Console Kernel starting 阶段，HTTP-only Boot 不创建
// Kernel 也不会失败。参数只接受显式 console.Command 或 console.CommandFactory，不做文件扫描、
// 字符串类名 discovery 或 Horizon 私有 helper。
func Commands(values ...any) error {
	declarations, err := normalizeCommandDeclarations(values)
	if err != nil {
		return err
	}
	registrar, err := container.Make[kernel.StartingRegistrar](kernel.StartingRegistrarKey)
	if err != nil {
		return fmt.Errorf("provider commands: console starting registrar is not configured: %w", err)
	}
	return registrar(func(k *kernel.Kernel) error {
		return k.ResolveCommands(declarations...)
	})
}

// normalizeCommandDeclarations 校验 provider 命令声明的公开输入类型。
//
// 逻辑说明：声明阶段不调用 factory，避免 provider Boot 提前构造命令依赖；factory 返回 nil、
// Definition 错误、重复命令和 alias 冲突统一交给 Kernel.ResolveCommands 在 starting 阶段处理。
func normalizeCommandDeclarations(values []any) ([]any, error) {
	out := make([]any, 0, len(values))
	for _, value := range values {
		switch command := value.(type) {
		case nil:
			return nil, fmt.Errorf("provider commands: command is nil")
		case console.Command:
			if isNilCommand(command) {
				return nil, fmt.Errorf("provider commands: command is nil")
			}
			out = append(out, command)
		case console.CommandFactory:
			if command == nil {
				return nil, fmt.Errorf("provider commands: factory is nil")
			}
			out = append(out, command)
		default:
			return nil, fmt.Errorf("provider commands: unsupported type %T", value)
		}
	}
	return out, nil
}

func isNilCommand(command console.Command) bool {
	if command == nil {
		return true
	}
	value := reflect.ValueOf(command)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
