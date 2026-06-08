// Package artisan 提供 Application 作用域的 Console Kernel 包级 facade。
//
// 需求背景：HTTP handler、service、provider、job 和测试代码通常拿不到 console.CommandContext，
// 但仍需要复用当前应用已经装配好的 Console Kernel 来执行命令或声明 starting callback。
// 设计思路：本包只负责从当前 Application 容器解析 Kernel，并委托给 Kernel 的公开方法；
// 命令解析、silent 输出、Definition/Cobra 校验和 starting callback 执行仍全部归属于 kernel 包。
package artisan

import (
	"context"
	"errors"
	"fmt"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/kernel"
)

var (
	// ErrKernelNotBound 表示当前 Application 容器中尚未绑定 Console Kernel。
	//
	// 用途：调用方可通过 errors.Is(err, artisan.ErrKernelNotBound) 区分“已存在当前
	// Application 容器但尚未创建/绑定 Kernel”的启动顺序错误。
	ErrKernelNotBound = errors.New("artisan kernel is not bound")
)

// Resolve 从当前 Application 容器解析 Console Kernel。
//
// 错误语义：没有当前容器时保留 container.ErrNoCurrentContainer，便于定位 Application
// 尚未创建或已经关闭；有容器但没有 Kernel 时返回 ErrKernelNotBound。
func Resolve() (*kernel.Kernel, error) {
	k, err := container.Make[*kernel.Kernel](kernel.ArtisanFacadeKey)
	if err == nil {
		return k, nil
	}
	if errors.Is(err, container.ErrFactoryNotRegistered) || errors.Is(err, container.ErrFactoryReturnedNil) {
		return nil, fmt.Errorf("artisan resolve kernel: %w", ErrKernelNotBound)
	}
	return nil, fmt.Errorf("artisan resolve kernel: %w", err)
}

// Call 通过当前 Application 绑定的 Console Kernel 执行命令。
//
// 参数说明：ctx 是命令执行上下文；无 input 时 signature 使用字符串签名格式，
// 有 input 时 signature 应为命令名。
// 逻辑说明：本函数不解析命令、不创建 Kernel，只解析当前绑定并委托 Kernel.Call。
func Call(ctx context.Context, signature string, input ...console.CallInput) error {
	k, err := Resolve()
	if err != nil {
		return err
	}
	return k.Call(ctx, signature, input...)
}

// CallSilently 通过当前 Application 绑定的 Console Kernel 静默执行命令。
//
// 逻辑说明：silent 输出语义继续由 Kernel.CallSilently 负责，facade 层只处理当前
// Application registry 到 Kernel 实例的解析。
func CallSilently(ctx context.Context, signature string, input ...console.CallInput) error {
	k, err := Resolve()
	if err != nil {
		return err
	}
	return k.CallSilently(ctx, signature, input...)
}

// All 通过当前 Application 绑定的 Console Kernel 返回所有已注册命令定义。
//
// 逻辑说明：本函数只解析当前 Kernel 并委托 Kernel.All；starting callbacks、延迟命令注册、
// hidden command 是否包含、Definition 快照复制等语义都由 Kernel 统一负责。
func All() ([]console.Definition, error) {
	k, err := Resolve()
	if err != nil {
		return nil, err
	}
	return k.All()
}

// Starting 将 Console application starting callbacks 注册到当前 Application 绑定的 Kernel。
//
// 参数说明：callbacks 在 Kernel 首次执行 RunContext/Call/CallSilently 前运行，调用方应在回调中
// 通过 Kernel.ResolveCommand 或 ResolveCommands 注册延迟命令，不能绕过 Kernel 的 Definition 校验。
// 设计说明：callback 状态归属当前 Kernel 实例，不写入 foundation.Application runtime，也不写入进程级默认 Kernel。
func Starting(callbacks ...kernel.StartingCallback) error {
	k, err := Resolve()
	if err != nil {
		return err
	}
	return k.Starting(callbacks...)
}
