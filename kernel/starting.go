package kernel

import (
	"context"
	"fmt"

	"github.com/prismgo/framework/event"
)

// runStartingCallbacks 执行当前 Kernel 的 Console starting 回调。
//
// 用途：保证 starting 回调发生在 Application boot 成功之后、CLI 命令解析/执行之前。
// 设计思路：每个 Kernel 实例只执行一次，后续 list/help、programmatic call 或调度器复用同一
// Kernel 时不会重复挂载命令，避免重复名称和 alias 冲突。
// 错误语义：任一回调失败时立即返回，目标 CLI 命令不会执行；Application 生命周期入口仍会继续
// 关闭资源，并由 Application.RunContext 合并 close 错误。
func (k *Kernel) runStartingCallbacks() error {
	if k == nil {
		return fmt.Errorf("kernel starting: kernel is nil")
	}

	for {
		callbacks, wait, done := k.prepareStartingCallbacks()
		if done {
			return nil
		}
		if wait != nil {
			<-wait
			continue
		}
		if err := k.dispatchStartingEvent(); err != nil {
			k.finishStartingCallbacks(err)
			return err
		}
		if len(callbacks) == 0 {
			k.finishStartingCallbacks(nil)
			return nil
		}
		for _, callback := range callbacks {
			if callback == nil {
				continue
			}
			if err := callback(k); err != nil {
				k.finishStartingCallbacks(err)
				return err
			}
		}
		k.finishStartingCallbacks(nil)
		return nil
	}
}

// prepareStartingCallbacks 读取当前 starting 执行窗口，并决定本次调用是执行、等待还是直接返回。
//
// 设计思路：只有拿到“执行权”的调用方才会真正运行 callbacks；并发进入时，其他调用方只等待
// 当前批次结束，避免重复注册命令或在失败后留下部分状态。
func (k *Kernel) prepareStartingCallbacks() ([]StartingCallback, <-chan struct{}, bool) {
	k.startingMu.Lock()
	defer k.startingMu.Unlock()
	if k.startingState == startingStateSucceeded {
		return nil, nil, true
	}
	if k.startingState == startingStateRunning {
		return nil, k.startingWait, false
	}
	callbacks := append([]StartingCallback(nil), k.starting...)
	if k.application != nil {
		callbacks = append(callbacks, k.application.StartingCallbacks()...)
	}
	k.startingState = startingStateRunning
	k.startingWait = make(chan struct{})
	return callbacks, nil, false
}

// finishStartingCallbacks 根据本轮执行结果提交 starting 状态，并唤醒等待中的调用方。
//
// 参数说明：err 为 nil 表示本轮 callbacks 已全部成功完成；非 nil 表示本轮失败，后续调用仍允许重试。
func (k *Kernel) finishStartingCallbacks(err error) {
	k.startingMu.Lock()
	defer k.startingMu.Unlock()
	if err == nil {
		k.startingState = startingStateSucceeded
	} else {
		k.startingState = startingStatePending
	}
	if k.startingWait != nil {
		close(k.startingWait)
		k.startingWait = nil
	}
}

// dispatchStartingEvent 在当前 Kernel 生命周期内只派发一次 console.application.starting 事件。
//
// 需求背景：即便没有任何 starting callback，该生命周期事件也要保持稳定可观测，供外部监听器感知
// Console Application 已进入命令解析前阶段。
func (k *Kernel) dispatchStartingEvent() error {
	k.startingMu.Lock()
	alreadyDispatched := k.startingEventDispatched
	if !alreadyDispatched {
		k.startingEventDispatched = true
	}
	k.startingMu.Unlock()
	if alreadyDispatched {
		return nil
	}
	event.Dispatch(context.Background(), event.ConsoleApplicationStarting{KernelName: k.rootCommandName()})
	return nil
}

// hasPendingStartingCallbacks 判断当前 Kernel 是否仍存在尚未成功完成的 starting 注册机会。
//
// 用途：供调度器 resolver 在注册阶段决定是否保留一次“延迟到执行期再解析命令”的机会，避免把
// 真正不存在的命令也一律延后报错。
func (k *Kernel) hasPendingStartingCallbacks() bool {
	if k == nil {
		return false
	}
	k.startingMu.Lock()
	defer k.startingMu.Unlock()
	return k.startingState != startingStateSucceeded && (len(k.starting) > 0 || (k.application != nil && len(k.application.StartingCallbacks()) > 0))
}

// Starting 注册当前 Kernel 实例的 Console application starting callbacks。
//
// 参数说明：callbacks 会在该 Kernel 首次 RunContext、Call 或 CallSilently 前按注册顺序执行。
// 逻辑说明：回调状态保存在 Kernel 实例上，避免连续创建多个 Application 时复用旧应用状态；
// started 后拒绝继续追加，防止命令列表在一次 Kernel 生命周期内出现不可预测变化。
func (k *Kernel) Starting(callbacks ...StartingCallback) error {
	if k == nil {
		return fmt.Errorf("kernel starting: kernel is nil")
	}
	k.startingMu.Lock()
	defer k.startingMu.Unlock()
	if k.startingState == startingStateSucceeded {
		return fmt.Errorf("kernel starting: callbacks already ran")
	}
	k.starting = append(k.starting, callbacks...)
	return nil
}
