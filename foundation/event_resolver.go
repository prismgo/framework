package foundation

import (
	"fmt"

	containercontract "github.com/prismgo/framework/contracts/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
)

// resolveEventDispatcher 从指定 Application 容器解析事件总线。
//
// 设计原因：v10 起不再通过 event.ResolveIn 临时切换 current container。生命周期事件属于
// Application 自身行为，必须显式使用该 Application 持有的容器解析依赖。
func resolveEventDispatcher(c containercontract.Container) (eventcontract.Dispatcher, error) {
	if c == nil {
		return nil, fmt.Errorf("foundation: container is nil")
	}
	raw, err := c.Make(ContainerKeyEventDispatcher)
	if err != nil {
		return nil, err
	}
	bus, ok := raw.(eventcontract.Dispatcher)
	if !ok || bus == nil {
		return nil, fmt.Errorf("foundation: %s resolved %T, want event.Dispatcher", ContainerKeyEventDispatcher, raw)
	}
	return bus, nil
}
