package horizon

import "github.com/prismgo/framework/facade"

const managerFacadeKey = "horizon.manager"

// Resolve 从当前 Application 容器解析 Horizon manager。
func Resolve() *Manager {
	return facade.Resolve[*Manager](managerFacadeKey)
}
