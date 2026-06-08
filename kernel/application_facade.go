package kernel

import (
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

// ArtisanFacadeKey 是 Application 容器中保存 Console Kernel 的固定键。
//
// 设计说明：键定义在 kernel 包，避免 kernel.NewApplicationKernel 反向依赖 artisan 包；
// artisan 包使用同一个键解析 Kernel，从而保持“构造方绑定、facade 方读取”的单向依赖。
const ArtisanFacadeKey = "artisan.kernel"

// BindApplicationKernel 将 Kernel 绑定到指定 Application 容器。
//
// 参数说明：c 是当前 Application 的容器契约；k 是当前 Application 对应的 Console Kernel。
// 需求背景：NewApplicationKernel 必须在框架内部完成绑定，保证 provider/service 等非命令上下文
// 可以通过 artisan facade 调用当前应用 Kernel，而不需要 main.go 手工装配。
func BindApplicationKernel(c containercontract.Container, k *Kernel) error {
	if c == nil {
		return container.ErrNoCurrentContainer
	}
	return c.Instance(ArtisanFacadeKey, k)
}
