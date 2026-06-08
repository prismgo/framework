package facade

import "github.com/prismgo/framework/container"

// Resolve 从当前 Application 容器解析服务。
// 需求背景：facade 入口属于严格便捷入口，解析错误如果被吞掉会让调用方拿到零值继续执行。
// 因此容器未装配、key 无绑定、类型不匹配或 factory 返回错误时，统一 panic 暴露装配问题。
func Resolve[T any](key string) T {
	val, err := container.Make[T](key)
	if err != nil {
		panic(err)
	}
	return val
}
