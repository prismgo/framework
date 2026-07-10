package foundation

// 容器 key 常量定义
//
// 设计说明：集中管理容器 key，避免 magic strings 分散在代码中导致拼写错误和维护困难。
// 所有容器 key 应在此处定义，确保一致性和可追溯性。
const (
	// ContainerKeyEventDispatcher 事件分发器在容器中的注册 key
	ContainerKeyEventDispatcher = "event.dispatcher"

	// ContainerKeyExceptionHandler 异常处理器在容器中的注册 key
	ContainerKeyExceptionHandler = "exception.handler"

	// ContainerKeyConfigDefault 默认配置实例在容器中的注册 key
	ContainerKeyConfigDefault = "config.default"
)
