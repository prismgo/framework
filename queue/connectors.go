package queue

import (
	"fmt"
	"sync"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

var (
	connectorRegistryMu sync.RWMutex
	connectorRegistry   = map[string]queuecontract.Connector{}
)

// registerConnector 写入包级自定义 connector registry。
//
// 需求背景：自定义 queue driver 需要和 cache/filesystem/logger/session 一样支持业务包在
// init() 阶段注册，此时 Application 容器和 Manager 可能尚未创建，因此注册表必须独立于
// Manager 实例存在。空名称和 nil connector 不代表有效 driver，直接忽略；同名注册采用
// 后者覆盖前者，便于测试和业务模块按加载顺序替换实现。
func registerConnector(name string, connector queuecontract.Connector) {
	name = normalizeDriverName(name)
	if name == "" || connector == nil {
		return
	}
	connectorRegistryMu.Lock()
	connectorRegistry[name] = connector
	connectorRegistryMu.Unlock()
}

// lookupConnector 从包级 registry 读取自定义 connector。
//
// 参数 name 是连接配置中的 driver 名称，读取时再次规范化，保证配置大小写和注册大小写
// 不影响匹配结果。返回 bool 用于区分未注册和注册了 nil 的无效场景。
func lookupConnector(name string) (queuecontract.Connector, bool) {
	connectorRegistryMu.RLock()
	connector, ok := connectorRegistry[normalizeDriverName(name)]
	connectorRegistryMu.RUnlock()
	return connector, ok
}

func connectorConfig(spec ConnectionConfig) map[string]any {
	return map[string]any{"_spec": cloneConnectionConfig(spec)}
}

func connectorSpec(name string, config map[string]any) (ConnectionConfig, error) {
	spec, ok := config["_spec"].(ConnectionConfig)
	if !ok {
		return ConnectionConfig{}, fmt.Errorf("queue: connector %q missing connection config", name)
	}
	return spec, nil
}
