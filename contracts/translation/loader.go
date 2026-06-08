// Package translation 定义翻译系统的公共契约。
package translation

// Loader 是翻译资源文件加载器的基础契约。
//
// 用途：自定义加载器（如数据库加载器、远程加载器）必须实现此接口。
// 加载器负责从各类来源读取翻译资源，但不负责 key 解析、fallback 或 placeholder 替换。
type Loader interface {
	Load(locale, group, namespace string) (map[string]any, error)

	AddNamespace(namespace, hint string)
	AddPath(path string)
	AddJSONPath(path string)

	Namespaces() map[string]string
	Paths() []string
	JSONPaths() []string
}
