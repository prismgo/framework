package config

import "github.com/prismgo/framework/facade"

const serviceKey = "config.default"

// Resolve 从当前 Application 容器解析配置 Repository。
func Resolve() *Config {
	return facade.Resolve[*Config](serviceKey)
}

// Clone 复制当前配置访问器，返回持有独立配置仓库的新实例。
func Clone() *Config {
	return Resolve().Clone()
}

// Empty 判断当前配置访问器是否尚未持有任何配置项。
func Empty() bool {
	return Resolve().Empty()
}

// Reload 从项目根目录 .env 重新加载当前配置对象。
func Reload() error {
	return Resolve().Reload()
}

// Get 通过默认配置读取字符串配置，缺失时回退到默认值。
func Get(path string, defaultValue ...any) string {
	return Resolve().Get(path, defaultValue...)
}

// GetString 通过默认配置读取字符串配置。
func GetString(path string, defaultValue ...any) string {
	return Resolve().GetString(path, defaultValue...)
}

// GetInt 通过默认配置读取整型配置。
func GetInt(path string, defaultValue ...any) int {
	return Resolve().GetInt(path, defaultValue...)
}

// GetFloat64 通过默认配置读取 float64 配置。
func GetFloat64(path string, defaultValue ...any) float64 {
	return Resolve().GetFloat64(path, defaultValue...)
}

// GetInt64 通过默认配置读取 int64 配置。
func GetInt64(path string, defaultValue ...any) int64 {
	return Resolve().GetInt64(path, defaultValue...)
}

// GetUint 通过默认配置读取 uint 配置。
func GetUint(path string, defaultValue ...any) uint {
	return Resolve().GetUint(path, defaultValue...)
}

// GetBool 通过默认配置读取布尔配置。
func GetBool(path string, defaultValue ...any) bool {
	return Resolve().GetBool(path, defaultValue...)
}

// GetStringMapString 通过默认配置读取 map[string]string 配置。
func GetStringMapString(path string) map[string]string {
	return Resolve().GetStringMapString(path)
}

// GetStringMap 通过默认配置读取 map[string]any 配置。
func GetStringMap(path string) map[string]any {
	return Resolve().GetStringMap(path)
}
