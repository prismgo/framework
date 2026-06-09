// Package support 存放辅助方法。
package support

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/prismgo/framework/container"
	pathutil "github.com/prismgo/framework/internal/path"
)

type configStringReader interface {
	GetString(path string, defaultValue ...any) string
}

func Empty(val interface{}) bool {
	if val == nil {
		return true
	}
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.String, reflect.Array:
		return v.Len() == 0
	case reflect.Map, reflect.Slice:
		return v.Len() == 0 || v.IsNil()
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return reflect.DeepEqual(val, reflect.Zero(v.Type()).Interface())
}

// ParseInt 把字符串参数解析为整数，非法时回退到给定默认值。
func ParseInt(value string, fallback int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

// URL 生成基于应用 app.url 的完整 URL。
//
// 设计思路：
//   - 优先读取当前应用容器中的 config.default，并通过本地接口读取 app.url，避免 support
//     反向 import config 造成包循环；
//   - 当前应用未启动或配置服务不可用时，按 config/app.go 的默认值回退；
//   - path 已经是完整 URL 时原样返回，避免破坏 CDN、外部跳转等调用方显式传入的地址；
//   - parameters 对齐 Laravel 的 additional URL segments，作为路径段追加并编码。
//
// 参数说明：
//   - path：相对路径、以 / 开头的路径，或完整 URL；
//   - parameters：追加到 URL 后部的路径段；支持普通值、slice/array，以及 map 的值。
func URL(path string, parameters ...any) string {
	if isAbsoluteURL(path) {
		return path
	}
	base := strings.TrimRight(applicationURL(), "/")
	relative := strings.TrimLeft(path, "/")
	segments := urlParameterSegments(parameters)
	if len(segments) > 0 {
		relative = joinURLPath(relative, segments)
	}
	if relative == "" {
		return base
	}
	return base + "/" + relative
}

// BasePath 返回相对当前应用根目录的路径。
func BasePath(path ...string) string {
	return resolvePath("path.base", path...)
}

// AppPath 返回相对当前应用 app 目录的路径。
func AppPath(path ...string) string {
	return resolvePath("path.app", path...)
}

// ConfigPath 返回相对当前应用 config 目录的路径。
func ConfigPath(path ...string) string {
	return resolvePath("path.config", path...)
}

// DatabasePath 返回相对当前应用 database 目录的路径。
func DatabasePath(path ...string) string {
	return resolvePath("path.database", path...)
}

// PublicPath 返回相对当前应用 public 目录的路径。
func PublicPath(path ...string) string {
	return resolvePath("path.public", path...)
}

// ResourcePath 返回相对当前应用 resources 目录的路径。
func ResourcePath(path ...string) string {
	return resolvePath("path.resources", path...)
}

// StoragePath 返回相对当前应用 storage 目录的路径。
func StoragePath(path ...string) string {
	return resolvePath("path.storage", path...)
}

// LangPath 返回相对当前应用 lang 目录的路径。
func LangPath(path ...string) string {
	return resolvePath("path.lang", path...)
}

func applicationURL() string {
	if configured := configuredApplicationURL(); configured != "" {
		return configured
	}
	if env := strings.TrimSpace(os.Getenv("APP_URL")); env != "" {
		return env
	}
	return "http://localhost:8080"
}

func configuredApplicationURL() string {
	cfg, err := container.Make[configStringReader]("config.default")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(cfg.GetString("app.url", ""))
}

func isAbsoluteURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && (parsed.IsAbs() || strings.HasPrefix(value, "//"))
}

func urlParameterSegments(parameters []any) []string {
	segments := make([]string, 0, len(parameters))
	for _, parameter := range flattenURLParameters(parameters) {
		value := strings.Trim(fmt.Sprint(parameter), "/")
		if value != "" {
			segments = append(segments, url.PathEscape(value))
		}
	}
	return segments
}

func flattenURLParameters(parameters []any) []any {
	flattened := make([]any, 0, len(parameters))
	for _, parameter := range parameters {
		flattened = append(flattened, flattenURLParameter(parameter)...)
	}
	return flattened
}

func flattenURLParameter(parameter any) []any {
	if parameter == nil {
		return nil
	}
	value := reflect.ValueOf(parameter)
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		items := make([]any, 0, value.Len())
		for i := 0; i < value.Len(); i++ {
			items = append(items, value.Index(i).Interface())
		}
		return items
	case reflect.Map:
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i].Interface()) < fmt.Sprint(keys[j].Interface()) })
		items := make([]any, 0, len(keys))
		for _, key := range keys {
			items = append(items, value.MapIndex(key).Interface())
		}
		return items
	default:
		return []any{parameter}
	}
}

func joinURLPath(path string, segments []string) string {
	if path == "" {
		return strings.Join(segments, "/")
	}
	return strings.TrimRight(path, "/") + "/" + strings.Join(segments, "/")
}

func resolvePath(key string, path ...string) string {
	root := container.Value[string](key)
	if root == "" {
		root = pathutil.Detect()
		switch key {
		case "path.app":
			root = filepath.Join(root, "app")
		case "path.config":
			root = filepath.Join(root, "config")
		case "path.database":
			root = filepath.Join(root, "database")
		case "path.public":
			root = filepath.Join(root, "public")
		case "path.resources":
			root = filepath.Join(root, "resources")
		case "path.storage":
			root = filepath.Join(root, "storage")
		case "path.lang":
			root = filepath.Join(root, "lang")
		}
	}
	path = normalizePathSegments(key, path)
	return pathutil.Join(root, path...)
}

func normalizePathSegments(key string, path []string) []string {
	if len(path) == 0 {
		return path
	}
	first := strings.TrimSpace(path[0])
	if first == "" {
		path[0] = first
		return path
	}
	cleaned := filepath.Clean(first)
	if filepath.IsAbs(cleaned) {
		path[0] = cleaned
		return path
	}
	prefix := pathPrefixForKey(key)
	if cleaned == "." || cleaned == prefix {
		return path[1:]
	}
	if prefix != "" {
		prefixWithSeparator := prefix + string(filepath.Separator)
		cleaned = strings.TrimPrefix(cleaned, prefixWithSeparator)
	}
	path[0] = cleaned
	return path
}

func pathPrefixForKey(key string) string {
	switch key {
	case "path.app":
		return "app"
	case "path.config":
		return "config"
	case "path.database":
		return "database"
	case "path.public":
		return "public"
	case "path.resources":
		return "resources"
	case "path.storage":
		return "storage"
	case "path.lang":
		return "lang"
	default:
		return ""
	}
}

// IsProduction 判断当前是否为生产环境。
//
// 判断链：APP_ENV 环境变量 -> "production" 兜底。
// 对齐 migration requireForceInProduction 的 commandEnvironment() 逻辑。
//
// 用途：publish 注册表、vendor:publish 命令以及其他需要区分生产环境的场景。
func IsProduction() bool {
	env := strings.TrimSpace(os.Getenv("APP_ENV"))
	if env == "" {
		env = "production"
	}
	return strings.EqualFold(env, "production")
}
