package database

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unicode"

	"gorm.io/gorm"
)

// MigrationFunc 描述一个可被迁移命令执行的 Go 迁移函数。
//
// 用途：业务 migration 文件在 init 阶段注册 up/down 处理器，命令运行时按文件名查找执行。
// 设计说明：注册表属于数据库迁移领域能力，命令包只负责调度，不直接持有业务迁移注册状态。
type MigrationFunc func(*gorm.DB) error

// MigrationFuncMap 描述 migration key 到执行函数的映射关系。
type MigrationFuncMap map[string]MigrationFunc

// SeedFunc 描述一个 Seeder 执行函数。
type SeedFunc func(*gorm.DB) error

// SeedFuncMap 描述 seeder class 到执行函数的映射关系。
type SeedFuncMap map[string]SeedFunc

// DefaultSeederClass 是 db:seed 未指定 --class 时使用的默认 seeder class。
const DefaultSeederClass = "DatabaseSeeder"

// MigrationEntry 保存单个 migration 的 up/down 执行器。
type MigrationEntry struct {
	Up   MigrationFunc
	Down MigrationFunc
}

var (
	migrationRegistryMu sync.RWMutex
	migrationRegistry   = map[string]MigrationEntry{}

	seederRegistryMu sync.RWMutex
	seederRegistry   = map[string]SeedFunc{}
)

// RegisterMigration 自动按 up 函数所在迁移文件注册 up/down 执行器。
//
// 命名约定：
// - up 必须定义在对应 migration 文件中；
// - 注册名自动取 up 函数所在文件名（去掉 .go），与目录扫描结果保持一致；
// - down 可为匿名函数，注册名仍然只取 up 函数来源，避免回滚闭包影响推导。
func RegisterMigration(up, down MigrationFunc) {
	name, err := inferMigrationName(up)
	if err != nil {
		panic(err)
	}
	RegisterMigrationAs(name, up, down)
}

// RegisterMigrationAs 按指定迁移名注册 up/down 执行器。
//
// 用途：测试、临时工具或非常规文件名场景需要显式指定迁移名时使用；
// 常规业务 migration 应优先使用 RegisterMigration，让注册名从文件名自动推导。
func RegisterMigrationAs(name string, up, down MigrationFunc) {
	name = strings.TrimSpace(name)
	if name == "" {
		panic("database migration register: migration name is empty")
	}
	migrationRegistryMu.Lock()
	migrationRegistry[name] = MigrationEntry{Up: up, Down: down}
	migrationRegistryMu.Unlock()
}

// MigrationByName 返回指定迁移名对应的 up/down 执行器。
func MigrationByName(name string) (MigrationEntry, bool) {
	migrationRegistryMu.RLock()
	defer migrationRegistryMu.RUnlock()
	entry, ok := migrationRegistry[name]
	return entry, ok
}

// RegisterSeeder 自动按 Seed 函数所在 seeder 文件注册 class 与执行函数映射。
//
// 命名约定：
// - Seed 函数必须定义在对应 seeder 文件中；
// - 短 class 名来自文件名去掉时间前缀后的 PascalCase，例如 database_seeder -> DatabaseSeeder；
// - 同时注册包路径命名空间别名，兼容 Laravel 风格 --class 参数。
func RegisterSeeder(fn SeedFunc) {
	classNames, err := inferSeederClassNames(fn)
	if err != nil {
		panic(err)
	}
	for _, className := range classNames {
		RegisterSeederAs(className, fn)
	}
}

// RegisterSeederAs 按指定 class 名注册 seeder 执行函数。
//
// 用途：测试、兼容旧 class 名或非常规 seeder 命名场景显式注册；
// 常规业务 seeder 应优先使用 RegisterSeeder 自动从源码位置推导 class 名。
func RegisterSeederAs(className string, fn SeedFunc) {
	className = strings.TrimSpace(className)
	if className == "" {
		panic("database migration register: seeder class name is empty")
	}
	seederRegistryMu.Lock()
	seederRegistry[className] = fn
	seederRegistryMu.Unlock()
}

// SeederByClass 返回指定 seeder class 对应的执行函数。
func SeederByClass(className string) (SeedFunc, bool) {
	seederRegistryMu.RLock()
	defer seederRegistryMu.RUnlock()
	fn, ok := seederRegistry[className]
	return fn, ok
}

// SeederClassNames 返回当前已注册的 seeder class 名称快照。
func SeederClassNames() []string {
	seederRegistryMu.RLock()
	defer seederRegistryMu.RUnlock()
	names := make([]string, 0, len(seederRegistry))
	for name := range seederRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// EnsureSeederRegistered 校验指定 seeder class 已注册。
func EnsureSeederRegistered(className string) error {
	if _, ok := SeederByClass(className); ok {
		return nil
	}
	return fmt.Errorf("seeder class %q is not registered, available: %s", className, SeederClassNames())
}

type functionSource struct {
	FilePath    string
	PackagePath string
}

func inferMigrationName(fn MigrationFunc) (string, error) {
	source, err := sourceForFunction(fn)
	if err != nil {
		return "", fmt.Errorf("infer migration name: %w", err)
	}
	name := migrationNameFromSourceFile(source.FilePath)
	if name == "" {
		return "", fmt.Errorf("infer migration name: source file %q has no usable name", source.FilePath)
	}
	return name, nil
}

func inferSeederClassNames(fn SeedFunc) ([]string, error) {
	source, err := sourceForFunction(fn)
	if err != nil {
		return nil, fmt.Errorf("infer seeder class: %w", err)
	}
	names := seederClassNamesFromSource(source)
	if len(names) == 0 {
		return nil, fmt.Errorf("infer seeder class: source file %q has no usable class name", source.FilePath)
	}
	return names, nil
}

func sourceForFunction(fn any) (functionSource, error) {
	value := reflect.ValueOf(fn)
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		return functionSource{}, fmt.Errorf("handler must be a non-nil function")
	}
	runtimeFn := runtime.FuncForPC(value.Pointer())
	if runtimeFn == nil {
		return functionSource{}, fmt.Errorf("runtime function metadata is unavailable")
	}
	file, _ := runtimeFn.FileLine(value.Pointer())
	if strings.TrimSpace(file) == "" {
		return functionSource{}, fmt.Errorf("runtime source file is unavailable")
	}
	return functionSource{
		FilePath:    file,
		PackagePath: packagePathFromRuntimeName(runtimeFn.Name()),
	}, nil
}

func migrationNameFromSourceFile(filePath string) string {
	base := filepath.Base(filePath)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

func seederClassNamesFromSource(source functionSource) []string {
	shortClass := seederClassNameFromFile(source.FilePath)
	if shortClass == "" {
		return nil
	}
	names := []string{shortClass}
	if namespace := namespaceFromPackagePath(source.PackagePath); namespace != "" {
		names = append(names, namespace+"\\"+shortClass)
	}
	return uniqueStrings(names)
}

func seederClassNameFromFile(filePath string) string {
	name := migrationNameFromSourceFile(filePath)
	name = trimLeadingTimestamp(name)
	return pascalCaseIdentifier(name)
}

func trimLeadingTimestamp(name string) string {
	index := strings.Index(name, "_")
	if index < 11 {
		return name
	}
	prefix := name[:index]
	for _, r := range prefix {
		if r < '0' || r > '9' {
			return name
		}
	}
	return name[index+1:]
}

func packagePathFromRuntimeName(name string) string {
	lastSlash := strings.LastIndex(name, "/")
	searchFrom := lastSlash + 1
	dot := strings.Index(name[searchFrom:], ".")
	if dot < 0 {
		return ""
	}
	return name[:searchFrom+dot]
}

func namespaceFromPackagePath(packagePath string) string {
	parts := strings.Split(packagePath, "/")
	names := make([]string, 0, len(parts))
	for _, part := range parts {
		if name := pascalCaseIdentifier(part); name != "" {
			names = append(names, name)
		}
	}
	return strings.Join(names, "\\")
}

func pascalCaseIdentifier(value string) string {
	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	var builder strings.Builder
	for _, word := range words {
		runes := []rune(strings.ToLower(word))
		if len(runes) == 0 {
			continue
		}
		runes[0] = unicode.ToUpper(runes[0])
		builder.WriteString(string(runes))
	}
	return builder.String()
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
