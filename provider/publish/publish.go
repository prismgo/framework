// Package publish 提供可发布资源的中心化注册与查询。
//
// 需求背景：对齐 Laravel 13 ServiceProvider.publishes() 语义。
// 扩展包在 ServiceProvider.Boot() 中通过 provider.Publishes() 注册资源映射，
// vendor:publish 命令运行时会收集所有已注册的条目并复制到目标路径。
//
// 生产环境（support.IsProduction() == true）下：
//   - Register() 静默跳过，不注册任何资源
//   - Copy() 返回错误
//   - DryRun() 返回错误
package publish

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prismgo/framework/support"
)

// migrationPattern 匹配 Laravel 风格的迁移文件名中的日期戳部分。
var migrationPattern = regexp.MustCompile(`\d{4}_(\d{2})_(\d{2})_(\d{6})_`)

// Entry 表示一条可发布资源记录。
type Entry struct {
	// Provider 是注册此条目的 provider 简短名称（由调用方显式传入）。
	Provider string
	// Source 是源绝对路径（Register 已自动将相对路径解析为绝对路径）。
	Source string
	// Target 是目标绝对路径。
	Target string
	// Tags 是资源类型标签（如 "lang", "config", "migrations"）。
	Tags []string
	// IsMigration 标记该条目是否属于迁移文件，用于 dry-run 和日期更新。
	IsMigration bool
}

var global = &registry{
	entries: make([]Entry, 0, 16),
}

type registry struct {
	mu      sync.RWMutex
	entries []Entry
}

// Register 注册可发布资源记录。
//
// providerName 是调用方传入的 provider 简短标识（如 "translation"、"acme"）。
// paths 的 key 为相对于调用方源文件的资源路径，value 为绝对目标路径。
// 内部自动通过 runtime.Caller(1) 定位调用方源文件目录，将相对路径解析为绝对路径。
//
// 如果 tags 中包含 "migrations"，所有条目会被标记为 IsMigration=true。
//
// production 环境下静默跳过，不注册任何资源。
func Register(providerName string, paths map[string]string, tags ...string) error {
	if support.IsProduction() {
		return nil
	}

	if len(paths) == 0 {
		return nil
	}

	providerName = strings.TrimSpace(providerName)
	if providerName == "" {
		return fmt.Errorf("publish: provider name is required")
	}

	// runtime.Caller skip 说明：
	//   Caller(0) = Register 自身
	//   Caller(1) = provider.Publishes()（在 provider/publishes.go）
	//   因此 callerDir 是扩展包 ServiceProvider 所在目录
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		return fmt.Errorf("publish: cannot resolve caller source file")
	}
	callerDir := filepath.Dir(filename)

	cleanTags := cleanupTags(tags)
	isMigration := hasTag(cleanTags, "migrations")

	global.mu.Lock()
	defer global.mu.Unlock()

	for relSrc, target := range paths {
		if relSrc == "" || target == "" {
			continue
		}
		source := resolveSourcePath(relSrc, callerDir)
		global.entries = append(global.entries, Entry{
			Provider:    providerName,
			Source:      source,
			Target:      target,
			Tags:        cleanTags,
			IsMigration: isMigration,
		})
	}

	return nil
}

// Entries 返回匹配条件的可发布资源条目。
//
// provider: 非空时只返回指定 provider 的条目。
// tags:     非空时只返回包含任一指定标签的条目。
// 两者可同时使用（交集过滤），都为空时返回全部。
func Entries(provider string, tags []string) []Entry {
	global.mu.RLock()
	defer global.mu.RUnlock()

	if provider == "" && len(tags) == 0 {
		result := make([]Entry, len(global.entries))
		copy(result, global.entries)
		return result
	}

	tagSet := makeStringSet(tags)
	var result []Entry
	for _, e := range global.entries {
		if provider != "" && e.Provider != provider {
			continue
		}
		if len(tagSet) > 0 && !hasAnyTag(tagSet, e.Tags) {
			continue
		}
		result = append(result, e)
	}
	return result
}

// Providers 返回所有已注册发布资源的 provider 名称列表。
func Providers() []string {
	global.mu.RLock()
	defer global.mu.RUnlock()

	seen := make(map[string]struct{})
	var result []string
	for _, e := range global.entries {
		if _, ok := seen[e.Provider]; !ok {
			seen[e.Provider] = struct{}{}
			result = append(result, e.Provider)
		}
	}
	return result
}

// Tags 返回所有已注册的 tag 名称列表。
func Tags() []string {
	global.mu.RLock()
	defer global.mu.RUnlock()

	seen := make(map[string]struct{})
	var result []string
	for _, e := range global.entries {
		for _, t := range e.Tags {
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				result = append(result, t)
			}
		}
	}
	return result
}

// Clear 清空注册表（用于测试）。
func Clear() {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.entries = global.entries[:0]
}

// IsAvailable 返回当前环境下 publish 功能是否可用。
func IsAvailable() bool {
	return !support.IsProduction()
}

// DryRunItem 表示 dry-run 模式下的一条文件发布计划。
type DryRunItem struct {
	Provider    string
	Source      string
	Target      string
	Tag         string
	IsMigration bool
	Exists      bool
}

// Copy 将匹配条件的资源条目复制到目标路径。
//
// 参数说明：
//   - provider: 非空时只复制指定 provider 的条目
//   - tags:     非空时只复制包含任一指定标签的条目
//   - force:    true 时覆盖已存在文件
//   - existing: true 时仅处理目标已存在的文件（对齐 Laravel --existing）
//
// 生产环境下直接返回错误。
func Copy(provider string, tags []string, force bool, existing bool) (published, skipped int, err error) {
	if support.IsProduction() {
		return 0, 0, fmt.Errorf("vendor:publish is not available in production environment")
	}

	entries := Entries(provider, tags)
	publishedAt := time.Now()
	effectiveForce := force || existing

	for _, entry := range entries {
		info, statErr := os.Stat(entry.Source)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}
			return published, skipped, fmt.Errorf("publish: source %s: %w", entry.Source, statErr)
		}

		if info.IsDir() {
			dirCopied, dirSkipped, cpErr := copyDirectory(entry.Source, entry.Target, effectiveForce, existing)
			if cpErr != nil {
				return published, skipped, cpErr
			}
			published += dirCopied
			skipped += dirSkipped
		} else {
			if shouldSkipFile(entry.Target, effectiveForce, existing) {
				skipped++
				continue
			}
			target := entry.Target
			if entry.IsMigration {
				target = updateMigrationDate(target, publishedAt)
				publishedAt = publishedAt.Add(time.Second)
			}
			written, cpErr := copySingleFile(entry.Source, target, effectiveForce)
			if cpErr != nil {
				return published, skipped, cpErr
			}
			if written {
				published++
			} else {
				skipped++
			}
		}
	}

	return published, skipped, nil
}

// DryRun 返回将要发布的文件列表（不实际复制）。
//
// 生产环境下直接返回错误。
func DryRun(provider string, tags []string) ([]DryRunItem, error) {
	if support.IsProduction() {
		return nil, fmt.Errorf("vendor:publish is not available in production environment")
	}

	entries := Entries(provider, tags)
	publishedAt := time.Now()
	var items []DryRunItem

	for _, entry := range entries {
		info, statErr := os.Stat(entry.Source)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}
			return nil, fmt.Errorf("publish: source %s: %w", entry.Source, statErr)
		}

		if info.IsDir() {
			dirItems, dirErr := dryRunDirectory(entry.Source, entry.Target, entry.Provider, entry.Tags)
			if dirErr != nil {
				return nil, dirErr
			}
			items = append(items, dirItems...)
		} else {
			target := entry.Target
			if entry.IsMigration {
				target = updateMigrationDate(target, publishedAt)
				publishedAt = publishedAt.Add(time.Second)
			}
			items = append(items, DryRunItem{
				Provider:    entry.Provider,
				Source:      entry.Source,
				Target:      target,
				Tag:         strings.Join(entry.Tags, ","),
				IsMigration: entry.IsMigration,
				Exists:      fileExists(target),
			})
		}
	}

	return items, nil
}

// shouldSkipFile 根据 force 和 existing 参数判断是否应跳过该文件。
func shouldSkipFile(target string, force bool, existing bool) bool {
	targetExists := fileExists(target)
	if existing {
		return !targetExists
	}
	return !force && targetExists
}

// updateMigrationDate 将迁移文件名中的日期戳替换为当前时间。
//
// 对齐 Laravel ensureMigrationNameIsUpToDate() 逻辑：
// 只有文件名匹配 YYYY_MM_DD_HHMMSS_ 格式时才替换。
func updateMigrationDate(target string, publishedAt time.Time) string {
	base := filepath.Base(target)
	if migrationPattern.MatchString(base) {
		newName := migrationPattern.ReplaceAllString(base, publishedAt.Format("2006_01_02_150405_"))
		return filepath.Join(filepath.Dir(target), newName)
	}
	return target
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// resolveSourcePath 将相对路径解析为绝对路径。
//
// 如果 relPath 已是绝对路径，直接返回。
// 否则以 callerDir 为基准拼接。
func resolveSourcePath(relPath, callerDir string) string {
	if filepath.IsAbs(relPath) {
		return filepath.Clean(relPath)
	}
	return filepath.Join(callerDir, relPath)
}

// copyDirectory 递归复制目录内容。
//
// 返回值 copied 仅统计真正新创建的文件数，skipCount 为因已存在或 --existing 过滤而跳过的文件数。
func copyDirectory(source, target string, force bool, existing bool) (copied int, skipCount int, err error) {
	err = filepath.WalkDir(source, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(source, path)
		if relErr != nil {
			return relErr
		}
		targetPath := filepath.Join(target, rel)
		if d.IsDir() {
			return os.MkdirAll(targetPath, 0755)
		}
		if shouldSkipFile(targetPath, force, existing) {
			skipCount++
			return nil
		}
		written, cpErr := copySingleFile(path, targetPath, force)
		if cpErr != nil {
			return cpErr
		}
		if written {
			copied++
		} else {
			skipCount++
		}
		return nil
	})
	return copied, skipCount, err
}

// dryRunDirectory 返回目录中所有文件的 dry-run 计划。
func dryRunDirectory(source, target, provider string, tags []string) ([]DryRunItem, error) {
	var items []DryRunItem
	tag := strings.Join(tags, ",")
	err := filepath.WalkDir(source, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(source, path)
		if relErr != nil {
			return relErr
		}
		targetPath := filepath.Join(target, rel)
		if d.IsDir() {
			return nil
		}
		items = append(items, DryRunItem{
			Provider: provider,
			Source:   path,
			Target:   targetPath,
			Tag:      tag,
			Exists:   fileExists(targetPath),
		})
		return nil
	})
	return items, err
}

// copySingleFile 复制单个文件。
//
// 第一个返回值 written 表示文件是否被真正写入（false 表示因已存在且非 force 而跳过）。
func copySingleFile(source, target string, force bool) (written bool, err error) {
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return false, err
	}
	if !force {
		if _, err := os.Stat(target); err == nil {
			return false, nil
		}
	}
	data, err := os.ReadFile(source)
	if err != nil {
		return false, err
	}
	return true, os.WriteFile(target, data, 0644)
}

func cleanupTags(tags []string) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, t := range tags {
		t = strings.TrimSpace(strings.ToLower(t))
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		result = append(result, t)
	}
	return result
}

func makeStringSet(items []string) map[string]struct{} {
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item != "" {
			set[item] = struct{}{}
		}
	}
	return set
}

func hasAnyTag(set map[string]struct{}, items []string) bool {
	if len(set) == 0 {
		return true
	}
	for _, item := range items {
		if _, ok := set[item]; ok {
			return true
		}
	}
	return false
}

// hasTag 检查 tags 列表中是否包含指定标签。
func hasTag(tags []string, target string) bool {
	for _, t := range tags {
		if t == target {
			return true
		}
	}
	return false
}
