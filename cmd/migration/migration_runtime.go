package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	dbregistry "github.com/prismgo/framework/database"

	"gorm.io/gorm"
)

// MigrationDependencies 描述迁移与种子命令需要的目录注册器。
//
// 设计说明：
// 1. 命令层只依赖“路径提供能力”，不直接依赖业务包；
// 2. 由 bootstrap 在启动阶段声明路径，命令运行阶段按路径扫描文件。
type MigrationDependencies struct {
	// MigrationPaths 返回 migration 扫描目录列表。
	MigrationPaths func() []string
	// SeedPaths 返回 seeder 扫描目录列表。
	SeedPaths func() []string
}

func firstMigrationDependencies(values ...MigrationDependencies) MigrationDependencies {
	if len(values) == 0 {
		return MigrationDependencies{}
	}
	return values[0]
}

func (d MigrationDependencies) paths() []string {
	if d.MigrationPaths == nil {
		return nil
	}
	return append([]string(nil), d.MigrationPaths()...)
}

func (d MigrationDependencies) seedPaths() []string {
	if d.SeedPaths == nil {
		return nil
	}
	return append([]string(nil), d.SeedPaths()...)
}

type migrationSpec struct {
	Name     string
	FilePath string
}

var migrationFilePattern = regexp.MustCompile(`^\d{11,}_[a-z0-9_]+\.go$`)

// collectMigrations 扫描并收集符合命名规则的迁移文件。
//
// 规则：仅接受 `20260428xxx_xxx.go` 这类时间前缀文件，按文件名升序执行，
// 与 Laravel 迁移的时间顺序语义保持一致。
func collectMigrations(paths []string, realpath bool) ([]migrationSpec, error) {
	resolvedPaths, err := resolveMigrationPaths(paths, realpath)
	if err != nil {
		return nil, err
	}
	found := make(map[string]string, len(resolvedPaths)*2)
	for _, migrationPath := range resolvedPaths {
		entries, readErr := os.ReadDir(migrationPath)
		if readErr != nil {
			return nil, fmt.Errorf("read migration path %s failed: %w", migrationPath, readErr)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !migrationFilePattern.MatchString(name) {
				continue
			}
			migrationName := strings.TrimSuffix(name, filepath.Ext(name))
			source := filepath.Join(migrationPath, name)
			if existsPath, exists := found[migrationName]; exists {
				return nil, fmt.Errorf("duplicated migration %s in multiple paths (%s, %s)", migrationName, existsPath, source)
			}
			found[migrationName] = source
		}
	}

	migrations := make([]migrationSpec, 0, len(found))
	for name, source := range found {
		migrations = append(migrations, migrationSpec{Name: name, FilePath: source})
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Name < migrations[j].Name
	})
	return migrations, nil
}

func resolveMigrationPaths(paths []string, realpath bool) ([]string, error) {
	return resolveSourcePaths(paths, realpath, "database/migrations", "migration")
}

func resolveSeedPaths(paths []string, realpath bool) ([]string, error) {
	return resolveSourcePaths(paths, realpath, "database/seeders", "seeder")
}

// resolveSourcePaths 解析并校验路径配置。
//
// 参数说明：
// - paths: 原始路径列表，允许为空（为空时使用 defaultPath）
// - realpath: true 表示调用方已传入绝对路径，false 表示相对当前工作目录解析
// - defaultPath: 当 paths 为空时使用的默认目录
// - label: 错误信息中的路径类别（migration/seeder）
func resolveSourcePaths(paths []string, realpath bool, defaultPath, label string) ([]string, error) {
	if len(paths) == 0 {
		paths = []string{defaultPath}
	}

	seen := make(map[string]struct{}, len(paths))
	result := make([]string, 0, len(paths))
	workingDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	for _, raw := range paths {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		resolved := raw
		if !realpath && !filepath.IsAbs(resolved) {
			resolved = filepath.Join(workingDir, resolved)
		}
		absolute, absErr := filepath.Abs(resolved)
		if absErr != nil {
			return nil, absErr
		}
		info, statErr := os.Stat(absolute)
		if statErr != nil {
			return nil, fmt.Errorf("%s path %s is invalid: %w", label, absolute, statErr)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s path %s is not a directory", label, absolute)
		}
		if _, exists := seen[absolute]; exists {
			continue
		}
		seen[absolute] = struct{}{}
		result = append(result, absolute)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no %s path available", label)
	}
	return result, nil
}

type migrationRecord struct {
	ID        uint   `gorm:"primaryKey"`
	Migration string `gorm:"size:191;uniqueIndex"`
	Batch     int    `gorm:"index"`
}

func (migrationRecord) TableName() string {
	return "migrations"
}

type migrationStore struct {
	db *gorm.DB
}

func newMigrationStore(db *gorm.DB) migrationStore {
	return migrationStore{db: db}
}

func (s migrationStore) ensureTable() error {
	return s.db.AutoMigrate(&migrationRecord{})
}

func (s migrationStore) hasTable() bool {
	return s.db.Migrator().HasTable(&migrationRecord{})
}

func (s migrationStore) listAll() ([]migrationRecord, error) {
	var records []migrationRecord
	if err := s.db.Order("id ASC").Find(&records).Error; err != nil {
		return nil, err
	}
	return records, nil
}

func (s migrationStore) appliedMap() (map[string]migrationRecord, error) {
	records, err := s.listAll()
	if err != nil {
		return nil, err
	}
	result := make(map[string]migrationRecord, len(records))
	for _, record := range records {
		result[record.Migration] = record
	}
	return result, nil
}

func (s migrationStore) nextBatch() (int, error) {
	var batchValue int
	if err := s.db.Model(&migrationRecord{}).Select("COALESCE(MAX(batch), 0)").Scan(&batchValue).Error; err != nil {
		return 0, err
	}
	return batchValue + 1, nil
}

func (s migrationStore) markApplied(migration string, batch int) error {
	return s.db.Create(&migrationRecord{Migration: migration, Batch: batch}).Error
}

func (s migrationStore) deleteApplied(migration string) error {
	return s.db.Where("migration = ?", migration).Delete(&migrationRecord{}).Error
}

func (s migrationStore) rollbackCandidates(step, batch int) ([]migrationRecord, error) {
	query := s.db.Model(&migrationRecord{}).Order("id DESC")
	switch {
	case batch > 0:
		query = query.Where("batch = ?", batch)
	case step > 0:
		query = query.Limit(step)
	default:
		var latestBatch int
		if err := s.db.Model(&migrationRecord{}).Select("COALESCE(MAX(batch), 0)").Scan(&latestBatch).Error; err != nil {
			return nil, err
		}
		if latestBatch <= 0 {
			return nil, nil
		}
		query = query.Where("batch = ?", latestBatch)
	}
	var records []migrationRecord
	if err := query.Find(&records).Error; err != nil {
		return nil, err
	}
	return records, nil
}

func applyMigrationUp(db *gorm.DB, migration migrationSpec, pretend bool) error {
	return applyMigration(db, migration, pretend, true)
}

func applyMigrationDown(db *gorm.DB, migration migrationSpec, pretend bool) error {
	return applyMigration(db, migration, pretend, false)
}

func applyMigrationAndTrack(db *gorm.DB, migration migrationSpec, batch int) error {
	if db == nil {
		return fmt.Errorf("migration db is nil")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := applyMigrationWithTx(tx, migration, true); err != nil {
			return err
		}
		return newMigrationStore(tx).markApplied(migration.Name, batch)
	})
}

func rollbackMigrationAndTrack(db *gorm.DB, migration migrationSpec) error {
	if db == nil {
		return fmt.Errorf("migration db is nil")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := applyMigrationWithTx(tx, migration, false); err != nil {
			return err
		}
		return newMigrationStore(tx).deleteApplied(migration.Name)
	})
}

func applyMigration(db *gorm.DB, migration migrationSpec, pretend bool, up bool) error {
	if pretend {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		return applyMigrationWithTx(tx, migration, up)
	})
}

func applyMigrationWithTx(tx *gorm.DB, migration migrationSpec, up bool) error {
	entry, exists := dbregistry.MigrationByName(migration.Name)
	if !exists {
		return fmt.Errorf("migration %s is not registered", migration.Name)
	}
	if up {
		if entry.Up == nil {
			return fmt.Errorf("migration %s has no up handler", migration.Name)
		}
		return entry.Up(tx)
	}
	if entry.Down == nil {
		return fmt.Errorf("migration %s has no down handler", migration.Name)
	}
	return entry.Down(tx)
}

func describeMigrationOperation(migration migrationSpec, up bool) string {
	action := "up"
	if !up {
		action = "down"
	}
	path := migration.FilePath
	if path == "" {
		path = "<missing>"
	}
	return fmt.Sprintf("%s [%s] (%s)", migration.Name, action, path)
}

func migrationIndex(migrations []migrationSpec) map[string]migrationSpec {
	result := make(map[string]migrationSpec, len(migrations))
	for _, migration := range migrations {
		result[migration.Name] = migration
	}
	return result
}

func parsePositiveInt(value string) int {
	parsed, _ := strconv.Atoi(strings.TrimSpace(value))
	if parsed < 0 {
		return 0
	}
	return parsed
}
