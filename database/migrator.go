package database

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// quoteIdentifier 转义 SQL 标识符（表名、列名等），防止 SQL 注入
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// TableOptions 返回指定 GORM dialect 的建表选项，未知 dialect 返回空串。
// engine 为空时默认使用 InnoDB。
// 在 AutoMigrate 之前可通过 db.Set("gorm:table_options", opts) 注入。
func TableOptions(dialect, engine string) string {
	if strings.EqualFold(strings.TrimSpace(dialect), "mysql") {
		if engine == "" {
			engine = "InnoDB"
		}
		return fmt.Sprintf("ENGINE=%s DEFAULT CHARSET=utf8mb4", engine)
	}
	return ""
}

// ManagedTableNames 通过 GORM 解析给定模型的实际表名列表。
// 用于迁移后的 ALTER 操作（如强制 InnoDB、建立联合唯一索引）。
func ManagedTableNames(db *gorm.DB, models []any) ([]string, error) {
	names := make([]string, 0, len(models))
	for _, m := range models {
		stmt := &gorm.Statement{DB: db}
		if err := stmt.Parse(m); err != nil {
			return nil, fmt.Errorf("parse model table name failed: %w", err)
		}
		names = append(names, stmt.Schema.Table)
	}
	return names, nil
}

// EnsureInnoDB 将所有受管 MySQL 表强制切换为 InnoDB 引擎。非 MySQL dialect 直接跳过。
// GORM AutoMigrate 无法改变已有表的 ENGINE，因此需要此辅助完成引擎切换。
func EnsureInnoDB(db *gorm.DB, models []any) error {
	if TableOptions(db.Name(), "InnoDB") == "" {
		return nil
	}
	names, err := ManagedTableNames(db, models)
	if err != nil {
		return err
	}
	for _, name := range names {
		engine, err := currentMySQLTableEngine(db, name)
		if err != nil {
			return fmt.Errorf("query table %s engine failed: %w", name, err)
		}
		if !ShouldAlterEngine(engine) {
			continue
		}
		if err := db.Exec(fmt.Sprintf("ALTER TABLE %s ENGINE=InnoDB", quoteIdentifier(name))).Error; err != nil {
			return fmt.Errorf("alter table %s engine failed: %w", name, err)
		}
	}
	return nil
}

// ShouldAlterEngine 判断给定引擎是否需要切换为 InnoDB。
// 空字符串或 InnoDB 以外的引擎均返回 true。
func ShouldAlterEngine(engine string) bool {
	return !strings.EqualFold(strings.TrimSpace(engine), "InnoDB")
}

// CompositeUniqueIndex 描述一条联合唯一索引的声明。
// GORM AutoMigrate 无法跨嵌入 struct 字段自动建立联合唯一索引，因此需要通过 SQL 显式创建。
type CompositeUniqueIndex struct {
	// Table 是目标表名（不带反引号）。
	Table string
	// Name 是索引名称，用于幂等检查。
	Name string
	// Columns 是逗号分隔的列名列表，顺序决定索引效率（如 "tenant_id, phone"）。
	// 含保留字段时可使用反引号（如 "tenant_id, `key`"）。
	Columns string
}

// CompositeIndex 描述一条普通（非唯一）联合索引声明。
// 用于跨嵌入 struct 的联合索引，GORM AutoMigrate 无法自动处理此类索引。
type CompositeIndex struct {
	// Table 是目标表名（不带反引号）。
	Table string
	// Name 是索引名称，用于幂等检查。
	Name string
	// Columns 是逗号分隔的列名列表，顺序决定索引效率（如 "tenant_id, status, created_at"）。
	Columns string
}

// EnsureCompositeIndexes 为 MySQL 和 SQLite 创建普通联合索引（幂等）。
// 其它 dialect 会被直接忽略，便于在测试环境（如 sqlite）下平滑运行。
func EnsureCompositeIndexes(db *gorm.DB, indexes []CompositeIndex) error {
	dialect := strings.ToLower(strings.TrimSpace(db.Name()))
	switch dialect {
	case "mysql":
		for _, idx := range indexes {
			exists, err := IndexExists(db, idx.Table, idx.Name)
			if err != nil {
				return fmt.Errorf("check index %s.%s failed: %w", idx.Table, idx.Name, err)
			}
			if exists {
				continue
			}
			sql := fmt.Sprintf(
				"ALTER TABLE %s ADD INDEX %s (%s)",
				quoteIdentifier(idx.Table), quoteIdentifier(idx.Name), idx.Columns,
			)
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("create index %s.%s failed: %w", idx.Table, idx.Name, err)
			}
		}
	case "sqlite", "sqlite3":
		for _, idx := range indexes {
			sql := fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
				quoteIdentifier(idx.Name), quoteIdentifier(idx.Table), idx.Columns,
			)
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("create sqlite index %s.%s failed: %w", idx.Table, idx.Name, err)
			}
		}
	}
	return nil
}

// EnsureCompositeUniqueIndexes 为 MySQL 和 SQLite 创建联合唯一索引（幂等）。
// 其它 dialect 会被直接忽略，便于在测试环境（如 sqlite）下平滑运行。
func EnsureCompositeUniqueIndexes(db *gorm.DB, indexes []CompositeUniqueIndex) error {
	dialect := strings.ToLower(strings.TrimSpace(db.Name()))
	switch dialect {
	case "mysql":
		for _, idx := range indexes {
			exists, err := IndexExists(db, idx.Table, idx.Name)
			if err != nil {
				return fmt.Errorf("check index %s.%s failed: %w", idx.Table, idx.Name, err)
			}
			if exists {
				continue
			}
			sql := fmt.Sprintf(
				"ALTER TABLE %s ADD UNIQUE INDEX %s (%s)",
				quoteIdentifier(idx.Table), quoteIdentifier(idx.Name), idx.Columns,
			)
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("create index %s.%s failed: %w", idx.Table, idx.Name, err)
			}
		}
	case "sqlite", "sqlite3":
		// SQLite: CREATE UNIQUE INDEX IF NOT EXISTS（幂等，无需预先检查）。
		for _, idx := range indexes {
			sql := fmt.Sprintf(
				"CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s)",
				quoteIdentifier(idx.Name), quoteIdentifier(idx.Table), idx.Columns,
			)
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("create sqlite index %s.%s failed: %w", idx.Table, idx.Name, err)
			}
		}
	}
	return nil
}

// DropIndex 描述一条需要删除的废弃索引。
type DropIndex struct {
	// Table 是目标表名（不带反引号）。
	Table string
	// Name 是索引名称。
	Name string
}

// DropObsoleteIndexes 删除废弃索引（幂等）。仅在 MySQL 下执行，SQLite 跳过（测试库每次重建无需清理）。
func DropObsoleteIndexes(db *gorm.DB, indexes []DropIndex) error {
	if TableOptions(db.Name(), "InnoDB") == "" {
		return nil
	}
	for _, idx := range indexes {
		exists, err := IndexExists(db, idx.Table, idx.Name)
		if err != nil {
			return fmt.Errorf("check index %s.%s failed: %w", idx.Table, idx.Name, err)
		}
		if !exists {
			continue
		}
		sql := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", quoteIdentifier(idx.Table), quoteIdentifier(idx.Name))
		if err := db.Exec(sql).Error; err != nil {
			return fmt.Errorf("drop index %s.%s failed: %w", idx.Table, idx.Name, err)
		}
	}
	return nil
}

// IndexExists 查询 MySQL information_schema，判断指定表的指定索引是否已存在。
func IndexExists(db *gorm.DB, table, indexName string) (bool, error) {
	var count int64
	sql := `SELECT COUNT(1) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`
	if err := db.Raw(sql, table, indexName).Scan(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// PrimaryKeyColumns 返回指定 MySQL 表当前主键列（按顺序）。
// 表不存在或无主键时返回空切片。
func PrimaryKeyColumns(db *gorm.DB, table string) ([]string, error) {
	var columns []string
	sql := `SELECT column_name FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE() AND table_name = ? AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position`
	if err := db.Raw(sql, table).Scan(&columns).Error; err != nil {
		return nil, err
	}
	return columns, nil
}

// SameColumns 判断两组列名是否完全一致且顺序相同。
// 用于识别目标联合主键是否已就绪，避免重复 ALTER 造成错误。
func SameColumns(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// currentMySQLTableEngine 读取 information_schema，返回指定表当前使用的存储引擎。
// 表不存在时返回空串，调用方据此跳过（新装场景）。
func currentMySQLTableEngine(db *gorm.DB, tableName string) (string, error) {
	var result struct {
		Engine string
	}
	query := `SELECT engine FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = ?
		LIMIT 1`
	tx := db.Raw(query, tableName).Scan(&result)
	if tx.Error != nil {
		return "", tx.Error
	}
	if tx.RowsAffected == 0 {
		return "", nil
	}
	return result.Engine, nil
}
