// Package schema 提供 Laravel 风格的数据库结构构造器。
//
// 设计目标：
//  1. 让 Go migration 能使用接近 Laravel Schema / Blueprint 的声明式 API；
//  2. 统一表结构变更入口，避免业务迁移直接散落 raw SQL 或直接依赖 GORM AutoMigrate；
//  3. 生产环境优先支持 MySQL，测试环境支持 SQLite，并对不支持的方言显式返回错误。
package schema

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/prismgo/framework/database"
	"gorm.io/gorm"
	gormschema "gorm.io/gorm/schema"
)

// ErrUnsupportedFeature 表示当前数据库方言无法安全执行请求的 Schema 操作。
//
// 例如 SQLite 不支持直接修改列类型，Schema 会返回该错误，而不是隐式重建表导致索引、
// 外键或约束丢失。
var ErrUnsupportedFeature = errors.New("schema: unsupported feature for current dialect")

// Builder 负责在指定 GORM 连接上执行 Schema 操作。
//
// 使用方式：
//  1. migration 中调用 schema.Bind(tx) 或 schema.New(tx)，确保表结构变更与数据回填处于同一事务；
//  2. 普通代码中也可以通过 New(db) 创建独立构造器；
//  3. 未显式绑定连接时，会从 database facade 延迟解析默认连接。
type Builder struct {
	mu sync.RWMutex
	db *gorm.DB
}

// New 创建 Schema 构造器。
//
// 参数 values 可选；传入 *gorm.DB 时构造器会固定使用该连接，否则在执行时解析默认数据库连接。
func New(values ...*gorm.DB) *Builder {
	b := &Builder{}
	if len(values) > 0 {
		b.db = values[0]
	}
	return b
}

// Bind 将构造器绑定到指定数据库连接，并返回自身用于链式调用。
//
// migration 执行期间应传入当前事务 tx，避免 Schema 操作绕过迁移事务。
func (b *Builder) Bind(db *gorm.DB) *Builder {
	if b == nil {
		return New(db)
	}
	b.mu.Lock()
	b.db = db
	b.mu.Unlock()
	return b
}

// Connection 基于 database.connections.{name} 创建新的 Schema 构造器。
//
// 如果连接创建失败，错误会暂存在返回的 Builder 中，并在后续执行 Create/Table 等操作时返回。
func (b *Builder) Connection(name string) *Builder {
	db, err := database.OpenConnection(name)
	if err != nil {
		return &Builder{db: errorDB(err)}
	}
	return New(db)
}

// Create 根据 Blueprint 创建数据表。
//
// 为保证迁移幂等性，当目标表已经存在时会直接跳过，不重复执行 CREATE TABLE。
func (b *Builder) Create(table string, fn func(*Blueprint)) error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	if db.Migrator().HasTable(table) {
		return nil
	}
	blueprint := NewBlueprint(table, createTable)
	fn(blueprint)
	return b.executeBlueprint(db, blueprint)
}

// Table 对既有表执行结构变更。
//
// 常见用途包括新增字段、修改字段、重命名字段、删除字段、添加或删除索引。
func (b *Builder) Table(table string, fn func(*Blueprint)) error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	blueprint := NewBlueprint(table, alterTable)
	fn(blueprint)
	return b.executeBlueprint(db, blueprint)
}

// Rename 重命名数据表。
//
// 当来源表不存在或目标表已存在时直接跳过，避免重复执行迁移时报错。
func (b *Builder) Rename(from, to string) error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	if !db.Migrator().HasTable(from) {
		return nil
	}
	if db.Migrator().HasTable(to) {
		return nil
	}
	return db.Migrator().RenameTable(from, to)
}

// Drop 在表存在时删除表。
func (b *Builder) Drop(table string) error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	if !db.Migrator().HasTable(table) {
		return nil
	}
	return db.Migrator().DropTable(table)
}

// DropIfExists 在表存在时删除表。
//
// 该方法语义对齐 Laravel Schema::dropIfExists。
func (b *Builder) DropIfExists(table string) error {
	return b.Drop(table)
}

// HasTable 判断指定表是否存在。
func (b *Builder) HasTable(table string) bool {
	db, err := b.resolve()
	if err != nil {
		return false
	}
	return db.Migrator().HasTable(table)
}

// HasColumn 判断指定表字段是否存在。
func (b *Builder) HasColumn(table, column string) bool {
	db, err := b.resolve()
	if err != nil {
		return false
	}
	return db.Migrator().HasColumn(table, column)
}

// HasIndex 判断指定表上的命名索引是否存在。
// WhenTableHasColumn 仅在指定字段存在时执行 fn。
//
// 该方法用于历史兼容迁移，避免对不存在字段执行 DROP、RENAME 或数据回填。
func (b *Builder) WhenTableHasColumn(table, column string, fn func() error) error {
	if !b.HasColumn(table, column) {
		return nil
	}
	return fn()
}

// WhenTableDoesntHaveColumn 仅在指定字段不存在时执行 fn。
//
// 该方法通常用于增量补列或兼容旧环境初始化。
func (b *Builder) WhenTableDoesntHaveColumn(table, column string, fn func() error) error {
	if b.HasColumn(table, column) {
		return nil
	}
	return fn()
}

// EnableForeignKeyConstraints 启用外键约束检查。
//
// MySQL 使用 FOREIGN_KEY_CHECKS，SQLite 使用 PRAGMA foreign_keys。
func (b *Builder) EnableForeignKeyConstraints() error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	switch dialect(db) {
	case "mysql":
		return db.Exec("SET FOREIGN_KEY_CHECKS=1").Error
	case "sqlite", "sqlite3":
		return db.Exec("PRAGMA foreign_keys = ON").Error
	default:
		return unsupported("enable foreign key constraints", db)
	}
}

// DisableForeignKeyConstraints 禁用外键约束检查。
//
// 常用于批量重建表或历史数据清理；使用后应恢复约束检查。
func (b *Builder) DisableForeignKeyConstraints() error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	switch dialect(db) {
	case "mysql":
		return db.Exec("SET FOREIGN_KEY_CHECKS=0").Error
	case "sqlite", "sqlite3":
		return db.Exec("PRAGMA foreign_keys = OFF").Error
	default:
		return unsupported("disable foreign key constraints", db)
	}
}

// WithoutForeignKeyConstraints 在禁用外键约束检查的上下文中执行 fn。
//
// 无论 fn 是否返回错误，方法都会尝试恢复外键约束检查；如果 fn 返回错误，则优先返回 fn 的错误。
func (b *Builder) WithoutForeignKeyConstraints(fn func() error) error {
	if err := b.DisableForeignKeyConstraints(); err != nil {
		return err
	}
	runErr := fn()
	enableErr := b.EnableForeignKeyConstraints()
	if runErr != nil {
		return runErr
	}
	return enableErr
}

// SyncModels 通过 GORM 底层 migrator 同步模型表结构。
//
// 设计背景：当前项目历史迁移长期依赖 GORM 模型定义；为了逐步迁移到 Schema DSL，
// 该方法作为过渡入口承接原建表和补列能力，但不再直接调用 AutoMigrate。
//
// 注意：新写 migration 应优先使用 Create/Table 显式声明表结构，避免继续扩大模型驱动迁移。
func (b *Builder) SyncModels(models ...any) error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	if opts := database.TableOptions(db.Name()); opts != "" {
		db = db.Set("gorm:table_options", opts)
	}
	migrator := db.Migrator()
	for _, model := range models {
		stmt, err := parseModel(db, model)
		if err != nil {
			return err
		}
		applyModelDefaults(stmt)
		if !migrator.HasTable(model) {
			if err := migrator.CreateTable(model); err != nil {
				return fmt.Errorf("schema sync create model table failed: %w", err)
			}
			continue
		}
		for _, dbName := range stmt.Schema.DBNames {
			field := stmt.Schema.FieldsByDBName[dbName]
			if field == nil || field.IgnoreMigration || migrator.HasColumn(model, dbName) {
				continue
			}
			if err := migrator.AddColumn(model, field.Name); err != nil {
				return fmt.Errorf("schema sync add model column %s failed: %w", dbName, err)
			}
		}
	}
	return nil
}

func parseModel(db *gorm.DB, model any) (*gorm.Statement, error) {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return nil, fmt.Errorf("schema sync parse model failed: %w", err)
	}
	return stmt, nil
}

func applyModelDefaults(stmt *gorm.Statement) {
	if stmt == nil || stmt.Schema == nil {
		return
	}
	for _, field := range stmt.Schema.Fields {
		if shouldApplyDefaultMorphKey(field, stmt.Schema) {
			continue
		}
		if length, ok := defaultStringLengthForField(field); ok {
			field.Size = length
		}
		if precision, ok := defaultTimePrecisionForField(field); ok {
			field.Precision = precision
		}
	}
}

func defaultStringLengthForField(field *gormschema.Field) (int, bool) {
	length := defaultStringLengthValue()
	if field == nil || field.Size > 0 || length <= 0 {
		return 0, false
	}
	if field.DataType != gormschema.String && field.GORMDataType != gormschema.String {
		return 0, false
	}
	if _, ok := field.TagSettings["SIZE"]; ok {
		return 0, false
	}
	if _, ok := field.TagSettings["TYPE"]; ok {
		return 0, false
	}
	return length, true
}

func defaultTimePrecisionForField(field *gormschema.Field) (int, bool) {
	precision := defaultTimePrecisionValue()
	if field == nil || precision == nil || *precision < 0 || field.Precision > 0 {
		return 0, false
	}
	if field.DataType != gormschema.Time && field.GORMDataType != gormschema.Time {
		return 0, false
	}
	if _, ok := field.TagSettings["PRECISION"]; ok {
		return 0, false
	}
	if _, ok := field.TagSettings["TYPE"]; ok {
		return 0, false
	}
	return *precision, true
}

func shouldApplyDefaultMorphKey(field *gormschema.Field, parsed *gormschema.Schema) bool {
	if field == nil || parsed == nil || field.Size > 0 || field.DataType != gormschema.String {
		return false
	}
	kind := defaultMorphKeyTypeValue()
	if kind != "uuid" && kind != "ulid" {
		return false
	}
	if _, ok := field.TagSettings["SIZE"]; ok {
		return false
	}
	if _, ok := field.TagSettings["TYPE"]; ok {
		return false
	}
	if !strings.HasSuffix(field.DBName, "_id") {
		return false
	}
	typeField := strings.TrimSuffix(field.DBName, "_id") + "_type"
	if parsed.FieldsByDBName[typeField] == nil {
		return false
	}
	if kind == "uuid" {
		field.Size = 36
	} else {
		field.Size = 26
	}
	return true
}

func (b *Builder) executeBlueprint(db *gorm.DB, blueprint *Blueprint) error {
	sqls, err := blueprint.Compile(db)
	if err != nil {
		return err
	}
	for _, query := range sqls {
		if strings.TrimSpace(query) == "" {
			continue
		}
		if err := db.Exec(query).Error; err != nil {
			return fmt.Errorf("execute schema SQL failed: %w; sql=%s", err, query)
		}
	}
	return nil
}

func (b *Builder) resolve() (*gorm.DB, error) {
	if b == nil {
		return nil, errors.New("schema: nil builder")
	}
	b.mu.RLock()
	db := b.db
	b.mu.RUnlock()
	if db != nil {
		if err := db.Error; err != nil {
			return nil, err
		}
		return db, nil
	}
	db = database.Resolve()
	if db == nil {
		return nil, errors.New("schema: database connection is nil")
	}
	return db, nil
}

func errorDB(err error) *gorm.DB {
	return &gorm.DB{Error: err}
}

func dialect(db *gorm.DB) string {
	if db == nil || db.Dialector == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(db.Name()))
}

func unsupported(feature string, db *gorm.DB) error {
	return fmt.Errorf("%w: %s on %s", ErrUnsupportedFeature, feature, dialect(db))
}
