package schema

import (
	"crypto/sha1"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/prismgo/framework/database"
	"gorm.io/gorm"
)

type tableCommand string

const (
	createTable tableCommand = "create"
	alterTable  tableCommand = "alter"
)

// Blueprint 描述一次表创建或表结构变更。
//
// 使用方式：在 schema.Create/schema.Table 的回调中通过 Blueprint 声明字段、索引、
// 外键和列级变更，最终由 Builder 根据当前数据库方言编译为 SQL。
type Blueprint struct {
	table       string
	command     tableCommand
	columns     []*ColumnDefinition
	indexes     []*IndexDefinition
	foreignKeys []*ForeignKeyDefinition
	commands    []rawCommand
}

type rawCommand struct {
	sql   string
	kind  string
	from  string
	to    string
	names []string
}

var (
	defaultOptionsMu    sync.RWMutex
	defaultStringLength = 255
	defaultTimePrec     *int
	defaultMorphKeyType = "int"
)

func setDefaultStringLength(length int) {
	if length > 0 {
		defaultOptionsMu.Lock()
		defaultStringLength = length
		defaultOptionsMu.Unlock()
	}
}

func setDefaultTimePrecision(precision *int) {
	defaultOptionsMu.Lock()
	defer defaultOptionsMu.Unlock()
	if precision != nil && *precision < 0 {
		defaultTimePrec = nil
		return
	}
	if precision == nil {
		defaultTimePrec = nil
		return
	}
	value := *precision
	defaultTimePrec = &value
}

func setDefaultMorphKeyType(kind string) {
	defaultOptionsMu.Lock()
	defer defaultOptionsMu.Unlock()
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "uuid", "ulid":
		defaultMorphKeyType = strings.ToLower(strings.TrimSpace(kind))
	default:
		defaultMorphKeyType = "int"
	}
}

func defaultStringLengthValue() int {
	defaultOptionsMu.RLock()
	defer defaultOptionsMu.RUnlock()
	return defaultStringLength
}

func defaultTimePrecisionValue() *int {
	defaultOptionsMu.RLock()
	defer defaultOptionsMu.RUnlock()
	if defaultTimePrec == nil {
		return nil
	}
	value := *defaultTimePrec
	return &value
}

func defaultMorphKeyTypeValue() string {
	defaultOptionsMu.RLock()
	defer defaultOptionsMu.RUnlock()
	return defaultMorphKeyType
}

// NewBlueprint 创建表结构蓝图。
//
// 该函数主要供 Builder 内部和单元测试使用；业务迁移应优先通过 schema.Create/schema.Table
// 获取 Blueprint，避免手动选择 create/alter 命令。
func NewBlueprint(table string, command tableCommand) *Blueprint {
	return &Blueprint{table: table, command: command}
}

// TableName 返回当前 Blueprint 操作的表名。
func (b *Blueprint) TableName() string {
	return b.table
}

// Raw 追加原始 SQL 命令。
//
// 仅应用于 Schema DSL 无法表达的驱动特定修复，例如历史表结构的一次性补丁；
// 常规建表、改字段、建索引应优先使用 Blueprint 方法，便于测试和方言兼容。
func (b *Blueprint) Raw(sql string) {
	b.commands = append(b.commands, rawCommand{sql: sql})
}

// RenameColumn 重命名既有字段。
//
// 如果来源字段不存在或目标字段已存在，会直接跳过，保证迁移可重复执行。
func (b *Blueprint) RenameColumn(from, to string) {
	b.commands = append(b.commands, rawCommand{kind: "renameColumn", from: from, to: to})
}

// DropColumn 删除一个或多个字段。
//
// 不存在的字段会被忽略，避免历史环境和测试环境重复执行迁移时报错。
func (b *Blueprint) DropColumn(columns ...string) {
	b.commands = append(b.commands, rawCommand{kind: "dropColumn", names: columns})
}

// DropColumns 删除字段切片。
//
// 该方法是 DropColumn 的切片参数版本，便于调用方直接传入动态字段列表。
func (b *Blueprint) DropColumns(columns []string) {
	b.DropColumn(columns...)
}

// DropMorphs 删除 Laravel morphs 约定字段。
//
// 会同时尝试删除 {name}_id、{name}_type 以及对应的默认联合索引。
func (b *Blueprint) DropMorphs(name string) {
	b.DropIndex(defaultIndexName(b.table, "index", []string{name + "_id", name + "_type"}))
	b.DropColumn(name+"_id", name+"_type")
}

// DropRememberToken 删除 Laravel 约定的 remember_token 字段。
func (b *Blueprint) DropRememberToken() {
	b.DropColumn("remember_token")
}

// DropSoftDeletes 删除软删除字段 deleted_at。
//
// 会同步尝试删除字段级默认索引，避免删除字段后遗留无效索引。
func (b *Blueprint) DropSoftDeletes() {
	b.DropIndex(defaultIndexName(b.table, "index", []string{"deleted_at"}))
	b.DropColumn("deleted_at")
}

// DropSoftDeletesTz 删除带时区语义的软删除字段。
//
// 当前项目统一使用 MySQL datetime/timestamp 语义，因此该方法等价于 DropSoftDeletes。
func (b *Blueprint) DropSoftDeletesTz() {
	b.DropSoftDeletes()
}

// DropTimestamps 删除 created_at 和 updated_at 字段。
func (b *Blueprint) DropTimestamps() {
	b.DropColumn("created_at", "updated_at")
}

// DropTimestampsTz 删除带时区语义的时间戳字段。
//
// 当前实现等价于 DropTimestamps。
func (b *Blueprint) DropTimestampsTz() {
	b.DropTimestamps()
}

// DropConstrainedForeignId 删除约定外键和对应字段。
//
// 该方法会按默认命名规则删除外键约束，再删除字段本身；适合回滚 foreignId().Constrained()。
func (b *Blueprint) DropConstrainedForeignId(column string) {
	b.DropForeign(defaultIndexName(b.table, "foreign", []string{column}))
	b.DropColumn(column)
}

// DropForeignIdFor 删除约定外键字段。
//
// 与 DropConstrainedForeignId 不同，该方法只删除字段，不删除外键约束。
func (b *Blueprint) DropForeignIdFor(column string) {
	b.DropColumn(column)
}

// Compile 将 Blueprint 编译为当前数据库方言的 SQL。
//
// 当前支持 MySQL 与 SQLite；其它方言返回 ErrUnsupportedFeature，避免生成不可控 SQL。
func (b *Blueprint) Compile(db *gorm.DB) ([]string, error) {
	switch dialect(db) {
	case "mysql":
		return b.compileMySQL(db)
	case "sqlite", "sqlite3":
		return b.compileSQLite(db)
	default:
		return nil, unsupported("compile blueprint", db)
	}
}

func (b *Blueprint) compileMySQL(db *gorm.DB) ([]string, error) {
	var sqls []string
	switch b.command {
	case createTable:
		parts := make([]string, 0, len(b.columns)+len(b.indexes)+len(b.foreignKeys))
		for _, col := range b.columns {
			parts = append(parts, col.compileMySQL())
		}
		parts = append(parts, b.inlineIndexSQL("mysql")...)
		parts = append(parts, b.inlineForeignSQL("mysql")...)
		if len(parts) == 0 {
			return nil, fmt.Errorf("schema: create table %s has no columns", b.table)
		}
		query := fmt.Sprintf("CREATE TABLE `%s` (%s)", b.table, strings.Join(parts, ", "))
		if opts := database.TableOptions(db.Name(), "InnoDB"); opts != "" {
			query += " " + opts
		}
		sqls = append(sqls, query)
	case alterTable:
		for _, col := range b.columns {
			if col.change {
				if !db.Migrator().HasColumn(b.table, col.name) {
					return nil, fmt.Errorf("schema: cannot change missing column %s.%s", b.table, col.name)
				}
				sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN %s", b.table, col.compileMySQL()))
				continue
			}
			if db.Migrator().HasColumn(b.table, col.name) {
				continue
			}
			sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN %s", b.table, col.compileMySQL()))
		}
		sqls = append(sqls, b.alterIndexSQL(db, "mysql")...)
		sqls = append(sqls, b.alterForeignSQL("mysql")...)
		for _, cmd := range b.commands {
			compiled, err := b.compileCommand(db, cmd)
			if err != nil {
				return nil, err
			}
			sqls = append(sqls, compiled...)
		}
	}
	return sqls, nil
}

func (b *Blueprint) compileSQLite(db *gorm.DB) ([]string, error) {
	var sqls []string
	switch b.command {
	case createTable:
		parts := make([]string, 0, len(b.columns)+len(b.indexes))
		for _, col := range b.columns {
			parts = append(parts, col.compileSQLite())
		}
		parts = append(parts, b.inlineIndexSQL("sqlite")...)
		if len(parts) == 0 {
			return nil, fmt.Errorf("schema: create table %s has no columns", b.table)
		}
		sqls = append(sqls, fmt.Sprintf("CREATE TABLE `%s` (%s)", b.table, strings.Join(parts, ", ")))
		sqls = append(sqls, b.sqliteCreateIndexSQL()...)
	case alterTable:
		for _, col := range b.columns {
			if col.change {
				return nil, unsupported("change column", db)
			}
			if db.Migrator().HasColumn(b.table, col.name) {
				continue
			}
			sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN %s", b.table, col.compileSQLite()))
		}
		sqls = append(sqls, b.alterIndexSQL(db, "sqlite")...)
		for _, cmd := range b.commands {
			compiled, err := b.compileCommand(db, cmd)
			if err != nil {
				return nil, err
			}
			sqls = append(sqls, compiled...)
		}
	}
	return sqls, nil
}

func (b *Blueprint) compileCommand(db *gorm.DB, cmd rawCommand) ([]string, error) {
	if cmd.sql != "" {
		return []string{cmd.sql}, nil
	}
	switch cmd.kind {
	case "renameColumn":
		if cmd.from == "" || cmd.to == "" {
			return nil, fmt.Errorf("schema: rename column requires source and target")
		}
		if !db.Migrator().HasColumn(b.table, cmd.from) || db.Migrator().HasColumn(b.table, cmd.to) {
			return nil, nil
		}
		return []string{fmt.Sprintf("ALTER TABLE `%s` RENAME COLUMN `%s` TO `%s`", b.table, cmd.from, cmd.to)}, nil
	case "dropColumn":
		sqls := make([]string, 0, len(cmd.names))
		for _, name := range cmd.names {
			if strings.TrimSpace(name) == "" || !db.Migrator().HasColumn(b.table, name) {
				continue
			}
			sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", b.table, name))
		}
		return sqls, nil
	default:
		return nil, nil
	}
}

// ColumnDefinition 描述单个字段及其 Laravel 风格修饰符。
//
// 字段对象由 Blueprint 的列类型方法创建，例如 table.String("name", 64)；
// 调用 Nullable、Default、Index、Change 等方法会继续修改同一个字段声明。
type ColumnDefinition struct {
	blueprint       *Blueprint
	name            string
	kind            string
	length          int
	precision       *int
	total           int
	places          int
	allowed         []string
	nullable        bool
	unsigned        bool
	autoIncrement   bool
	primary         bool
	unique          bool
	index           bool
	defaultValue    *string
	comment         string
	first           bool
	after           string
	charset         string
	collation       string
	useCurrent      bool
	useCurrentOnUpd bool
	storedAs        string
	virtualAs       string
	invisible       bool
	change          bool
}

func (b *Blueprint) addColumn(kind, name string) *ColumnDefinition {
	col := &ColumnDefinition{blueprint: b, name: name, kind: kind}
	b.columns = append(b.columns, col)
	return col
}

// Id 新增自增无符号 bigint 主键。
//
// 未传字段名时默认使用 id。
func (b *Blueprint) Id(name ...string) *ColumnDefinition {
	return b.BigIncrements(optionalName("id", name...))
}

// Increments 新增自增无符号 int 主键。
func (b *Blueprint) Increments(name string) *ColumnDefinition {
	return b.UnsignedInteger(name).AutoIncrement().Primary()
}

// BigIncrements 新增自增无符号 bigint 主键。
func (b *Blueprint) BigIncrements(name string) *ColumnDefinition {
	return b.UnsignedBigInteger(name).AutoIncrement().Primary()
}

// TinyIncrements 新增自增无符号 tinyint 主键。
func (b *Blueprint) TinyIncrements(name string) *ColumnDefinition {
	return b.UnsignedTinyInteger(name).AutoIncrement().Primary()
}

// SmallIncrements 新增自增无符号 smallint 主键。
func (b *Blueprint) SmallIncrements(name string) *ColumnDefinition {
	return b.UnsignedSmallInteger(name).AutoIncrement().Primary()
}

// MediumIncrements 新增自增无符号 mediumint 主键。
func (b *Blueprint) MediumIncrements(name string) *ColumnDefinition {
	return b.UnsignedMediumInteger(name).AutoIncrement().Primary()
}

// String 新增 varchar 字段，默认长度 255。
func (b *Blueprint) String(name string, length ...int) *ColumnDefinition {
	col := b.addColumn("string", name)
	col.length = optionalInt(defaultStringLengthValue(), length...)
	return col
}

// Char 新增 char 字段，默认长度 255。
func (b *Blueprint) Char(name string, length ...int) *ColumnDefinition {
	col := b.addColumn("char", name)
	col.length = optionalInt(defaultStringLengthValue(), length...)
	return col
}

// Text 新增长文本字段。
func (b *Blueprint) Text(name string) *ColumnDefinition { return b.addColumn("text", name) }

// TinyText 新增 tinytext 字段。
func (b *Blueprint) TinyText(name string) *ColumnDefinition { return b.addColumn("tinyText", name) }

// MediumText 新增 mediumtext 字段。
func (b *Blueprint) MediumText(name string) *ColumnDefinition { return b.addColumn("mediumText", name) }

// LongText 新增 longtext 字段。
func (b *Blueprint) LongText(name string) *ColumnDefinition { return b.addColumn("longText", name) }

// Boolean 新增布尔字段。
func (b *Blueprint) Boolean(name string) *ColumnDefinition { return b.addColumn("boolean", name) }

// Integer 新增 int 字段。
func (b *Blueprint) Integer(name string) *ColumnDefinition { return b.addColumn("integer", name) }

// BigInteger 新增 bigint 字段。
func (b *Blueprint) BigInteger(name string) *ColumnDefinition { return b.addColumn("bigInteger", name) }

// MediumInteger 新增 mediumint 字段。
func (b *Blueprint) MediumInteger(name string) *ColumnDefinition {
	return b.addColumn("mediumInteger", name)
}

// SmallInteger 新增 smallint 字段。
func (b *Blueprint) SmallInteger(name string) *ColumnDefinition {
	return b.addColumn("smallInteger", name)
}

// TinyInteger 新增 tinyint 字段。
func (b *Blueprint) TinyInteger(name string) *ColumnDefinition {
	return b.addColumn("tinyInteger", name)
}

// UnsignedInteger 新增无符号 int 字段。
func (b *Blueprint) UnsignedInteger(name string) *ColumnDefinition {
	return b.Integer(name).Unsigned()
}

// UnsignedBigInteger 新增无符号 bigint 字段。
func (b *Blueprint) UnsignedBigInteger(name string) *ColumnDefinition {
	return b.BigInteger(name).Unsigned()
}

// UnsignedMediumInteger 新增无符号 mediumint 字段。
func (b *Blueprint) UnsignedMediumInteger(name string) *ColumnDefinition {
	return b.MediumInteger(name).Unsigned()
}

// UnsignedSmallInteger 新增无符号 smallint 字段。
func (b *Blueprint) UnsignedSmallInteger(name string) *ColumnDefinition {
	return b.SmallInteger(name).Unsigned()
}

// UnsignedTinyInteger 新增无符号 tinyint 字段。
func (b *Blueprint) UnsignedTinyInteger(name string) *ColumnDefinition {
	return b.TinyInteger(name).Unsigned()
}

// Float 新增 float 字段，默认精度为 8,2。
func (b *Blueprint) Float(name string, totalAndPlaces ...int) *ColumnDefinition {
	col := b.addColumn("float", name)
	col.total, col.places = optionalPrecision(8, 2, totalAndPlaces...)
	return col
}

// Double 新增 double 字段，默认精度为 8,2。
func (b *Blueprint) Double(name string, totalAndPlaces ...int) *ColumnDefinition {
	col := b.addColumn("double", name)
	col.total, col.places = optionalPrecision(8, 2, totalAndPlaces...)
	return col
}

// Decimal 新增 decimal 字段，默认精度为 8,2。
func (b *Blueprint) Decimal(name string, totalAndPlaces ...int) *ColumnDefinition {
	col := b.addColumn("decimal", name)
	col.total, col.places = optionalPrecision(8, 2, totalAndPlaces...)
	return col
}

// UnsignedDecimal 新增无符号 decimal 字段。
func (b *Blueprint) UnsignedDecimal(name string, totalAndPlaces ...int) *ColumnDefinition {
	return b.Decimal(name, totalAndPlaces...).Unsigned()
}

// Date 新增 date 字段。
func (b *Blueprint) Date(name string) *ColumnDefinition { return b.addColumn("date", name) }

// DateTime 新增 datetime 字段。
func (b *Blueprint) DateTime(name string) *ColumnDefinition {
	return b.addColumn("dateTime", name).withDefaultPrecision()
}

// DateTimeTz 新增带时区语义的 datetime 字段，当前实现按 datetime 编译。
func (b *Blueprint) DateTimeTz(name string) *ColumnDefinition {
	return b.addColumn("dateTime", name).withDefaultPrecision()
}

// Time 新增 time 字段。
func (b *Blueprint) Time(name string) *ColumnDefinition {
	return b.addColumn("time", name).withDefaultPrecision()
}

// TimeTz 新增带时区语义的 time 字段，当前实现按 time 编译。
func (b *Blueprint) TimeTz(name string) *ColumnDefinition {
	return b.addColumn("time", name).withDefaultPrecision()
}

// Timestamp 新增 timestamp 字段。
func (b *Blueprint) Timestamp(name string) *ColumnDefinition {
	return b.addColumn("timestamp", name).withDefaultPrecision()
}

// TimestampTz 新增带时区语义的 timestamp 字段。
func (b *Blueprint) TimestampTz(name string) *ColumnDefinition {
	return b.addColumn("timestamp", name).withDefaultPrecision()
}

// Year 新增 year 字段。
func (b *Blueprint) Year(name string) *ColumnDefinition { return b.addColumn("year", name) }

// Binary 新增二进制字段。
func (b *Blueprint) Binary(name string) *ColumnDefinition { return b.addColumn("binary", name) }

// Json 新增 JSON 字段。
func (b *Blueprint) Json(name string) *ColumnDefinition { return b.addColumn("json", name) }

// Jsonb 新增 JSONB 字段；MySQL/SQLite 下按 JSON 兼容处理。
func (b *Blueprint) Jsonb(name string) *ColumnDefinition { return b.addColumn("json", name) }

// Uuid 新增 UUID 字段。
func (b *Blueprint) Uuid(name string) *ColumnDefinition { return b.Char(name, 36) }

// Ulid 新增 ULID 字段。
func (b *Blueprint) Ulid(name string) *ColumnDefinition { return b.Char(name, 26) }

// IpAddress 新增 IP 地址字段。
func (b *Blueprint) IpAddress(name string) *ColumnDefinition {
	return b.String(name, 45)
}

// MacAddress 新增 MAC 地址字段。
func (b *Blueprint) MacAddress(name string) *ColumnDefinition {
	return b.String(name, 17)
}

// RememberToken 新增 Laravel 约定的 remember_token 字段。
func (b *Blueprint) RememberToken() *ColumnDefinition {
	return b.String("remember_token", 100).Nullable()
}

// Enum 新增枚举字段。
func (b *Blueprint) Enum(name string, allowed []string) *ColumnDefinition {
	col := b.addColumn("enum", name)
	col.allowed = allowed
	return col
}

// Set 新增 MySQL set 字段。
func (b *Blueprint) Set(name string, allowed []string) *ColumnDefinition {
	col := b.addColumn("set", name)
	col.allowed = allowed
	return col
}

// Geometry 新增 geometry 字段。
func (b *Blueprint) Geometry(name string) *ColumnDefinition { return b.addColumn("geometry", name) }

// Geography 新增 geography 字段。
func (b *Blueprint) Geography(name string) *ColumnDefinition { return b.addColumn("geography", name) }

// Point 新增 point 字段。
func (b *Blueprint) Point(name string) *ColumnDefinition { return b.addColumn("point", name) }

// LineString 新增 linestring 字段。
func (b *Blueprint) LineString(name string) *ColumnDefinition { return b.addColumn("linestring", name) }

// Polygon 新增 polygon 字段。
func (b *Blueprint) Polygon(name string) *ColumnDefinition { return b.addColumn("polygon", name) }

// Vector 新增向量字段，dimensions 表示向量维度。
func (b *Blueprint) Vector(name string, dimensions int) *ColumnDefinition {
	col := b.addColumn("vector", name)
	col.length = dimensions
	return col
}

// ForeignId 新增无符号 bigint 外键 ID 字段。
func (b *Blueprint) ForeignId(name string) *ColumnDefinition {
	return b.UnsignedBigInteger(name)
}

// ForeignIdFor 新增约定外键 ID 字段。
//
// Go 版本不解析模型类型，调用方应直接传入最终字段名。
func (b *Blueprint) ForeignIdFor(name string) *ColumnDefinition {
	return b.ForeignId(name)
}

// Morphs 新增 Laravel morphs 约定字段和联合索引。
func (b *Blueprint) Morphs(name string) {
	b.morphID(name + "_id")
	b.String(name+"_type", 255)
	b.Index(name+"_id", name+"_type")
}

// NullableMorphs 新增允许为空的 morphs 约定字段和联合索引。
func (b *Blueprint) NullableMorphs(name string) {
	b.morphID(name + "_id").Nullable()
	b.String(name+"_type", 255).Nullable()
	b.Index(name+"_id", name+"_type")
}

func (b *Blueprint) morphID(name string) *ColumnDefinition {
	switch defaultMorphKeyTypeValue() {
	case "uuid":
		return b.Uuid(name)
	case "ulid":
		return b.Ulid(name)
	default:
		return b.UnsignedBigInteger(name)
	}
}

// Timestamps 新增 created_at 和 updated_at 字段。
func (b *Blueprint) Timestamps() {
	b.Timestamp("created_at").Nullable()
	b.Timestamp("updated_at").Nullable()
}

// NullableTimestamps 新增可空时间戳字段。
func (b *Blueprint) NullableTimestamps() { b.Timestamps() }

// TimestampsTz 新增带时区语义的时间戳字段。
func (b *Blueprint) TimestampsTz() { b.Timestamps() }

// SoftDeletes 新增软删除字段 deleted_at 及默认索引。
func (b *Blueprint) SoftDeletes() { b.Timestamp("deleted_at").Nullable().Index() }

// SoftDeletesTz 新增带时区语义的软删除字段。
func (b *Blueprint) SoftDeletesTz() { b.SoftDeletes() }

func (c *ColumnDefinition) withDefaultPrecision() *ColumnDefinition {
	precision := defaultTimePrecisionValue()
	if precision == nil {
		return c
	}
	c.precision = precision
	return c
}

// Nullable 将字段标记为允许 NULL。
func (c *ColumnDefinition) Nullable() *ColumnDefinition { c.nullable = true; return c }

// NotNull 将字段标记为 NOT NULL。
func (c *ColumnDefinition) NotNull() *ColumnDefinition { c.nullable = false; return c }

// Unsigned 将数值字段标记为无符号类型。
func (c *ColumnDefinition) Unsigned() *ColumnDefinition { c.unsigned = true; return c }

// AutoIncrement 将字段标记为自增字段。
func (c *ColumnDefinition) AutoIncrement() *ColumnDefinition { c.autoIncrement = true; return c }

// Primary 将字段标记为主键。
func (c *ColumnDefinition) Primary() *ColumnDefinition { c.primary = true; return c }

// Unique 为字段添加唯一约束。
//
// 传入 false 时会按默认命名规则删除字段唯一索引，便于 change() 场景移除唯一约束。
func (c *ColumnDefinition) Unique(enabled ...bool) *ColumnDefinition {
	c.unique = optionalBool(true, enabled...)
	if !c.unique {
		c.blueprint.DropIndex(defaultIndexName(c.blueprint.table, "unique", []string{c.name}))
	}
	return c
}

// Index 为字段添加普通索引。
//
// 传入 false 时会按默认命名规则删除字段索引，便于 change() 场景移除索引。
func (c *ColumnDefinition) Index(enabled ...bool) *ColumnDefinition {
	c.index = optionalBool(true, enabled...)
	if !c.index {
		c.blueprint.DropIndex(defaultIndexName(c.blueprint.table, "index", []string{c.name}))
		return c
	}
	c.blueprint.Index(c.name)
	return c
}

// Default 设置字段默认值。
//
// 字符串会自动转义为 SQL 字面量；CURRENT_TIMESTAMP、NULL 和括号表达式会按原样保留。
func (c *ColumnDefinition) Default(value any) *ColumnDefinition {
	v := sqlLiteral(value)
	c.defaultValue = &v
	return c
}

// Comment 设置字段注释。
func (c *ColumnDefinition) Comment(value string) *ColumnDefinition {
	c.comment = value
	return c
}

// First 将新增字段放到表的第一个位置，仅 MySQL 生效。
func (c *ColumnDefinition) First() *ColumnDefinition { c.first = true; return c }

// After 将新增字段放到指定字段之后，仅 MySQL 生效。
func (c *ColumnDefinition) After(column string) *ColumnDefinition {
	c.after = column
	return c
}

// Charset 设置字段字符集，仅字符串类字段在 MySQL 下有意义。
func (c *ColumnDefinition) Charset(value string) *ColumnDefinition {
	c.charset = value
	return c
}

// Collation 设置字段排序规则，仅字符串类字段在 MySQL 下有意义。
func (c *ColumnDefinition) Collation(value string) *ColumnDefinition {
	c.collation = value
	return c
}

// UseCurrent 将时间字段默认值设置为 CURRENT_TIMESTAMP。
func (c *ColumnDefinition) UseCurrent() *ColumnDefinition { c.useCurrent = true; return c }

// UseCurrentOnUpdate 设置时间字段更新时自动写入 CURRENT_TIMESTAMP。
func (c *ColumnDefinition) UseCurrentOnUpdate() *ColumnDefinition {
	c.useCurrentOnUpd = true
	return c
}

// StoredAs 声明存储生成列表达式。
//
// 当前仅记录语义，后续如需要生成列可在编译器中扩展。
func (c *ColumnDefinition) StoredAs(expr string) *ColumnDefinition {
	c.storedAs = expr
	return c
}

// VirtualAs 声明虚拟生成列表达式。
//
// 当前仅记录语义，后续如需要生成列可在编译器中扩展。
func (c *ColumnDefinition) VirtualAs(expr string) *ColumnDefinition {
	c.virtualAs = expr
	return c
}

// Invisible 将字段标记为 MySQL invisible column。
func (c *ColumnDefinition) Invisible() *ColumnDefinition { c.invisible = true; return c }

// From 保留 Laravel integer 起始值修饰符入口。
//
// 当前 MySQL/SQLite 编译器不依赖该值，因此方法只保持链式 API 兼容。
func (c *ColumnDefinition) From(_ int) *ColumnDefinition { return c }

// Instant 保留 Laravel instant algorithm 修饰符入口。
//
// 当前实现不自动拼接 ALTER ALGORITHM，调用方如需驱动特定行为可使用 Raw。
func (c *ColumnDefinition) Instant() *ColumnDefinition { return c }

// Lock 保留 Laravel lock 修饰符入口。
//
// 当前实现不自动拼接 LOCK 子句，避免 SQLite 测试环境不可执行。
func (c *ColumnDefinition) Lock(_ string) *ColumnDefinition { return c }

// Change 将字段声明标记为对既有字段的修改。
//
// MySQL 会编译为 ALTER TABLE ... MODIFY COLUMN；SQLite 不支持直接修改字段类型、
// 可空性和默认值，因此返回明确的不支持错误，避免隐式重建表导致约束丢失。
func (c *ColumnDefinition) Change() *ColumnDefinition {
	c.change = true
	return c
}

// Constrained 为当前 foreignId 字段添加约定外键。
//
// 默认引用去掉 _id 后加 s 的表名以及 id 字段；也可以显式传入表名和字段名。
func (c *ColumnDefinition) Constrained(tableAndColumn ...string) *ForeignKeyDefinition {
	table := strings.TrimSuffix(c.name, "_id") + "s"
	column := "id"
	if len(tableAndColumn) > 0 && tableAndColumn[0] != "" {
		table = tableAndColumn[0]
	}
	if len(tableAndColumn) > 1 && tableAndColumn[1] != "" {
		column = tableAndColumn[1]
	}
	return c.blueprint.Foreign(c.name).References(column).On(table)
}

func (c *ColumnDefinition) compileMySQL() string {
	parts := []string{quote(c.name), c.mysqlType()}
	if !c.nullable && !c.primary {
		parts = append(parts, "NOT NULL")
	}
	if c.nullable {
		parts = append(parts, "NULL")
	}
	if c.defaultValue != nil {
		parts = append(parts, "DEFAULT "+*c.defaultValue)
	}
	if c.useCurrent {
		parts = append(parts, "DEFAULT CURRENT_TIMESTAMP")
	}
	if c.useCurrentOnUpd {
		parts = append(parts, "ON UPDATE CURRENT_TIMESTAMP")
	}
	if c.autoIncrement {
		parts = append(parts, "AUTO_INCREMENT")
	}
	if c.comment != "" {
		parts = append(parts, "COMMENT "+sqlLiteral(c.comment))
	}
	if c.charset != "" {
		parts = append(parts, "CHARACTER SET "+c.charset)
	}
	if c.collation != "" {
		parts = append(parts, "COLLATE "+c.collation)
	}
	if c.invisible {
		parts = append(parts, "INVISIBLE")
	}
	if c.primary {
		parts = append(parts, "PRIMARY KEY")
	}
	if c.unique {
		parts = append(parts, "UNIQUE")
	}
	if c.first {
		parts = append(parts, "FIRST")
	}
	if c.after != "" {
		parts = append(parts, "AFTER "+quote(c.after))
	}
	return strings.Join(parts, " ")
}

func (c *ColumnDefinition) compileSQLite() string {
	parts := []string{quote(c.name), c.sqliteType()}
	if c.primary {
		parts = append(parts, "PRIMARY KEY")
	}
	if c.autoIncrement && c.primary {
		parts = append(parts, "AUTOINCREMENT")
	}
	if !c.nullable && !c.primary {
		parts = append(parts, "NOT NULL")
	}
	if c.defaultValue != nil {
		parts = append(parts, "DEFAULT "+*c.defaultValue)
	}
	if c.unique {
		parts = append(parts, "UNIQUE")
	}
	return strings.Join(parts, " ")
}

func (c *ColumnDefinition) mysqlType() string {
	unsigned := ""
	if c.unsigned {
		unsigned = " unsigned"
	}
	switch c.kind {
	case "string":
		return fmt.Sprintf("varchar(%d)", c.length)
	case "char":
		return fmt.Sprintf("char(%d)", c.length)
	case "tinyText":
		return "tinytext"
	case "mediumText":
		return "mediumtext"
	case "longText":
		return "longtext"
	case "text":
		return "text"
	case "boolean":
		return "tinyint(1)"
	case "tinyInteger":
		return "tinyint" + unsigned
	case "smallInteger":
		return "smallint" + unsigned
	case "mediumInteger":
		return "mediumint" + unsigned
	case "integer":
		return "int" + unsigned
	case "bigInteger":
		return "bigint" + unsigned
	case "float":
		return fmt.Sprintf("float(%d,%d)%s", c.total, c.places, unsigned)
	case "double":
		return fmt.Sprintf("double(%d,%d)%s", c.total, c.places, unsigned)
	case "decimal":
		return fmt.Sprintf("decimal(%d,%d)%s", c.total, c.places, unsigned)
	case "date":
		return "date"
	case "dateTime":
		if c.precision != nil {
			return fmt.Sprintf("datetime(%d)", *c.precision)
		}
		return "datetime"
	case "time":
		if c.precision != nil {
			return fmt.Sprintf("time(%d)", *c.precision)
		}
		return "time"
	case "timestamp":
		if c.precision != nil {
			return fmt.Sprintf("timestamp(%d)", *c.precision)
		}
		return "timestamp"
	case "year":
		return "year"
	case "binary":
		return "blob"
	case "json":
		return "json"
	case "enum":
		return "enum(" + quotedList(c.allowed) + ")"
	case "set":
		return "set(" + quotedList(c.allowed) + ")"
	case "geometry", "geography", "point", "linestring", "polygon":
		return c.kind
	case "vector":
		return fmt.Sprintf("vector(%d)", c.length)
	default:
		return c.kind
	}
}

func (c *ColumnDefinition) sqliteType() string {
	switch c.kind {
	case "tinyInteger", "smallInteger", "mediumInteger", "integer", "bigInteger", "boolean":
		return "integer"
	case "float", "double":
		return "real"
	case "decimal":
		return "numeric"
	case "binary":
		return "blob"
	case "date", "dateTime", "time", "timestamp", "year":
		return "datetime"
	default:
		return "text"
	}
}

// IndexDefinition 描述一次索引操作。
//
// 支持创建、删除、重命名普通索引、唯一索引、全文索引、空间索引和主键索引。
type IndexDefinition struct {
	kind    string
	name    string
	columns []string
	drop    bool
	rename  string
}

// Primary 新增主键索引。
func (b *Blueprint) Primary(columns ...string) *IndexDefinition {
	return b.addIndex("primary", "PRIMARY", columns...)
}

// Unique 新增唯一索引，索引名按 Laravel 默认规则生成。
func (b *Blueprint) Unique(columns ...string) *IndexDefinition {
	return b.addIndex("unique", defaultIndexName(b.table, "unique", columns), columns...)
}

// Index 新增普通索引，索引名按 Laravel 默认规则生成。
func (b *Blueprint) Index(columns ...string) *IndexDefinition {
	return b.addIndex("index", defaultIndexName(b.table, "index", columns), columns...)
}

// FullText 新增全文索引。
//
// MySQL 使用 FULLTEXT INDEX；SQLite 测试环境降级为普通索引，保证迁移可执行。
func (b *Blueprint) FullText(columns ...string) *IndexDefinition {
	return b.addIndex("fulltext", defaultIndexName(b.table, "fulltext", columns), columns...)
}

// SpatialIndex 新增空间索引。
//
// MySQL 使用 SPATIAL INDEX；SQLite 测试环境降级为普通索引，保证迁移可执行。
func (b *Blueprint) SpatialIndex(columns ...string) *IndexDefinition {
	return b.addIndex("spatial", defaultIndexName(b.table, "spatial", columns), columns...)
}

// UniqueNamed 使用指定名称新增唯一索引。
func (b *Blueprint) UniqueNamed(name string, columns ...string) *IndexDefinition {
	return b.addIndex("unique", name, columns...)
}

// IndexNamed 使用指定名称新增普通索引。
func (b *Blueprint) IndexNamed(name string, columns ...string) *IndexDefinition {
	return b.addIndex("index", name, columns...)
}

// DropIndex 删除指定名称的索引。
func (b *Blueprint) DropIndex(name string) {
	b.indexes = append(b.indexes, &IndexDefinition{name: name, drop: true})
}

// DropUnique 删除指定名称的唯一索引。
func (b *Blueprint) DropUnique(name string) { b.DropIndex(name) }

// DropPrimary 删除主键索引。
func (b *Blueprint) DropPrimary() {
	b.indexes = append(b.indexes, &IndexDefinition{name: "PRIMARY", drop: true})
}

// DropFullText 删除指定名称的全文索引。
func (b *Blueprint) DropFullText(name string) { b.DropIndex(name) }

// DropSpatialIndex 删除指定名称的空间索引。
func (b *Blueprint) DropSpatialIndex(name string) { b.DropIndex(name) }

// RenameIndex 重命名索引。
//
// 当前仅 MySQL 编译为 RENAME INDEX；SQLite 会跳过该操作，因为其原生重命名索引支持有限。
func (b *Blueprint) RenameIndex(from, to string) {
	b.indexes = append(b.indexes, &IndexDefinition{name: from, rename: to})
}

func (b *Blueprint) addIndex(kind, name string, columns ...string) *IndexDefinition {
	idx := &IndexDefinition{kind: kind, name: name, columns: columns}
	b.indexes = append(b.indexes, idx)
	return idx
}

// Name 覆盖自动生成的索引名称。
func (i *IndexDefinition) Name(name string) *IndexDefinition {
	i.name = name
	return i
}

func (b *Blueprint) inlineIndexSQL(driver string) []string {
	var sqls []string
	for _, idx := range b.indexes {
		if idx.drop || idx.rename != "" {
			continue
		}
		if idx.kind == "index" || idx.kind == "fulltext" || idx.kind == "spatial" {
			continue
		}
		if driver == "sqlite" && idx.kind == "primary" && len(idx.columns) == 1 {
			continue
		}
		if driver == "sqlite" {
			if sql := idx.inlineSQLiteSQL(); sql != "" {
				sqls = append(sqls, sql)
			}
			continue
		}
		sqls = append(sqls, idx.inlineSQL())
	}
	return sqls
}

func (i *IndexDefinition) inlineSQL() string {
	switch i.kind {
	case "primary":
		return "PRIMARY KEY (" + quotedColumns(i.columns) + ")"
	case "unique":
		return "UNIQUE KEY " + quote(i.name) + " (" + quotedColumns(i.columns) + ")"
	default:
		return "KEY " + quote(i.name) + " (" + quotedColumns(i.columns) + ")"
	}
}

func (i *IndexDefinition) inlineSQLiteSQL() string {
	switch i.kind {
	case "primary":
		return "PRIMARY KEY (" + quotedColumns(i.columns) + ")"
	case "unique":
		return "UNIQUE (" + quotedColumns(i.columns) + ")"
	default:
		return ""
	}
}

func (b *Blueprint) alterIndexSQL(db *gorm.DB, driver string) []string {
	var sqls []string
	for _, idx := range b.indexes {
		if idx.drop {
			if !db.Migrator().HasIndex(b.table, idx.name) && idx.name != "PRIMARY" {
				continue
			}
			if driver == "mysql" {
				if idx.name == "PRIMARY" {
					sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` DROP PRIMARY KEY", b.table))
				} else {
					sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`", b.table, idx.name))
				}
			} else {
				sqls = append(sqls, fmt.Sprintf("DROP INDEX IF EXISTS `%s`", idx.name))
			}
			continue
		}
		if idx.rename != "" {
			if driver == "mysql" {
				sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` RENAME INDEX `%s` TO `%s`", b.table, idx.name, idx.rename))
			}
			continue
		}
		if db.Migrator().HasIndex(b.table, idx.name) {
			continue
		}
		if driver == "mysql" {
			sqls = append(sqls, idx.alterMySQL(b.table))
		} else {
			sqls = append(sqls, idx.createSQLite(b.table))
		}
	}
	return sqls
}

func (b *Blueprint) sqliteCreateIndexSQL() []string {
	var sqls []string
	for _, idx := range b.indexes {
		if idx.drop || idx.rename != "" || idx.kind == "primary" {
			continue
		}
		sqls = append(sqls, idx.createSQLite(b.table))
	}
	return sqls
}

func (i *IndexDefinition) alterMySQL(table string) string {
	switch i.kind {
	case "primary":
		return fmt.Sprintf("ALTER TABLE `%s` ADD PRIMARY KEY (%s)", table, quotedColumns(i.columns))
	case "unique":
		return fmt.Sprintf("ALTER TABLE `%s` ADD UNIQUE INDEX `%s` (%s)", table, i.name, quotedColumns(i.columns))
	case "fulltext":
		return fmt.Sprintf("ALTER TABLE `%s` ADD FULLTEXT INDEX `%s` (%s)", table, i.name, quotedColumns(i.columns))
	case "spatial":
		return fmt.Sprintf("ALTER TABLE `%s` ADD SPATIAL INDEX `%s` (%s)", table, i.name, quotedColumns(i.columns))
	default:
		return fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (%s)", table, i.name, quotedColumns(i.columns))
	}
}

func (i *IndexDefinition) createSQLite(table string) string {
	unique := ""
	if i.kind == "unique" {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS `%s` ON `%s` (%s)", unique, i.name, table, quotedColumns(i.columns))
}

// ForeignKeyDefinition 描述一次外键操作。
//
// 支持设置引用表、引用字段、ON DELETE 和 ON UPDATE 行为。
type ForeignKeyDefinition struct {
	name       string
	columns    []string
	refTable   string
	refColumns []string
	onUpdate   string
	onDelete   string
	drop       bool
}

// Foreign 新增外键约束声明。
func (b *Blueprint) Foreign(columns ...string) *ForeignKeyDefinition {
	fk := &ForeignKeyDefinition{columns: columns, refColumns: []string{"id"}}
	fk.name = defaultIndexName(b.table, "foreign", columns)
	b.foreignKeys = append(b.foreignKeys, fk)
	return fk
}

// DropForeign 删除指定名称的外键约束。
func (b *Blueprint) DropForeign(name string) {
	b.foreignKeys = append(b.foreignKeys, &ForeignKeyDefinition{name: name, drop: true})
}

// Name 覆盖自动生成的外键名称。
func (f *ForeignKeyDefinition) Name(name string) *ForeignKeyDefinition { f.name = name; return f }

// References 设置外键引用字段。
func (f *ForeignKeyDefinition) References(columns ...string) *ForeignKeyDefinition {
	f.refColumns = columns
	return f
}

// On 设置外键引用表。
func (f *ForeignKeyDefinition) On(table string) *ForeignKeyDefinition { f.refTable = table; return f }

// OnUpdate 设置 ON UPDATE 行为。
func (f *ForeignKeyDefinition) OnUpdate(action string) *ForeignKeyDefinition {
	f.onUpdate = action
	return f
}

// OnDelete 设置 ON DELETE 行为。
func (f *ForeignKeyDefinition) OnDelete(action string) *ForeignKeyDefinition {
	f.onDelete = action
	return f
}

// CascadeOnDelete 设置 ON DELETE CASCADE。
func (f *ForeignKeyDefinition) CascadeOnDelete() *ForeignKeyDefinition { return f.OnDelete("CASCADE") }

// RestrictOnDelete 设置 ON DELETE RESTRICT。
func (f *ForeignKeyDefinition) RestrictOnDelete() *ForeignKeyDefinition {
	return f.OnDelete("RESTRICT")
}

// NullOnDelete 设置 ON DELETE SET NULL。
func (f *ForeignKeyDefinition) NullOnDelete() *ForeignKeyDefinition { return f.OnDelete("SET NULL") }

// NoActionOnDelete 设置 ON DELETE NO ACTION。
func (f *ForeignKeyDefinition) NoActionOnDelete() *ForeignKeyDefinition {
	return f.OnDelete("NO ACTION")
}

// CascadeOnUpdate 设置 ON UPDATE CASCADE。
func (f *ForeignKeyDefinition) CascadeOnUpdate() *ForeignKeyDefinition { return f.OnUpdate("CASCADE") }

// RestrictOnUpdate 设置 ON UPDATE RESTRICT。
func (f *ForeignKeyDefinition) RestrictOnUpdate() *ForeignKeyDefinition {
	return f.OnUpdate("RESTRICT")
}

// NullOnUpdate 设置 ON UPDATE SET NULL。
func (f *ForeignKeyDefinition) NullOnUpdate() *ForeignKeyDefinition { return f.OnUpdate("SET NULL") }

// NoActionOnUpdate 设置 ON UPDATE NO ACTION。
func (f *ForeignKeyDefinition) NoActionOnUpdate() *ForeignKeyDefinition {
	return f.OnUpdate("NO ACTION")
}

func (b *Blueprint) inlineForeignSQL(driver string) []string {
	if driver != "mysql" {
		return nil
	}
	var sqls []string
	for _, fk := range b.foreignKeys {
		if fk.drop {
			continue
		}
		sqls = append(sqls, fk.inlineSQL())
	}
	return sqls
}

func (b *Blueprint) alterForeignSQL(driver string) []string {
	if driver != "mysql" {
		return nil
	}
	var sqls []string
	for _, fk := range b.foreignKeys {
		if fk.drop {
			sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` DROP FOREIGN KEY `%s`", b.table, fk.name))
			continue
		}
		sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s` ADD %s", b.table, fk.inlineSQL()))
	}
	return sqls
}

func (f *ForeignKeyDefinition) inlineSQL() string {
	parts := []string{
		"CONSTRAINT", quote(f.name),
		"FOREIGN KEY (" + quotedColumns(f.columns) + ")",
		"REFERENCES", quote(f.refTable), "(" + quotedColumns(f.refColumns) + ")",
	}
	if f.onDelete != "" {
		parts = append(parts, "ON DELETE "+f.onDelete)
	}
	if f.onUpdate != "" {
		parts = append(parts, "ON UPDATE "+f.onUpdate)
	}
	return strings.Join(parts, " ")
}

func optionalName(fallback string, values ...string) string {
	if len(values) == 0 || values[0] == "" {
		return fallback
	}
	return values[0]
}

func optionalInt(fallback int, values ...int) int {
	if len(values) == 0 || values[0] <= 0 {
		return fallback
	}
	return values[0]
}

func optionalBool(fallback bool, values ...bool) bool {
	if len(values) == 0 {
		return fallback
	}
	return values[0]
}

func optionalPrecision(defaultTotal, defaultPlaces int, values ...int) (int, int) {
	total, places := defaultTotal, defaultPlaces
	if len(values) > 0 {
		total = values[0]
	}
	if len(values) > 1 {
		places = values[1]
	}
	return total, places
}

func quote(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func quotedColumns(columns []string) string {
	out := make([]string, 0, len(columns))
	for _, col := range columns {
		out = append(out, quote(strings.Trim(col, "` ")))
	}
	return strings.Join(out, ", ")
}

func quotedList(values []string) string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, sqlLiteral(value))
	}
	return strings.Join(out, ",")
}

func sqlLiteral(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case string:
		upper := strings.ToUpper(strings.TrimSpace(v))
		if upper == "CURRENT_TIMESTAMP" || upper == "NULL" || strings.HasPrefix(upper, "(") {
			return v
		}
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprint(v)
	}
}

var invalidIndexNameChars = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func defaultIndexName(table, kind string, columns []string) string {
	base := table + "_" + strings.Join(columns, "_") + "_" + kind
	base = strings.ToLower(invalidIndexNameChars.ReplaceAllString(base, "_"))
	if len(base) > 64 {
		sum := sha1.Sum([]byte(base))
		hash := fmt.Sprintf("%x", sum[:])[:10]
		return base[:64-len(hash)-1] + "_" + hash
	}
	return base
}
