package schema

import (
	"database/sql"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// SchemaInfo 描述数据库 schema。
type SchemaInfo struct {
	Name string
}

// TableInfo 描述数据表元信息。
type TableInfo struct {
	Name   string
	Schema string
	Type   string
}

// ViewInfo 描述视图元信息。
type ViewInfo struct {
	Name       string
	Schema     string
	Definition string
}

// TypeInfo 描述数据库自定义类型元信息。
type TypeInfo struct {
	Name   string
	Schema string
	Type   string
}

// ColumnInfo 描述字段元信息。
type ColumnInfo struct {
	Name          string
	Type          string
	FullType      string
	Nullable      bool
	Default       string
	Comment       string
	Primary       bool
	AutoIncrement bool
	Unique        bool
	Length        int64
	Precision     int64
	Scale         int64
}

// IndexInfo 描述索引元信息。
type IndexInfo struct {
	Name    string
	Columns []string
	Type    string
	Unique  bool
	Primary bool
}

// ForeignKeyInfo 描述外键元信息。
type ForeignKeyInfo struct {
	Name           string
	Columns        []string
	ForeignTable   string
	ForeignColumns []string
	OnUpdate       string
	OnDelete       string
}

// CreateDatabase 创建数据库。当前仅 MySQL 支持真实执行。
func (b *Builder) CreateDatabase(name string) (bool, error) {
	if strings.TrimSpace(name) == "" {
		return false, fmt.Errorf("schema: database name is required")
	}
	// 先完成输入校验，再解析数据库连接，避免无效参数被 facade 装配错误掩盖。
	db, err := b.resolve()
	if err != nil {
		return false, err
	}
	switch dialect(db) {
	case "mysql":
		return true, db.Exec("CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(name)).Error
	default:
		return false, unsupported("create database", db)
	}
}

// DropDatabaseIfExists 删除数据库。当前仅 MySQL 支持真实执行。
func (b *Builder) DropDatabaseIfExists(name string) (bool, error) {
	if strings.TrimSpace(name) == "" {
		return false, fmt.Errorf("schema: database name is required")
	}
	// 先完成输入校验，再解析数据库连接，避免无效参数被 facade 装配错误掩盖。
	db, err := b.resolve()
	if err != nil {
		return false, err
	}
	switch dialect(db) {
	case "mysql":
		return true, db.Exec("DROP DATABASE IF EXISTS " + quoteIdentifier(name)).Error
	default:
		return false, unsupported("drop database", db)
	}
}

// GetSchemas 返回当前连接可见的 schema 列表。
func (b *Builder) GetSchemas() ([]SchemaInfo, error) {
	db, err := b.resolve()
	if err != nil {
		return nil, err
	}
	switch dialect(db) {
	case "mysql":
		var rows []struct{ Name string }
		if err := db.Raw("SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME").Scan(&rows).Error; err != nil {
			return nil, err
		}
		return schemaRows(rows), nil
	case "sqlite", "sqlite3":
		var rows []struct{ Name string }
		if err := db.Raw("PRAGMA database_list").Scan(&rows).Error; err != nil {
			return nil, err
		}
		return schemaRows(rows), nil
	default:
		return nil, unsupported("get schemas", db)
	}
}

// HasView 判断视图是否存在。
func (b *Builder) HasView(view string) bool {
	views, err := b.GetViews(nil)
	if err != nil {
		return false
	}
	for _, item := range views {
		if item.Name == view || schemaQualifiedName(item.Schema, item.Name) == view {
			return true
		}
	}
	return false
}

// GetTables 返回表详情列表。
func (b *Builder) GetTables(schemaFilter any) ([]TableInfo, error) {
	db, err := b.resolve()
	if err != nil {
		return nil, err
	}
	schemas := normalizeSchemas(schemaFilter)
	switch dialect(db) {
	case "mysql":
		current := db.Migrator().CurrentDatabase()
		if len(schemas) == 0 {
			schemas = []string{current}
		}
		var rows []TableInfo
		err := db.Raw(
			"SELECT TABLE_NAME AS name, TABLE_SCHEMA AS `schema`, TABLE_TYPE AS type FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ? ORDER BY TABLE_SCHEMA, TABLE_NAME",
			schemas,
		).Scan(&rows).Error
		return rows, err
	case "sqlite", "sqlite3":
		var rows []TableInfo
		err := db.Raw("SELECT name, 'main' AS `schema`, type FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name").Scan(&rows).Error
		return rows, err
	default:
		return nil, unsupported("get tables", db)
	}
}

// GetTableListing 返回表名列表。
func (b *Builder) GetTableListing(schemaFilter any, schemaQualified ...bool) ([]string, error) {
	tables, err := b.GetTables(schemaFilter)
	if err != nil {
		return nil, err
	}
	qualified := optionalBool(true, schemaQualified...)
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		if qualified {
			names = append(names, schemaQualifiedName(table.Schema, table.Name))
			continue
		}
		names = append(names, table.Name)
	}
	return names, nil
}

// GetViews 返回视图详情列表。
func (b *Builder) GetViews(schemaFilter any) ([]ViewInfo, error) {
	db, err := b.resolve()
	if err != nil {
		return nil, err
	}
	schemas := normalizeSchemas(schemaFilter)
	switch dialect(db) {
	case "mysql":
		if len(schemas) == 0 {
			schemas = []string{db.Migrator().CurrentDatabase()}
		}
		var rows []ViewInfo
		err := db.Raw(
			"SELECT TABLE_NAME AS name, TABLE_SCHEMA AS `schema`, VIEW_DEFINITION AS definition FROM information_schema.VIEWS WHERE TABLE_SCHEMA IN ? ORDER BY TABLE_SCHEMA, TABLE_NAME",
			schemas,
		).Scan(&rows).Error
		return rows, err
	case "sqlite", "sqlite3":
		var rows []ViewInfo
		err := db.Raw("SELECT name, 'main' AS `schema`, sql AS definition FROM sqlite_master WHERE type = 'view' ORDER BY name").Scan(&rows).Error
		return rows, err
	default:
		return nil, unsupported("get views", db)
	}
}

// GetTypes 返回自定义类型列表。MySQL/SQLite 无独立用户类型语义，返回空列表。
func (b *Builder) GetTypes(_ any) ([]TypeInfo, error) {
	db, err := b.resolve()
	if err != nil {
		return nil, err
	}
	switch dialect(db) {
	case "mysql", "sqlite", "sqlite3":
		return []TypeInfo{}, nil
	default:
		return nil, unsupported("get types", db)
	}
}

// HasColumns 判断多个字段是否全部存在。
func (b *Builder) HasColumns(table string, columns []string) bool {
	for _, column := range columns {
		if !b.HasColumn(table, column) {
			return false
		}
	}
	return true
}

// WhenTableHasIndex 在索引存在时执行 fn。
func (b *Builder) WhenTableHasIndex(table string, index any, fn func() error, indexType ...string) error {
	if !b.HasIndex(table, index, indexType...) {
		return nil
	}
	return fn()
}

// WhenTableDoesntHaveIndex 在索引不存在时执行 fn。
func (b *Builder) WhenTableDoesntHaveIndex(table string, index any, fn func() error, indexType ...string) error {
	if b.HasIndex(table, index, indexType...) {
		return nil
	}
	return fn()
}

// GetColumnType 返回字段类型。
func (b *Builder) GetColumnType(table, column string, fullDefinition ...bool) (string, error) {
	columns, err := b.GetColumns(table)
	if err != nil {
		return "", err
	}
	full := optionalBool(false, fullDefinition...)
	for _, item := range columns {
		if item.Name != column {
			continue
		}
		if full && item.FullType != "" {
			return item.FullType, nil
		}
		if item.Type != "" {
			return item.Type, nil
		}
		return item.FullType, nil
	}
	return "", fmt.Errorf("schema: column %s.%s not found", table, column)
}

// GetColumns 返回字段详情列表。
func (b *Builder) GetColumns(table string) ([]ColumnInfo, error) {
	db, err := b.resolve()
	if err != nil {
		return nil, err
	}
	columns, err := db.Migrator().ColumnTypes(table)
	if err != nil {
		return nil, err
	}
	items := make([]ColumnInfo, 0, len(columns))
	for _, column := range columns {
		item := ColumnInfo{Name: column.Name(), Type: column.DatabaseTypeName()}
		if value, ok := column.ColumnType(); ok {
			item.FullType = value
		}
		if value, ok := column.Nullable(); ok {
			item.Nullable = value
		}
		if value, ok := column.DefaultValue(); ok {
			item.Default = value
		}
		if value, ok := column.Comment(); ok {
			item.Comment = value
		}
		if value, ok := column.PrimaryKey(); ok {
			item.Primary = value
		}
		if value, ok := column.AutoIncrement(); ok {
			item.AutoIncrement = value
		}
		if value, ok := column.Unique(); ok {
			item.Unique = value
		}
		if value, ok := column.Length(); ok {
			item.Length = value
		}
		if precision, scale, ok := column.DecimalSize(); ok {
			item.Precision = precision
			item.Scale = scale
		}
		items = append(items, item)
	}
	return items, nil
}

// GetColumnListing 返回字段名列表。
func (b *Builder) GetColumnListing(table string) ([]string, error) {
	columns, err := b.GetColumns(table)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names, nil
}

// GetIndexes 返回索引详情列表。
func (b *Builder) GetIndexes(table string) ([]IndexInfo, error) {
	db, err := b.resolve()
	if err != nil {
		return nil, err
	}
	indexes, err := db.Migrator().GetIndexes(table)
	if err != nil {
		return nil, err
	}
	items := make([]IndexInfo, 0, len(indexes))
	for _, index := range indexes {
		item := IndexInfo{Name: index.Name(), Columns: index.Columns(), Type: "index"}
		if value, ok := index.PrimaryKey(); ok {
			item.Primary = value
		}
		if value, ok := index.Unique(); ok {
			item.Unique = value
		}
		if item.Primary {
			item.Type = "primary"
		} else if item.Unique {
			item.Type = "unique"
		}
		if option := strings.ToLower(index.Option()); strings.Contains(option, "fulltext") {
			item.Type = "fulltext"
		} else if strings.Contains(option, "spatial") {
			item.Type = "spatial"
		}
		items = append(items, item)
	}
	return items, nil
}

// GetIndexListing 返回索引名列表。
func (b *Builder) GetIndexListing(table string) ([]string, error) {
	indexes, err := b.GetIndexes(table)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names, nil
}

// HasIndex 判断索引名或列集合是否存在。
func (b *Builder) HasIndex(table string, index any, indexType ...string) bool {
	db, err := b.resolve()
	if err != nil {
		return false
	}
	if name, ok := index.(string); ok && len(indexType) == 0 && db.Migrator().HasIndex(table, name) {
		return true
	}
	indexes, err := b.GetIndexes(table)
	if err != nil {
		return false
	}
	wantColumns := normalizeIndexColumns(index)
	wantName, _ := index.(string)
	for _, item := range indexes {
		if !matchesIndexType(item, indexType) {
			continue
		}
		if wantName != "" && item.Name == wantName {
			return true
		}
		if len(wantColumns) > 0 && stringSlicesEqual(item.Columns, wantColumns) {
			return true
		}
	}
	return false
}

// GetForeignKeys 返回外键详情列表。
func (b *Builder) GetForeignKeys(table string) ([]ForeignKeyInfo, error) {
	db, err := b.resolve()
	if err != nil {
		return nil, err
	}
	switch dialect(db) {
	case "mysql":
		return b.getMySQLForeignKeys(db, table)
	case "sqlite", "sqlite3":
		return b.getSQLiteForeignKeys(db, table)
	default:
		return nil, unsupported("get foreign keys", db)
	}
}

// DropColumns 删除一组字段。
func (b *Builder) DropColumns(table string, columns ...string) error {
	return b.Table(table, func(blueprint *Blueprint) {
		blueprint.DropColumn(columns...)
	})
}

// DropAllTables 删除当前 schema 下所有表。
func (b *Builder) DropAllTables() error {
	tables, err := b.GetTables(nil)
	if err != nil {
		return err
	}
	return b.WithoutForeignKeyConstraints(func() error {
		for _, table := range tables {
			if err := b.DropIfExists(table.Name); err != nil {
				return err
			}
		}
		return nil
	})
}

// DropAllViews 删除当前 schema 下所有视图。
func (b *Builder) DropAllViews() error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	views, err := b.GetViews(nil)
	if err != nil {
		return err
	}
	for _, view := range views {
		if err := db.Migrator().DropView(view.Name); err != nil {
			return err
		}
	}
	return nil
}

// DropAllTypes 删除自定义类型。MySQL/SQLite 无独立用户类型语义，保持空操作。
func (b *Builder) DropAllTypes() error {
	_, err := b.GetTypes(nil)
	return err
}

// EnsureVectorExtensionExists 确保向量扩展存在。MySQL/SQLite 不支持扩展管理。
func (b *Builder) EnsureVectorExtensionExists(schemaName ...string) error {
	return b.EnsureExtensionExists("vector", schemaName...)
}

// EnsureExtensionExists 确保扩展存在。MySQL/SQLite 不支持扩展管理。
func (b *Builder) EnsureExtensionExists(name string, _ ...string) error {
	db, err := b.resolve()
	if err != nil {
		return err
	}
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("schema: extension name is required")
	}
	return unsupported("ensure extension exists", db)
}

func (b *Builder) getMySQLForeignKeys(db *gorm.DB, table string) ([]ForeignKeyInfo, error) {
	var rows []struct {
		Name          string
		ColumnName    string
		ForeignTable  string
		ForeignColumn string
		OnUpdate      string
		OnDelete      string
	}
	err := db.Raw(`
SELECT k.CONSTRAINT_NAME AS name, k.COLUMN_NAME AS column_name, k.REFERENCED_TABLE_NAME AS foreign_table,
       k.REFERENCED_COLUMN_NAME AS foreign_column, r.UPDATE_RULE AS on_update, r.DELETE_RULE AS on_delete
FROM information_schema.KEY_COLUMN_USAGE k
JOIN information_schema.REFERENTIAL_CONSTRAINTS r
  ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
WHERE k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = ? AND k.REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION`, table).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	grouped := map[string]*ForeignKeyInfo{}
	for _, row := range rows {
		item := grouped[row.Name]
		if item == nil {
			item = &ForeignKeyInfo{Name: row.Name, ForeignTable: row.ForeignTable, OnUpdate: row.OnUpdate, OnDelete: row.OnDelete}
			grouped[row.Name] = item
		}
		item.Columns = append(item.Columns, row.ColumnName)
		item.ForeignColumns = append(item.ForeignColumns, row.ForeignColumn)
	}
	return foreignMapValues(grouped), nil
}

func (b *Builder) getSQLiteForeignKeys(db *gorm.DB, table string) ([]ForeignKeyInfo, error) {
	rows, err := db.Raw("PRAGMA foreign_key_list(" + quoteIdentifier(table) + ")").Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grouped := map[string]*ForeignKeyInfo{}
	for rows.Next() {
		var (
			id, seq            int
			refTable, from, to string
			onUpdate, onDelete string
			match              sql.NullString
		)
		if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		name := fmt.Sprintf("fk_%s_%d", table, id)
		item := grouped[name]
		if item == nil {
			item = &ForeignKeyInfo{Name: name, ForeignTable: refTable, OnUpdate: onUpdate, OnDelete: onDelete}
			grouped[name] = item
		}
		item.Columns = append(item.Columns, from)
		item.ForeignColumns = append(item.ForeignColumns, to)
	}
	return foreignMapValues(grouped), rows.Err()
}

func schemaRows(rows []struct{ Name string }) []SchemaInfo {
	out := make([]SchemaInfo, 0, len(rows))
	for _, row := range rows {
		out = append(out, SchemaInfo{Name: row.Name})
	}
	return out
}

func normalizeSchemas(value any) []string {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []string{v}
	case []string:
		return v
	default:
		return nil
	}
}

func normalizeIndexColumns(index any) []string {
	switch v := index.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, fmt.Sprint(item))
		}
		return out
	default:
		return nil
	}
}

func matchesIndexType(index IndexInfo, types []string) bool {
	if len(types) == 0 || strings.TrimSpace(types[0]) == "" {
		return true
	}
	want := strings.ToLower(strings.TrimSpace(types[0]))
	if want == "index" && index.Type == "" {
		return true
	}
	return strings.ToLower(index.Type) == want
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func schemaQualifiedName(schemaName, name string) string {
	if schemaName == "" {
		return name
	}
	return schemaName + "." + name
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func foreignMapValues(values map[string]*ForeignKeyInfo) []ForeignKeyInfo {
	out := make([]ForeignKeyInfo, 0, len(values))
	for _, value := range values {
		out = append(out, *value)
	}
	return out
}
