package schema

import (
	"github.com/prismgo/framework/facade"
	"gorm.io/gorm"
)

const serviceKey = "database.schema"

// Resolve 从当前 Application 容器解析 Schema Builder。
func Resolve() *Builder {
	return facade.Resolve[*Builder](serviceKey)
}

func DefaultStringLength(length int) {
	setDefaultStringLength(length)
}

func DefaultTimePrecision(precision *int) {
	setDefaultTimePrecision(precision)
}

func DefaultMorphKeyType(kind string) {
	setDefaultMorphKeyType(kind)
}

func MorphUsingUuids() {
	setDefaultMorphKeyType("uuid")
}

func MorphUsingUlids() {
	setDefaultMorphKeyType("ulid")
}

func Bind(db *gorm.DB) *Builder {
	return New(db)
}

func Connection(name string) *Builder {
	return Resolve().Connection(name)
}

func CreateDatabase(name string) (bool, error) {
	return Resolve().CreateDatabase(name)
}

func DropDatabaseIfExists(name string) (bool, error) {
	return Resolve().DropDatabaseIfExists(name)
}

func GetSchemas() ([]SchemaInfo, error) {
	return Resolve().GetSchemas()
}

func HasTable(table string) bool {
	return Resolve().HasTable(table)
}

func HasView(view string) bool {
	return Resolve().HasView(view)
}

func GetTables(schemaFilter any) ([]TableInfo, error) {
	return Resolve().GetTables(schemaFilter)
}

func GetTableListing(schemaFilter any, schemaQualified ...bool) ([]string, error) {
	return Resolve().GetTableListing(schemaFilter, schemaQualified...)
}

func GetViews(schemaFilter any) ([]ViewInfo, error) {
	return Resolve().GetViews(schemaFilter)
}

func GetTypes(schemaFilter any) ([]TypeInfo, error) {
	return Resolve().GetTypes(schemaFilter)
}

func HasColumn(table, column string) bool {
	return Resolve().HasColumn(table, column)
}

func HasColumns(table string, columns []string) bool {
	return Resolve().HasColumns(table, columns)
}

func WhenTableHasColumn(table, column string, fn func() error) error {
	return Resolve().WhenTableHasColumn(table, column, fn)
}

func WhenTableDoesntHaveColumn(table, column string, fn func() error) error {
	return Resolve().WhenTableDoesntHaveColumn(table, column, fn)
}

func WhenTableHasIndex(table string, index any, fn func() error, indexType ...string) error {
	return Resolve().WhenTableHasIndex(table, index, fn, indexType...)
}

func WhenTableDoesntHaveIndex(table string, index any, fn func() error, indexType ...string) error {
	return Resolve().WhenTableDoesntHaveIndex(table, index, fn, indexType...)
}

func GetColumnType(table, column string, fullDefinition ...bool) (string, error) {
	return Resolve().GetColumnType(table, column, fullDefinition...)
}

func GetColumnListing(table string) ([]string, error) {
	return Resolve().GetColumnListing(table)
}

func GetColumns(table string) ([]ColumnInfo, error) {
	return Resolve().GetColumns(table)
}

func GetIndexes(table string) ([]IndexInfo, error) {
	return Resolve().GetIndexes(table)
}

func GetIndexListing(table string) ([]string, error) {
	return Resolve().GetIndexListing(table)
}

func HasIndex(table string, index any, indexType ...string) bool {
	return Resolve().HasIndex(table, index, indexType...)
}

func GetForeignKeys(table string) ([]ForeignKeyInfo, error) {
	return Resolve().GetForeignKeys(table)
}

func Create(table string, fn func(*Blueprint)) error {
	return Resolve().Create(table, fn)
}

func Table(table string, fn func(*Blueprint)) error {
	return Resolve().Table(table, fn)
}

func Drop(table string) error {
	return Resolve().Drop(table)
}

func DropIfExists(table string) error {
	return Resolve().DropIfExists(table)
}

func DropColumns(table string, columns ...string) error {
	return Resolve().DropColumns(table, columns...)
}

func DropAllTables() error {
	return Resolve().DropAllTables()
}

func DropAllViews() error {
	return Resolve().DropAllViews()
}

func DropAllTypes() error {
	return Resolve().DropAllTypes()
}

func Rename(from, to string) error {
	return Resolve().Rename(from, to)
}

func EnableForeignKeyConstraints() error {
	return Resolve().EnableForeignKeyConstraints()
}

func DisableForeignKeyConstraints() error {
	return Resolve().DisableForeignKeyConstraints()
}

func WithoutForeignKeyConstraints(fn func() error) error {
	return Resolve().WithoutForeignKeyConstraints(fn)
}

func EnsureVectorExtensionExists(schemaName ...string) error {
	return Resolve().EnsureVectorExtensionExists(schemaName...)
}

func EnsureExtensionExists(name string, schemaName ...string) error {
	return Resolve().EnsureExtensionExists(name, schemaName...)
}

func SyncModels(models ...any) error {
	return Resolve().SyncModels(models...)
}
