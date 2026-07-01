package schema

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupHasColumnsTestDB(t *testing.T) *Builder {
	t.Helper()
	// 使用唯一数据库名避免测试间共享状态
	dbName := "file:" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// 创建测试表
	if err := db.Exec("CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)").Error; err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	return New(db)
}

func TestHasColumns_AllExist(t *testing.T) {
	builder := setupHasColumnsTestDB(t)

	// 所有列都存在
	if !builder.HasColumns("test_users", []string{"id", "name", "email"}) {
		t.Error("expected all columns to exist")
	}
}

func TestHasColumns_SomeMissing(t *testing.T) {
	builder := setupHasColumnsTestDB(t)

	// phone 列不存在
	if builder.HasColumns("test_users", []string{"id", "name", "phone"}) {
		t.Error("expected HasColumns to return false when column missing")
	}
}

func TestHasColumns_EmptyList(t *testing.T) {
	builder := setupHasColumnsTestDB(t)

	// 空列表应该返回 true
	if !builder.HasColumns("test_users", []string{}) {
		t.Error("expected HasColumns to return true for empty column list")
	}
}

func TestHasColumns_TableNotExists(t *testing.T) {
	builder := setupHasColumnsTestDB(t)

	// 表不存在应该返回 false
	if builder.HasColumns("non_existent_table", []string{"id"}) {
		t.Error("expected HasColumns to return false for non-existent table")
	}
}

func TestHasColumns_SingleColumn(t *testing.T) {
	builder := setupHasColumnsTestDB(t)

	// 单列存在
	if !builder.HasColumns("test_users", []string{"email"}) {
		t.Error("expected single column to exist")
	}

	// 单列不存在
	if builder.HasColumns("test_users", []string{"phone"}) {
		t.Error("expected single column to not exist")
	}
}
