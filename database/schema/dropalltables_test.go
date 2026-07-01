package schema

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupDropAllTablesTestDB(t *testing.T) *Builder {
	t.Helper()
	dbName := "file:" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// 创建多个测试表
	if err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").Error; err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}
	if err := db.Exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)").Error; err != nil {
		t.Fatalf("failed to create posts table: %v", err)
	}
	if err := db.Exec("CREATE TABLE comments (id INTEGER PRIMARY KEY, content TEXT)").Error; err != nil {
		t.Fatalf("failed to create comments table: %v", err)
	}

	return New(db)
}

func TestDropAllTables_RemovesAllTables(t *testing.T) {
	builder := setupDropAllTablesTestDB(t)

	// 验证表存在
	if !builder.HasTable("users") {
		t.Fatal("users table should exist before drop")
	}
	if !builder.HasTable("posts") {
		t.Fatal("posts table should exist before drop")
	}
	if !builder.HasTable("comments") {
		t.Fatal("comments table should exist before drop")
	}

	// 删除所有表
	if err := builder.DropAllTables(); err != nil {
		t.Fatalf("DropAllTables failed: %v", err)
	}

	// 验证所有表已被删除
	if builder.HasTable("users") {
		t.Error("users table should be dropped")
	}
	if builder.HasTable("posts") {
		t.Error("posts table should be dropped")
	}
	if builder.HasTable("comments") {
		t.Error("comments table should be dropped")
	}
}

func TestDropAllTables_EmptyDatabase(t *testing.T) {
	dbName := "file:" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	builder := New(db)

	// 空数据库应该不报错
	if err := builder.DropAllTables(); err != nil {
		t.Fatalf("DropAllTables on empty database should not fail: %v", err)
	}
}
