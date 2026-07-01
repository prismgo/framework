package schema

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *Builder {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// 创建测试表
	if err := db.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").Error; err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// 创建索引
	if err := db.Exec("CREATE INDEX idx_name ON test_table(name)").Error; err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
	if err := db.Exec("CREATE UNIQUE INDEX idx_email ON test_table(email)").Error; err != nil {
		t.Fatalf("failed to create unique index: %v", err)
	}

	return New(db)
}

func TestHasIndex_WithStringAndType(t *testing.T) {
	builder := setupTestDB(t)

	tests := []struct {
		name      string
		table     string
		index     any
		indexType []string
		want      bool
	}{
		{
			name:      "string index name without type - should find idx_name",
			table:     "test_table",
			index:     "idx_name",
			indexType: nil,
			want:      true,
		},
		{
			name:      "string index name with matching type - should find idx_email as unique",
			table:     "test_table",
			index:     "idx_email",
			indexType: []string{"unique"},
			want:      true,
		},
		{
			name:      "string index name with non-matching type - idx_name is not unique",
			table:     "test_table",
			index:     "idx_name",
			indexType: []string{"unique"},
			want:      false,
		},
		{
			name:      "string index name with wrong type - idx_email is unique not index",
			table:     "test_table",
			index:     "idx_email",
			indexType: []string{"index"},
			want:      false,
		},
		{
			name:      "column list without type - should find idx_name",
			table:     "test_table",
			index:     []string{"name"},
			indexType: nil,
			want:      true,
		},
		{
			name:      "column list with matching type - should find idx_email",
			table:     "test_table",
			index:     []string{"email"},
			indexType: []string{"unique"},
			want:      true,
		},
		{
			name:      "column list with non-matching type - idx_name is not unique",
			table:     "test_table",
			index:     []string{"name"},
			indexType: []string{"unique"},
			want:      false,
		},
		{
			name:      "non-existent index",
			table:     "test_table",
			index:     "idx_nonexistent",
			indexType: nil,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := builder.HasIndex(tt.table, tt.index, tt.indexType...)
			if got != tt.want {
				t.Errorf("HasIndex(%q, %v, %v) = %v, want %v",
					tt.table, tt.index, tt.indexType, got, tt.want)
			}
		})
	}
}
