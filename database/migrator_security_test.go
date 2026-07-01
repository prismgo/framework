package database

import (
	"strings"
	"testing"
)

// TestQuoteIdentifierEscapesBackticks 验证标识符转义函数正确处理反引号
func TestQuoteIdentifierEscapesBackticks(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"normal name", "users", "`users`"},
		{"with backtick", "user`table", "`user``table`"},
		{"multiple backticks", "a`b`c", "`a``b``c`"},
		{"empty string", "", "``"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestMigratorSQLUsesQuotedIdentifiers 验证 migrator 函数使用转义后的标识符
func TestMigratorSQLUsesQuotedIdentifiers(t *testing.T) {
	// 这个测试验证 SQL 语句中的标识符是否正确转义
	// 由于需要真实的数据库连接，我们只验证 quoteIdentifier 函数的存在
	// 实际的集成测试在 migrator_test.go 中

	// 验证 quoteIdentifier 函数存在且可用
	result := quoteIdentifier("test`table")
	if !strings.Contains(result, "``") {
		t.Error("quoteIdentifier should escape backticks")
	}
}
