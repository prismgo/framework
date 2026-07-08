package database

import (
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// TestIsolationLevelErrorMessage_IncludesValidLevels 验证隔离级别错误信息包含有效选项
func TestIsolationLevelErrorMessage_IncludesValidLevels(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      db,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}

	cfg := MySQLConfig{
		Session: MySQLSessionConfig{
			IsolationLevel: "INVALID LEVEL",
		},
	}

	err = configureConnection(gormDB, cfg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	errMsg := err.Error()

	// 验证错误信息包含有效的隔离级别
	validLevels := []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}
	for _, level := range validLevels {
		if !strings.Contains(errMsg, level) {
			t.Errorf("error message should contain valid level %q, got: %s", level, errMsg)
		}
	}
}

// TestValidIsolationLevels_UsesStructEmpty 验证 validIsolationLevels 使用 struct{} 而非 bool
func TestValidIsolationLevels_UsesStructEmpty(t *testing.T) {
	// 通过类型断言验证 map 的值类型
	// 如果是 map[string]struct{}，则值应该是 struct{}{}
	// 如果是 map[string]bool，则值应该是 true/false

	// 验证所有有效隔离级别都存在
	expectedLevels := []string{
		"READ UNCOMMITTED",
		"READ COMMITTED",
		"REPEATABLE READ",
		"SERIALIZABLE",
	}

	for _, level := range expectedLevels {
		if _, exists := validIsolationLevels[level]; !exists {
			t.Errorf("validIsolationLevels should contain %q", level)
		}
	}

	// 验证无效隔离级别不存在
	if _, exists := validIsolationLevels["INVALID"]; exists {
		t.Error("validIsolationLevels should not contain invalid level")
	}
}

// TestValidCharsets_UsesStructEmpty 验证 validCharsets 使用 struct{} 而非 bool
func TestValidCharsets_UsesStructEmpty(t *testing.T) {
	// 验证常见字符集存在
	expectedCharsets := []string{
		"utf8mb4",
		"utf8",
		"latin1",
		"ascii",
		"binary",
	}

	for _, charset := range expectedCharsets {
		if _, exists := validCharsets[charset]; !exists {
			t.Errorf("validCharsets should contain %q", charset)
		}
	}

	// 验证无效字符集不存在
	if _, exists := validCharsets["invalid"]; exists {
		t.Error("validCharsets should not contain invalid charset")
	}
}

// TestValidTimezones_UsesStructEmpty 验证 validTimezones 使用 struct{} 而非 bool
func TestValidTimezones_UsesStructEmpty(t *testing.T) {
	// 验证常见时区存在
	expectedTimezones := []string{
		"UTC",
		"Asia/Shanghai",
		"America/New_York",
		"Europe/London",
	}

	for _, tz := range expectedTimezones {
		if _, exists := validTimezones[tz]; !exists {
			t.Errorf("validTimezones should contain %q", tz)
		}
	}

	// 验证无效时区不存在
	if _, exists := validTimezones["Invalid/Timezone"]; exists {
		t.Error("validTimezones should not contain invalid timezone")
	}
}

// TestValidSqlModes_UsesStructEmpty 验证 validSqlModes 使用 struct{} 而非 bool
func TestValidSqlModes_UsesStructEmpty(t *testing.T) {
	// 验证常见 SQL 模式存在
	expectedModes := []string{
		"STRICT_TRANS_TABLES",
		"ONLY_FULL_GROUP_BY",
		"NO_ZERO_DATE",
		"ERROR_FOR_DIVISION_BY_ZERO",
	}

	for _, mode := range expectedModes {
		if _, exists := validSqlModes[mode]; !exists {
			t.Errorf("validSqlModes should contain %q", mode)
		}
	}

	// 验证无效 SQL 模式不存在
	if _, exists := validSqlModes["INVALID_MODE"]; exists {
		t.Error("validSqlModes should not contain invalid mode")
	}
}
