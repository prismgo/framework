package database

import (
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// TestConfigureConnection_ValidatesBeforeSQL 验证 configureConnection 在执行任何 SQL 之前进行验证
// 这样即使验证失败，也不会留下未关闭的连接
func TestConfigureConnection_ValidatesBeforeSQL(t *testing.T) {
	tests := []struct {
		name        string
		cfg         MySQLConfig
		errContains string
	}{
		{
			name: "invalid charset",
			cfg: MySQLConfig{
				Session: MySQLSessionConfig{
					Charset:   "invalid_charset",
					Collation: "utf8mb4_unicode_ci",
				},
			},
			errContains: "invalid charset",
		},
		{
			name: "invalid collation prefix",
			cfg: MySQLConfig{
				Session: MySQLSessionConfig{
					Charset:   "utf8mb4",
					Collation: "utf8_unicode_ci",
				},
			},
			errContains: "invalid collation",
		},
		{
			name: "invalid timezone",
			cfg: MySQLConfig{
				Session: MySQLSessionConfig{
					Timezone: "Invalid/Timezone",
				},
			},
			errContains: "invalid timezone",
		},
		{
			name: "invalid isolation level",
			cfg: MySQLConfig{
				Session: MySQLSessionConfig{
					IsolationLevel: "INVALID LEVEL",
				},
			},
			errContains: "invalid isolation level",
		},
		{
			name: "invalid sql mode",
			cfg: MySQLConfig{
				Session: MySQLSessionConfig{
					Modes: []string{"INVALID_MODE"},
				},
			},
			errContains: "invalid sql mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
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

			// 不设置任何 mock 期望
			// 如果验证在 SQL 之前执行，应该立即失败且没有未满足的期望
			err = configureConnection(gormDB, tt.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
			}

			// 验证没有执行任何 SQL（所有期望都应满足）
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unexpected SQL executed: %v", err)
			}
		})
	}
}

// TestOpen_RejectsUnsupportedDriver 验证 Open 函数拒绝不支持的驱动
func TestOpen_RejectsUnsupportedDriver(t *testing.T) {
	db, err := Open("sqlite", "file::memory:?cache=shared", MySQLConfig{})
	if err == nil {
		t.Fatal("expected error for unsupported driver, got nil")
	}
	if db != nil {
		t.Error("expected nil db for unsupported driver")
	}
	if !strings.Contains(err.Error(), "unsupported driver") {
		t.Errorf("expected 'unsupported driver' error, got %q", err.Error())
	}
}
