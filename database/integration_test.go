package database

import (
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prismgo/framework/internal/version"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// TestIntegration_MySQLVersionAndIsolationLevels 集成测试：验证不同 MySQL 版本和隔离级别的完整修复效果
// 覆盖场景：
// 1. MySQL 5.7.x（旧版本，需要 NO_AUTO_CREATE_USER）
// 2. MySQL 8.0.10（刚好低于 8.0.11 临界点）
// 3. MySQL 8.0.11（临界版本，移除 NO_AUTO_CREATE_USER）
// 4. MySQL 8.0.32（常见版本）
// 5. MySQL 9.0.0（新版本）
// 6. 带后缀的版本号（如 Ubuntu 定制版本）
// 7. 所有合法的隔离级别
// 8. 非法隔离级别的拒绝
func TestIntegration_MySQLVersionAndIsolationLevels(t *testing.T) {
	// 定义测试矩阵：MySQL 版本 × 隔离级别
	testMatrix := []struct {
		name            string
		mysqlVersion    string
		isolationLevel  string
		expectSqlMode   string
		expectIsolation string
		expectError     bool
		errorContains   string
	}{
		// MySQL 5.7.40（旧版本）+ 各种隔离级别
		{
			name:            "MySQL 5.7.40 with READ UNCOMMITTED",
			mysqlVersion:    "5.7.40",
			isolationLevel:  "READ UNCOMMITTED",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ UNCOMMITTED",
		},
		{
			name:            "MySQL 5.7.40 with READ COMMITTED",
			mysqlVersion:    "5.7.40",
			isolationLevel:  "READ COMMITTED",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ COMMITTED",
		},
		{
			name:            "MySQL 5.7.40 with REPEATABLE READ",
			mysqlVersion:    "5.7.40",
			isolationLevel:  "REPEATABLE READ",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "REPEATABLE READ",
		},
		{
			name:            "MySQL 5.7.40 with SERIALIZABLE",
			mysqlVersion:    "5.7.40",
			isolationLevel:  "SERIALIZABLE",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "SERIALIZABLE",
		},

		// MySQL 8.0.10（刚好低于 8.0.11）
		{
			name:            "MySQL 8.0.10 with READ COMMITTED",
			mysqlVersion:    "8.0.10",
			isolationLevel:  "READ COMMITTED",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ COMMITTED",
		},

		// MySQL 8.0.11（临界版本，移除 NO_AUTO_CREATE_USER）
		{
			name:            "MySQL 8.0.11 with READ COMMITTED",
			mysqlVersion:    "8.0.11",
			isolationLevel:  "READ COMMITTED",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ COMMITTED",
		},

		// MySQL 8.0.32（常见版本）+ 各种隔离级别
		{
			name:            "MySQL 8.0.32 with READ UNCOMMITTED",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "READ UNCOMMITTED",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ UNCOMMITTED",
		},
		{
			name:            "MySQL 8.0.32 with READ COMMITTED",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "READ COMMITTED",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ COMMITTED",
		},
		{
			name:            "MySQL 8.0.32 with REPEATABLE READ",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "REPEATABLE READ",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "REPEATABLE READ",
		},
		{
			name:            "MySQL 8.0.32 with SERIALIZABLE",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "SERIALIZABLE",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "SERIALIZABLE",
		},

		// MySQL 9.0.0（新版本）
		{
			name:            "MySQL 9.0.0 with READ COMMITTED",
			mysqlVersion:    "9.0.0",
			isolationLevel:  "READ COMMITTED",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ COMMITTED",
		},

		// 带后缀的版本号（Ubuntu 定制版本）
		{
			name:            "MySQL 8.0.32 Ubuntu with READ COMMITTED",
			mysqlVersion:    "8.0.32-0ubuntu0.20.04.1",
			isolationLevel:  "READ COMMITTED",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ COMMITTED",
		},

		// 大小写不敏感测试
		{
			name:            "MySQL 8.0.32 with lowercase read committed",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "read committed",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ COMMITTED",
		},
		{
			name:            "MySQL 8.0.32 with mixed case Read Committed",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "Read Committed",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "READ COMMITTED",
		},

		// 非法隔离级别测试
		{
			name:            "MySQL 8.0.32 with INVALID LEVEL",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "INVALID LEVEL",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "", // 不会执行，因为会在检查时被拒绝
			expectError:     true,
			errorContains:   "invalid isolation level",
		},
		{
			name:            "MySQL 8.0.32 with SNAPSHOT",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "SNAPSHOT",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "", // 不会执行，因为会在检查时被拒绝
			expectError:     true,
			errorContains:   "invalid isolation level",
		},
		{
			name:            "MySQL 8.0.32 with SQL injection attempt",
			mysqlVersion:    "8.0.32",
			isolationLevel:  "READ COMMITTED; DROP TABLE users;",
			expectSqlMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			expectIsolation: "", // 不会执行，因为会在检查时被拒绝
			expectError:     true,
			errorContains:   "invalid isolation level",
		},
	}

	for _, tt := range testMatrix {
		t.Run(tt.name, func(t *testing.T) {
			// 创建 sqlmock
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("open sqlmock: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			// 创建 GORM DB
			gormDB, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      db,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{})
			if err != nil {
				t.Fatalf("open gorm: %v", err)
			}

			// 只有在不会提前失败的情况下才设置 SQL 期望
			if !tt.expectError {
				// Mock SELECT VERSION() 返回指定的 MySQL 版本
				versionRows := sqlmock.NewRows([]string{"VERSION()"}).AddRow(tt.mysqlVersion)
				mock.ExpectQuery("SELECT VERSION\\(\\)").WillReturnRows(versionRows)

				// Mock SET SESSION sql_mode
				mock.ExpectExec("SET SESSION sql_mode='" + tt.expectSqlMode + "'").
					WillReturnResult(sqlmock.NewResult(0, 0))

				// 如果有期望的隔离级别，Mock SET SESSION TRANSACTION ISOLATION LEVEL
				if tt.expectIsolation != "" {
					mock.ExpectExec("SET SESSION TRANSACTION ISOLATION LEVEL " + tt.expectIsolation).
						WillReturnResult(sqlmock.NewResult(0, 0))
				}
			}

			// 执行 configureConnection
			err = configureConnection(gormDB, MySQLConfig{
				Session: MySQLSessionConfig{
					Strict:         true,
					IsolationLevel: tt.isolationLevel,
				},
			})

			// 验证结果
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errorContains)
				}
				if !strings.Contains(err.Error(), tt.errorContains) {
					t.Fatalf("expected error containing %q, got: %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
			}

			// 验证所有期望都被满足
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestIntegration_VersionComparisonEdgeCases 集成测试：版本号比较的边界情况
func TestIntegration_VersionComparisonEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		version string
		target  string
		want    bool
		desc    string
	}{
		// 临界点测试
		{
			name:    "8.0.10 vs 8.0.11",
			version: "8.0.10",
			target:  "8.0.11",
			want:    false,
			desc:    "8.0.10 应该小于 8.0.11（临界点）",
		},
		{
			name:    "8.0.11 vs 8.0.11",
			version: "8.0.11",
			target:  "8.0.11",
			want:    true,
			desc:    "8.0.11 应该等于 8.0.11（临界点）",
		},
		{
			name:    "8.0.12 vs 8.0.11",
			version: "8.0.12",
			target:  "8.0.11",
			want:    true,
			desc:    "8.0.12 应该大于 8.0.11（临界点）",
		},

		// 双位数版本号
		{
			name:    "8.0.9 vs 8.0.11",
			version: "8.0.9",
			target:  "8.0.11",
			want:    false,
			desc:    "8.0.9 应该小于 8.0.11（双位数比较）",
		},
		{
			name:    "8.0.99 vs 8.0.11",
			version: "8.0.99",
			target:  "8.0.11",
			want:    true,
			desc:    "8.0.99 应该大于 8.0.11（双位数比较）",
		},
		{
			name:    "10.0.0 vs 8.0.11",
			version: "10.0.0",
			target:  "8.0.11",
			want:    true,
			desc:    "10.0.0 应该大于 8.0.11（主版本号比较）",
		},

		// 带后缀的版本号
		{
			name:    "8.0.32-0ubuntu0.20.04.1 vs 8.0.11",
			version: "8.0.32-0ubuntu0.20.04.1",
			target:  "8.0.11",
			want:    true,
			desc:    "Ubuntu 定制版本 8.0.32 应该大于 8.0.11",
		},
		{
			name:    "8.0.10-log vs 8.0.11",
			version: "8.0.10-log",
			target:  "8.0.11",
			want:    false,
			desc:    "带 -log 后缀的 8.0.10 应该小于 8.0.11",
		},

		// 空值测试
		{
			name:    "empty version vs 8.0.11",
			version: "",
			target:  "8.0.11",
			want:    false,
			desc:    "空版本号应该小于任何目标版本",
		},
		{
			name:    "8.0.32 vs empty target",
			version: "8.0.32",
			target:  "",
			want:    true,
			desc:    "空目标版本应该返回 true",
		},
		{
			name:    "both empty",
			version: "",
			target:  "",
			want:    true,
			desc:    "两个空版本应该相等",
		},

		// 无效格式
		{
			name:    "invalid vs 8.0.11",
			version: "invalid",
			target:  "8.0.11",
			want:    false,
			desc:    "无效版本号应该返回 false",
		},
		{
			name:    "8.0.32 vs invalid",
			version: "8.0.32",
			target:  "invalid",
			want:    true,
			desc:    "无效目标版本应该返回 true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := version.AtLeast(tt.version, tt.target)
			if got != tt.want {
				t.Errorf("%s: version.AtLeast(%q, %q) = %v, want %v",
					tt.desc, tt.version, tt.target, got, tt.want)
			}
		})
	}
}

// TestIntegration_SqlModeGeneration 集成测试：不同版本下 SQL 模式的生成
func TestIntegration_SqlModeGeneration(t *testing.T) {
	tests := []struct {
		name          string
		mysqlVersion  string
		strict        bool
		customModes   []string
		expectSqlMode string
	}{
		// 自定义 Modes 优先级最高
		{
			name:          "custom modes override version",
			mysqlVersion:  "5.7.40",
			strict:        true,
			customModes:   []string{"STRICT_TRANS_TABLES", "NO_ZERO_DATE"},
			expectSqlMode: "STRICT_TRANS_TABLES,NO_ZERO_DATE",
		},

		// 非严格模式
		{
			name:          "non-strict mode",
			mysqlVersion:  "8.0.32",
			strict:        false,
			customModes:   nil,
			expectSqlMode: "NO_ENGINE_SUBSTITUTION",
		},

		// 严格模式 + 旧版本（5.7.x）
		{
			name:          "strict mode MySQL 5.7.40",
			mysqlVersion:  "5.7.40",
			strict:        true,
			customModes:   nil,
			expectSqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
		},

		// 严格模式 + 临界版本（8.0.10）
		{
			name:          "strict mode MySQL 8.0.10",
			mysqlVersion:  "8.0.10",
			strict:        true,
			customModes:   nil,
			expectSqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
		},

		// 严格模式 + 临界版本（8.0.11）
		{
			name:          "strict mode MySQL 8.0.11",
			mysqlVersion:  "8.0.11",
			strict:        true,
			customModes:   nil,
			expectSqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		},

		// 严格模式 + 新版本（8.0.32）
		{
			name:          "strict mode MySQL 8.0.32",
			mysqlVersion:  "8.0.32",
			strict:        true,
			customModes:   nil,
			expectSqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		},

		// 严格模式 + 新版本（9.0.0）
		{
			name:          "strict mode MySQL 9.0.0",
			mysqlVersion:  "9.0.0",
			strict:        true,
			customModes:   nil,
			expectSqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建 sqlmock
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("open sqlmock: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			// 创建 GORM DB
			gormDB, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      db,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{})
			if err != nil {
				t.Fatalf("open gorm: %v", err)
			}

			// 只有在 strict=true 且无自定义 Modes 时才会查询版本
			if tt.strict && len(tt.customModes) == 0 {
				versionRows := sqlmock.NewRows([]string{"VERSION()"}).AddRow(tt.mysqlVersion)
				mock.ExpectQuery("SELECT VERSION\\(\\)").WillReturnRows(versionRows)
			}

			// 调用 getSqlMode
			cfg := MySQLConfig{
				Session: MySQLSessionConfig{
					Strict: tt.strict,
					Modes:  tt.customModes,
				},
			}
			gotSqlMode := getSqlMode(cfg, gormDB)

			// 验证 SQL 模式
			if gotSqlMode != tt.expectSqlMode {
				t.Errorf("getSqlMode() = %q, want %q", gotSqlMode, tt.expectSqlMode)
			}

			// 验证所有期望都被满足
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unfulfilled expectations: %v", err)
			}
		})
	}
}
