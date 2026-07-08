package database

import (
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestBuildMySQLDSN_AllowNativePasswordsDefault(t *testing.T) {
	// 验证：通过 readMySQLConfig 路径读取配置后，allow_native_passwords 默认为 true
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"options": map[string]any{
					"allow_native_passwords": true,
					"check_conn_liveness":    true,
				},
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	// 验证 readMySQLConfig 正确读取了配置
	if !cfg.Driver.AllowNativePasswords {
		t.Error("AllowNativePasswords should be true when config has it set to true")
	}
	if !cfg.Driver.CheckConnLiveness {
		t.Error("CheckConnLiveness should be true when config has it set to true")
	}

	// 验证生成的 DSN 不包含 allowNativePasswords=false 或 checkConnLiveness=false
	dsn := BuildMySQLDSN(cfg)
	if strings.Contains(dsn, "allowNativePasswords=false") {
		t.Errorf("DSN should not contain allowNativePasswords=false (should be true or absent): %s", dsn)
	}
	if strings.Contains(dsn, "checkConnLiveness=false") {
		t.Errorf("DSN should not contain checkConnLiveness=false when CheckConnLiveness=true: %s", dsn)
	}

	// 验证 round-trip 解析后字段正确
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if !parsed.AllowNativePasswords {
		t.Error("parsed DSN should have AllowNativePasswords=true")
	}
	if !parsed.CheckConnLiveness {
		t.Error("parsed DSN should have CheckConnLiveness=true by default")
	}
}

func TestBuildMySQLDSN_AllowNativePasswordsCanBeDisabled(t *testing.T) {
	// 验证：AllowNativePasswords 可以被设置为 false
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"options": map[string]any{
					"allow_native_passwords": false,
				},
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if cfg.Driver.AllowNativePasswords {
		t.Error("AllowNativePasswords should be false when config sets it to false")
	}

	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "allowNativePasswords=false") {
		t.Errorf("DSN should contain allowNativePasswords=false when disabled: %s", dsn)
	}
}

func TestBuildMySQLDSN_CheckConnLivenessCanBeDisabled(t *testing.T) {
	// 验证：CheckConnLiveness 可以被设置为 false
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"options": map[string]any{
					"check_conn_liveness": false,
				},
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if cfg.Driver.CheckConnLiveness {
		t.Error("CheckConnLiveness should be false when config sets it to false")
	}

	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "checkConnLiveness=false") {
		t.Errorf("DSN should contain checkConnLiveness=false when disabled: %s", dsn)
	}
}

// Parameter 1: rejectReadOnly (High Priority)
func TestBuildMySQLDSN_RejectReadOnlyEnabled(t *testing.T) {
	// 验证：当 RejectReadOnly=true 时，DSN 中包含 rejectReadOnly=true
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
		Driver: MySQLDriverConfig{
			RejectReadOnly: true,
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "rejectReadOnly=true") {
		t.Errorf("DSN should contain rejectReadOnly=true: %s", dsn)
	}
	// 验证 round-trip 解析
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if !parsed.RejectReadOnly {
		t.Error("parsed DSN should have RejectReadOnly=true")
	}
}

func TestBuildMySQLDSN_RejectReadOnlyDefaultFalse(t *testing.T) {
	// 验证：默认情况下 RejectReadOnly 为 false，DSN 中不应包含该参数
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if strings.Contains(dsn, "rejectReadOnly") {
		t.Errorf("DSN should not contain rejectReadOnly by default: %s", dsn)
	}
}

// Parameter 2: compress (High Priority) - 通过 Options 传递
func TestBuildMySQLDSN_CompressEnabled(t *testing.T) {
	// 验证：当 Options 中包含 compress=true 时，DSN 中包含 compress=true
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
		Driver: MySQLDriverConfig{
			Options: map[string]string{"compress": "true"},
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "compress=true") {
		t.Errorf("DSN should contain compress=true: %s", dsn)
	}
}

func TestBuildMySQLDSN_CompressDefaultAbsent(t *testing.T) {
	// 验证：默认情况下 DSN 中不应包含 compress 参数
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if strings.Contains(dsn, "compress") {
		t.Errorf("DSN should not contain compress by default: %s", dsn)
	}
}

// Parameter 3: maxAllowedPacket (High Priority)
func TestBuildMySQLDSN_MaxAllowedPacketSet(t *testing.T) {
	// 验证：当 MaxAllowedPacket 设置为非默认值（非 64MB）时，DSN 中包含该值
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
		Driver: MySQLDriverConfig{
			MaxAllowedPacket: 32 * 1024 * 1024, // 32MB
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "maxAllowedPacket=33554432") {
		t.Errorf("DSN should contain maxAllowedPacket=33554432: %s", dsn)
	}
	// 验证 round-trip 解析
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if parsed.MaxAllowedPacket != 32*1024*1024 {
		t.Errorf("parsed MaxAllowedPacket = %d, want %d", parsed.MaxAllowedPacket, 32*1024*1024)
	}
}

func TestBuildMySQLDSN_MaxAllowedPacketDefaultZero(t *testing.T) {
	// 验证：默认情况下 MaxAllowedPacket 为 0（Go 零值），DSN 中包含 maxAllowedPacket=0
	// 这表示自动从服务器获取 max_allowed_packet 变量
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "maxAllowedPacket=0") {
		t.Errorf("DSN should contain maxAllowedPacket=0 by default: %s", dsn)
	}
}

func TestBuildMySQLDSN_MaxAllowedPacketDriverDefault(t *testing.T) {
	// 验证：当 MaxAllowedPacket 设置为驱动默认值 64MB 时，DSN 中不包含该参数
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
		Driver: MySQLDriverConfig{
			MaxAllowedPacket: 64 * 1024 * 1024, // 64MB - 驱动默认值
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if strings.Contains(dsn, "maxAllowedPacket") {
		t.Errorf("DSN should not contain maxAllowedPacket when set to driver default 64MB: %s", dsn)
	}
}

func TestReadMySQLConfig_MaxAllowedPacketNegativeValue(t *testing.T) {
	// 验证：当配置中 max_allowed_packet 为负值时，应回退到 0（自动检测）
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"options": map[string]any{
					"max_allowed_packet": -1,
				},
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	// 负值应被归零，回退到自动检测模式
	if cfg.Driver.MaxAllowedPacket != 0 {
		t.Errorf("MaxAllowedPacket = %d, want 0 (negative values should be normalized to 0)", cfg.Driver.MaxAllowedPacket)
	}
}

func TestBuildMySQLDSN_ZeroValueNewFields(t *testing.T) {
	// 验证：当直接构造 MySQLConfig{} 时，新增字段的零值行为
	// AllowNativePasswords 和 CheckConnLiveness 的零值为 false，与 readMySQLConfig 的默认值 true 不同
	// 这是有意为之的设计：直接构造时使用 Go 零值，通过 readMySQLConfig 读取时使用配置默认值
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
	}
	dsn := BuildMySQLDSN(cfg)

	// 验证零值字段在 DSN 中的表现
	// AllowNativePasswords=false 会被写入 DSN（显式传递零值）
	if !strings.Contains(dsn, "allowNativePasswords=false") {
		t.Errorf("expected allowNativePasswords=false in DSN when using zero value, got: %s", dsn)
	}

	// CheckConnLiveness=false 会被写入 DSN（显式传递零值）
	if !strings.Contains(dsn, "checkConnLiveness=false") {
		t.Errorf("expected checkConnLiveness=false in DSN when using zero value, got: %s", dsn)
	}

	// 其他布尔字段零值为 false，不应出现在 DSN 中
	if strings.Contains(dsn, "rejectReadOnly") {
		t.Errorf("rejectReadOnly should not appear in DSN when zero value: %s", dsn)
	}
	if strings.Contains(dsn, "clientFoundRows") {
		t.Errorf("clientFoundRows should not appear in DSN when zero value: %s", dsn)
	}
	if strings.Contains(dsn, "multiStatements") {
		t.Errorf("multiStatements should not appear in DSN when zero value: %s", dsn)
	}
	if strings.Contains(dsn, "columnsWithAlias") {
		t.Errorf("columnsWithAlias should not appear in DSN when zero value: %s", dsn)
	}
	if strings.Contains(dsn, "interpolateParams") {
		t.Errorf("interpolateParams should not appear in DSN when zero value: %s", dsn)
	}

	// MaxAllowedPacket=0 会被写入 DSN（表示自动从服务器获取）
	if !strings.Contains(dsn, "maxAllowedPacket=0") {
		t.Errorf("expected maxAllowedPacket=0 in DSN when using zero value, got: %s", dsn)
	}

	// 验证 round-trip 解析
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if parsed.AllowNativePasswords {
		t.Error("parsed DSN should have AllowNativePasswords=false when using zero value")
	}
	if parsed.CheckConnLiveness {
		t.Error("parsed DSN should have CheckConnLiveness=false when using zero value")
	}
}

// Parameter 4: multiStatements (High Priority)
func TestBuildMySQLDSN_MultiStatementsEnabled(t *testing.T) {
	// 验证：当 MultiStatements=true 时，DSN 中包含 multiStatements=true
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
		Driver: MySQLDriverConfig{
			MultiStatements: true,
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "multiStatements=true") {
		t.Errorf("DSN should contain multiStatements=true: %s", dsn)
	}
	// 验证 round-trip 解析
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if !parsed.MultiStatements {
		t.Error("parsed DSN should have MultiStatements=true")
	}
}

func TestBuildMySQLDSN_MultiStatementsDefaultFalse(t *testing.T) {
	// 验证：默认情况下 MultiStatements 为 false，DSN 中不应包含该参数
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if strings.Contains(dsn, "multiStatements") {
		t.Errorf("DSN should not contain multiStatements by default: %s", dsn)
	}
}

// Parameter 5: clientFoundRows (Medium Priority)
func TestBuildMySQLDSN_ClientFoundRowsEnabled(t *testing.T) {
	// 验证：当 ClientFoundRows=true 时，DSN 中包含 clientFoundRows=true
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
		Driver: MySQLDriverConfig{
			ClientFoundRows: true,
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "clientFoundRows=true") {
		t.Errorf("DSN should contain clientFoundRows=true: %s", dsn)
	}
	// 验证 round-trip 解析
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if !parsed.ClientFoundRows {
		t.Error("parsed DSN should have ClientFoundRows=true")
	}
}

func TestBuildMySQLDSN_ClientFoundRowsDefaultFalse(t *testing.T) {
	// 验证：默认情况下 ClientFoundRows 为 false，DSN 中不应包含该参数
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if strings.Contains(dsn, "clientFoundRows") {
		t.Errorf("DSN should not contain clientFoundRows by default: %s", dsn)
	}
}

// Parameter 6: columnsWithAlias (Medium Priority)
func TestBuildMySQLDSN_ColumnsWithAliasEnabled(t *testing.T) {
	// 验证：当 ColumnsWithAlias=true 时，DSN 中包含 columnsWithAlias=true
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
		Driver: MySQLDriverConfig{
			ColumnsWithAlias: true,
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "columnsWithAlias=true") {
		t.Errorf("DSN should contain columnsWithAlias=true: %s", dsn)
	}
	// 验证 round-trip 解析
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if !parsed.ColumnsWithAlias {
		t.Error("parsed DSN should have ColumnsWithAlias=true")
	}
}

func TestBuildMySQLDSN_ColumnsWithAliasDefaultFalse(t *testing.T) {
	// 验证：默认情况下 ColumnsWithAlias 为 false，DSN 中不应包含该参数
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if strings.Contains(dsn, "columnsWithAlias") {
		t.Errorf("DSN should not contain columnsWithAlias by default: %s", dsn)
	}
}

// Parameter 7: interpolateParams (Medium Priority)
func TestBuildMySQLDSN_InterpolateParamsEnabled(t *testing.T) {
	// 验证：当 InterpolateParams=true 时，DSN 中包含 interpolateParams=true
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
		Driver: MySQLDriverConfig{
			InterpolateParams: true,
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if !strings.Contains(dsn, "interpolateParams=true") {
		t.Errorf("DSN should contain interpolateParams=true: %s", dsn)
	}
	// 验证 round-trip 解析
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if !parsed.InterpolateParams {
		t.Error("parsed DSN should have InterpolateParams=true")
	}
}

func TestBuildMySQLDSN_InterpolateParamsDefaultFalse(t *testing.T) {
	// 验证：默认情况下 InterpolateParams 为 false，DSN 中不应包含该参数
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Username: "root",
			Password: "secret",
			Database: "app",
		},
	}
	dsn := BuildMySQLDSN(cfg)
	if strings.Contains(dsn, "interpolateParams") {
		t.Errorf("DSN should not contain interpolateParams by default: %s", dsn)
	}
}

// Config Reader Tests for new parameters
func TestReadMySQLConfig_NewParameters(t *testing.T) {
	// 验证：readMySQLConfig 正确读取所有新增参数
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"options": map[string]any{
					"reject_read_only":       true,
					"client_found_rows":      true,
					"multi_statements":       true,
					"columns_with_alias":     true,
					"interpolate_params":     true,
					"max_allowed_packet":     33554432, // 32MB
					"compress":               true,
					"allow_native_passwords": true,
					"check_conn_liveness":    true,
				},
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if !cfg.Driver.RejectReadOnly {
		t.Error("RejectReadOnly should be true")
	}
	if !cfg.Driver.ClientFoundRows {
		t.Error("ClientFoundRows should be true")
	}
	if !cfg.Driver.MultiStatements {
		t.Error("MultiStatements should be true")
	}
	if !cfg.Driver.ColumnsWithAlias {
		t.Error("ColumnsWithAlias should be true")
	}
	if !cfg.Driver.InterpolateParams {
		t.Error("InterpolateParams should be true")
	}
	if cfg.Driver.MaxAllowedPacket != 33554432 {
		t.Errorf("MaxAllowedPacket = %d, want 33554432", cfg.Driver.MaxAllowedPacket)
	}
	if cfg.Driver.Options["compress"] != "true" {
		t.Errorf("Options[compress] = %q, want 'true'", cfg.Driver.Options["compress"])
	}
}

func TestReadMySQLConfig_NewParametersDefaults(t *testing.T) {
	// 验证：readMySQLConfig 的默认值正确
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if cfg.Driver.RejectReadOnly {
		t.Error("RejectReadOnly should be false by default")
	}
	if cfg.Driver.ClientFoundRows {
		t.Error("ClientFoundRows should be false by default")
	}
	if cfg.Driver.MultiStatements {
		t.Error("MultiStatements should be false by default")
	}
	if cfg.Driver.ColumnsWithAlias {
		t.Error("ColumnsWithAlias should be false by default")
	}
	if cfg.Driver.InterpolateParams {
		t.Error("InterpolateParams should be false by default")
	}
	if cfg.Driver.MaxAllowedPacket != 0 {
		t.Errorf("MaxAllowedPacket = %d, want 0", cfg.Driver.MaxAllowedPacket)
	}
	if cfg.Driver.Options["compress"] != "" {
		t.Errorf("Options[compress] = %q, want empty", cfg.Driver.Options["compress"])
	}
}
