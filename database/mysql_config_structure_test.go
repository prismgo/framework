package database

import (
	"strings"
	"testing"
)

// TestMySQLConfig_NamedFieldAccess 验证通过命名字段访问嵌入结构体
func TestMySQLConfig_NamedFieldAccess(t *testing.T) {
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			DSN:      "custom-dsn",
			Host:     "localhost",
			Port:     "3306",
			Username: "root",
			Password: "secret",
			Database: "testdb",
		},
	}

	if cfg.Connection.DSN != "custom-dsn" {
		t.Errorf("DSN = %s, want custom-dsn", cfg.Connection.DSN)
	}
	if cfg.Connection.Host != "localhost" {
		t.Errorf("Host = %s, want localhost", cfg.Connection.Host)
	}
	if cfg.Connection.Port != "3306" {
		t.Errorf("Port = %s, want 3306", cfg.Connection.Port)
	}
	if cfg.Connection.Username != "root" {
		t.Errorf("Username = %s, want root", cfg.Connection.Username)
	}
	if cfg.Connection.Password != "secret" {
		t.Errorf("Password = %s, want secret", cfg.Connection.Password)
	}
	if cfg.Connection.Database != "testdb" {
		t.Errorf("Database = %s, want testdb", cfg.Connection.Database)
	}
}

// TestMySQLConfig_EmbeddedStructAccess 验证可以通过嵌入结构体名访问字段
func TestMySQLConfig_EmbeddedStructAccess(t *testing.T) {
	cfg := MySQLConfig{}

	// 验证：可以通过嵌入结构体名访问字段
	cfg.Connection.Host = "192.168.1.1"
	cfg.Connection.Port = "3307"
	cfg.Session.Charset = "utf8"
	cfg.Driver.AllowNativePasswords = true
	cfg.Schema.TablePrefix = "app_"

	if cfg.Connection.Host != "192.168.1.1" {
		t.Errorf("Connection.Host = %s, want 192.168.1.1", cfg.Connection.Host)
	}
	if cfg.Connection.Port != "3307" {
		t.Errorf("Connection.Port = %s, want 3307", cfg.Connection.Port)
	}
	if cfg.Session.Charset != "utf8" {
		t.Errorf("Session.Charset = %s, want utf8", cfg.Session.Charset)
	}
	if !cfg.Driver.AllowNativePasswords {
		t.Error("Driver.AllowNativePasswords = false, want true")
	}
	if cfg.Schema.TablePrefix != "app_" {
		t.Errorf("Schema.TablePrefix = %s, want app_", cfg.Schema.TablePrefix)
	}
}

// TestMySQLConfig_StructureResponsibilities 验证各结构体的职责划分
func TestMySQLConfig_StructureResponsibilities(t *testing.T) {
	cfg := MySQLConfig{
		// 连接基础信息
		Connection: MySQLConnectionConfig{
			DSN:        "",
			Host:       "localhost",
			Port:       "3306",
			Username:   "root",
			Password:   "secret",
			Database:   "testdb",
			UnixSocket: "",
		},
		// 会话级配置
		Session: MySQLSessionConfig{
			Charset:        "utf8mb4",
			ParseTime:      "true",
			Loc:            "Local",
			Collation:      "utf8mb4_unicode_ci",
			Timezone:       "Asia/Shanghai",
			Strict:         true,
			Modes:          []string{"STRICT_TRANS_TABLES"},
			IsolationLevel: "REPEATABLE READ",
		},
		// 驱动级配置
		Driver: MySQLDriverConfig{
			AllowNativePasswords: true,
			CheckConnLiveness:    true,
			RejectReadOnly:       false,
			ClientFoundRows:      false,
			MultiStatements:      false,
			ColumnsWithAlias:     false,
			InterpolateParams:    false,
			MaxAllowedPacket:     0,
			Options:              map[string]string{"timeout": "10s"},
		},
		// Schema 级配置
		Schema: MySQLSchemaConfig{
			TablePrefix:   "app_",
			Engine:        "InnoDB",
			PrefixIndexes: true,
			SSL: SSLConfig{
				CA:                 "/path/to/ca.pem",
				Cert:               "/path/to/cert.pem",
				Key:                "/path/to/key.pem",
				InsecureSkipVerify: false,
			},
		},
	}

	// 验证：各结构体的字段可以正确访问
	if cfg.Connection.Host != "localhost" {
		t.Errorf("Connection.Host = %s, want localhost", cfg.Connection.Host)
	}
	if cfg.Session.Charset != "utf8mb4" {
		t.Errorf("Session.Charset = %s, want utf8mb4", cfg.Session.Charset)
	}
	if !cfg.Driver.AllowNativePasswords {
		t.Error("Driver.AllowNativePasswords = false, want true")
	}
	if cfg.Schema.TablePrefix != "app_" {
		t.Errorf("Schema.TablePrefix = %s, want app_", cfg.Schema.TablePrefix)
	}
	if cfg.Schema.SSL.CA != "/path/to/ca.pem" {
		t.Errorf("Schema.SSL.CA = %s, want /path/to/ca.pem", cfg.Schema.SSL.CA)
	}
}

// TestMySQLConfig_BuildDSNWithEmbeddedStructs 验证 BuildMySQLDSN 正确处理嵌入结构体
func TestMySQLConfig_BuildDSNWithEmbeddedStructs(t *testing.T) {
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			Host:     "192.168.1.100",
			Port:     "3307",
			Username: "admin",
			Password: "password",
			Database: "production",
		},
		Session: MySQLSessionConfig{
			Charset: "utf8mb4",
		},
		Driver: MySQLDriverConfig{
			AllowNativePasswords: true,
			CheckConnLiveness:    true,
		},
		Schema: MySQLSchemaConfig{
			SSL: SSLConfig{
				CA: "/path/to/ca.pem",
			},
		},
	}

	dsn := BuildMySQLDSN(cfg)

	// 验证：DSN 包含连接信息
	if !strings.Contains(dsn, "admin:password@") {
		t.Errorf("DSN should contain credentials: %s", dsn)
	}
	if !strings.Contains(dsn, "192.168.1.100:3307") {
		t.Errorf("DSN should contain host:port: %s", dsn)
	}
	if !strings.Contains(dsn, "/production") {
		t.Errorf("DSN should contain database: %s", dsn)
	}

	// 验证：DSN 包含 SSL 参数
	if !strings.Contains(dsn, "tls-ca=%2Fpath%2Fto%2Fca.pem") {
		t.Errorf("DSN should contain SSL CA: %s", dsn)
	}
}

// TestMySQLConfig_ZeroValueBehavior 验证零值行为
func TestMySQLConfig_ZeroValueBehavior(t *testing.T) {
	// 验证：直接构造 MySQLConfig{} 时，零值行为与重构前一致
	cfg := MySQLConfig{}

	// 验证：零值字段通过嵌入结构体访问
	if cfg.Connection.Host != "" {
		t.Errorf("Connection.Host = %s, want empty", cfg.Connection.Host)
	}

	if cfg.Driver.AllowNativePasswords {
		t.Error("Driver.AllowNativePasswords = true, want false (zero value)")
	}
}
