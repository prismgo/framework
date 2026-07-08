package database

import (
	"path/filepath"
	"testing"
	"time"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
)

// setupTestConfig 设置测试用的配置环境。
func setupTestConfig(t *testing.T, values map[string]any) {
	t.Helper()

	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	configpkg.Add("database", func() map[string]any {
		return values
	})

	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}

	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
}

func TestReadMySQLConfig_Defaults(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if cfg.Connection.Host != "127.0.0.1" {
		t.Errorf("Host = %q, want %q", cfg.Connection.Host, "127.0.0.1")
	}
	if cfg.Connection.Port != "3306" {
		t.Errorf("Port = %q, want %q", cfg.Connection.Port, "3306")
	}
	if cfg.Connection.Username != "root" {
		t.Errorf("Username = %q, want %q", cfg.Connection.Username, "root")
	}
	if cfg.Connection.Database != "prismgo" {
		t.Errorf("Database = %q, want %q", cfg.Connection.Database, "prismgo")
	}
	if cfg.Session.Charset != "utf8mb4" {
		t.Errorf("Charset = %q, want %q", cfg.Session.Charset, "utf8mb4")
	}
	if cfg.Session.ParseTime != "true" {
		t.Errorf("ParseTime = %q, want %q", cfg.Session.ParseTime, "true")
	}
	if cfg.Session.Loc != "Local" {
		t.Errorf("Loc = %q, want %q", cfg.Session.Loc, "Local")
	}
	if !cfg.Session.Strict {
		t.Error("Strict = false, want true")
	}
}

func TestReadMySQLConfig_CustomValues(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"host":            "192.168.1.100",
				"port":            "3307",
				"username":        "testuser",
				"password":        "testpass",
				"database":        "testdb",
				"charset":         "utf8",
				"parse_time":      "false",
				"loc":             "UTC",
				"unix_socket":     "/var/run/mysqld/mysqld.sock",
				"collation":       "utf8_unicode_ci",
				"prefix":          "test_",
				"strict":          false,
				"timezone":        "Asia/Shanghai",
				"isolation_level": "READ COMMITTED",
				"engine":          "InnoDB",
				"prefix_indexes":  true,
				"modes":           "STRICT_TRANS_TABLES,NO_ZERO_DATE",
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if cfg.Connection.Host != "192.168.1.100" {
		t.Errorf("Host = %q, want %q", cfg.Connection.Host, "192.168.1.100")
	}
	if cfg.Connection.Port != "3307" {
		t.Errorf("Port = %q, want %q", cfg.Connection.Port, "3307")
	}
	if cfg.Connection.Username != "testuser" {
		t.Errorf("Username = %q, want %q", cfg.Connection.Username, "testuser")
	}
	if cfg.Connection.Password != "testpass" {
		t.Errorf("Password = %q, want %q", cfg.Connection.Password, "testpass")
	}
	if cfg.Connection.Database != "testdb" {
		t.Errorf("Database = %q, want %q", cfg.Connection.Database, "testdb")
	}
	if cfg.Session.Charset != "utf8" {
		t.Errorf("Charset = %q, want %q", cfg.Session.Charset, "utf8")
	}
	if cfg.Session.ParseTime != "false" {
		t.Errorf("ParseTime = %q, want %q", cfg.Session.ParseTime, "false")
	}
	if cfg.Session.Loc != "UTC" {
		t.Errorf("Loc = %q, want %q", cfg.Session.Loc, "UTC")
	}
	if cfg.Connection.UnixSocket != "/var/run/mysqld/mysqld.sock" {
		t.Errorf("UnixSocket = %q, want %q", cfg.Connection.UnixSocket, "/var/run/mysqld/mysqld.sock")
	}
	if cfg.Session.Collation != "utf8_unicode_ci" {
		t.Errorf("Collation = %q, want %q", cfg.Session.Collation, "utf8_unicode_ci")
	}
	if cfg.Schema.TablePrefix != "test_" {
		t.Errorf("TablePrefix = %q, want %q", cfg.Schema.TablePrefix, "test_")
	}
	if cfg.Session.Strict {
		t.Error("Strict = true, want false")
	}
	if cfg.Session.Timezone != "Asia/Shanghai" {
		t.Errorf("Timezone = %q, want %q", cfg.Session.Timezone, "Asia/Shanghai")
	}
	if cfg.Session.IsolationLevel != "READ COMMITTED" {
		t.Errorf("IsolationLevel = %q, want %q", cfg.Session.IsolationLevel, "READ COMMITTED")
	}
	if cfg.Schema.Engine != "InnoDB" {
		t.Errorf("Engine = %q, want %q", cfg.Schema.Engine, "InnoDB")
	}
	if !cfg.Schema.PrefixIndexes {
		t.Error("PrefixIndexes = false, want true")
	}
	if len(cfg.Session.Modes) != 2 {
		t.Fatalf("len(Modes) = %d, want 2", len(cfg.Session.Modes))
	}
	if cfg.Session.Modes[0] != "STRICT_TRANS_TABLES" {
		t.Errorf("Modes[0] = %q, want %q", cfg.Session.Modes[0], "STRICT_TRANS_TABLES")
	}
	if cfg.Session.Modes[1] != "NO_ZERO_DATE" {
		t.Errorf("Modes[1] = %q, want %q", cfg.Session.Modes[1], "NO_ZERO_DATE")
	}
}

func TestReadMySQLConfig_SSLConfig(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"ssl": map[string]any{
					"ca":                   "/path/to/ca.pem",
					"cert":                 "/path/to/client-cert.pem",
					"key":                  "/path/to/client-key.pem",
					"insecure_skip_verify": true,
				},
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if cfg.Schema.SSL.CA != "/path/to/ca.pem" {
		t.Errorf("SSL.CA = %q, want %q", cfg.Schema.SSL.CA, "/path/to/ca.pem")
	}
	if cfg.Schema.SSL.Cert != "/path/to/client-cert.pem" {
		t.Errorf("SSL.Cert = %q, want %q", cfg.Schema.SSL.Cert, "/path/to/client-cert.pem")
	}
	if cfg.Schema.SSL.Key != "/path/to/client-key.pem" {
		t.Errorf("SSL.Key = %q, want %q", cfg.Schema.SSL.Key, "/path/to/client-key.pem")
	}
	if !cfg.Schema.SSL.InsecureSkipVerify {
		t.Error("SSL.InsecureSkipVerify = false, want true")
	}
}

func TestReadMySQLConfig_Options(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"options": map[string]any{
					"timeout":       "30s",
					"read_timeout":  "10s",
					"write_timeout": "5s",
				},
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if cfg.Driver.Options == nil {
		t.Fatal("Options = nil, want non-nil")
	}
	if cfg.Driver.Options["timeout"] != "30s" {
		t.Errorf("Options[timeout] = %q, want %q", cfg.Driver.Options["timeout"], "30s")
	}
	if cfg.Driver.Options["readTimeout"] != "10s" {
		t.Errorf("Options[readTimeout] = %q, want %q", cfg.Driver.Options["readTimeout"], "10s")
	}
	if cfg.Driver.Options["writeTimeout"] != "5s" {
		t.Errorf("Options[writeTimeout] = %q, want %q", cfg.Driver.Options["writeTimeout"], "5s")
	}
}

func TestReadMySQLConfig_ModesWithSpaces(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"modes": " STRICT_TRANS_TABLES , NO_ZERO_DATE , ERROR_FOR_DIVISION_BY_ZERO ",
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if len(cfg.Session.Modes) != 3 {
		t.Fatalf("len(Modes) = %d, want 3", len(cfg.Session.Modes))
	}
	if cfg.Session.Modes[0] != "STRICT_TRANS_TABLES" {
		t.Errorf("Modes[0] = %q, want %q", cfg.Session.Modes[0], "STRICT_TRANS_TABLES")
	}
	if cfg.Session.Modes[1] != "NO_ZERO_DATE" {
		t.Errorf("Modes[1] = %q, want %q", cfg.Session.Modes[1], "NO_ZERO_DATE")
	}
	if cfg.Session.Modes[2] != "ERROR_FOR_DIVISION_BY_ZERO" {
		t.Errorf("Modes[2] = %q, want %q", cfg.Session.Modes[2], "ERROR_FOR_DIVISION_BY_ZERO")
	}
}

func TestReadPoolConfig_Defaults(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{},
		},
	})

	cfg := readPoolConfig("database.connections.mysql")

	if cfg.MaxOpenConns != 30 {
		t.Errorf("MaxOpenConns = %d, want %d", cfg.MaxOpenConns, 30)
	}
	if cfg.MaxIdleConns != 10 {
		t.Errorf("MaxIdleConns = %d, want %d", cfg.MaxIdleConns, 10)
	}
	if cfg.ConnMaxLifetime != time.Hour {
		t.Errorf("ConnMaxLifetime = %v, want %v", cfg.ConnMaxLifetime, time.Hour)
	}
	if cfg.ConnMaxIdleTime != 10*time.Minute {
		t.Errorf("ConnMaxIdleTime = %v, want %v", cfg.ConnMaxIdleTime, 10*time.Minute)
	}
}

func TestReadPoolConfig_CustomValues(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"max_open_conns":     100,
				"max_idle_conns":     20,
				"conn_max_lifetime":  "2h",
				"conn_max_idle_time": "30m",
			},
		},
	})

	cfg := readPoolConfig("database.connections.mysql")

	if cfg.MaxOpenConns != 100 {
		t.Errorf("MaxOpenConns = %d, want %d", cfg.MaxOpenConns, 100)
	}
	if cfg.MaxIdleConns != 20 {
		t.Errorf("MaxIdleConns = %d, want %d", cfg.MaxIdleConns, 20)
	}
	if cfg.ConnMaxLifetime != 2*time.Hour {
		t.Errorf("ConnMaxLifetime = %v, want %v", cfg.ConnMaxLifetime, 2*time.Hour)
	}
	if cfg.ConnMaxIdleTime != 30*time.Minute {
		t.Errorf("ConnMaxIdleTime = %v, want %v", cfg.ConnMaxIdleTime, 30*time.Minute)
	}
}

func TestReadPoolConfig_DurationInSeconds(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"conn_max_lifetime":  "3600",
				"conn_max_idle_time": "600",
			},
		},
	})

	cfg := readPoolConfig("database.connections.mysql")

	if cfg.ConnMaxLifetime != time.Hour {
		t.Errorf("ConnMaxLifetime = %v, want %v", cfg.ConnMaxLifetime, time.Hour)
	}
	if cfg.ConnMaxIdleTime != 10*time.Minute {
		t.Errorf("ConnMaxIdleTime = %v, want %v", cfg.ConnMaxIdleTime, 10*time.Minute)
	}
}

// TestReadMySQLDriverOptions_Defaults 测试驱动选项的默认值。
func TestReadMySQLDriverOptions_Defaults(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	// 验证安全默认值
	if cfg.Driver.AllowNativePasswords {
		t.Error("AllowNativePasswords = true, want false (安全默认值)")
	}
	if !cfg.Driver.CheckConnLiveness {
		t.Error("CheckConnLiveness = false, want true")
	}
	if cfg.Driver.RejectReadOnly {
		t.Error("RejectReadOnly = true, want false")
	}
	if cfg.Driver.ClientFoundRows {
		t.Error("ClientFoundRows = true, want false")
	}
	if cfg.Driver.MultiStatements {
		t.Error("MultiStatements = true, want false (安全默认值)")
	}
	if cfg.Driver.ColumnsWithAlias {
		t.Error("ColumnsWithAlias = true, want false")
	}
	if cfg.Driver.InterpolateParams {
		t.Error("InterpolateParams = true, want false (安全默认值)")
	}
	if cfg.Driver.MaxAllowedPacket != 0 {
		t.Errorf("MaxAllowedPacket = %d, want 0", cfg.Driver.MaxAllowedPacket)
	}
}

// TestReadMySQLDriverOptions_MaxAllowedPacket 测试 MaxAllowedPacket 的边界校验。
func TestReadMySQLDriverOptions_MaxAllowedPacket(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected int
	}{
		{"负值归零", -100, 0},
		{"超过1GB归零", 2 << 30, 0}, // 2GB
		{"正常值保留", 64 * 1024 * 1024, 64 * 1024 * 1024},
		{"1GB边界值", 1 << 30, 1 << 30},
		{"零值自动检测", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupTestConfig(t, map[string]any{
				"connections": map[string]any{
					"mysql": map[string]any{
						"options": map[string]any{
							"max_allowed_packet": tt.value,
						},
					},
				},
			})

			cfg := readMySQLConfig("database.connections.mysql")
			if cfg.Driver.MaxAllowedPacket != tt.expected {
				t.Errorf("MaxAllowedPacket = %d, want %d", cfg.Driver.MaxAllowedPacket, tt.expected)
			}
		})
	}
}

// TestReadMySQLDriverOptions_CustomValues 测试驱动选项的自定义配置读取。
func TestReadMySQLDriverOptions_CustomValues(t *testing.T) {
	setupTestConfig(t, map[string]any{
		"connections": map[string]any{
			"mysql": map[string]any{
				"options": map[string]any{
					"allow_native_passwords": true,
					"check_conn_liveness":    false,
					"reject_read_only":       true,
					"client_found_rows":      true,
					"multi_statements":       true,
					"columns_with_alias":     true,
					"interpolate_params":     true,
					"max_allowed_packet":     32 * 1024 * 1024, // 32MB
				},
			},
		},
	})

	cfg := readMySQLConfig("database.connections.mysql")

	if !cfg.Driver.AllowNativePasswords {
		t.Error("AllowNativePasswords = false, want true")
	}
	if cfg.Driver.CheckConnLiveness {
		t.Error("CheckConnLiveness = true, want false")
	}
	if !cfg.Driver.RejectReadOnly {
		t.Error("RejectReadOnly = false, want true")
	}
	if !cfg.Driver.ClientFoundRows {
		t.Error("ClientFoundRows = false, want true")
	}
	if !cfg.Driver.MultiStatements {
		t.Error("MultiStatements = false, want true")
	}
	if !cfg.Driver.ColumnsWithAlias {
		t.Error("ColumnsWithAlias = false, want true")
	}
	if !cfg.Driver.InterpolateParams {
		t.Error("InterpolateParams = false, want true")
	}
	if cfg.Driver.MaxAllowedPacket != 32*1024*1024 {
		t.Errorf("MaxAllowedPacket = %d, want %d", cfg.Driver.MaxAllowedPacket, 32*1024*1024)
	}
}
