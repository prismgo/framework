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

	if cfg.Host != "127.0.0.1" {
		t.Errorf("Host = %q, want %q", cfg.Host, "127.0.0.1")
	}
	if cfg.Port != "3306" {
		t.Errorf("Port = %q, want %q", cfg.Port, "3306")
	}
	if cfg.Username != "root" {
		t.Errorf("Username = %q, want %q", cfg.Username, "root")
	}
	if cfg.Database != "prismgo" {
		t.Errorf("Database = %q, want %q", cfg.Database, "prismgo")
	}
	if cfg.Charset != "utf8mb4" {
		t.Errorf("Charset = %q, want %q", cfg.Charset, "utf8mb4")
	}
	if cfg.ParseTime != "true" {
		t.Errorf("ParseTime = %q, want %q", cfg.ParseTime, "true")
	}
	if cfg.Loc != "Local" {
		t.Errorf("Loc = %q, want %q", cfg.Loc, "Local")
	}
	if !cfg.Strict {
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

	if cfg.Host != "192.168.1.100" {
		t.Errorf("Host = %q, want %q", cfg.Host, "192.168.1.100")
	}
	if cfg.Port != "3307" {
		t.Errorf("Port = %q, want %q", cfg.Port, "3307")
	}
	if cfg.Username != "testuser" {
		t.Errorf("Username = %q, want %q", cfg.Username, "testuser")
	}
	if cfg.Password != "testpass" {
		t.Errorf("Password = %q, want %q", cfg.Password, "testpass")
	}
	if cfg.Database != "testdb" {
		t.Errorf("Database = %q, want %q", cfg.Database, "testdb")
	}
	if cfg.Charset != "utf8" {
		t.Errorf("Charset = %q, want %q", cfg.Charset, "utf8")
	}
	if cfg.ParseTime != "false" {
		t.Errorf("ParseTime = %q, want %q", cfg.ParseTime, "false")
	}
	if cfg.Loc != "UTC" {
		t.Errorf("Loc = %q, want %q", cfg.Loc, "UTC")
	}
	if cfg.UnixSocket != "/var/run/mysqld/mysqld.sock" {
		t.Errorf("UnixSocket = %q, want %q", cfg.UnixSocket, "/var/run/mysqld/mysqld.sock")
	}
	if cfg.Collation != "utf8_unicode_ci" {
		t.Errorf("Collation = %q, want %q", cfg.Collation, "utf8_unicode_ci")
	}
	if cfg.TablePrefix != "test_" {
		t.Errorf("TablePrefix = %q, want %q", cfg.TablePrefix, "test_")
	}
	if cfg.Strict {
		t.Error("Strict = true, want false")
	}
	if cfg.Timezone != "Asia/Shanghai" {
		t.Errorf("Timezone = %q, want %q", cfg.Timezone, "Asia/Shanghai")
	}
	if cfg.IsolationLevel != "READ COMMITTED" {
		t.Errorf("IsolationLevel = %q, want %q", cfg.IsolationLevel, "READ COMMITTED")
	}
	if cfg.Engine != "InnoDB" {
		t.Errorf("Engine = %q, want %q", cfg.Engine, "InnoDB")
	}
	if !cfg.PrefixIndexes {
		t.Error("PrefixIndexes = false, want true")
	}
	if len(cfg.Modes) != 2 {
		t.Fatalf("len(Modes) = %d, want 2", len(cfg.Modes))
	}
	if cfg.Modes[0] != "STRICT_TRANS_TABLES" {
		t.Errorf("Modes[0] = %q, want %q", cfg.Modes[0], "STRICT_TRANS_TABLES")
	}
	if cfg.Modes[1] != "NO_ZERO_DATE" {
		t.Errorf("Modes[1] = %q, want %q", cfg.Modes[1], "NO_ZERO_DATE")
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

	if cfg.SSL.CA != "/path/to/ca.pem" {
		t.Errorf("SSL.CA = %q, want %q", cfg.SSL.CA, "/path/to/ca.pem")
	}
	if cfg.SSL.Cert != "/path/to/client-cert.pem" {
		t.Errorf("SSL.Cert = %q, want %q", cfg.SSL.Cert, "/path/to/client-cert.pem")
	}
	if cfg.SSL.Key != "/path/to/client-key.pem" {
		t.Errorf("SSL.Key = %q, want %q", cfg.SSL.Key, "/path/to/client-key.pem")
	}
	if !cfg.SSL.InsecureSkipVerify {
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

	if cfg.Options == nil {
		t.Fatal("Options = nil, want non-nil")
	}
	if cfg.Options["timeout"] != "30s" {
		t.Errorf("Options[timeout] = %q, want %q", cfg.Options["timeout"], "30s")
	}
	if cfg.Options["readTimeout"] != "10s" {
		t.Errorf("Options[readTimeout] = %q, want %q", cfg.Options["readTimeout"], "10s")
	}
	if cfg.Options["writeTimeout"] != "5s" {
		t.Errorf("Options[writeTimeout] = %q, want %q", cfg.Options["writeTimeout"], "5s")
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

	if len(cfg.Modes) != 3 {
		t.Fatalf("len(Modes) = %d, want 3", len(cfg.Modes))
	}
	if cfg.Modes[0] != "STRICT_TRANS_TABLES" {
		t.Errorf("Modes[0] = %q, want %q", cfg.Modes[0], "STRICT_TRANS_TABLES")
	}
	if cfg.Modes[1] != "NO_ZERO_DATE" {
		t.Errorf("Modes[1] = %q, want %q", cfg.Modes[1], "NO_ZERO_DATE")
	}
	if cfg.Modes[2] != "ERROR_FOR_DIVISION_BY_ZERO" {
		t.Errorf("Modes[2] = %q, want %q", cfg.Modes[2], "ERROR_FOR_DIVISION_BY_ZERO")
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
