package database

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestBuildMySQLDSNUsesDefaults(t *testing.T) {
	dsn := BuildMySQLDSN(MySQLConfig{
		Username: "root",
		Password: "secret",
		Database: "app",
	})
	if !strings.Contains(dsn, "root:secret@tcp(127.0.0.1:3306)/app") {
		t.Fatalf("unexpected dsn: %s", dsn)
	}
	if !strings.Contains(dsn, "charset=utf8mb4") {
		t.Fatalf("missing default charset: %s", dsn)
	}
	if !strings.Contains(dsn, "parseTime=true") {
		t.Fatalf("missing default parseTime: %s", dsn)
	}
	if !strings.Contains(dsn, "loc=Local") {
		t.Fatalf("missing default loc: %s", dsn)
	}
}

func TestBuildMySQLDSNHonorsExplicitDSN(t *testing.T) {
	got := BuildMySQLDSN(MySQLConfig{DSN: "custom-dsn"})
	if got != "custom-dsn" {
		t.Fatalf("explicit DSN should win, got %q", got)
	}
}

func TestBuildMySQLDSNRoundTripsSpecialCharacters(t *testing.T) {
	want := MySQLConfig{
		Host:     "db.internal",
		Port:     "3307",
		Username: "user@name/with?chars",
		Password: "p@ss:word/with@chars?",
		Database: "tenant/db:name",
		Charset:  "utf8mb4",
		Loc:      "Local",
	}

	parsed, err := mysqldriver.ParseDSN(BuildMySQLDSN(want))
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if parsed.User != want.Username {
		t.Fatalf("user = %q, want %q", parsed.User, want.Username)
	}
	if parsed.Passwd != want.Password {
		t.Fatalf("password = %q, want %q", parsed.Passwd, want.Password)
	}
	if parsed.DBName != want.Database {
		t.Fatalf("dbname = %q, want %q", parsed.DBName, want.Database)
	}
	if parsed.Addr != want.Host+":"+want.Port {
		t.Fatalf("addr = %q, want %q", parsed.Addr, want.Host+":"+want.Port)
	}
	if !parsed.ParseTime {
		t.Fatal("expected parseTime to round-trip true")
	}
	if got := parsed.Loc.String(); got != want.Loc {
		t.Fatalf("loc = %q, want %q", got, want.Loc)
	}
}

func TestOpenRejectsUnknownDriver(t *testing.T) {
	_, err := Open("sqlite", "file::memory:?cache=shared")
	if err == nil {
		t.Fatalf("expected unsupported driver error")
	}
	if !strings.Contains(err.Error(), "unsupported driver") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGORMLoggerLevelFollowsAppDebug(t *testing.T) {
	if got := gormLoggerLevelFromDebug(false); got != logger.Warn {
		t.Fatalf("debug false logger level = %v, want %v", got, logger.Warn)
	}
	if got := gormLoggerLevelFromDebug(true); got != logger.Info {
		t.Fatalf("debug true logger level = %v, want %v", got, logger.Info)
	}
}

func TestServiceProviderRegistersExplicitRegistry(t *testing.T) {
	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(databaseProviderApp{registry: registry}); err != nil {
		t.Fatalf("service provider register failed: %v", err)
	}
}

func TestServiceProviderNameAndBoot(t *testing.T) {
	provider := ServiceProvider{}
	if got := provider.Name(); got != "database" {
		t.Fatalf("provider name = %q, want database", got)
	}
	if err := provider.Boot(databaseProviderApp{registry: container.NewContainer()}); err != nil {
		t.Fatalf("service provider boot failed: %v", err)
	}
}

func TestRegisterFactoryInRequiresRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })
	if _, err := container.Make[*gorm.DB]("database.default"); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatalf("Make nil error = %v, want ErrNoCurrentContainer", err)
	}
}

type databaseProviderApp struct{ registry containercontract.Container }

func (a databaseProviderApp) Container() containercontract.Container { return a.registry }

func TestApplySQLConnectionPoolConfig(t *testing.T) {
	sqlDB := &sql.DB{}
	cfg := connectionPoolConfig{
		MaxOpenConns:    23,
		MaxIdleConns:    7,
		ConnMaxLifetime: 45 * time.Minute,
		ConnMaxIdleTime: 6 * time.Minute,
	}

	applySQLConnectionPoolConfig(sqlDB, cfg)

	stats := sqlDB.Stats()
	if stats.MaxOpenConnections != cfg.MaxOpenConns {
		t.Fatalf("expected max open conns %d, got %d", cfg.MaxOpenConns, stats.MaxOpenConnections)
	}
}

func TestParseDurationSecondsOrText(t *testing.T) {
	fallback := 5 * time.Minute
	cases := []struct {
		name  string
		input string
		want  time.Duration
	}{
		{name: "duration text", input: "1h", want: time.Hour},
		{name: "seconds string", input: "600", want: 10 * time.Minute},
		{name: "blank fallback", input: "", want: fallback},
		{name: "invalid fallback", input: "oops", want: fallback},
		{name: "invalid duration text fallback", input: "1x", want: fallback},
		{name: "fractional seconds", input: "1.5", want: 1500 * time.Millisecond},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseDurationSecondsOrText(tc.input, fallback)
			if got != tc.want {
				t.Fatalf("expected %s, got %s", tc.want, got)
			}
		})
	}
}

func TestBuildMySQLDSNWithUnixSocket(t *testing.T) {
	dsn := BuildMySQLDSN(MySQLConfig{
		UnixSocket: "/var/run/mysqld/mysqld.sock",
		Username:   "root",
		Password:   "secret",
		Database:   "app",
	})
	if !strings.Contains(dsn, "unix(/var/run/mysqld/mysqld.sock)") {
		t.Fatalf("expected unix socket in DSN, got: %s", dsn)
	}
	// socket 模式下不应包含 tcp
	if strings.Contains(dsn, "tcp(") {
		t.Fatalf("unix socket DSN should not contain tcp, got: %s", dsn)
	}
}

func TestBuildMySQLDSNWithTCPIfNoSocket(t *testing.T) {
	dsn := BuildMySQLDSN(MySQLConfig{
		Host:     "127.0.0.1",
		Port:     "3306",
		Username: "root",
		Database: "app",
	})
	if !strings.Contains(dsn, "tcp(127.0.0.1:3306)") {
		t.Fatalf("expected tcp in DSN when no socket, got: %s", dsn)
	}
}

func TestBuildDSNByDriver(t *testing.T) {
	cases := []struct {
		name   string
		driver string
		cfg    MySQLConfig
		check  func(string) bool
	}{
		{
			name:   "mysql driver uses BuildMySQLDSN",
			driver: "mysql",
			cfg: MySQLConfig{
				Host:     "localhost",
				Port:     "3306",
				Username: "root",
				Database: "test",
			},
			check: func(dsn string) bool {
				return strings.Contains(dsn, "root@") && strings.Contains(dsn, "test")
			},
		},
		{
			name:   "sqlite driver uses raw DSN",
			driver: "sqlite",
			cfg: MySQLConfig{
				DSN: "file::memory:?cache=shared",
			},
			check: func(dsn string) bool {
				return dsn == "file::memory:?cache=shared"
			},
		},
		{
			name:   "unknown driver returns empty DSN",
			driver: "unknown",
			cfg:    MySQLConfig{},
			check: func(dsn string) bool {
				return dsn == ""
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := buildDSNByDriver(tc.driver, tc.cfg)
			if !tc.check(dsn) {
				t.Fatalf("driver %s DSN check failed, got %q", tc.driver, dsn)
			}
		})
	}
}
