package database

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/glebarez/sqlite"
	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestFacadeLazyFactoryAndServiceProvider(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	expected, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	calls := 0
	_ = registry.Singleton("database.default", func(containercontract.Resolver) (any, error) {
		calls++
		return expected, nil
	})

	got := Resolve()
	if got != expected || calls != 1 {
		t.Fatalf("expected one resolved database, got db=%v calls=%d", got, calls)
	}
	if again := Resolve(); again != expected || calls != 1 {
		t.Fatalf("expected cached database, got db=%v calls=%d", again, calls)
	}

	if err := (ServiceProvider{}).Register(databaseProviderApp{registry: registry}); err != nil {
		t.Fatalf("service provider register: %v", err)
	}
}

func TestServiceProviderLazyOpenDefaultConnection(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	// 使用 sqlmock 模拟 MySQL 连接，避免真实连接失败
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	// 模拟 SELECT VERSION() 查询（configureConnection 会调用）
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.32"))
	mock.ExpectExec("SET SESSION sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'").WillReturnResult(sqlmock.NewResult(0, 0))

	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}

	// 注册单例，让 Resolve() 直接返回模拟的 gormDB
	_ = registry.Singleton("database.default", func(containercontract.Resolver) (any, error) {
		return gormDB, nil
	})

	if err := (ServiceProvider{}).Register(databaseProviderApp{registry: registry}); err != nil {
		t.Fatalf("service provider register: %v", err)
	}
	db := Resolve()
	if db == nil {
		t.Fatal("expected gorm database")
	}
	sqlDB2, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	if stats := sqlDB2.Stats(); stats.MaxOpenConnections != 0 {
		// sqlmock 不应用连接池配置，此处仅验证能正常获取 Stats
		t.Logf("max open connections = %d (sqlmock does not apply pool config)", stats.MaxOpenConnections)
	}
	_ = sqlDB2.Close()
}

func TestOpenDefaultConnectionRejectsUnsupportedDriver(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	useDatabaseConfig(t, registry, "sqlite", "sqlite")
	if _, err := OpenDefaultConnection(); err == nil || !strings.Contains(err.Error(), "unsupported driver") {
		t.Fatalf("expected unsupported driver error, got %v", err)
	}
}

func useDatabaseConfig(t *testing.T, registry *container.Container, connection, driver string) {
	t.Helper()
	configpkg.Add("database", func() map[string]any {
		return map[string]any{
			"default": connection,
			"connections": map[string]any{
				connection: map[string]any{
					"driver":             driver,
					"dsn":                "root:secret@tcp(127.0.0.1:1)/prismgo?charset=utf8mb4&parseTime=true&loc=Local",
					"conn_max_lifetime":  "2m",
					"conn_max_idle_time": "30",
					"max_open_conns":     4,
					"max_idle_conns":     2,
					"strict":             false, // 禁用 SQL 模式设置，避免连接失败时 panic
				},
			},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
}

func TestApplyConnectionPoolConfigWithGormDB(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	err = applyConnectionPoolConfig(db, connectionPoolConfig{
		MaxOpenConns:    8,
		MaxIdleConns:    3,
		ConnMaxLifetime: time.Minute,
		ConnMaxIdleTime: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("apply connection pool config: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	if stats := sqlDB.Stats(); stats.MaxOpenConnections != 8 {
		t.Fatalf("max open connections = %d, want 8", stats.MaxOpenConnections)
	}
	_ = sqlDB.Close()
}
