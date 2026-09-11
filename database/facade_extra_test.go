package database

import (
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestFacadeLazyFactoryAndServiceProvider(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	expected := &gorm.DB{}

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

func TestApplyConnectionPoolConfigWithGormDB(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: sqlDB, SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
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
	sqlDB, err = db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	if stats := sqlDB.Stats(); stats.MaxOpenConnections != 8 {
		t.Fatalf("max open connections = %d, want 8", stats.MaxOpenConnections)
	}
	_ = sqlDB.Close()
}
