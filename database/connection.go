// Package database 提供 GORM 数据库连接的通用构造工具，屏蔽不同 driver 的差异。
// 当前仅内置 MySQL；如需扩展 SQLite / Postgres 等，可新增同名 DSN 构造器后在 Open 中分支。
package database

import (
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	configpkg "github.com/prismgo/framework/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// MySQLConfig 描述 MySQL 连接所需的全部参数。
// 留空的字段会在 BuildMySQLDSN 中回退为常见默认值。
// 若 DSN 非空，则直接作为最终连接串，其余字段被忽略。
type MySQLConfig struct {
	DSN         string
	Host        string
	Port        string
	Username    string
	Password    string
	Database    string
	Charset     string
	ParseTime   string
	Loc         string
	UnixSocket  string // UNIX socket 路径，非空时覆盖 Host/Port
	Collation   string // 字符集排序规则，连接后通过 SET NAMES 应用
	TablePrefix string // 表名前缀，通过 GORM NamingStrategy 应用
}

// Open 根据 driver 字符串打开 GORM 数据库连接。
// 目前仅支持 driver == "mysql"；其他 driver 立即返回错误，避免隐式回退到 MySQL 造成困惑。
func Open(driver, dsn string) (*gorm.DB, error) {
	driver = strings.ToLower(strings.TrimSpace(driver))
	switch driver {
	case "", "mysql":
		return gorm.Open(mysql.New(mysql.Config{
			DSN:                       dsn,
			SkipInitializeWithVersion: true,
		}), &gorm.Config{
			DisableAutomaticPing: true,
			Logger:               gormLoggerFromDebug(configpkg.GetBool("app.debug", false)),
		})
	default:
		return nil, fmt.Errorf("database: unsupported driver: %s", driver)
	}
}

func gormLoggerFromDebug(debug bool) logger.Interface {
	return logger.Default.LogMode(gormLoggerLevelFromDebug(debug))
}

func gormLoggerLevelFromDebug(debug bool) logger.LogLevel {
	if debug {
		return logger.Info
	}
	return logger.Warn
}

// BuildMySQLDSN 根据 MySQLConfig 拼接 MySQL 连接串。
// 优先使用显式提供的 cfg.DSN；否则按 Laravel 风格字段回退默认值并拼装。
func BuildMySQLDSN(cfg MySQLConfig) string {
	if dsn := strings.TrimSpace(cfg.DSN); dsn != "" {
		return dsn
	}

	host := defaultIfBlank(cfg.Host, "127.0.0.1")
	port := defaultIfBlank(cfg.Port, "3306")
	username := defaultIfBlank(cfg.Username, "root")
	database := defaultIfBlank(cfg.Database, "")
	charset := defaultIfBlank(cfg.Charset, "utf8mb4")
	parseTime := defaultIfBlank(cfg.ParseTime, "true")
	loc := defaultIfBlank(cfg.Loc, "Local")

	return (&mysqldriver.Config{
		User:   username,
		Passwd: cfg.Password,
		Net:    "tcp",
		Addr:   net.JoinHostPort(host, port),
		DBName: database,
		Params: map[string]string{
			"charset":   charset,
			"parseTime": parseTime,
			"loc":       loc,
		},
	}).FormatDSN()
}

// buildDSNByDriver 根据驱动类型构建 DSN。
// 目前仅支持 mysql 驱动，其他驱动返回空字符串。
func buildDSNByDriver(driver string, cfg MySQLConfig) string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "mysql", "":
		return BuildMySQLDSN(cfg)
	case "sqlite", "sqlite3":
		return cfg.DSN
	default:
		return ""
	}
}

// defaultIfBlank 当 v 为空白字符串时返回 fallback。
func defaultIfBlank(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

type connectionPoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func applyConnectionPoolConfig(db *gorm.DB, cfg connectionPoolConfig) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	applySQLConnectionPoolConfig(sqlDB, cfg)
	return nil
}

func applySQLConnectionPoolConfig(sqlDB *sql.DB, cfg connectionPoolConfig) {
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
}

func parseDurationSecondsOrText(value string, fallback time.Duration) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	if strings.ContainsAny(value, "hmsu") {
		d, err := time.ParseDuration(value)
		if err == nil {
			return d
		}
		return fallback
	}
	n, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return time.Duration(n * float64(time.Second))
}

// OpenDefaultConnection 根据应用配置仓库创建默认数据库连接，目前仅支持 MySQL。
func OpenDefaultConnection() (*gorm.DB, error) {
	connection := configpkg.GetString("database.default", "mysql")
	return OpenConnection(connection)
}

// OpenConnection 根据给定连接名创建数据库连接，连接配置来源于 database.connections.{name}。
func OpenConnection(connection string) (*gorm.DB, error) {
	connection = strings.TrimSpace(connection)
	if connection == "" {
		connection = configpkg.GetString("database.default", "mysql")
	}
	prefix := "database.connections." + connection
	driver := configpkg.GetString(prefix+".driver", "mysql")
	dsn := buildDSNByDriver(driver, MySQLConfig{
		DSN:       configpkg.GetString(prefix+".dsn", ""),
		Host:      configpkg.GetString(prefix+".host", "127.0.0.1"),
		Port:      configpkg.GetString(prefix+".port", "3306"),
		Username:  configpkg.GetString(prefix+".username", "root"),
		Password:  configpkg.GetString(prefix+".password", ""),
		Database:  configpkg.GetString(prefix+".database", "prismgo"),
		Charset:   configpkg.GetString(prefix+".charset", "utf8mb4"),
		ParseTime: configpkg.GetString(prefix+".parse_time", "true"),
		Loc:       configpkg.GetString(prefix+".loc", "Local"),
	})

	db, err := Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	if err := applyConnectionPoolConfig(db, connectionPoolConfig{
		MaxOpenConns:    configpkg.GetInt(prefix+".max_open_conns", 30),
		MaxIdleConns:    configpkg.GetInt(prefix+".max_idle_conns", 10),
		ConnMaxLifetime: parseDurationSecondsOrText(configpkg.GetString(prefix+".conn_max_lifetime", "1h"), time.Hour),
		ConnMaxIdleTime: parseDurationSecondsOrText(configpkg.GetString(prefix+".conn_max_idle_time", "10m"), 10*time.Minute),
	}); err != nil {
		if sqlDB, closeErr := db.DB(); closeErr == nil {
			_ = sqlDB.Close()
		}
		return nil, err
	}
	return db, nil
}
