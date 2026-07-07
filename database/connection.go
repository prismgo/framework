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
	"gorm.io/gorm/schema"
)

// SSLConfig 描述 SSL/TLS 连接配置。
type SSLConfig struct {
	CA                 string // CA 证书路径
	Cert               string // 客户端证书路径
	Key                string // 客户端密钥路径
	InsecureSkipVerify bool   // 是否跳过证书验证
}

// toDSNParams 将 SSLConfig 转换为 DSN 参数。
// 如果有任何 TLS 参数，自动启用 tls。
func (s SSLConfig) toDSNParams() map[string]string {
	params := make(map[string]string)

	if s.CA != "" {
		params["tls-ca"] = s.CA
	}
	if s.Cert != "" {
		params["tls-cert"] = s.Cert
	}
	if s.Key != "" {
		params["tls-key"] = s.Key
	}
	if s.InsecureSkipVerify {
		params["tls-skip-verify"] = "true"
	}

	// 如果有任何 TLS 参数，启用 tls
	if len(params) > 0 {
		params["tls"] = "true"
	}

	return params
}

// MySQLConfig 描述 MySQL 连接所需的全部参数。
// 留空的字段会在 BuildMySQLDSN 中回退为常见默认值。
// 若 DSN 非空，则直接作为最终连接串，其余字段被忽略。
type MySQLConfig struct {
	DSN            string
	Host           string
	Port           string
	Username       string
	Password       string
	Database       string
	Charset        string
	ParseTime      string
	Loc            string
	UnixSocket     string   // UNIX socket 路径，非空时覆盖 Host/Port
	Collation      string   // 字符集排序规则，连接后通过 SET NAMES 应用
	TablePrefix    string   // 表名前缀，通过 GORM NamingStrategy 应用
	Strict         bool     // 是否启用严格模式（默认 true），当 Modes 非空时被忽略
	Modes          []string // 自定义 SQL 模式列表，优先级高于 Strict，非空时使用此列表
	Timezone       string   // 会话时区，空值跳过设置
	IsolationLevel string   // 事务隔离级别，空值跳过设置

	// Phase 3 新增字段
	Engine  string            // 默认存储引擎，空值使用 InnoDB
	SSL     SSLConfig         // SSL/TLS 配置
	Options map[string]string // 通用 DSN 参数，优先级高于默认值
}

// Open 根据 driver 字符串打开 GORM 数据库连接。
// 目前仅支持 driver == "mysql"；其他 driver 立即返回错误，避免隐式回退到 MySQL 造成困惑。
// cfg 用于传递 TablePrefix 等连接级配置到 GORM。
func Open(driver, dsn string, cfg MySQLConfig) (*gorm.DB, error) {
	driver = strings.ToLower(strings.TrimSpace(driver))
	switch driver {
	case "", "mysql":
		db, err := gorm.Open(mysql.New(mysql.Config{
			DSN:                       dsn,
			SkipInitializeWithVersion: true,
		}), &gorm.Config{
			DisableAutomaticPing: true,
			Logger:               gormLoggerFromDebug(configpkg.GetBool("app.debug", false)),
			NamingStrategy: schema.NamingStrategy{
				TablePrefix: cfg.TablePrefix,
			},
		})
		if err != nil {
			return nil, err
		}
		if err := configureConnection(db, cfg); err != nil {
			return nil, err
		}
		return db, nil
	default:
		return nil, fmt.Errorf("database: unsupported driver: %s", driver)
	}
}

// configureConnection 在连接建立后执行连接级配置（字符集、SQL 模式、时区、事务隔离级别）。
// 如果配置失败，返回错误并关闭底层 SQL 连接。
func configureConnection(db *gorm.DB, cfg MySQLConfig) error {
	// 1. 设置字符集和排序规则
	collation := strings.TrimSpace(cfg.Collation)
	if collation != "" {
		charset := defaultIfBlank(cfg.Charset, "utf8mb4")
		stmt := fmt.Sprintf("SET NAMES '%s' COLLATE '%s'", charset, collation)
		if err := db.Exec(stmt).Error; err != nil {
			if sqlDB, closeErr := db.DB(); closeErr == nil {
				_ = sqlDB.Close()
			}
			return fmt.Errorf("set collation failed: %w", err)
		}
	}

	// 2. 设置 SQL 模式
	if sqlMode := getSqlMode(cfg, db); sqlMode != "" {
		stmt := fmt.Sprintf("SET SESSION sql_mode='%s'", sqlMode)
		if err := db.Exec(stmt).Error; err != nil {
			if sqlDB, closeErr := db.DB(); closeErr == nil {
				_ = sqlDB.Close()
			}
			return fmt.Errorf("set sql_mode failed: %w", err)
		}
	}

	// 3. 设置时区
	if timezone := strings.TrimSpace(cfg.Timezone); timezone != "" {
		stmt := fmt.Sprintf("SET time_zone='%s'", timezone)
		if err := db.Exec(stmt).Error; err != nil {
			if sqlDB, closeErr := db.DB(); closeErr == nil {
				_ = sqlDB.Close()
			}
			return fmt.Errorf("set timezone failed: %w", err)
		}
	}

	// 4. 设置事务隔离级别
	if isolation := strings.TrimSpace(cfg.IsolationLevel); isolation != "" {
		stmt := fmt.Sprintf("SET SESSION TRANSACTION ISOLATION LEVEL %s", isolation)
		if err := db.Exec(stmt).Error; err != nil {
			if sqlDB, closeErr := db.DB(); closeErr == nil {
				_ = sqlDB.Close()
			}
			return fmt.Errorf("set isolation level failed: %w", err)
		}
	}

	return nil
}

// getMySQLVersion 查询 MySQL 版本号。
// 查询失败时返回 "8.0.11" 作为安全默认值（MySQL 8.0.11+ 移除了 NO_AUTO_CREATE_USER）。
func getMySQLVersion(db *gorm.DB) string {
	var version string
	if err := db.Raw("SELECT VERSION()").Scan(&version).Error; err != nil {
		return "8.0.11"
	}
	return version
}

// getSqlMode 根据配置计算 MySQL 的 sql_mode。
// 优先级：Modes > Strict > 版本检测默认值。
// - Modes 非空时直接拼接返回
// - Strict=false 时返回 NO_ENGINE_SUBSTITUTION
// - Strict=true 时根据 MySQL 版本生成默认模式（8.0.11+ 不含 NO_AUTO_CREATE_USER）
func getSqlMode(cfg MySQLConfig, db *gorm.DB) string {
	// 优先使用自定义模式
	if len(cfg.Modes) > 0 {
		return strings.Join(cfg.Modes, ",")
	}

	// 非严格模式
	if !cfg.Strict {
		return "NO_ENGINE_SUBSTITUTION"
	}

	// 严格模式：根据 MySQL 版本生成默认模式
	// MySQL 8.0.11+ 移除了 NO_AUTO_CREATE_USER
	version := getMySQLVersion(db)
	if version >= "8.0.11" {
		return "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
	}
	return "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
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

	// 默认使用 TCP 协议；若指定了 UNIX socket 路径，则切换到 unix 协议并忽略 Host/Port。
	netProto := "tcp"
	addr := net.JoinHostPort(host, port)
	if socket := strings.TrimSpace(cfg.UnixSocket); socket != "" {
		netProto = "unix"
		addr = socket
	}

	return (&mysqldriver.Config{
		User:   username,
		Passwd: cfg.Password,
		Net:    netProto,
		Addr:   addr,
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
	mysqlCfg := MySQLConfig{
		DSN:            configpkg.GetString(prefix+".dsn", ""),
		Host:           configpkg.GetString(prefix+".host", "127.0.0.1"),
		Port:           configpkg.GetString(prefix+".port", "3306"),
		Username:       configpkg.GetString(prefix+".username", "root"),
		Password:       configpkg.GetString(prefix+".password", ""),
		Database:       configpkg.GetString(prefix+".database", "prismgo"),
		Charset:        configpkg.GetString(prefix+".charset", "utf8mb4"),
		ParseTime:      configpkg.GetString(prefix+".parse_time", "true"),
		Loc:            configpkg.GetString(prefix+".loc", "Local"),
		UnixSocket:     configpkg.GetString(prefix+".unix_socket", ""),
		Collation:      configpkg.GetString(prefix+".collation", ""),
		TablePrefix:    configpkg.GetString(prefix+".prefix", ""),
		Strict:         configpkg.GetBool(prefix+".strict", true),
		Timezone:       configpkg.GetString(prefix+".timezone", ""),
		IsolationLevel: configpkg.GetString(prefix+".isolation_level", ""),
	}
	// 读取 modes 配置（逗号分隔的字符串数组）
	if modesStr := configpkg.GetString(prefix+".modes", ""); modesStr != "" {
		mysqlCfg.Modes = strings.Split(modesStr, ",")
		for i := range mysqlCfg.Modes {
			mysqlCfg.Modes[i] = strings.TrimSpace(mysqlCfg.Modes[i])
		}
	}
	dsn := buildDSNByDriver(driver, mysqlCfg)

	db, err := Open(driver, dsn, mysqlCfg)
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
