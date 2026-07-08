// Package database 提供 GORM 数据库连接的通用构造工具，屏蔽不同 driver 的差异。
// 当前仅内置 MySQL；如需扩展 SQLite / Postgres 等，可新增同名 DSN 构造器后在 Open 中分支。
package database

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/internal/version"
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

// validate 验证 SSL 证书文件路径是否存在。
// 如果任何证书路径不存在，返回错误以提前发现配置问题。
func (s SSLConfig) validate() error {
	if s.CA != "" {
		if _, err := os.Stat(s.CA); os.IsNotExist(err) {
			return fmt.Errorf("SSL CA certificate not found: %s", s.CA)
		}
	}
	if s.Cert != "" {
		if _, err := os.Stat(s.Cert); os.IsNotExist(err) {
			return fmt.Errorf("SSL client certificate not found: %s", s.Cert)
		}
	}
	if s.Key != "" {
		if _, err := os.Stat(s.Key); os.IsNotExist(err) {
			return fmt.Errorf("SSL client key not found: %s", s.Key)
		}
	}
	return nil
}

// MySQLConnectionConfig 描述连接基础信息（连接建立时必需）。
// 这些字段在建立 TCP/Unix Socket 连接时使用，是连接建立的最小必需配置。
type MySQLConnectionConfig struct {
	DSN        string // 完整 DSN，非空时忽略其他字段
	Host       string // MySQL 主机地址，默认 127.0.0.1
	Port       string // MySQL 端口，默认 3306
	Username   string // 用户名，默认 root
	Password   string // 密码
	Database   string // 数据库名
	UnixSocket string // UNIX socket 路径，非空时覆盖 Host/Port
}

// MySQLSessionConfig 描述会话级配置（连接建立后通过 SQL 设置）。
// 这些字段在连接建立后通过 SET 语句应用到会话中。
type MySQLSessionConfig struct {
	Charset        string   // 字符集，默认 utf8mb4
	ParseTime      string   // 是否解析 time.Time，默认 true
	Loc            string   // 时区位置，默认 Local
	Collation      string   // 字符集排序规则，连接后通过 SET NAMES 应用
	Timezone       string   // 会话时区，空值跳过设置
	Strict         bool     // 是否启用严格模式（默认 true），当 Modes 非空时被忽略
	Modes          []string // 自定义 SQL 模式列表，优先级高于 Strict，非空时使用此列表
	IsolationLevel string   // 事务隔离级别，空值跳过设置
}

// MySQLDriverConfig 描述 go-sql-driver/mysql 驱动级配置。
// 这些字段直接映射到 go-sql-driver/mysql 的 Config 结构体，影响驱动行为。
type MySQLDriverConfig struct {
	// AllowNativePasswords 是否允许使用 MySQL 原生密码认证。
	// MySQL 8.0+ 默认使用 caching_sha2_password，但部分用户仍使用 mysql_native_password。
	// 对应 go-sql-driver/mysql 的 Config.AllowNativePasswords。
	// 通过 config/database.go 中 connections.mysql.options.allow_native_passwords 配置。
	// 注意：通过 readMySQLConfig 读取时默认为 false（安全默认原则）；直接构造 MySQLConfig 时零值也为 false。
	// 需要兼容旧版 MySQL 用户时，显式配置 allow_native_passwords=true。
	AllowNativePasswords bool

	// CheckConnLiveness 是否在使用连接前检查其活性。
	// 启用后，驱动会在从连接池取出连接时发送 Ping 探活，增加一次网络 RTT。
	// 高并发短查询场景可考虑关闭以提升吞吐量，但需接受偶因连接失效导致的首次查询失败。
	// 对应 go-sql-driver/mysql 的 Config.CheckConnLiveness。
	// 通过 config/database.go 中 connections.mysql.options.check_conn_liveness 配置。
	// 注意：通过 readMySQLConfig 读取时默认为 true；直接构造 MySQLConfig 时零值为 false。
	// 如需与驱动默认行为一致，请显式设置为 true。
	CheckConnLiveness bool

	// RejectReadOnly 是否拒绝连接到只读 MySQL 实例。
	// 启用后，如果驱动检测到连接为只读模式，会返回错误而非执行写操作。
	// 主从架构中确保写入操作不会发送到只读节点时非常有用。
	// 对应 go-sql-driver/mysql 的 Config.RejectReadOnly。
	// 通过 config/database.go 中 connections.mysql.options.reject_read_only 配置。
	RejectReadOnly bool

	// ClientFoundRows 控制 UPDATE 语句返回的行数含义。
	// 启用后，UPDATE 返回匹配行数而非实际修改行数。
	// 适用于需要知道有多少行符合条件而非实际修改了多少行的业务场景。
	// 对应 go-sql-driver/mysql 的 Config.ClientFoundRows。
	// 通过 config/database.go 中 connections.mysql.options.client_found_rows 配置。
	ClientFoundRows bool

	// MultiStatements 是否允许单个查询中包含多条 SQL 语句。
	// 启用后，可以在一次查询中执行多条以分号分隔的 SQL 语句。
	// 注意：? 参数只能用于第一条语句。适用于数据库迁移脚本和批量操作。
	// 警告：启用后若任何查询包含用户输入，攻击者可通过分号注入任意 SQL 语句，
	// 请确保仅在完全可控的 SQL 执行路径（如迁移脚本）中启用。
	// 对应 go-sql-driver/mysql 的 Config.MultiStatements。
	// 通过 config/database.go 中 connections.mysql.options.multi_statements 配置。
	MultiStatements bool

	// ColumnsWithAlias 控制 sql.Rows.Columns() 返回的列名格式。
	// 启用后，列名会包含表别名，格式为 "表别名.列名"。
	// 例如：SELECT u.id FROM users as u 会返回 "u.id" 而非 "id"。
	// 对应 go-sql-driver/mysql 的 Config.ColumnsWithAlias。
	// 通过 config/database.go 中 connections.mysql.options.columns_with_alias 配置。
	ColumnsWithAlias bool

	// InterpolateParams 是否将占位符 ? 插值到查询字符串中。
	// 启用后，驱动会将参数直接拼接到 SQL 中，减少一次网络往返。
	//
	// 安全警告：不能与以下多字节编码一起使用，否则可能导致 SQL 注入：
	//   - BIG5, CP932, GB2312, GBK, SJIS
	// 这些编码的某些字节序列可能与转义字符 \' 组合，绕过安全检查。
	// 若使用 utf8、utf8mb4、latin1 等单字节或安全编码，则可安全启用。
	// 务必确保所有查询参数来源可信，避免用户输入直接拼接。
	//
	// 对应 go-sql-driver/mysql 的 Config.InterpolateParams。
	// 通过 config/database.go 中 connections.mysql.options.interpolate_params 配置。
	InterpolateParams bool

	// MaxAllowedPacket 设置允许的最大数据包大小（字节）。
	// 默认值为 0，表示每次连接时自动从服务器获取 max_allowed_packet 变量。
	// 设置为正整数可指定固定值（如 64*1024*1024 = 64MB）。
	// 处理大查询或大结果集时需要调整此值。
	// 对应 go-sql-driver/mysql 的 Config.MaxAllowedPacket。
	// 通过 config/database.go 中 connections.mysql.options.max_allowed_packet 配置。
	MaxAllowedPacket int

	// Options 通用 DSN 参数，优先级高于默认值
	Options map[string]string
}

// MySQLSchemaConfig 描述 Schema 级配置（GORM NamingStrategy 使用）。
// 这些字段影响 GORM 的表名、索引名生成策略。
type MySQLSchemaConfig struct {
	TablePrefix   string    // 表名前缀，通过 GORM NamingStrategy 应用
	Engine        string    // 默认存储引擎，空值使用 InnoDB
	PrefixIndexes bool      // 是否前缀索引名
	SSL           SSLConfig // SSL/TLS 配置
}

// MySQLConfig 描述 MySQL 连接所需的全部参数。
// 通过组合四个命名子结构体实现按职责分层。
// 留空的字段会在 BuildMySQLDSN 中回退为常见默认值。
// 若 DSN 非空，则直接作为最终连接串，其余字段被忽略。
//
// 结构体分层：
//   - Connection: 连接基础信息（连接建立时必需）
//   - Session: 会话级配置（连接建立后通过 SQL 设置）
//   - Driver: 驱动级配置（go-sql-driver/mysql 特定选项）
//   - Schema: Schema 级配置（GORM NamingStrategy 使用）
//
// 注意：此重构将原有的扁平结构拆分为嵌套结构，外部构造 MySQLConfig 的代码需要更新字段访问路径。
// 例如：原 cfg.Host 需改为 cfg.Connection.Host。
//
// 构造完成后不应修改 Driver.Options 和 Session.Modes，因为这些是引用类型，
// 值传递时仅拷贝 header，底层数据结构共享。
type MySQLConfig struct {
	Connection MySQLConnectionConfig
	Session    MySQLSessionConfig
	Driver     MySQLDriverConfig
	Schema     MySQLSchemaConfig
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
				TablePrefix: cfg.Schema.TablePrefix,
			},
		})
		if err != nil {
			return nil, err
		}
		if err := configureConnection(db, cfg); err != nil {
			// 配置失败时统一关闭连接
			if sqlDB, closeErr := db.DB(); closeErr == nil {
				if closeErr = sqlDB.Close(); closeErr != nil {
					exception.Report(context.Background(), closeErr, map[string]any{"component": "database", "operation": "close_on_config_failure"})
				}
			}
			return nil, err
		}
		return db, nil
	default:
		return nil, fmt.Errorf("database: unsupported driver: %s", driver)
	}
}

// validIsolationLevels 定义 MySQL 支持的事务隔离级别白名单。
var validIsolationLevels = map[string]struct{}{
	"READ UNCOMMITTED": {},
	"READ COMMITTED":   {},
	"REPEATABLE READ":  {},
	"SERIALIZABLE":     {},
}

// validCharsets 定义 MySQL 支持的完整字符集白名单。
var validCharsets = map[string]struct{}{
	"utf8mb4": {}, "utf8": {}, "utf8mb3": {}, "latin1": {}, "ascii": {},
	"binary": {}, "big5": {}, "cp1250": {}, "cp1251": {}, "cp1256": {},
	"cp1257": {}, "cp850": {}, "cp852": {}, "cp866": {}, "cp932": {},
	"dec8": {}, "euckr": {}, "gb18030": {}, "gb2312": {}, "gbk": {},
	"geostd8": {}, "greek": {}, "hebrew": {}, "hp8": {}, "keybcs2": {},
	"koi8r": {}, "koi8u": {}, "latin2": {}, "latin5": {}, "latin7": {},
	"macce": {}, "macroman": {}, "sjis": {}, "swe7": {}, "tis620": {},
	"ucs2": {}, "ujis": {}, "utf16": {}, "utf32": {}, "armscii8": {},
}

// validateCharset 验证 charset 是否在 MySQL 支持的白名单中。
// MySQL 对 charset 大小写不敏感，因此验证前统一转为小写。
func validateCharset(charset string) error {
	if charset == "" {
		return fmt.Errorf("charset cannot be empty")
	}
	// 规范化为小写后匹配白名单
	normalizedCharset := strings.ToLower(charset)
	if _, ok := validCharsets[normalizedCharset]; !ok {
		return fmt.Errorf("invalid charset: %s", charset)
	}
	return nil
}

// validateCollation 验证 collation 是否以对应的 charset 为前缀，并防止 SQL 注入。
// MySQL 对 collation 大小写不敏感，因此验证前统一转为小写。
func validateCollation(charset, collation string) error {
	if collation == "" {
		return fmt.Errorf("collation cannot be empty")
	}
	// 检查是否包含特殊字符（防止 SQL 注入）
	if strings.ContainsAny(collation, "';\"\\") {
		return fmt.Errorf("collation contains invalid characters: %s", collation)
	}
	// 规范化为小写后比较
	normalizedCharset := strings.ToLower(charset)
	normalizedCollation := strings.ToLower(collation)
	// collation 必须完全等于 charset（如 binary）或以 charset + "_" 开头
	if normalizedCollation != normalizedCharset && !strings.HasPrefix(normalizedCollation, normalizedCharset+"_") {
		return fmt.Errorf("collation %s does not match charset %s", collation, charset)
	}
	return nil
}

// validTimezones 定义 MySQL 支持的常用命名时区白名单。
var validTimezones = map[string]struct{}{
	"UTC":        {},
	"US/Eastern": {}, "US/Central": {}, "US/Mountain": {}, "US/Pacific": {},
	"Asia/Shanghai": {}, "Asia/Tokyo": {}, "Asia/Hong_Kong": {}, "Asia/Singapore": {},
	"Europe/London": {}, "Europe/Paris": {}, "Europe/Berlin": {}, "Europe/Moscow": {},
	"America/New_York": {}, "America/Chicago": {}, "America/Denver": {}, "America/Los_Angeles": {},
}

// validateTimezone 验证时区是否为 MySQL 支持的格式（SYSTEM、命名时区或偏移量）。
func validateTimezone(tz string) error {
	if tz == "" {
		return fmt.Errorf("timezone cannot be empty")
	}
	// 检查是否包含非法字符（防止 SQL 注入）
	if strings.ContainsAny(tz, "';\"\\") {
		return fmt.Errorf("timezone contains invalid characters: %s", tz)
	}
	// 检查是否为 SYSTEM 关键字（大小写不敏感）
	if strings.ToUpper(tz) == "SYSTEM" {
		return nil
	}
	// 如果是偏移量格式（以 + 或 - 开头）
	if strings.HasPrefix(tz, "+") || strings.HasPrefix(tz, "-") {
		return validateOffsetTimezone(tz)
	}
	// 否则检查是否在命名时区白名单中
	if _, ok := validTimezones[tz]; !ok {
		return fmt.Errorf("invalid timezone: %s", tz)
	}
	return nil
}

// validateOffsetTimezone 验证偏移量格式的时区（如 +08:00, -05:00, +8:00, -5:00）。
// MySQL 支持标准格式 ±HH:MM 和非标准格式 ±H:MM。
func validateOffsetTimezone(tz string) error {
	// 检查是否包含非法字符
	if strings.ContainsAny(tz, "';\"\\") {
		return fmt.Errorf("invalid offset timezone format: %s", tz)
	}
	// 格式：±H:MM 或 ±HH:MM
	// 查找冒号位置
	colonIdx := strings.Index(tz, ":")
	if colonIdx == -1 {
		return fmt.Errorf("invalid offset timezone format: %s", tz)
	}
	// 验证小时部分（冒号前）
	hourPart := tz[1:colonIdx]
	if len(hourPart) == 0 || len(hourPart) > 2 {
		return fmt.Errorf("invalid offset timezone format: %s", tz)
	}
	hour, err := strconv.Atoi(hourPart)
	if err != nil || hour < 0 || hour > 23 {
		return fmt.Errorf("invalid offset timezone hour: %s", tz)
	}
	// 验证分钟部分（冒号后）
	minutePart := tz[colonIdx+1:]
	if len(minutePart) != 2 {
		return fmt.Errorf("invalid offset timezone format: %s", tz)
	}
	minute, err := strconv.Atoi(minutePart)
	if err != nil || minute < 0 || minute > 59 {
		return fmt.Errorf("invalid offset timezone minute: %s", tz)
	}
	return nil
}

// validSqlModes 定义 MySQL 支持的 SQL 模式白名单。
var validSqlModes = map[string]struct{}{
	"ANSI":                       {},
	"DB2":                        {},
	"MSSQL":                      {},
	"MYSQL323":                   {},
	"MYSQL40":                    {},
	"ORACLE":                     {},
	"POSTGRESQL":                 {},
	"TRADITIONAL":                {},
	"ALLOW_INVALID_DATES":        {},
	"ANSI_QUOTES":                {},
	"ERROR_FOR_DIVISION_BY_ZERO": {},
	"HIGH_NOT_PRECEDENCE":        {},
	"IGNORE_SPACE":               {},
	"NO_AUTO_CREATE_USER":        {},
	"NO_AUTO_VALUE_ON_ZERO":      {},
	"NO_BACKSLASH_ESCAPES":       {},
	"NO_DIR_IN_CREATE":           {},
	"NO_ENGINE_SUBSTITUTION":     {},
	"NO_FIELD_OPTIONS":           {},
	"NO_KEY_OPTIONS":             {},
	"NO_TABLE_OPTIONS":           {},
	"NO_UNSIGNED_SUBTRACTION":    {},
	"NO_ZERO_DATE":               {},
	"NO_ZERO_IN_DATE":            {},
	"ONLY_FULL_GROUP_BY":         {},
	"PAD_CHAR_TO_FULL_LENGTH":    {},
	"PIPES_AS_CONCAT":            {},
	"REAL_AS_FLOAT":              {},
	"STRICT_ALL_TABLES":          {},
	"STRICT_TRANS_TABLES":        {},
	"TIME_TRUNCATE_FRACTIONAL":   {},
}

// validateSqlMode 验证单个 SQL 模式是否在白名单中。
// MySQL 对 SQL mode 大小写不敏感，因此验证前统一转为大写。
func validateSqlMode(mode string) error {
	if mode == "" {
		return fmt.Errorf("sql mode cannot be empty")
	}
	// 检查是否包含非法字符（防止 SQL 注入）
	if strings.ContainsAny(mode, "';\"\\,") {
		return fmt.Errorf("sql mode contains invalid characters: %s", mode)
	}
	// 规范化为大写后匹配白名单
	normalizedMode := strings.ToUpper(mode)
	if _, ok := validSqlModes[normalizedMode]; !ok {
		return fmt.Errorf("invalid sql mode: %s", mode)
	}
	return nil
}

// validateSqlModes 验证 SQL 模式列表中的所有模式。
func validateSqlModes(modes []string) error {
	for _, mode := range modes {
		if err := validateSqlMode(mode); err != nil {
			return err
		}
	}
	return nil
}

// dangerousCharsetsForInterpolate 定义与 InterpolateParams 不兼容的多字节编码列表。
// 这些编码的某些字节序列可能与转义字符 \' 组合，绕过安全检查，导致 SQL 注入风险。
var dangerousCharsetsForInterpolate = map[string]struct{}{
	"big5":   {},
	"cp932":  {},
	"gb2312": {},
	"gbk":    {},
	"sjis":   {},
}

// validateInterpolateParamsWithCharset 验证 InterpolateParams 是否与危险字符集组合使用。
// 如果 InterpolateParams 启用且字符集属于多字节不安全编码，返回错误以防止 SQL 注入。
func validateInterpolateParamsWithCharset(charset string) error {
	normalizedCharset := strings.ToLower(charset)
	if _, dangerous := dangerousCharsetsForInterpolate[normalizedCharset]; dangerous {
		return fmt.Errorf("InterpolateParams cannot be used with charset %s due to SQL injection risk", normalizedCharset)
	}
	return nil
}

// configureConnection 在连接建立后执行连接级配置（字符集、SQL 模式、时区、事务隔离级别）。
// 如果配置失败，返回错误，由调用方负责关闭连接。
func configureConnection(db *gorm.DB, cfg MySQLConfig) error {
	// 先验证所有配置，确认都有效后再执行任何 SQL
	// 这样可以避免部分配置已经应用后才发现问题

	// 1. 验证 charset 和 collation
	collation := strings.TrimSpace(cfg.Session.Collation)
	charset := ""
	if collation != "" {
		charset = defaultIfBlank(cfg.Session.Charset, "utf8mb4")
		if err := validateCharset(charset); err != nil {
			return fmt.Errorf("invalid charset: %w", err)
		}
		if err := validateCollation(charset, collation); err != nil {
			return fmt.Errorf("invalid collation: %w", err)
		}
	}

	// 2. 验证 SQL 模式
	if len(cfg.Session.Modes) > 0 {
		if err := validateSqlModes(cfg.Session.Modes); err != nil {
			return fmt.Errorf("invalid sql modes: %w", err)
		}
	}

	// 3. 验证时区
	timezone := strings.TrimSpace(cfg.Session.Timezone)
	if timezone != "" {
		if err := validateTimezone(timezone); err != nil {
			return fmt.Errorf("invalid timezone: %w", err)
		}
	}

	// 4. 验证事务隔离级别
	isolation := strings.TrimSpace(cfg.Session.IsolationLevel)
	upperIsolation := ""
	if isolation != "" {
		upperIsolation = strings.ToUpper(isolation)
		if _, ok := validIsolationLevels[upperIsolation]; !ok {
			return fmt.Errorf("invalid isolation level: %s (valid levels: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE)", isolation)
		}
	}

	// 5. 验证 InterpolateParams 与危险字符集组合
	if cfg.Driver.InterpolateParams {
		charsetForValidation := defaultIfBlank(cfg.Session.Charset, "utf8mb4")
		if err := validateInterpolateParamsWithCharset(charsetForValidation); err != nil {
			return err
		}
	}

	// 6. 验证 SSL 证书文件路径
	if err := cfg.Schema.SSL.validate(); err != nil {
		return fmt.Errorf("SSL validation failed: %w", err)
	}

	// 所有验证通过，开始执行 SQL

	// 1. 设置字符集和排序规则
	if collation != "" {
		stmt := fmt.Sprintf("SET NAMES '%s' COLLATE '%s'", charset, collation)
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("set collation failed: %w", err)
		}
	}

	// 2. 设置 SQL 模式
	if sqlMode := getSqlMode(cfg, db); sqlMode != "" {
		stmt := fmt.Sprintf("SET SESSION sql_mode='%s'", sqlMode)
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("set sql_mode failed: %w", err)
		}
	}

	// 3. 设置时区
	if timezone != "" {
		stmt := fmt.Sprintf("SET time_zone='%s'", timezone)
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("set timezone failed: %w", err)
		}
	}

	// 4. 设置事务隔离级别
	if upperIsolation != "" {
		stmt := fmt.Sprintf("SET SESSION TRANSACTION ISOLATION LEVEL %s", upperIsolation)
		if err := db.Exec(stmt).Error; err != nil {
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
	if len(cfg.Session.Modes) > 0 {
		return strings.Join(cfg.Session.Modes, ",")
	}

	// 非严格模式
	if !cfg.Session.Strict {
		return "NO_ENGINE_SUBSTITUTION"
	}

	// 严格模式：根据 MySQL 版本生成默认模式
	// MySQL 8.0.11+ 移除了 NO_AUTO_CREATE_USER
	ver := getMySQLVersion(db)
	if version.AtLeast(ver, "8.0.11") {
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
// 优先使用显式提供的 cfg.Connection.DSN；否则按 Laravel 风格字段回退默认值并拼装。
func BuildMySQLDSN(cfg MySQLConfig) string {
	if dsn := strings.TrimSpace(cfg.Connection.DSN); dsn != "" {
		return dsn
	}

	host := defaultIfBlank(cfg.Connection.Host, "127.0.0.1")
	port := defaultIfBlank(cfg.Connection.Port, "3306")
	username := defaultIfBlank(cfg.Connection.Username, "root")
	database := defaultIfBlank(cfg.Connection.Database, "")
	charset := defaultIfBlank(cfg.Session.Charset, "utf8mb4")
	parseTime := defaultIfBlank(cfg.Session.ParseTime, "true")
	loc := defaultIfBlank(cfg.Session.Loc, "Local")

	// 默认使用 TCP 协议；若指定了 UNIX socket 路径，则切换到 unix 协议并忽略 Host/Port。
	netProto := "tcp"
	addr := net.JoinHostPort(host, port)
	if socket := strings.TrimSpace(cfg.Connection.UnixSocket); socket != "" {
		netProto = "unix"
		addr = socket
	}

	// 构建参数：默认值
	params := map[string]string{
		"charset":   charset,
		"parseTime": parseTime,
		"loc":       loc,
	}

	// 合并 SSL 参数（优先级高于默认值）
	for k, v := range cfg.Schema.SSL.toDSNParams() {
		params[k] = v
	}

	// 合并 Options（优先级最高）
	for k, v := range cfg.Driver.Options {
		params[k] = v
	}

	return (&mysqldriver.Config{
		User:                 username,
		Passwd:               cfg.Connection.Password,
		Net:                  netProto,
		Addr:                 addr,
		DBName:               database,
		Params:               params,
		AllowNativePasswords: cfg.Driver.AllowNativePasswords,
		CheckConnLiveness:    cfg.Driver.CheckConnLiveness,
		RejectReadOnly:       cfg.Driver.RejectReadOnly,
		ClientFoundRows:      cfg.Driver.ClientFoundRows,
		MultiStatements:      cfg.Driver.MultiStatements,
		ColumnsWithAlias:     cfg.Driver.ColumnsWithAlias,
		InterpolateParams:    cfg.Driver.InterpolateParams,
		MaxAllowedPacket:     cfg.Driver.MaxAllowedPacket,
	}).FormatDSN()
}

// buildDSNByDriver 根据驱动类型构建 DSN。
// 目前仅支持 mysql 驱动，其他驱动返回空字符串。
func buildDSNByDriver(driver string, cfg MySQLConfig) string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "mysql", "":
		return BuildMySQLDSN(cfg)
	case "sqlite", "sqlite3":
		return cfg.Connection.DSN
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
	mysqlCfg := readMySQLConfig(prefix)

	dsn := buildDSNByDriver(driver, mysqlCfg)

	db, err := Open(driver, dsn, mysqlCfg)
	if err != nil {
		return nil, err
	}
	if err := applyConnectionPoolConfig(db, readPoolConfig(prefix)); err != nil {
		if sqlDB, closeErr := db.DB(); closeErr == nil {
			if closeErr = sqlDB.Close(); closeErr != nil {
				exception.Report(context.Background(), closeErr, map[string]any{"component": "database", "operation": "close_on_pool_config_failure"})
			}
		}
		return nil, err
	}
	return db, nil
}

// setOption 设置 Options map 中的键值对，自动初始化 map。
func (cfg *MySQLConfig) setOption(key, value string) {
	if cfg.Driver.Options == nil {
		cfg.Driver.Options = make(map[string]string)
	}
	cfg.Driver.Options[key] = value
}

// readMySQLDriverOptions 读取 MySQL 驱动级配置选项。
func readMySQLDriverOptions(prefix string, cfg *MySQLConfig) {
	// 读取 AllowNativePasswords 配置
	// 允许使用 MySQL 原生密码认证，对应 go-sql-driver/mysql 的 Config.AllowNativePasswords。
	// 默认 false 以遵循安全默认原则，推荐使用 caching_sha2_password（MySQL 8.0+ 默认）。
	// 需要兼容旧版 MySQL 用户时，显式配置 allow_native_passwords=true。
	cfg.Driver.AllowNativePasswords = configpkg.GetBool(prefix+".options.allow_native_passwords", false)

	// 读取 CheckConnLiveness 配置
	// 是否在使用连接前检查其活性，对应 go-sql-driver/mysql 的 Config.CheckConnLiveness。
	// 默认 true 以匹配 go-sql-driver/mysql NewConfig() 的默认行为。
	cfg.Driver.CheckConnLiveness = configpkg.GetBool(prefix+".options.check_conn_liveness", true)

	// 读取 RejectReadOnly 配置
	// 是否拒绝连接到只读 MySQL 实例，对应 go-sql-driver/mysql 的 Config.RejectReadOnly。
	// 默认 false。
	cfg.Driver.RejectReadOnly = configpkg.GetBool(prefix+".options.reject_read_only", false)

	// 读取 ClientFoundRows 配置
	// UPDATE 语句返回匹配行数而非实际修改行数，对应 go-sql-driver/mysql 的 Config.ClientFoundRows。
	// 默认 false。
	cfg.Driver.ClientFoundRows = configpkg.GetBool(prefix+".options.client_found_rows", false)

	// 读取 MultiStatements 配置
	// 是否允许单个查询中包含多条 SQL 语句，对应 go-sql-driver/mysql 的 Config.MultiStatements。
	// 默认 false。
	cfg.Driver.MultiStatements = configpkg.GetBool(prefix+".options.multi_statements", false)

	// 读取 ColumnsWithAlias 配置
	// 列名包含表别名，对应 go-sql-driver/mysql 的 Config.ColumnsWithAlias。
	// 默认 false。
	cfg.Driver.ColumnsWithAlias = configpkg.GetBool(prefix+".options.columns_with_alias", false)

	// 读取 InterpolateParams 配置
	// 是否将占位符插值到查询字符串中，对应 go-sql-driver/mysql 的 Config.InterpolateParams。
	// 默认 false。
	cfg.Driver.InterpolateParams = configpkg.GetBool(prefix+".options.interpolate_params", false)

	// 读取 MaxAllowedPacket 配置
	// 最大数据包大小（字节），对应 go-sql-driver/mysql 的 Config.MaxAllowedPacket。
	// 默认 0 表示自动从服务器获取。
	// 注意：默认值 0 会在每次连接建立时执行额外查询，增加约 1 次网络 RTT。
	// 在高并发短连接场景下，建议显式设置固定值以避免此开销。
	// 负值或超过 1GB 的值无意义，归零处理以回退到自动检测模式。
	cfg.Driver.MaxAllowedPacket = configpkg.GetInt(prefix+".options.max_allowed_packet", 0)
	if cfg.Driver.MaxAllowedPacket < 0 || cfg.Driver.MaxAllowedPacket > 1<<30 {
		cfg.Driver.MaxAllowedPacket = 0
	}

	// 读取 Compress 配置（通过 Options map 传递）
	// 是否启用 zlib 压缩，对应 go-sql-driver/mysql 的 compress DSN 参数。
	// 默认 false。
	// 注意：压缩会减少网络带宽但增加 CPU 开销。对于大结果集或慢速网络环境有益，
	// 但对于小查询或局域网环境可能降低整体性能。
	if compress := configpkg.GetBool(prefix+".options.compress", false); compress {
		cfg.setOption("compress", "true")
	}
}

// readMySQLConfig 从配置中读取 MySQL 连接配置。
func readMySQLConfig(prefix string) MySQLConfig {
	cfg := MySQLConfig{
		Connection: MySQLConnectionConfig{
			DSN:        configpkg.GetString(prefix+".dsn", ""),
			Host:       configpkg.GetString(prefix+".host", "127.0.0.1"),
			Port:       configpkg.GetString(prefix+".port", "3306"),
			Username:   configpkg.GetString(prefix+".username", "root"),
			Password:   configpkg.GetString(prefix+".password", ""),
			Database:   configpkg.GetString(prefix+".database", "prismgo"),
			UnixSocket: configpkg.GetString(prefix+".unix_socket", ""),
		},
		Session: MySQLSessionConfig{
			Charset:        configpkg.GetString(prefix+".charset", "utf8mb4"),
			ParseTime:      configpkg.GetString(prefix+".parse_time", "true"),
			Loc:            configpkg.GetString(prefix+".loc", "Local"),
			Collation:      configpkg.GetString(prefix+".collation", ""),
			Timezone:       configpkg.GetString(prefix+".timezone", ""),
			Strict:         configpkg.GetBool(prefix+".strict", true),
			IsolationLevel: configpkg.GetString(prefix+".isolation_level", ""),
		},
		Schema: MySQLSchemaConfig{
			TablePrefix:   configpkg.GetString(prefix+".prefix", ""),
			Engine:        configpkg.GetString(prefix+".engine", ""),
			PrefixIndexes: configpkg.GetBool(prefix+".prefix_indexes", false),
		},
	}

	// 读取 modes 配置（逗号分隔的字符串数组）
	if modesStr := configpkg.GetString(prefix+".modes", ""); modesStr != "" {
		cfg.Session.Modes = strings.Split(modesStr, ",")
		for i := range cfg.Session.Modes {
			cfg.Session.Modes[i] = strings.TrimSpace(cfg.Session.Modes[i])
		}
	}

	// 读取 SSL 配置
	cfg.Schema.SSL = SSLConfig{
		CA:                 configpkg.GetString(prefix+".ssl.ca", ""),
		Cert:               configpkg.GetString(prefix+".ssl.cert", ""),
		Key:                configpkg.GetString(prefix+".ssl.key", ""),
		InsecureSkipVerify: configpkg.GetBool(prefix+".ssl.insecure_skip_verify", false),
	}

	// 读取 Options 配置（通用 DSN 参数）
	// 注意：configpkg 不支持 map[string]string，需要通过 JSON 或其他方式读取
	// 当前实现：从 options 前缀读取常见参数
	if timeout := configpkg.GetString(prefix+".options.timeout", ""); timeout != "" {
		cfg.setOption("timeout", timeout)
	}
	if readTimeout := configpkg.GetString(prefix+".options.read_timeout", ""); readTimeout != "" {
		cfg.setOption("readTimeout", readTimeout)
	}
	if writeTimeout := configpkg.GetString(prefix+".options.write_timeout", ""); writeTimeout != "" {
		cfg.setOption("writeTimeout", writeTimeout)
	}

	// 读取驱动级配置选项
	readMySQLDriverOptions(prefix, &cfg)

	return cfg
}

// readPoolConfig 从配置中读取连接池配置。
func readPoolConfig(prefix string) connectionPoolConfig {
	return connectionPoolConfig{
		MaxOpenConns:    configpkg.GetInt(prefix+".max_open_conns", 30),
		MaxIdleConns:    configpkg.GetInt(prefix+".max_idle_conns", 10),
		ConnMaxLifetime: parseDurationSecondsOrText(configpkg.GetString(prefix+".conn_max_lifetime", "1h"), time.Hour),
		ConnMaxIdleTime: parseDurationSecondsOrText(configpkg.GetString(prefix+".conn_max_idle_time", "10m"), 10*time.Minute),
	}
}
