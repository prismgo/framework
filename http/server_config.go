package http

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/prismgo/framework/config"
)

const (
	defaultServerPort               = "8080"
	defaultServerRequestTimeout     = 15 * time.Second
	defaultServerReadHeaderTimeout  = 5 * time.Second
	defaultServerWriteTimeout       = 30 * time.Second
	defaultServerIdleTimeout        = 60 * time.Second
	defaultServerMaxHeaderBytes     = 1 << 20
	defaultServerMaxMultipartMemory = 32 << 20
)

// ServerConfig 汇总 HTTP Server 和 Gin Engine 的启动期配置。
//
// 用途：把监听地址、读写超时、优雅停机、请求大小、代理 IP 与中间件开关集中解析，
// 避免启动命令、server 构造和中间件分别读取零散配置。
type ServerConfig struct {
	Host               string
	Port               string
	Debug              bool
	ReadTimeout        time.Duration
	ReadHeaderTimeout  time.Duration
	WriteTimeout       time.Duration
	IdleTimeout        time.Duration
	ShutdownTimeout    time.Duration
	MaxHeaderBytes     int
	MaxMultipartMemory int64
	TrustedProxies     []string
	ClientIPHeaders    []string
	// AccessLog 控制 Gin 内置访问日志中间件。
	AccessLog bool
	// ExceptionHandler 控制统一 HTTP 异常处理中间件。
	//
	// 该开关刻意替代旧的 error-log/recovery/panic-stack 分散配置；
	// 细粒度行为应通过 foundation.WithExceptions
	// 在代码中扩展。
	ExceptionHandler bool
}

func (cfg ServerConfig) AccessLogEnabled() bool {
	return cfg.AccessLog
}

func (cfg ServerConfig) ExceptionHandlerEnabled() bool {
	return cfg.ExceptionHandler
}

// CurrentServerConfig 从当前全局配置仓库读取 HTTP Server 配置。
func CurrentServerConfig() ServerConfig {
	legacyTimeout := serverDuration("app.server.timeout", defaultServerRequestTimeout)

	return ServerConfig{
		Host:               strings.TrimSpace(config.GetString("app.server.host", "")),
		Port:               normalizeServerPort(config.GetString("app.server.port", defaultServerPort)),
		Debug:              config.GetBool("app.debug", false),
		ReadTimeout:        serverDuration("app.server.read_timeout", legacyTimeout),
		ReadHeaderTimeout:  serverDuration("app.server.read_header_timeout", defaultServerReadHeaderTimeout),
		WriteTimeout:       serverDuration("app.server.write_timeout", legacyTimeout),
		IdleTimeout:        serverDuration("app.server.idle_timeout", defaultServerIdleTimeout),
		ShutdownTimeout:    serverDuration("app.server.shutdown_timeout", legacyTimeout),
		MaxHeaderBytes:     positiveInt(config.GetInt("app.server.max_header_bytes", defaultServerMaxHeaderBytes), defaultServerMaxHeaderBytes),
		MaxMultipartMemory: positiveInt64(config.GetInt64("app.server.max_multipart_memory", defaultServerMaxMultipartMemory), defaultServerMaxMultipartMemory),
		TrustedProxies:     splitServerList(config.GetString("app.server.trusted_proxies", "")),
		ClientIPHeaders:    splitServerList(config.GetString("app.server.client_ip_headers", "X-Forwarded-For,X-Real-IP")),
		AccessLog:          config.GetBool("app.server.access_log", true),
		ExceptionHandler:   config.GetBool("app.server.exception_handler", true),
	}
}

// Addr 返回 http.Server 可直接使用的监听地址。
func (cfg ServerConfig) Addr() string {
	port := normalizeServerPort(cfg.Port)
	host := strings.TrimSpace(cfg.Host)
	if host == "" {
		return ":" + port
	}
	if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	}
	return net.JoinHostPort(host, port)
}

func serverDuration(path string, fallback time.Duration) time.Duration {
	return parseServerDuration(config.GetString(path, ""), fallback)
}

func parseServerDuration(value string, fallback time.Duration) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	if d, err := time.ParseDuration(value); err == nil && d >= 0 {
		return d
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func normalizeServerPort(port string) string {
	port = strings.TrimSpace(port)
	if port == "" {
		return defaultServerPort
	}
	return validPort(strings.TrimPrefix(port, ":"))
}

func validPort(port string) string {
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return defaultServerPort
	}
	return port
}

func splitServerList(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func positiveInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func positiveInt64(value, fallback int64) int64 {
	if value > 0 {
		return value
	}
	return fallback
}
