package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/http/middleware"
)

// ApplicationServerConfigurator 描述应用侧 HTTP Engine 装配函数。
//
// 需求背景：HTTP routes 和 middleware 已由 foundation.Application 持有，http 不再保存
// 进程级注册表。调用方传入由 Application 内部生成的装配闭包，避免向业务侧暴露第二套路由入口。
type ApplicationServerConfigurator func(engine *gin.Engine, useInternalMiddlewares func(*gin.Engine)) error

type serverBuildConfig struct {
	baseContext context.Context
	server      ServerConfig
}

// ServerBuildOption 配置 NewApplicationServer 的可选行为。
type ServerBuildOption func(*serverBuildConfig)

// WithBaseContext 指定 http.Server 为请求派生 context 时使用的基础 context。
func WithBaseContext(ctx context.Context) ServerBuildOption {
	return func(cfg *serverBuildConfig) {
		cfg.baseContext = ctx
	}
}

// WithServerTimeout 指定 HTTP Server 读写超时时间。
func WithServerTimeout(timeout time.Duration) ServerBuildOption {
	return func(cfg *serverBuildConfig) {
		if timeout > 0 {
			cfg.server.ReadTimeout = timeout
			cfg.server.WriteTimeout = timeout
		}
	}
}

// WithServerConfig 指定 HTTP Server 的完整构建配置。
func WithServerConfig(serverConfig ServerConfig) ServerBuildOption {
	return func(cfg *serverBuildConfig) {
		cfg.server = serverConfig
	}
}

// NewApplicationServer 使用显式应用注册器创建标准 HTTP Server。
//
// 用途：将 Gin Engine 创建、中间件注册、路由注册、监听地址和超时配置收敛到 prismgo/http。
// 设计说明：业务声明来源由 foundation/bootstrap 决定；http 只消费 Application 生成的装配闭包，
// 不再提供包级 fallback 或 bridge 注册入口。
func NewApplicationServer(port string, configure ApplicationServerConfigurator, opts ...ServerBuildOption) (*http.Server, error) {
	if configure == nil {
		return nil, fmt.Errorf("http: HTTP routes registrar is not configured")
	}

	cfg := &serverBuildConfig{server: CurrentServerConfig()}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	if port != "" {
		cfg.server.Port = port
	}

	gin.SetMode(ginModeFromDebug(cfg.server.Debug))
	engine := gin.New()
	if err := applyServerConfigToEngine(engine, cfg.server); err != nil {
		return nil, err
	}
	if err := configure(engine, func(engine *gin.Engine) {
		middleware.UseWithConfig(engine, cfg.server)
	}); err != nil {
		return nil, err
	}

	server := NewServerWithConfig(engine, cfg.server)
	if cfg.baseContext != nil {
		server.BaseContext = func(_ net.Listener) context.Context {
			return cfg.baseContext
		}
	}
	return server, nil
}

func ginModeFromDebug(debug bool) string {
	if debug {
		return gin.DebugMode
	}
	return gin.ReleaseMode
}

func applyServerConfigToEngine(engine *gin.Engine, cfg ServerConfig) error {
	engine.MaxMultipartMemory = cfg.MaxMultipartMemory
	if len(cfg.ClientIPHeaders) > 0 {
		engine.RemoteIPHeaders = cfg.ClientIPHeaders
	}
	if err := engine.SetTrustedProxies(cfg.TrustedProxies); err != nil {
		return fmt.Errorf("http: configure trusted proxies: %w", err)
	}
	return nil
}
