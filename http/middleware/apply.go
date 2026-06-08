package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/exception"
)

// ServerConfig is the small HTTP server configuration surface used by the
// built-in middleware chain.
type ServerConfig interface {
	AccessLogEnabled() bool
	ExceptionHandlerEnabled() bool
}

// Use registers the current prismgo built-in HTTP middleware chain.
func Use(engine *gin.Engine) {
	UseWithConfig(engine, currentConfig{})
}

// UseWithConfig registers the prismgo built-in HTTP middleware chain.
func UseWithConfig(engine *gin.Engine, cfg ServerConfig) {
	if engine == nil {
		return
	}
	if cfg == nil {
		cfg = currentConfig{}
	}
	if cfg.AccessLogEnabled() {
		engine.Use(gin.Logger())
	}
	engine.Use(Deferred())
	engine.Use(Event(event.Resolve()))
	if cfg.ExceptionHandlerEnabled() {
		h := exception.Resolve()
		if h == nil {
			h = exception.New()
		}
		engine.Use(Exception(h))
	}
}

type currentConfig struct{}

func (currentConfig) AccessLogEnabled() bool {
	return config.GetBool("app.server.access_log", true)
}

func (currentConfig) ExceptionHandlerEnabled() bool {
	return config.GetBool("app.server.exception_handler", true)
}
