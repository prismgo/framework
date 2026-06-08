package requestid

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const Header = "X-Request-ID"

const ContextKey = "request_id"

type Options struct {
	Header         string
	Generator      func(*gin.Context) string
	Validator      func(string) bool
	ResponseHeader bool
}

type Option func(*Options)

func WithHeader(header string) Option {
	return func(opts *Options) {
		if header = strings.TrimSpace(header); header != "" {
			opts.Header = header
		}
	}
}

func WithGenerator(generator func(*gin.Context) string) Option {
	return func(opts *Options) {
		if generator != nil {
			opts.Generator = generator
		}
	}
}

func WithValidator(validator func(string) bool) Option {
	return func(opts *Options) {
		if validator != nil {
			opts.Validator = validator
		}
	}
}

func WithResponseHeader(enabled bool) Option {
	return func(opts *Options) {
		opts.ResponseHeader = enabled
	}
}

func Middleware(opts ...Option) gin.HandlerFunc {
	cfg := DefaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return func(c *gin.Context) {
		id := strings.TrimSpace(c.GetHeader(cfg.Header))
		if !cfg.Validator(id) {
			id = Get(c)
		}
		if !cfg.Validator(id) {
			id = cfg.Generator(c)
		}
		Set(c, id)
		if cfg.ResponseHeader {
			c.Writer.Header().Set(cfg.Header, id)
		}
		c.Next()
	}
}

func Get(c *gin.Context) string {
	if c == nil {
		return ""
	}
	value, ok := c.Get(ContextKey)
	if !ok {
		return ""
	}
	id, ok := value.(string)
	if !ok {
		return ""
	}
	return id
}

func Set(c *gin.Context, id string) {
	if c == nil || id == "" {
		return
	}
	c.Set(ContextKey, id)
}

func DefaultOptions() Options {
	return Options{
		Header:         Header,
		Generator:      func(*gin.Context) string { return uuid.NewString() },
		Validator:      DefaultValidator,
		ResponseHeader: true,
	}
}

func DefaultValidator(id string) bool {
	id = strings.TrimSpace(id)
	return id != "" && len(id) <= 128 && !strings.ContainsAny(id, "\r\n")
}
