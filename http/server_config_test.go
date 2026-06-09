package http

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/config"
)

func TestCurrentServerConfigDefaults(t *testing.T) {
	loadHTTPServerConfig(t, "")

	cfg := CurrentServerConfig()
	if cfg.Host != "" || cfg.Port != "8080" || cfg.Addr() != ":8080" {
		t.Fatalf("unexpected default addr config: %+v addr=%s", cfg, cfg.Addr())
	}
	if cfg.Debug {
		t.Fatalf("expected debug to default false: %+v", cfg)
	}
	if cfg.ReadTimeout != 15*time.Second ||
		cfg.ReadHeaderTimeout != 5*time.Second ||
		cfg.WriteTimeout != 30*time.Second ||
		cfg.IdleTimeout != 60*time.Second ||
		cfg.ShutdownTimeout != 15*time.Second {
		t.Fatalf("unexpected default timeout config: %+v", cfg)
	}
	if cfg.MaxHeaderBytes != 1<<20 || cfg.MaxMultipartMemory != 32<<20 {
		t.Fatalf("unexpected default size config: %+v", cfg)
	}
	if !reflect.DeepEqual(cfg.ClientIPHeaders, []string{"X-Forwarded-For", "X-Real-IP"}) {
		t.Fatalf("unexpected client ip headers: %#v", cfg.ClientIPHeaders)
	}
	if !cfg.AccessLog || !cfg.ExceptionHandler {
		t.Fatalf("expected middleware switches to default true: %+v", cfg)
	}
}

func TestCurrentServerConfigOverridesAndLegacyTimeout(t *testing.T) {
	loadHTTPServerConfig(t, ""+
		"SERVER_HOST=127.0.0.1\n"+
		"SERVER_PORT=9090\n"+
		"SERVER_TIMEOUT=20\n"+
		"SERVER_READ_TIMEOUT=2m\n"+
		"SERVER_READ_HEADER_TIMEOUT=3s\n"+
		"SERVER_IDLE_TIMEOUT=45s\n"+
		"SERVER_SHUTDOWN_TIMEOUT=7\n"+
		"SERVER_MAX_HEADER_BYTES=2048\n"+
		"SERVER_MAX_MULTIPART_MEMORY=4096\n"+
		"SERVER_TRUSTED_PROXIES=127.0.0.1,10.0.0.0/8\n"+
		"SERVER_CLIENT_IP_HEADERS=CF-Connecting-IP, X-Real-IP\n"+
		"SERVER_ACCESS_LOG=false\n"+
		"SERVER_EXCEPTION_HANDLER=false\n"+
		"APP_DEBUG=true\n")

	cfg := CurrentServerConfig()
	if cfg.Addr() != "127.0.0.1:9090" {
		t.Fatalf("addr = %s, want 127.0.0.1:9090", cfg.Addr())
	}
	if cfg.ReadTimeout != 2*time.Minute ||
		cfg.ReadHeaderTimeout != 3*time.Second ||
		cfg.WriteTimeout != 30*time.Second ||
		cfg.IdleTimeout != 45*time.Second ||
		cfg.ShutdownTimeout != 7*time.Second {
		t.Fatalf("unexpected timeout config: %+v", cfg)
	}
	if cfg.MaxHeaderBytes != 2048 || cfg.MaxMultipartMemory != 4096 {
		t.Fatalf("unexpected size config: %+v", cfg)
	}
	if !reflect.DeepEqual(cfg.TrustedProxies, []string{"127.0.0.1", "10.0.0.0/8"}) {
		t.Fatalf("trusted proxies = %#v", cfg.TrustedProxies)
	}
	if !reflect.DeepEqual(cfg.ClientIPHeaders, []string{"CF-Connecting-IP", "X-Real-IP"}) {
		t.Fatalf("client ip headers = %#v", cfg.ClientIPHeaders)
	}
	if cfg.AccessLog || cfg.ExceptionHandler {
		t.Fatalf("expected middleware switches to be false: %+v", cfg)
	}
	if !cfg.Debug {
		t.Fatalf("expected debug to be true: %+v", cfg)
	}
}

func TestNewApplicationServerSetsGinModeFromDebug(t *testing.T) {
	previousMode := gin.Mode()
	t.Cleanup(func() { gin.SetMode(previousMode) })

	for _, tc := range []struct {
		name  string
		debug bool
		want  string
	}{
		{name: "debug disabled", debug: false, want: gin.ReleaseMode},
		{name: "debug enabled", debug: true, want: gin.DebugMode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bindHTTPApplicationServerServices(t, nil)
			_, err := NewApplicationServer("", func(engine *gin.Engine, useInternalMiddlewares func(*gin.Engine)) error {
				return nil
			}, WithServerConfig(ServerConfig{Port: "8080", Debug: tc.debug}))
			if err != nil {
				t.Fatalf("NewApplicationServer returned error: %v", err)
			}
			if got := gin.Mode(); got != tc.want {
				t.Fatalf("gin mode = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestServerConfigAppliesToEngineAndServer(t *testing.T) {
	cfg := ServerConfig{
		Host:               "127.0.0.1",
		Port:               ":9000",
		ReadTimeout:        2 * time.Second,
		ReadHeaderTimeout:  3 * time.Second,
		WriteTimeout:       4 * time.Second,
		IdleTimeout:        5 * time.Second,
		MaxHeaderBytes:     8192,
		MaxMultipartMemory: 16384,
		TrustedProxies:     []string{"127.0.0.1"},
		ClientIPHeaders:    []string{"CF-Connecting-IP"},
	}

	engine := gin.New()
	if err := applyServerConfigToEngine(engine, cfg); err != nil {
		t.Fatalf("applyServerConfigToEngine returned error: %v", err)
	}
	if engine.MaxMultipartMemory != 16384 {
		t.Fatalf("MaxMultipartMemory = %d, want 16384", engine.MaxMultipartMemory)
	}
	if !reflect.DeepEqual(engine.RemoteIPHeaders, []string{"CF-Connecting-IP"}) {
		t.Fatalf("RemoteIPHeaders = %#v", engine.RemoteIPHeaders)
	}

	server := NewServerWithConfig(engine, cfg)
	if server.Addr != "127.0.0.1:9000" ||
		server.ReadTimeout != 2*time.Second ||
		server.ReadHeaderTimeout != 3*time.Second ||
		server.WriteTimeout != 4*time.Second ||
		server.IdleTimeout != 5*time.Second ||
		server.MaxHeaderBytes != 8192 {
		t.Fatalf("unexpected server config: %+v", server)
	}
}

func TestServerConfigHelpersCoverFallbackBranches(t *testing.T) {
	cfg := ServerConfig{Host: "127.0.0.1:7000", Port: ""}
	if got := cfg.Addr(); got != "127.0.0.1:8080" {
		t.Fatalf("addr = %q, want 127.0.0.1:8080", got)
	}
	if got := normalizeServerPort(" :9090 "); got != "9090" {
		t.Fatalf("normalized port = %q, want 9090", got)
	}
	if got := positiveInt(0, 42); got != 42 {
		t.Fatalf("positiveInt fallback = %d, want 42", got)
	}
	if got := positiveInt64(-1, 64); got != 64 {
		t.Fatalf("positiveInt64 fallback = %d, want 64", got)
	}
}

func TestApplicationServerConfigErrorBranches(t *testing.T) {
	if err := applyServerConfigToEngine(nil, ServerConfig{}); err != nil {
		t.Fatalf("nil engine config error = %v, want nil", err)
	}

	bindHTTPApplicationServerServices(t, nil)
	_, err := NewApplicationServer("", func(engine *gin.Engine, useInternalMiddlewares func(*gin.Engine)) error {
		return nil
	}, WithServerConfig(ServerConfig{Port: "8080", TrustedProxies: []string{"bad cidr"}}))
	if err == nil || !strings.Contains(err.Error(), "configure trusted proxies") {
		t.Fatalf("NewApplicationServer trusted proxy error = %v, want configure trusted proxies", err)
	}

	bindHTTPApplicationServerServices(t, nil)
	configureErr := errors.New("configure failed")
	_, err = NewApplicationServer("", func(engine *gin.Engine, useInternalMiddlewares func(*gin.Engine)) error {
		return configureErr
	}, WithServerConfig(ServerConfig{Port: "8080"}))
	if !errors.Is(err, configureErr) {
		t.Fatalf("NewApplicationServer configure error = %v, want %v", err, configureErr)
	}
}

func TestNewApplicationServerPortArgumentOverridesConfig(t *testing.T) {
	bindHTTPApplicationServerServices(t, nil)
	server, err := NewApplicationServer("9091", func(engine *gin.Engine, useInternalMiddlewares func(*gin.Engine)) error {
		return nil
	}, WithServerConfig(ServerConfig{Port: "8080"}))
	if err != nil {
		t.Fatalf("NewApplicationServer returned error: %v", err)
	}
	if server.Addr != ":9091" {
		t.Fatalf("server addr = %q, want :9091", server.Addr)
	}
}

func TestParseServerDurationFallsBackOnInvalidValues(t *testing.T) {
	fallback := 9 * time.Second
	if got := parseServerDuration("1500ms", fallback); got != 1500*time.Millisecond {
		t.Fatalf("duration = %s, want 1500ms", got)
	}
	if got := parseServerDuration("6", fallback); got != 6*time.Second {
		t.Fatalf("numeric duration = %s, want 6s", got)
	}
	if got := parseServerDuration("bad", fallback); got != fallback {
		t.Fatalf("invalid duration = %s, want %s", got, fallback)
	}
}

func loadHTTPServerConfig(t *testing.T, content string) {
	t.Helper()
	registerHTTPServerConfigForTest()

	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write env file failed: %v", err)
	}
	cfg := config.New()
	if err := cfg.ReloadFromFile(path); err != nil {
		t.Fatalf("init config failed: %v", err)
	}
	registry := useHTTPTestContainer(t)
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
}

func registerHTTPServerConfigForTest() {
	config.Add("app", func() map[string]any {
		return map[string]any{
			"debug": config.Env("APP_DEBUG", false),
			"server": map[string]any{
				"host":                 config.Env("SERVER_HOST", ""),
				"port":                 config.Env("SERVER_PORT", 8080),
				"timeout":              config.Env("SERVER_TIMEOUT", 15),
				"read_timeout":         config.Env("SERVER_READ_TIMEOUT", "15s"),
				"read_header_timeout":  config.Env("SERVER_READ_HEADER_TIMEOUT", "5s"),
				"write_timeout":        config.Env("SERVER_WRITE_TIMEOUT", "30s"),
				"idle_timeout":         config.Env("SERVER_IDLE_TIMEOUT", "60s"),
				"shutdown_timeout":     config.Env("SERVER_SHUTDOWN_TIMEOUT", "15s"),
				"max_header_bytes":     config.Env("SERVER_MAX_HEADER_BYTES", 1048576),
				"max_multipart_memory": config.Env("SERVER_MAX_MULTIPART_MEMORY", 33554432),
				"trusted_proxies":      config.Env("SERVER_TRUSTED_PROXIES", ""),
				"client_ip_headers":    config.Env("SERVER_CLIENT_IP_HEADERS", "X-Forwarded-For,X-Real-IP"),
				"access_log":           config.Env("SERVER_ACCESS_LOG", true),
				"exception_handler":    config.Env("SERVER_EXCEPTION_HANDLER", true),
			},
		}
	})
}
