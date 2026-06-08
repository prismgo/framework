package session

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
)

// testConfig 构造 session 包测试使用的最小配置。
//
// 需求背景：后续 Store、Manager、FileDriver 测试都需要隔离的临时目录和稳定 cookie 名称。
// 参数 t 用于创建临时目录并在失败时正确归属到调用测试。
func testConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Files = t.TempDir()
	cfg.Cookie.Name = "test_session"
	cfg.Lifetime = time.Hour
	return cfg
}

// testRequest 构造带 cookie 的 HTTP 请求。
//
// 参数 t 用于标记测试辅助函数；cookies 表示要写入请求头的客户端 cookie。该辅助函数
// 统一测试请求的 Host 和 URL，避免每个测试重复 httptest 样板代码。
func testRequest(t *testing.T, cookies ...*http.Cookie) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	return req
}

// testResponse 构造响应记录器。
//
// 参数 t 用于标记测试辅助函数。返回值用于后续断言 Set-Cookie、状态码和响应头。
func testResponse(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	return httptest.NewRecorder()
}

func useSessionTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func bindSessionManagerForTest(t *testing.T, manager *Manager) *container.Container {
	t.Helper()
	registry := useSessionTestContainer(t)
	if err := registry.Instance(serviceKey, manager); err != nil {
		t.Fatalf("bind session manager: %v", err)
	}
	return registry
}

func bindSessionConfigInRegistry(t *testing.T, registry *container.Container, cfg *configpkg.Config) {
	t.Helper()
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
}

// testNow 返回固定时间。
//
// 设计思路：session 过期、flash 生命周期和文件记录都依赖时间，使用固定时间可以让测试
// 不受当前系统时间影响。
func testNow() time.Time {
	return time.Date(2030, 5, 6, 12, 0, 0, 0, time.UTC)
}

// assertErrorIs 断言错误链包含目标错误。
//
// 参数 t 用于报告测试失败；err 是实际错误；target 是期望通过 errors.Is 匹配到的错误哨兵。
func assertErrorIs(t *testing.T, err error, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}
