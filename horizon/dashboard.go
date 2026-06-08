package horizon

import (
	"embed"
	"html"
	"strings"
)

//go:embed dashboard/resources/index.html dashboard/resources/css/app.css dashboard/resources/js/app.js dashboard/vendor/alpine.min.js
var dashboardAssets embed.FS

// DashboardHTML 返回内嵌的 Horizon Dashboard HTML。
//
// 参数说明：cfg 提供实际 horizon.path，用于生成页面路径和 API 前缀运行时变量。
// 设计思路：HTML、CSS、业务 JS 与 Alpine vendor 文件分开放在 dashboard 目录中，保持接近
// Laravel Horizon 的资源组织方式；运行时仍由 Go embed 打包进二进制，不依赖 public/vendor、
// web 构建产物、源码目录或远程 CDN。
func DashboardHTML(cfg Config) string {
	replacer := strings.NewReplacer(
		"{{HORIZON_PATH}}", html.EscapeString(cfg.DashboardPath()),
		"{{API_PREFIX}}", html.EscapeString(cfg.APIPrefix()),
		"{{DASHBOARD_CSS}}", dashboardAsset("dashboard/resources/css/app.css"),
		"{{DASHBOARD_JS}}", dashboardAsset("dashboard/resources/js/app.js"),
		"{{ALPINE_JS}}", dashboardAsset("dashboard/vendor/alpine.min.js"),
	)
	return replacer.Replace(dashboardAsset("dashboard/resources/index.html"))
}

func dashboardAsset(path string) string {
	body, err := dashboardAssets.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(body)
}
