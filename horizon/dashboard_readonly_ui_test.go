package horizon

import (
	"strings"
	"testing"
)

func TestDashboardHTMLIsEmbeddedReadOnlyAndUsesConfiguredPaths(t *testing.T) {
	html := DashboardHTML(Config{Path: "ops/horizon"})
	for _, want := range []string{
		"x-data",
		"data-api-prefix=\"/ops/horizon/api\"",
		"alpine:init",
		"window.Alpine",
		"Horizon",
		"Loading queue lengths...",
		"Authentication required",
		"Access denied",
		"Store unavailable",
		"Dashboard",
		"Metrics",
		"Batches",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("dashboard html missing %q", want)
		}
	}
	readOnlySource := strings.ToLower(
		dashboardAsset("dashboard/resources/index.html") +
			dashboardAsset("dashboard/resources/css/app.css") +
			dashboardAsset("dashboard/resources/js/app.js"),
	)
	for _, want := range []string{
		"/status",
		"/queues",
		"/metrics/current",
		"/metrics/history/queue/",
		"/batches",
		"/batches/",
		"Select a batch to inspect its read-only summary.",
		"Search batches by id or name",
		"@media (max-width: 900px)",
	} {
		if !strings.Contains(readOnlySource, strings.ToLower(want)) {
			t.Fatalf("dashboard source missing %q", want)
		}
	}
	for _, forbidden := range []string{
		"/horizon/assets",
		"/vendor/horizon",
		"public/vendor",
		"retry",
		"forget",
		"pause",
		"terminate",
		"monitoring",
		"monitoring delete",
		"payload",
		"raw envelope",
		"silenced jobs",
		"pending jobs",
		"completed jobs",
		"failed jobs",
		"/jobs/",
		"/jobs/silenced",
		"unpkg",
		"cdn.jsdelivr",
	} {
		if strings.Contains(readOnlySource, forbidden) {
			t.Fatalf("dashboard html must not contain %q", forbidden)
		}
	}
	for _, forbidden := range []string{"<script src=", "unpkg", "cdn.jsdelivr", "cdnjs"} {
		if strings.Contains(strings.ToLower(html), forbidden) {
			t.Fatalf("dashboard html must not load remote asset %q", forbidden)
		}
	}
}

func TestDashboardInitializesStatusOnlyAndRefreshesActiveTab(t *testing.T) {
	// 测试目的：Dashboard 首屏只能加载轻量 /status；tab 明细必须等用户点击 tab 或刷新当前 tab 时再请求。
	source := dashboardAsset("dashboard/resources/js/app.js")
	initBody := javascriptMethodBody(t, source, "async init()")
	if !strings.Contains(initBody, "'/status'") {
		t.Fatalf("init should request /status body=%s", initBody)
	}
	for _, forbidden := range []string{
		"Promise.all",
		"'/queues'",
		"'/metrics/current'",
		"'/jobs/",
		"'/batches",
		"'/monitoring'",
	} {
		if strings.Contains(initBody, forbidden) {
			t.Fatalf("init must not eagerly load %s body=%s", forbidden, initBody)
		}
	}
	html := dashboardAsset("dashboard/resources/index.html")
	if strings.Contains(html, `x-init="init()"`) {
		t.Fatalf("dashboard must rely on Alpine component auto-init only; explicit x-init would call init twice")
	}
	for _, want := range []string{
		`x-data="horizonDashboard"`,
		"x-on:click=\"activateTab(tab.key)\"",
		"x-on:click=\"refreshActiveTab()\"",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("dashboard html missing lazy tab control %q", want)
		}
	}
}

func TestDashboardOverviewRefreshRequestsStatusOnly(t *testing.T) {
	// 测试目的：Dashboard 首页刷新只同步 /status 摘要、queue_lengths 快照和 status 内聚合指标；
	// 不得连带刷新其他昂贵 tab 明细接口。
	source := dashboardAsset("dashboard/resources/js/app.js")
	refreshBody := javascriptMethodBody(t, source, "async refreshStatus()")
	if !strings.Contains(refreshBody, "'/status'") || !strings.Contains(refreshBody, "applyStatus") {
		t.Fatalf("refreshStatus should request /status and apply overview cards body=%s", refreshBody)
	}
	for _, forbidden := range []string{
		"loadTab",
		"refreshActiveTab",
		"'/queues'",
		"'/metrics/current'",
		"'/jobs/",
		"'/batches",
		"'/monitoring'",
	} {
		if strings.Contains(refreshBody, forbidden) {
			t.Fatalf("overview refresh must not trigger %s body=%s", forbidden, refreshBody)
		}
	}
	html := dashboardAsset("dashboard/resources/index.html")
	if !strings.Contains(html, `x-on:click="refreshStatus()"`) {
		t.Fatalf("dashboard refresh button should target status-only refresh")
	}
	if !strings.Contains(html, `x-show="activeTab === 'dashboard'" x-on:click="refreshStatus()"`) {
		t.Fatalf("dashboard refresh overview button should only be visible on dashboard tab")
	}
	if !strings.Contains(source, "data.queue_lengths") || !strings.Contains(source, "formatQueueLengthSize(item)") {
		t.Fatalf("dashboard should render queue lengths from /status queue_lengths")
	}
	for _, want := range []string{"statusData.jobs_per_minute", "statusData.jobs_past_hour", "statusData.total_processed"} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard should render dashboard metrics from /status field %q", want)
		}
	}
}

func TestDashboardOverviewCardsExposeStableNavigationOrDisabledReasons(t *testing.T) {
	// 测试目的：首页摘要卡片必须为后续进程/队列 tab 提供稳定入口；目标 tab 尚未落地时要给出可验证的 disabled reason。
	source := dashboardAsset("dashboard/resources/js/app.js")
	for _, want := range []string{
		"navigateOverviewCard(card)",
		"isOverviewCardNavigable(card)",
		"queue detail tab not implemented",
		"target_tab",
		"supervisors",
		"workers",
		"stale",
		"queues",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard source missing overview navigation contract %q", want)
		}
	}
	navBody := javascriptMethodBody(t, source, "async navigateOverviewCard(card)")
	if !strings.Contains(navBody, "activateTab(card.target_tab)") {
		t.Fatalf("overview card navigation should reuse tab activation body=%s", navBody)
	}
	for _, forbidden := range []string{"'/supervisors'", "'/workers'", "'/stale'", "'/queues'", "fetchJSON"} {
		if strings.Contains(navBody, forbidden) {
			t.Fatalf("overview card navigation must not direct-fetch %s body=%s", forbidden, navBody)
		}
	}
	html := dashboardAsset("dashboard/resources/index.html")
	for _, want := range []string{
		`x-on:click="navigateOverviewCard(card)"`,
		`:aria-disabled="String(!isOverviewCardNavigable(card))"`,
		`overviewCardDisabledReason(card)`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("dashboard html missing card accessibility contract %q", want)
		}
	}
}

func TestDashboardFieldTooltipsAreCentralizedAccessibleAndConfigAware(t *testing.T) {
	// 测试目的：tooltip 文案集中维护，首页字段实际落地，未来进程/队列字段预留稳定 key。
	source := dashboardAsset("dashboard/resources/js/app.js")
	for _, want := range []string{
		"tooltipRegistry",
		"tooltipText(key)",
		"overview.status",
		"overview.supervisors",
		"overview.workers",
		"overview.stale_supervisors",
		"overview.stale_workers",
		"overview.capabilities",
		"overview.jobs_per_minute",
		"overview.jobs_past_hour",
		"overview.total_processed",
		"horizon.store.heartbeat_ttl_seconds",
		"default value 60 seconds",
		"current value unavailable",
		"/status.capabilities",
		"disabled/unsupported reason",
		"horizon.observability.queue_lengths",
		"horizon.observability.event_metrics",
		"sample_window_ms",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard source missing tooltip contract %q", want)
		}
	}
	for _, key := range []string{
		"process.name",
		"process.pid",
		"process.cpu_percent",
		"process.memory_rss_bytes",
		"process.memory_percent",
		"process.goroutines",
		"process.last_heartbeat_at",
		"process.queues",
		"process.status",
		"process.host",
		"process.supervisor",
		"queue.size",
		"queue.avg_runtime",
		"queue.max_runtime",
		"queue.avg_memory",
		"queue.max_memory",
		"queue.wait",
		"queue.throughput",
		"queue.processed",
		"queue.failed",
		"queue.released",
	} {
		if !strings.Contains(source, key) {
			t.Fatalf("dashboard tooltip registry missing stable key %q", key)
		}
	}
	html := dashboardAsset("dashboard/resources/index.html")
	for _, want := range []string{
		`class="tooltip-trigger"`,
		`:aria-describedby="tooltipID(card.tooltip_key)"`,
		`role="tooltip"`,
		`x-text="tooltipText(card.tooltip_key)"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("dashboard html missing accessible tooltip contract %q", want)
		}
	}
}

func TestDashboardTabLoadersKeepAPIRequestsIsolated(t *testing.T) {
	// 测试目的：每个 tab 的加载函数只能请求自己的只读接口，避免用户点击一个 tab 时连带刷新其他 tab。
	source := dashboardAsset("dashboard/resources/js/app.js")
	batchesBody := javascriptMethodBody(t, source, "async loadBatches()")
	for _, forbidden := range []string{"'/jobs/", "'/metrics/current'", "'/monitoring'"} {
		if strings.Contains(batchesBody, forbidden) {
			t.Fatalf("batches loader must not request %s body=%s", forbidden, batchesBody)
		}
	}
	metricsBody := javascriptMethodBody(t, source, "async loadMetricsTab()")
	for _, forbidden := range []string{"'/jobs/", "'/batches", "'/monitoring'"} {
		if strings.Contains(metricsBody, forbidden) {
			t.Fatalf("metrics loader must not request %s body=%s", forbidden, metricsBody)
		}
	}
	if strings.Contains(source, "loadJobsTab") || strings.Contains(source, "'/jobs/") {
		t.Fatalf("dashboard must not keep legacy jobs tab loader")
	}
	processBody := javascriptMethodBody(t, source, "async loadProcessTab(")
	for _, want := range []string{"this.pagedPath(path)", "processLists[target] = data.items || []"} {
		if !strings.Contains(processBody, want) {
			t.Fatalf("process loader missing %s body=%s", want, processBody)
		}
	}
	for _, forbidden := range []string{"'/jobs/", "'/batches", "'/metrics/current'", "'/monitoring'"} {
		if strings.Contains(processBody, forbidden) {
			t.Fatalf("process loader must not request %s body=%s", forbidden, processBody)
		}
	}
}

func TestDashboardTabListsUseCompactDetailLayout(t *testing.T) {
	source := dashboardAsset("dashboard/resources/index.html") + dashboardAsset("dashboard/resources/css/app.css")
	for _, want := range []string{
		"class=\"table-wrap\"",
		"class=\"data-table\"",
		"class=\"cell-strong\"",
		"class=\"item-status\"",
		".table-wrap",
		".data-table",
		".item-status",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard compact layout missing %q", want)
		}
	}
}

func TestDashboardAssetsAreStoredAsLocalReadableFiles(t *testing.T) {
	for _, path := range []string{
		"dashboard/resources/index.html",
		"dashboard/resources/css/app.css",
		"dashboard/resources/js/app.js",
		"dashboard/vendor/alpine.min.js",
	} {
		if body := dashboardAsset(path); strings.TrimSpace(body) == "" {
			t.Fatalf("dashboard asset %s should be embedded and non-empty", path)
		}
	}
}

func TestDashboardAssetReturnsEmptyForMissingEmbeddedFile(t *testing.T) {
	if got := dashboardAsset("dashboard/resources/missing.txt"); got != "" {
		t.Fatalf("missing asset should return empty string, got %q", got)
	}
}

func TestCommandFactoriesRegisterInstallAndListenButNotPublish(t *testing.T) {
	names := map[string]bool{}
	for _, factory := range CommandFactories() {
		names[factory().Definition().Name] = true
	}
	if !names["horizon:install"] || !names["horizon:listen"] {
		t.Fatalf("command names = %#v, want install and listen", names)
	}
	if names["horizon:publish"] {
		t.Fatal("horizon:publish must not be registered")
	}
}

func javascriptMethodBody(t *testing.T, source, marker string) string {
	t.Helper()
	start := strings.Index(source, marker)
	if start < 0 {
		t.Fatalf("javascript method %q not found", marker)
	}
	open := strings.Index(source[start:], "{")
	if open < 0 {
		t.Fatalf("javascript method %q missing body", marker)
	}
	pos := start + open
	depth := 0
	for i := pos; i < len(source); i++ {
		switch source[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[pos : i+1]
			}
		}
	}
	t.Fatalf("javascript method %q body not closed", marker)
	return ""
}
