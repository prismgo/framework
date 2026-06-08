package horizon

import (
	"context"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	goprocess "github.com/prismgo/framework/process"
	"github.com/prismgo/framework/route"
)

const (
	queueLengthsDashboardLimit = 100
	// HorizonView 是 Horizon Dashboard 与只读 API 共用的权限标识。
	//
	// 需求背景：本轮 HTTP 能力只开放观察面，不开放 retry、pause、terminate 等写入口；
	// 因此应用侧只需要围绕该常量配置一个只读权限，再通过 Auth 中间件注入到路由组。
	HorizonView = "horizon.view"
)

// HTTPOptions 配置 Horizon Dashboard 页面与只读 API 的 HTTP 挂载。
//
// 使用方式：业务应用在注册路由时传入 Manager 和 Auth。Auth 应包含项目自己的登录校验、
// 权限注入以及 horizon.view 只读权限校验；Horizon 包本身不直接依赖业务 middleware，
// 避免通用包反向耦合 app/permission 或 app/middleware。
type HTTPOptions struct {
	// Manager 提供 Horizon 静态配置和 Store 解析。
	Manager *Manager
	// Auth 由业务应用注入，通常包含认证、权限加载和 HorizonView 校验。
	Auth []gin.HandlerFunc
}

// RegisterHTTPRoutes 通过 prismgo/route 声明 Horizon 页面与只读 API 路由。
//
// 参数说明：options.Manager 为空时使用默认 Manager；options.Auth 会挂载到页面和
// /{horizon.path}/api 下所有接口。设计上刻意只注册 GET 路由，不注册 pause、continue、
// retry、forget、clear、terminate 等写操作，避免 Dashboard 过早扩大权限面。
//
// 设计思路：Laravel Horizon 当前流程是在 provider boot 阶段按 horizon.path 前缀加载
// dashboard/API routes；Prismgo 这里保持相同挂载语义，但统一走 prismgo/route 包，由宿主
// HTTP server 在启动时 Mount 到 Gin。
func RegisterHTTPRoutes(options HTTPOptions) {
	manager := options.Manager
	if manager == nil {
		manager = Resolve()
	}
	if manager == nil {
		return
	}
	cfg := manager.Config()
	pagePath := cfg.DashboardPath()
	apiPrefix := cfg.APIPrefix()
	handlers := append([]gin.HandlerFunc(nil), options.Auth...)
	api := &httpAPI{manager: manager}

	route.Middleware(handlers...).Group(func() {
		route.Get(pagePath, api.dashboard)
		route.Prefix(apiPrefix).Group(func() {
			route.Get("/status", api.status)
			route.Get("/masters", api.masters)
			route.Get("/supervisors", api.supervisors)
			route.Get("/workers", api.workers)
			route.Get("/stale", api.stale)
			route.Get("/queues", api.queues)
			route.Get("/metrics/current", api.metricsCurrent)
			route.Get("/metrics/sources", api.metricSources)
			route.Get("/metrics/history/{kind}/{key}", api.metricsHistory)
			route.Get("/high-value-detail", api.highValueDetails)
			route.Get("/high-value-detail/{id}", api.highValueDetail)
			route.Get("/batches", api.batches)
			route.Get("/batches/{id}", api.batchDetail)
		})
	})
}

type httpAPI struct {
	manager *Manager
}

// store 延迟解析 Horizon Store，并把解析失败映射为稳定的只读 API 错误响应。
//
// 参数说明：c 是当前 Gin 请求上下文；返回值中的 bool 表示 handler 是否可以继续读取 Store。
// 安全边界：错误响应只返回错误摘要，不返回 Redis/RabbitMQ 连接参数或 broker 内部状态。
func (api *httpAPI) store(c *gin.Context) (Store, bool) {
	store, err := api.manager.ResolveStore(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "store_unavailable", "message": "Horizon store is unavailable."})
		return nil, false
	}
	return store, true
}

func (api *httpAPI) dashboard(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(DashboardHTML(api.manager.Config())))
}

// status 返回 Dashboard 首屏需要的轻量运行状态摘要。
//
// 逻辑说明：首屏只读取 Store 已经聚合好的 StatusSnapshot，master、supervisor、worker
// 明细改由独立列表接口按需读取，避免一次首屏请求重复触发大规模明细扫描。
func (api *httpAPI) status(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	now := time.Now().UTC()
	status, err := store.StatusSnapshot(c.Request.Context(), now)
	if err != nil {
		writeJSON(c, nil, err)
		return
	}
	lengths := QueueLengthSnapshot{}
	// /status 仅需队列计数，不应为计数而加载全量 EventMetricWindows。
	if snapshot, err := queueLengthSnapshotForRead(c.Request.Context(), api.manager.Config(), store, api.manager, now); err == nil {
		status.QueueCount = countQueueAggregateKeys(api.manager.Config(), snapshot)
		if len(snapshot.Queues) > queueLengthsDashboardLimit {
			snapshot.Queues = append([]QueueLengthBucket{}, snapshot.Queues[:queueLengthsDashboardLimit]...)
		}
		lengths = snapshot
	}
	applyStatusMetricSummary(c.Request.Context(), &status, store, api.manager.Config(), now)
	c.JSON(http.StatusOK, gin.H{
		"status":        status,
		"queue_lengths": lengths,
		"capabilities":  api.capabilities(),
	})
}

func applyStatusMetricSummary(ctx context.Context, status *StatusSnapshot, store Store, cfg Config, now time.Time) {
	if status == nil || !cfg.Observability.Enabled(ObservabilityEventMetrics) {
		return
	}
	query := normalizeMetricsSummaryQuery(EventMetricWindowQuery{}, now)
	windows, err := store.EventMetricRollupWindows(ctx, query)
	if err != nil {
		return
	}
	totalProcessed := aggregateMetricsTotals(windows).Processed
	jobsPastHour := processedInLastHour(windows, now)
	jobsPerMinute := math.Floor((float64(jobsPastHour)/60)*100) / 100
	status.TotalProcessed = &totalProcessed
	status.JobsPastHour = &jobsPastHour
	status.JobsPerMinute = &jobsPerMinute
}

func processedInLastHour(windows []EventMetricWindow, now time.Time) int64 {
	from := now.Add(-time.Hour)
	var processed int64
	for _, window := range windows {
		if window.WindowEnd.After(from) && window.WindowStart.Before(now) {
			processed += window.Processed
		}
	}
	return processed
}

func (api *httpAPI) masters(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	items, err := store.Masters(c.Request.Context(), time.Now().UTC())
	writeProcessPage(c, api.manager.ProcessObserver(), items, pageFromContext(c), masterProcessReadModel, err)
}

func (api *httpAPI) supervisors(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	items, err := store.Supervisors(c.Request.Context(), time.Now().UTC())
	writeProcessPage(c, api.manager.ProcessObserver(), items, pageFromContext(c), supervisorProcessReadModel, err)
}

func (api *httpAPI) workers(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	items, err := store.Workers(c.Request.Context(), time.Now().UTC())
	writeProcessPage(c, api.manager.ProcessObserver(), items, pageFromContext(c), workerProcessReadModel, err)
}

func (api *httpAPI) stale(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	now := time.Now().UTC()
	supervisors, err := store.Supervisors(c.Request.Context(), now)
	if err != nil {
		writeJSON(c, nil, err)
		return
	}
	workers, err := store.Workers(c.Request.Context(), now)
	if err != nil {
		writeJSON(c, nil, err)
		return
	}
	page := pageFromContext(c)
	c.JSON(http.StatusOK, gin.H{
		"supervisors": processPageEnvelope(c, api.manager.ProcessObserver(), filterStaleSupervisors(supervisors), page, supervisorProcessReadModel),
		"workers":     processPageEnvelope(c, api.manager.ProcessObserver(), filterStaleWorkers(workers), page, workerProcessReadModel),
	})
}

// writeProcessPage 输出统一进程 read model 分页响应。
//
// 逻辑说明：Store 负责返回基础 heartbeat 状态；这里先分页，再把当前页 PID 交给 prismgo/process
// observer 做有界采样，确保列表 API 不会因为全量进程数过多而触发高成本扫描。
func writeProcessPage[T any](c *gin.Context, observer goprocess.Observer, items []T, page PageRequest, toModel func(T) ProcessReadModel, err error) {
	if err != nil {
		writePageJSON(c, PageEnvelope[ProcessReadModel]{}, err)
		return
	}
	c.JSON(http.StatusOK, processPageEnvelope(c, observer, items, page, toModel))
}

func processPageEnvelope[T any](c *gin.Context, observer goprocess.Observer, items []T, page PageRequest, toModel func(T) ProcessReadModel) PageEnvelope[ProcessReadModel] {
	paged := pageSlice(items, page)
	return PageEnvelope[ProcessReadModel]{
		Items:      buildProcessReadModels(c.Request.Context(), observer, paged, toModel),
		Total:      len(items),
		Page:       page.Page,
		PageSize:   page.PageSize,
		Capability: "supported",
	}
}

func (api *httpAPI) queues(c *gin.Context) {
	page := pageFromContext(c)
	store, ok := api.store(c)
	if !ok {
		return
	}
	items, err := buildQueueReadModels(c.Request.Context(), api.manager.Config(), store, api.manager)
	if err != nil {
		writePageJSON(c, PageEnvelope[QueueReadModel]{}, err)
		return
	}
	c.JSON(http.StatusOK, makePageEnvelope(items, page))
}

func (api *httpAPI) metricsCurrent(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	query, ok := metricsWindowQueryFromContext(c, true)
	if !ok {
		return
	}
	model, err := buildMetricsCurrentReadModel(c.Request.Context(), api.manager, store, query)
	writeJSON(c, model, err)
}

func (api *httpAPI) metricSources(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	query, ok := metricsWindowQueryFromContext(c, false)
	if !ok {
		return
	}
	model, err := buildMetricSourcesReadModel(c.Request.Context(), store, query)
	writePageJSON(c, model, err)
}

func (api *httpAPI) metricsHistory(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	query, ok := metricsWindowQueryFromContext(c, false)
	if !ok {
		return
	}
	kind := strings.TrimSpace(c.Param("kind"))
	key := strings.TrimSpace(c.Param("key"))
	items, estimate, err := buildMetricsHistoryReadModel(c.Request.Context(), store, kind, key, query)
	if err != nil {
		writeJSON(c, nil, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"kind": kind, "key": key, "items": items.Items, "total": items.Total, "page": items.Page, "page_size": items.PageSize, "estimate": estimate})
}

func (api *httpAPI) highValueDetails(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	query, ok := highValueDetailQueryFromContext(c)
	if !ok {
		return
	}
	page, err := store.HighValueDetails(c.Request.Context(), query)
	if err != nil {
		writePageJSON(c, PageEnvelope[HighValueDetailReadModel]{}, err)
		return
	}
	c.JSON(http.StatusOK, PageEnvelope[HighValueDetailReadModel]{
		Items:      highValueDetailReadModels(page.Items),
		Total:      page.Total,
		Page:       page.Page,
		PageSize:   page.PageSize,
		Capability: "supported",
	})
}

func (api *httpAPI) highValueDetail(c *gin.Context) {
	store, ok := api.store(c)
	if !ok {
		return
	}
	id := strings.TrimSpace(c.Param("id"))
	detail, found, err := store.HighValueDetail(c.Request.Context(), id)
	if err != nil {
		writeJSON(c, nil, err)
		return
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "high_value_detail not found", "id": id})
		return
	}
	c.JSON(http.StatusOK, highValueDetailReadModel(detail))
}

func (api *httpAPI) batches(c *gin.Context) {
	page := pageFromContext(c)
	if !api.manager.Config().Observability.Enabled(ObservabilityBatchSummaries) {
		c.JSON(http.StatusOK, disabledPage[BatchSummary](page, "batch_summaries disabled"))
		return
	}
	store, ok := api.store(c)
	if !ok {
		return
	}
	query := strings.TrimSpace(firstNonEmpty(c.Query("query"), c.Query("search")))
	items, err := store.BatchesPage(c.Request.Context(), query, page)
	if items.Capability == "" {
		items.Capability = "supported"
	}
	writePageJSON(c, items, err)
}

func (api *httpAPI) batchDetail(c *gin.Context) {
	if !api.manager.Config().Observability.Enabled(ObservabilityBatchSummaries) {
		c.JSON(http.StatusOK, gin.H{
			"capability": "disabled",
			"id":         strings.TrimSpace(c.Param("id")),
			"reason":     "batch_summaries disabled",
		})
		return
	}
	store, ok := api.store(c)
	if !ok {
		return
	}
	id := strings.TrimSpace(c.Param("id"))
	summary, found, err := store.Batch(c.Request.Context(), id)
	if err != nil {
		writeJSON(c, nil, err)
		return
	}
	if found {
		c.JSON(http.StatusOK, gin.H{"capability": "summary", "batch": batchDetailReadModel(summary)})
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "batch not found", "id": id})
}

func (api *httpAPI) capabilities() map[string]string {
	cfg := api.manager.Config()
	return map[string]string{
		"batches":           capabilityState(cfg.Observability.Enabled(ObservabilityBatchSummaries)),
		"high_value_detail": capabilityState(cfg.Observability.Enabled(ObservabilityHighValueDetail)),
		"queue_lengths":     capabilityState(cfg.Observability.Enabled(ObservabilityQueueLengths)),
		"event_metrics":     capabilityState(cfg.Observability.Enabled(ObservabilityEventMetrics)),
		"waits":             capabilityState(cfg.Observability.Enabled(ObservabilityWaits)),
		"http_writes":       "unsupported",
	}
}

func capabilityState(enabled bool) string {
	if enabled {
		return "supported"
	}
	return "disabled"
}

func pageFromContext(c *gin.Context) PageRequest {
	return parsePageRequest(c.Query("page"), c.Query("page_size"))
}

// metricsWindowQueryFromContext 解析 metrics API 的事件窗口查询条件。
//
// 使用方式：`/metrics/current` 和 `/metrics/history/{kind}/{key}` 在读取 Store 前调用该函数；
// 返回 ok=false 时函数已经写出稳定 400 响应，handler 必须停止后续 Store 读取。
// 设计原因：issue 44 要求所有过滤参数在进入 Store 前完成校验，避免 Store 层同时承担 HTTP
// 参数错误语义，也避免 read model 拉取大页后再过滤。
func metricsWindowQueryFromContext(c *gin.Context, defaultLast24h bool) (EventMetricWindowQuery, bool) {
	query := EventMetricWindowQuery{Page: pageFromContext(c)}
	params := c.Request.URL.Query()
	query.OmitSourceDetails = !parseTruthyQueryParam(params, "source_details") || parseTruthyQueryParam(params, "summary_only")
	var ok bool
	if query.From, ok = parseMetricsRFC3339Param(c, params, "from"); !ok {
		return EventMetricWindowQuery{}, false
	}
	if query.To, ok = parseMetricsRFC3339Param(c, params, "to"); !ok {
		return EventMetricWindowQuery{}, false
	}
	if !query.From.IsZero() && !query.To.IsZero() && !query.From.Before(query.To) {
		writeInvalidParameter(c, "from", "from must be before to")
		return EventMetricWindowQuery{}, false
	}
	if defaultLast24h && query.From.IsZero() && query.To.IsZero() {
		query.To = time.Now().UTC()
		query.From = query.To.Add(-metricsSummaryWindow)
	}
	if !query.From.IsZero() && !query.To.IsZero() && query.To.Sub(query.From) > metricsSummaryWindow {
		writeInvalidParameter(c, "to", "metrics window range must not exceed 24h")
		return EventMetricWindowQuery{}, false
	}
	for _, field := range []string{"source_host", "source_environment", "source_supervisor", "connection", "queue"} {
		value, present, valid := parseMetricsSourceParam(c, params, field)
		if !valid {
			return EventMetricWindowQuery{}, false
		}
		if !present {
			continue
		}
		switch field {
		case "source_host":
			query.SourceHost = value
		case "source_environment":
			query.SourceEnvironment = value
		case "source_supervisor":
			query.SourceSupervisor = value
		case "connection":
			query.Connection = value
		case "queue":
			query.Queue = value
		}
	}
	return query, true
}

func parseTruthyQueryParam(params map[string][]string, field string) bool {
	value := strings.ToLower(strings.TrimSpace(firstQueryValue(params[field])))
	return value == "1" || value == "true" || value == "yes"
}

func highValueDetailQueryFromContext(c *gin.Context) (HighValueDetailQuery, bool) {
	query := HighValueDetailQuery{Page: pageFromContext(c)}
	params := c.Request.URL.Query()
	kind, present, valid := parseHighValueKindParam(c, params)
	if !valid {
		return HighValueDetailQuery{}, false
	}
	if present {
		query.Kind = kind
	}
	var ok bool
	if query.OccurredFrom, ok = parseHighValueRFC3339Param(c, params, "occurred_from"); !ok {
		return HighValueDetailQuery{}, false
	}
	if query.OccurredTo, ok = parseHighValueRFC3339Param(c, params, "occurred_to"); !ok {
		return HighValueDetailQuery{}, false
	}
	if !query.OccurredFrom.IsZero() && !query.OccurredTo.IsZero() && !query.OccurredFrom.Before(query.OccurredTo) {
		writeInvalidParameter(c, "occurred_from", "occurred_from must be before occurred_to")
		return HighValueDetailQuery{}, false
	}
	return query, true
}

func parseHighValueKindParam(c *gin.Context, params map[string][]string) (string, bool, bool) {
	values, present := params["kind"]
	if !present {
		return "", false, true
	}
	value := strings.TrimSpace(firstQueryValue(values))
	if !isAllowedHighValueDetailKind(value) || value == "" {
		writeInvalidParameter(c, "kind", "kind must be failed, poison, or slow_job")
		return "", true, false
	}
	return value, true, true
}

func parseHighValueRFC3339Param(c *gin.Context, params map[string][]string, field string) (time.Time, bool) {
	values, present := params[field]
	if !present {
		return time.Time{}, true
	}
	value := strings.TrimSpace(firstQueryValue(values))
	if value == "" {
		writeInvalidParameter(c, field, field+" must be a RFC3339 timestamp with timezone")
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		writeInvalidParameter(c, field, field+" must be a RFC3339 timestamp with timezone")
		return time.Time{}, false
	}
	return parsed, true
}

// parseMetricsRFC3339Param 解析带时区的 RFC3339 时间参数。
//
// 需求背景：范围过滤按事件窗口时间执行，调用方必须提供明确时区；缺少时区或格式非法会造成
// 多实例跨地域读取漂移，因此这里统一返回 `invalid_parameter` 和字段名。
func parseMetricsRFC3339Param(c *gin.Context, params map[string][]string, field string) (time.Time, bool) {
	values, present := params[field]
	if !present {
		return time.Time{}, true
	}
	value := strings.TrimSpace(firstQueryValue(values))
	if value == "" {
		writeInvalidParameter(c, field, field+" must be a RFC3339 timestamp with timezone")
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		writeInvalidParameter(c, field, field+" must be a RFC3339 timestamp with timezone")
		return time.Time{}, false
	}
	return parsed, true
}

// parseMetricsSourceParam 解析来源维度精确匹配参数。
//
// 设计边界：本轮不支持模糊、多值 OR、通配符或正则；空字符串参数也不是 unknown source 查询，
// 而是调用方参数错误，避免 `source_supervisor=` 与缺失 supervisor 诊断语义混淆。
func parseMetricsSourceParam(c *gin.Context, params map[string][]string, field string) (string, bool, bool) {
	values, present := params[field]
	if !present {
		return "", false, true
	}
	value := strings.TrimSpace(firstQueryValue(values))
	if value == "" {
		writeInvalidParameter(c, field, field+" must not be empty")
		return "", true, false
	}
	return value, true, true
}

// firstQueryValue 返回同名查询参数的第一个值。
//
// 设计原因：issue 44 本轮不支持多值 OR；HTTP 层只接受单值精确匹配语义，重复参数不扩展为集合查询。
func firstQueryValue(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// writeInvalidParameter 输出 metrics API 统一的 400 参数错误。
//
// 使用方式：解析 `from`、`to` 和来源维度时调用；响应保留稳定 error code 和 field，便于 Dashboard
// 或外部调用方按字段展示错误，不依赖自然语言 message。
func writeInvalidParameter(c *gin.Context, field string, message string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_parameter", "field": field, "message": message})
}

func makePageEnvelope[T any](items []T, page PageRequest) PageEnvelope[T] {
	return PageEnvelope[T]{
		Items:      pageSlice(items, page),
		Total:      len(items),
		Page:       page.Page,
		PageSize:   page.PageSize,
		Capability: "supported",
	}
}

func disabledPage[T any](page PageRequest, reason string) PageEnvelope[T] {
	return PageEnvelope[T]{
		Items:      []T{},
		Total:      0,
		Page:       page.Page,
		PageSize:   page.PageSize,
		Capability: "disabled",
		Reason:     reason,
	}
}

func writePageJSON[T any](c *gin.Context, value PageEnvelope[T], err error) {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read_failed", "message": "Horizon read failed."})
		return
	}
	c.JSON(http.StatusOK, value)
}

func writeJSON(c *gin.Context, value any, err error) {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read_failed", "message": "Horizon read failed."})
		return
	}
	c.JSON(http.StatusOK, value)
}

func filterStaleSupervisors(items []SupervisorState) []SupervisorState {
	out := make([]SupervisorState, 0, len(items))
	for _, item := range items {
		if item.Status == SupervisorStale {
			out = append(out, item)
		}
	}
	return out
}

func filterStaleWorkers(items []WorkerState) []WorkerState {
	out := make([]WorkerState, 0, len(items))
	for _, item := range items {
		if item.Status == WorkerStale {
			out = append(out, item)
		}
	}
	return out
}
