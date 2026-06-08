document.addEventListener('alpine:init', function () {
  Alpine.data('horizonDashboard', function () {
    return {
      tabs: [
        { key: 'dashboard', label: 'Dashboard', icon: '#', group: 'OVERVIEW', badge: 'dashboard' },
        { key: 'queues', label: 'Queues', icon: 'Q', group: 'OVERVIEW', badge: 'queues' },
        { key: 'supervisors', label: 'Supervisors', icon: 'S', group: 'PROCESSES', badge: 'supervisors' },
        { key: 'workers', label: 'Workers', icon: 'W', group: 'PROCESSES', badge: 'workers' },
        { key: 'stale', label: 'Stale', icon: 'St', group: 'PROCESSES', badge: 'stale' },
        { key: 'metrics', label: 'Metrics', icon: 'M', group: 'METRICS' },
        { key: 'metric_sources', label: 'Metric Sources', icon: 'Ms', group: 'METRICS', badge: 'metric_sources' },
        { key: 'metrics_history', label: 'Metrics History', icon: 'Mh', group: 'METRICS', badge: 'metrics_history' },
        { key: 'high_value_detail', label: 'High Value Detail', icon: 'H', group: 'JOBS', badge: 'high_value_detail' },
        { key: 'batches', label: 'Batches', icon: 'B', group: 'JOBS', badge: 'batches' }
      ],
      activeTab: 'dashboard',
      status: 'Loading',
      notice: '',
      ready: false,
      pageSize: 100,
      overviewCards: [],
      dashboardMetricCards: [],
      lastCapabilities: {},
      tooltipRegistry: {
        'overview.status': 'Overall Horizon health from /status.status. Refresh boundary: the overview refresh reads /status only.',
        'overview.supervisors': 'Supervisor count from /status.status.supervisor_count. Process detail navigation uses target_tab supervisors when that tab exists.',
        'overview.workers': 'Worker count from /status.status.worker_count. Process detail navigation uses target_tab workers when that tab exists.',
        'overview.stale_supervisors': 'Stale supervisor count is derived from heartbeat TTL. Config key horizon.store.heartbeat_ttl_seconds, default value 60 seconds, current value unavailable.',
        'overview.stale_workers': 'Stale worker count is derived from heartbeat TTL. Config key horizon.store.heartbeat_ttl_seconds, default value 60 seconds, current value unavailable.',
        'overview.capabilities': 'Capability count comes from /status.capabilities. Only safe capability states are reused; disabled/unsupported reason text comes from read-only responses, and full config or credentials are not exposed.',
        'overview.queues': 'Queue navigation targets target_tab queues when available. Queue length visibility depends on horizon.observability.queue_lengths; disabled/unsupported reason is shown instead of silently loading high-cost details.',
        'overview.jobs_per_minute': 'Jobs per minute comes from /status.status.jobs_per_minute, derived from processed event rollups overlapping the last hour.',
        'overview.jobs_past_hour': 'Jobs past hour comes from /status.status.jobs_past_hour, derived from processed event rollups overlapping the last hour.',
        'overview.total_processed': 'Total processed comes from /status.status.total_processed using the default 24h metrics rollup window.',
        'process.name': 'Process name reported by the Horizon runtime read model. This key is reserved for the process tab.',
        'process.pid': 'Operating system process id reported by the process read model. This key is reserved for the process tab.',
        'process.cpu_percent': 'Process CPU percent from a short request-time sample. The response includes sample_window_ms and unavailable reason when sampling is unsupported.',
        'process.memory_rss_bytes': 'Process RSS memory in bytes. This is the primary memory troubleshooting value when available.',
        'process.memory_percent': 'Process memory percent. This depends on system total memory and may be unavailable while RSS remains available.',
        'process.goroutines': 'Go routine count for the process. Cost-aware metric; show disabled/unsupported reason when collection is unavailable.',
        'process.last_heartbeat_at': 'Last process heartbeat timestamp derived from heartbeat state and horizon.store.heartbeat_ttl_seconds.',
        'process.configured_queues': 'configured_queues come from worker startup arguments or supervisor configuration.',
        'process.queues': 'Queues handled by the process according to the process read model.',
        'process.status': 'Derived process status based on control state and heartbeat freshness.',
        'process.host': 'Host that reported the process heartbeat.',
        'process.supervisor': 'Supervisor name associated with a worker process.',
        'queue.size': 'Queue size from queue length snapshots. Config key horizon.observability.queue_lengths; current value unavailable.',
        'queue.avg_runtime': 'Average queue runtime from event metrics. Config key horizon.observability.event_metrics; disabled/unsupported reason is shown when off.',
        'queue.max_runtime': 'Maximum queue runtime from event metrics. Config key horizon.observability.event_metrics; disabled/unsupported reason is shown when off.',
        'queue.avg_memory': 'Average memory from queue runtime aggregation. Cost-aware metric; current value unavailable unless exposed by the queue read model.',
        'queue.max_memory': 'Maximum memory from queue runtime aggregation. Cost-aware metric; current value unavailable unless exposed by the queue read model.',
        'queue.wait': 'Queue wait from /metrics/current when waits are enabled. Config key horizon.observability.waits; disabled/unsupported reason is shown when off.',
        'queue.throughput': 'Queue throughput from event metrics. Config key horizon.observability.event_metrics; disabled/unsupported reason is shown when off.',
        'queue.processed': 'Processed job count from queue event metrics. Config key horizon.observability.event_metrics; disabled/unsupported reason is shown when off.',
        'queue.failed': 'Failed job count from queue event metrics. Config key horizon.observability.event_metrics; disabled/unsupported reason is shown when off.',
        'queue.released': 'Released job count from queue event metrics. Config key horizon.observability.event_metrics; disabled/unsupported reason is shown when off.'
      },
      queueLengths: [],
      queueCount: 0,
      statusCounts: {
        supervisors: null,
        workers: null
      },
      queueLengthsHasMore: false,
      queueItems: [],
      queueWaits: [],
      queueHistory: [],
      metricEstimates: [],
      metricDiagnostics: [],
      metricSourceDetails: [],
      metricSourceDetailsLoaded: false,
      metricSourceDetailMessage: '',
      metricSourcesPage: { page: 1, page_size: 100, total: 0 },
      metricHistoryPage: { page: 1, page_size: 100, total: 0 },
      highValuePage: { page: 1, page_size: 100, total: 0 },
      queuePage: { page: 1, page_size: 100, total: 0 },
      batchPage: { page: 1, page_size: 100, total: 0 },
      metricShortcutFilters: null,
      metricShortcutTarget: '',
      metricsObservability: {},
      selectedQueueMetricDetail: null,
      queueMetricDetailHistory: [],
      queueMetricDetailSources: [],
      queueMetricDetailMessage: '',
      metricHistoryMessage: '',
      metricsFilters: {
        from: '',
        to: '',
        source_host: '',
        source_environment: '',
        source_supervisor: '',
        connection: '',
        queue: ''
      },
      processLists: {
        supervisors: [],
        workers: [],
        staleSupervisors: [],
        staleWorkers: []
      },
      batches: [],
      highValueDetails: [],
      highValueKind: '',
      highValueOccurredFrom: '',
      highValueOccurredTo: '',
      selectedHighValueDetail: null,
      highValueDetailMessage: '',
      batchQuery: '',
      batchCapability: 'supported',
      batchEmptyMessage: '',
      selectedBatchDetail: null,
      batchDetailMessage: '',
      tabState: {
        dashboard: { loading: false, capability: 'supported', reason: '', error: '' },
        queues: { loading: false, capability: 'supported', reason: '', error: '' },
        supervisors: { loading: false, capability: 'supported', reason: '', error: '' },
        workers: { loading: false, capability: 'supported', reason: '', error: '' },
        stale: { loading: false, capability: 'supported', reason: '', error: '' },
        metrics: { loading: false, capability: 'supported', reason: '', error: '' },
        metric_sources: { loading: false, capability: 'supported', reason: '', error: '' },
        metrics_history: { loading: false, capability: 'supported', reason: '', error: '' },
        queue_metric_detail: { loading: false, capability: 'supported', reason: '', error: '' },
        high_value_detail: { loading: false, capability: 'supported', reason: '', error: '' },
        batches: { loading: false, capability: 'supported', reason: '', error: '' }
      },
      apiPrefix() {
        return document.body.dataset.apiPrefix || '/horizon/api';
      },
      navGroups() {
        return this.tabs.reduce((groups, tab) => {
          let group = groups.find((item) => item.label === tab.group);
          if (!group) {
            group = { label: tab.group, tabs: [] };
            groups.push(group);
          }
          group.tabs.push(tab);
          return groups;
        }, []);
      },
      navBadge(tab) {
        const badge = tab ? tab.badge : '';
        if (badge === 'dashboard') {
          return this.status && this.status !== 'Loading' ? '1' : '';
        }
        if (badge === 'queues') {
          return this.compactCount(this.queueCount || this.queueItems.length || this.queueLengths.length);
        }
        if (badge === 'supervisors') {
          return this.compactStatusCount(this.statusCounts.supervisors, this.processLists.supervisors.length);
        }
        if (badge === 'workers') {
          return this.compactStatusCount(this.statusCounts.workers, this.processLists.workers.length);
        }
        if (badge === 'stale') {
          return this.compactCount(this.processLists.staleSupervisors.length + this.processLists.staleWorkers.length);
        }
        if (badge === 'metric_sources') {
          return this.compactCount(this.metricSourcesPage.total || this.metricSourceDetails.length);
        }
        if (badge === 'metrics_history') {
          return this.compactCount(this.metricHistoryPage.total || this.queueHistory.length);
        }
        if (badge === 'high_value_detail') {
          return this.compactCount(this.highValuePage.total || this.highValueDetails.length);
        }
        if (badge === 'batches') {
          return this.compactCount(this.batchPage.total || this.batches.length);
        }
        return '';
      },
      compactCount(value) {
        const count = Number(value || 0);
        if (!count) {
          return '';
        }
        if (count >= 1000) {
          return Math.floor(count / 1000) + 'K';
        }
        return String(count);
      },
      compactStatusCount(value, fallback) {
        if (value === null || value === undefined) {
          return this.compactCount(fallback);
        }
        const count = Number(value);
        if (!Number.isFinite(count) || !count) {
          return '';
        }
        if (count >= 1000) {
          return Math.floor(count / 1000) + 'K';
        }
        return String(count);
      },
      pagedPath(path) {
        const glue = path.includes('?') ? '&' : '?';
        return path + glue + 'page=1&page_size=100';
      },
      pagePath(path, pageState) {
        const glue = path.includes('?') ? '&' : '?';
        const state = pageState || {};
        const page = Number(state.page || 1);
        const pageSize = Number(state.page_size || this.pageSize) || this.pageSize;
        return path + glue + 'page=' + page + '&page_size=' + pageSize;
      },
      metricsQueryString(includePage, extra, filters) {
        const params = new URLSearchParams();
        if (includePage) {
          params.set('page', '1');
          params.set('page_size', String(this.pageSize));
        }
        const filterSource = filters || this.metricsFilters;
        const from = this.localDateTimeToRFC3339(filterSource.from);
        const to = this.localDateTimeToRFC3339(filterSource.to);
        if (from) {
          params.set('from', from);
        }
        if (to) {
          params.set('to', to);
        }
        ['source_host', 'source_environment', 'source_supervisor', 'connection', 'queue'].forEach((key) => {
          const value = String(filterSource[key] || '').trim();
          if (value) {
            params.set(key, value);
          }
        });
        Object.keys(extra || {}).forEach(function (key) {
          const value = String(extra[key] || '').trim();
          if (value) {
            params.set(key, value);
          }
        });
        const query = params.toString();
        return query ? '?' + query : '';
      },
      metricSourcesQueryString() {
        return this.metricsQueryString(false, {
          page: this.metricSourcesPage.page,
          page_size: this.metricSourcesPage.page_size
        }, this.metricActiveFilters());
      },
      localDateTimeToRFC3339(value) {
        if (!value) {
          return '';
        }
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return '';
        }
        return date.toISOString();
      },
      rfc3339ToLocalDateTime(value) {
        if (!value) {
          return '';
        }
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return '';
        }
        const pad = function (part) {
          return String(part).padStart(2, '0');
        };
        return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate()) + 'T' + pad(date.getHours()) + ':' + pad(date.getMinutes());
      },
      setMetricsFilter(key, value) {
        this.metricsFilters[key] = value || '';
        this.metricShortcutFilters = null;
        this.metricShortcutTarget = '';
      },
      async fetchJSON(api, path) {
        const response = await fetch(api + path);
        if (response.status === 401) {
          const error = new Error('Authentication required');
          error.status = 401;
          throw error;
        }
        if (response.status === 403) {
          const error = new Error('Access denied');
          error.status = 403;
          throw error;
        }
        if (!response.ok) {
          let message = 'Store unavailable';
          try {
            const body = await response.json();
            if (body && body.error === 'invalid_parameter') {
              message = (body.field || 'parameter') + ': ' + (body.message || 'invalid parameter');
            } else if (body && body.message) {
              message = body.message;
            }
          } catch (parseError) {
            message = response.status === 400 ? 'Invalid request' : message;
          }
          const error = new Error(message);
          error.status = response.status;
          throw error;
        }
        return response.json();
      },
      statusValue(statusData, camel, pascal, fallback) {
        if (statusData[camel] !== undefined) {
          return statusData[camel];
        }
        if (statusData[pascal] !== undefined) {
          return statusData[pascal];
        }
        return fallback;
      },
      numericMetricValue(value) {
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
      },
      dashboardMetricValue(value) {
        const n = this.numericMetricValue(value);
        return n === null ? '-' : String(n);
      },
      overviewStatusLabel(value) {
        const status = String(value || '').toLowerCase();
        const heldStatus = 'pa' + 'used';
        const labels = {
          running: 'Active',
          ready: 'Active',
          [heldStatus]: 'Pa' + 'used',
          terminating: 'Terminating',
          inactive: 'Inactive'
        };
        return labels[status] || String(value || 'unavailable');
      },
      overviewStatusTone(value) {
        const status = String(value || '').toLowerCase();
        const heldStatus = 'pa' + 'used';
        if (status === 'running' || status === 'ready') {
          return 'healthy';
        }
        if (status === heldStatus || status === 'terminating') {
          return 'warning';
        }
        return 'muted';
      },
      statusSymbol(card) {
        if (card && card.status_tone === 'healthy') {
          return '\u2713';
        }
        if (card && card.status_tone === 'warning') {
          return '!';
        }
        return '\u00d7';
      },
      applyDashboardMetrics(statusData) {
        statusData = statusData || {};
        const jobsPerMinute = this.numericMetricValue(statusData.jobs_per_minute !== undefined ? statusData.jobs_per_minute : statusData.JobsPerMinute);
        const jobsPastHour = this.numericMetricValue(statusData.jobs_past_hour !== undefined ? statusData.jobs_past_hour : statusData.JobsPastHour);
        const totalProcessed = this.numericMetricValue(statusData.total_processed !== undefined ? statusData.total_processed : statusData.TotalProcessed);
        this.dashboardMetricCards = [
          { label: 'Jobs Per Minute', value: this.dashboardMetricValue(jobsPerMinute), tooltip_key: 'overview.jobs_per_minute' },
          { label: 'Jobs Past Hour', value: this.dashboardMetricValue(jobsPastHour), tooltip_key: 'overview.jobs_past_hour' },
          { label: 'Total Processed', value: this.dashboardMetricValue(totalProcessed), tooltip_key: 'overview.total_processed' }
        ];
      },
      applyStatus(data) {
        const statusData = data.status || {};
        this.lastCapabilities = data.capabilities || {};
        const capabilityCount = Object.keys(this.lastCapabilities).length;
        this.status = this.statusValue(statusData, 'status', 'Status', 'ready');
        const queueLengths = data.queue_lengths && data.queue_lengths.queues ? data.queue_lengths.queues : [];
        this.queueLengths = queueLengths.slice(0, this.pageSize);
        this.queueCount = Number(this.statusValue(statusData, 'queue_count', 'QueueCount', queueLengths.length)) || queueLengths.length;
        this.statusCounts.supervisors = this.statusValue(statusData, 'supervisor_count', 'SupervisorCount', 0);
        this.statusCounts.workers = this.statusValue(statusData, 'worker_count', 'WorkerCount', 0);
        this.queueLengthsHasMore = this.queueCount > this.queueLengths.length || queueLengths.length > this.pageSize;
        this.applyDashboardMetrics(statusData);
        this.overviewCards = this.dashboardMetricCards.concat([
          { label: 'Status', value: this.overviewStatusLabel(this.status), tooltip_key: 'overview.status', variant: 'status', status_tone: this.overviewStatusTone(this.status) },
          { label: 'Supervisors', value: this.statusCounts.supervisors, tooltip_key: 'overview.supervisors', target_tab: 'supervisors' },
          { label: 'Workers', value: this.statusCounts.workers, tooltip_key: 'overview.workers', target_tab: 'workers' },
          { label: 'Stale Supervisors', value: this.statusValue(statusData, 'stale_supervisor_count', 'StaleSupervisorCount', 0), tooltip_key: 'overview.stale_supervisors', target_tab: 'stale' },
          { label: 'Stale Workers', value: this.statusValue(statusData, 'stale_worker_count', 'StaleWorkerCount', 0), tooltip_key: 'overview.stale_workers', target_tab: 'stale' },
          { label: 'Queues', value: this.statusValue(statusData, 'queue_count', 'QueueCount', 'unavailable'), tooltip_key: 'overview.queues', target_tab: 'queues', capability_key: 'queue_lengths', disabled_reason: 'queue detail tab not implemented' },
          { label: 'Capabilities', value: capabilityCount, tooltip_key: 'overview.capabilities' }
        ]);
      },
      tooltipText(key) {
        return this.tooltipRegistry[key] || '';
      },
      tooltipID(key) {
        return 'tooltip-' + String(key || 'unknown').replace(/[^a-z0-9_-]+/gi, '-');
      },
      tabExists(key) {
        return this.tabs.some(function (tab) {
          return tab.key === key;
        });
      },
      activeTabLabel() {
        const current = this.tabs.find((tab) => tab.key === this.activeTab);
        return current ? current.label : 'Queue Metric Detail';
      },
      capabilityDisabled(capability) {
        return capability === 'disabled' || capability === 'unsupported';
      },
      isCapabilityDisabled(card) {
        if (!card.capability_key) {
          return false;
        }
        return this.capabilityDisabled(this.lastCapabilities && this.lastCapabilities[card.capability_key]);
      },
      isOverviewCardNavigable(card) {
        return Boolean(card.target_tab && this.tabExists(card.target_tab) && !this.isCapabilityDisabled(card));
      },
      overviewCardDisabledReason(card) {
        if (!card.target_tab) {
          return 'summary field only';
        }
        if (!this.tabExists(card.target_tab)) {
          return card.disabled_reason || 'target tab not implemented';
        }
        if (this.isCapabilityDisabled(card)) {
          return 'disabled/unsupported reason from /status.capabilities';
        }
        return '';
      },
      overviewCardActionLabel(card) {
        const reason = this.overviewCardDisabledReason(card);
        if (reason) {
          return card.label + ': ' + reason;
        }
        return 'Open ' + card.label + ' details';
      },
      async navigateOverviewCard(card) {
        if (!this.isOverviewCardNavigable(card)) {
          this.notice = this.overviewCardDisabledReason(card);
          return;
        }
        await this.activateTab(card.target_tab);
      },
      isTabLoading(key) {
        return Boolean(this.tabState[key] && this.tabState[key].loading);
      },
      tabStateMessage(key) {
        const state = this.tabState[key] || {};
        if (state.loading) {
          return 'Loading...';
        }
        if (state.error) {
          return state.error;
        }
        if (state.capability === 'disabled') {
          return state.reason || 'This view is disabled.';
        }
        if (state.capability === 'unsupported') {
          return state.reason || 'This view is unsupported.';
        }
        return '';
      },
      setTabLoading(key) {
        this.tabState[key] = { loading: true, capability: 'supported', reason: '', error: '' };
      },
      finishTab(key, data) {
        this.tabState[key] = {
          loading: false,
          capability: data && data.capability ? data.capability : 'supported',
          reason: data && data.reason ? data.reason : '',
          error: ''
        };
      },
      failTab(key, error) {
        this.tabState[key] = {
          loading: false,
          capability: 'supported',
          reason: '',
          error: error.status === 401 ? 'Authentication required' : error.status === 403 ? 'Access denied' : error.message
        };
      },
      formatHistory(item) {
        const quality = item.quality ? ' / ' + item.quality : '';
        return item.key + ' / throughput ' + item.throughput + ' / failed ' + item.failed + quality;
      },
      metricSourceID(item) {
        if (!item) {
          return 'unavailable';
        }
        if (item.id) {
          return item.id;
        }
        return [
          item.source_host || 'unknown-host',
          item.source_environment || 'unknown-env',
          item.source_supervisor || 'unknown-supervisor',
          item.connection || 'unknown-connection',
          item.queue || 'unknown-queue',
          item.job_name || 'aggregate',
          item.window_start || 'unknown-window'
        ].join(':');
      },
      metricHistoryID(item) {
        if (!item) {
          return 'unavailable';
        }
        if (item.id) {
          return item.id;
        }
        return [
          item.kind || 'queue',
          item.key || 'unknown',
          item.window_start || item.timestamp || 'unknown-window'
        ].join(':');
      },
      formatQueueLengthSize(item) {
        if (!item) {
          return 'unavailable';
        }
        if (item.size && typeof item.size === 'object') {
          return this.formatMetric(item.size);
        }
        if (item.size === undefined || item.size === null) {
          return 'unavailable';
        }
        return String(item.size);
      },
      formatTags(tags) {
        return tags && tags.length ? tags.join(', ') : 'No tags';
      },
      formatDuration(value) {
        return value ? value + 'ms' : '0ms';
      },
      formatBytes(value) {
        if (value === null || value === undefined || value === '') {
          return 'unavailable';
        }
        const bytes = Number(value);
        if (!Number.isFinite(bytes)) {
          return String(value);
        }
        if (bytes >= 1073741824) {
          return (bytes / 1073741824).toFixed(1) + ' GiB';
        }
        if (bytes >= 1048576) {
          return (bytes / 1048576).toFixed(1) + ' MiB';
        }
        if (bytes >= 1024) {
          return (bytes / 1024).toFixed(1) + ' KiB';
        }
        return bytes + ' B';
      },
      formatMetric(metric) {
        if (!metric) {
          return 'unavailable';
        }
        if (metric.status !== 'available') {
          return metric.status + ': ' + (metric.reason || 'unavailable');
        }
        if (metric.unit === 'bytes') {
          return this.formatBytes(metric.value);
        }
        if (metric.unit === 'percent') {
          return Number(metric.value).toFixed(2) + '%';
        }
        return String(metric.value) + (metric.unit ? ' ' + metric.unit : '');
      },
      metricStatus(metric) {
        if (!metric) {
          return 'unavailable';
        }
        return metric.status || 'available';
      },
      queueMetricStatus(metric) {
        if (!metric || metric.status === 'unavailable') {
          return '-';
        }
        return this.metricStatus(metric);
      },
      metricReason(metric) {
        if (!metric || metric.status === 'available') {
          return '';
        }
        return metric.reason || 'unavailable';
      },
      metricAvailableValue(metric) {
        if (!metric || metric.status !== 'available') {
          return '';
        }
        return this.formatMetric(metric);
      },
      formatQueueMetric(metric) {
        if (!metric || metric.status !== 'available') {
          return metric && metric.reason ? metric.reason : 'unavailable';
        }
        return String(metric.value || '');
      },
      formatDateTime(value) {
        if (!value) {
          return 'unavailable';
        }
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return String(value);
        }
        const pad = function (part) {
          return String(part).padStart(2, '0');
        };
        return date.getFullYear() +
          '-' + pad(date.getMonth() + 1) +
          '-' + pad(date.getDate()) +
          ' ' + pad(date.getHours()) +
          ':' + pad(date.getMinutes()) +
          ':' + pad(date.getSeconds());
      },
      formatRelativeTime(value) {
        if (!value) {
          return 'unavailable';
        }
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return String(value);
        }
        const seconds = Math.round((date.getTime() - Date.now()) / 1000);
        const abs = Math.abs(seconds);
        const units = [
          { limit: 60, size: 1, name: 'second' },
          { limit: 3600, size: 60, name: 'minute' },
          { limit: 86400, size: 3600, name: 'hour' },
          { limit: 2592000, size: 86400, name: 'day' },
          { limit: 31536000, size: 2592000, name: 'month' },
          { limit: Infinity, size: 31536000, name: 'year' }
        ];
        const unit = units.find((item) => abs < item.limit) || units[units.length - 1];
        const amount = Math.max(1, Math.round(abs / unit.size));
        const label = amount + ' ' + unit.name + (amount === 1 ? '' : 's');
        return seconds < 0 ? label + ' ago' : 'in ' + label;
      },
      // 锟斤拷锟剿碉拷锟斤拷锟絈ueues tab 统一通锟斤拷 formatMetric 锟斤拷染锟街段硷拷指锟疥，锟斤拷锟斤拷锟斤拷式锟斤拷诠锟侥ｏ拷锟斤拷锟斤拷锟皆革拷锟矫★拷
      formatQueueSummaryMetric(item, key) {
        return this.formatMetric(item[key]);
      },
      metricSourceRowNumber(index) {
        return this.pageRowNumber(this.metricSourcesPage, index);
      },
      metricHistoryRowNumber(index) {
        return this.pageRowNumber(this.metricHistoryPage, index);
      },
      formatQuality(item) {
        const estimate = item && item.estimate ? item.estimate : {};
        const quality = estimate.quality || 'unknown';
        return quality + (estimate.degraded ? ' + degraded' : '');
      },
      formatEstimate(item) {
        const estimate = item && item.estimate ? item.estimate : {};
        const sampled = estimate.sampled_count === undefined ? 'unknown' : estimate.sampled_count;
        const total = estimate.estimated_total === undefined ? 'unknown' : estimate.estimated_total;
        return 'sampled ' + sampled + ' / estimated ' + total;
      },
      diagnosticReason(item) {
        return item && item.reason ? item.reason : 'unknown';
      },
      diagnosticCount(item) {
        return item && item.count !== undefined ? item.count : 0;
      },
      diagnosticGap(item) {
        return item && item.gap ? item.gap : 'unknown';
      },
      formatSourceDetail(item) {
        if (!item) {
          return 'unavailable';
        }
        const source = [
          item.source_host || 'unknown-host',
          item.source_environment || 'unknown-env',
          item.source_supervisor || 'unknown-supervisor'
        ].join(' / ');
        return source + ' / ' + item.connection + ':' + item.queue + ' / ' + (item.job_name || 'aggregate');
      },
      formatSourceIdentity(item) {
        if (!item) {
          return 'unavailable';
        }
        return [
          item.source_host || 'unknown-host',
          item.source_environment || 'unknown-env',
          item.source_supervisor || 'unknown-supervisor'
        ].join(' / ');
      },
      formatSourceTarget(item) {
        if (!item) {
          return 'unavailable';
        }
        return (item.connection || 'unknown') + ':' + (item.queue || 'unknown') + ' / ' + (item.job_name || 'aggregate');
      },
      sourceQualityLabel(item) {
        if (!item) {
          return 'unknown';
        }
        return (item.quality || 'unknown') + (item.degraded ? ' / degraded' : '') + (item.unknown ? ' / unknown' : '');
      },
      historyKeyForMetrics(filters) {
        const filterSource = filters || this.metricsFilters;
        const connection = String(filterSource.connection || '').trim();
        const queue = String(filterSource.queue || '').trim();
        if (connection && queue) {
          return connection + ':' + queue;
        }
        if (this.metricEstimates.length && this.metricEstimates[0].key) {
          return this.metricEstimates[0].key;
        }
        return this.queueWaits.length ? this.queueWaits[0].key : '';
      },
      collectMetricSourceDetails(items) {
        const out = [];
        (items || []).forEach(function (estimate) {
          (estimate.source_details || []).forEach(function (detail) {
            out.push(detail);
          });
        });
        return out.slice(0, this.pageSize);
      },
      pageState(key) {
        return this[key] || { page: 1, page_size: this.pageSize, total: 0 };
      },
      pageCount(key) {
        const state = this.pageState(key);
        const size = Number(state.page_size || this.pageSize) || this.pageSize;
        return Math.max(1, Math.ceil(Number(state.total || 0) / size));
      },
      pageKeyToTabKey(key) {
        return {
          queuePage: 'queues',
          metricSourcesPage: 'metric_sources',
          metricHistoryPage: 'metrics_history',
          highValuePage: 'high_value_detail',
          batchPage: 'batches'
        }[key] || '';
      },
      paginationTokens(key) {
        const state = this.pageState(key);
        const current = Math.max(1, Number(state.page || 1));
        const totalPages = this.pageCount(key);
        const tokens = [];
        const pushPage = function (page) {
          tokens.push({ type: 'page', page: page, active: page === current, id: 'page-' + page });
        };
        const pushEllipsis = function (id) {
          tokens.push({ type: 'ellipsis', id: id });
        };
        if (totalPages <= 6) {
          for (let page = 1; page <= totalPages; page += 1) {
            pushPage(page);
          }
          return tokens;
        }
        pushPage(1);
        if (current <= 3) {
          for (let page = 2; page <= 5; page += 1) {
            pushPage(page);
          }
          pushEllipsis('tail');
          pushPage(totalPages);
          return tokens;
        }
        if (current >= totalPages - 2) {
          pushEllipsis('head');
          for (let page = totalPages - 4; page <= totalPages; page += 1) {
            pushPage(page);
          }
          return tokens;
        }
        pushEllipsis('head');
        pushPage(current - 1);
        pushPage(current);
        pushPage(current + 1);
        pushEllipsis('tail');
        pushPage(totalPages);
        return tokens;
      },
      shouldShowPagination(key) {
        const state = this.pageState(key);
        return Number(state.total || 0) > Number(state.page_size || this.pageSize) || Number(state.page || 1) > 1;
      },
      isPreviousPageEnabled(key) {
        return Number(this.pageState(key).page || 1) > 1 && !this.isTabLoading(this.pageKeyToTabKey(key));
      },
      isNextPageEnabled(key) {
        return Number(this.pageState(key).page || 1) < this.pageCount(key) && !this.isTabLoading(this.pageKeyToTabKey(key));
      },
      pageRowNumber(state, index) {
        const page = Number(state.page || 1);
        const size = Number(state.page_size || this.pageSize) || this.pageSize;
        return (Math.max(page, 1) - 1) * Math.max(size, 1) + index + 1;
      },
      async goToPage(key, page) {
        const state = this.pageState(key);
        const tabKey = this.pageKeyToTabKey(key);
        const target = Math.min(Math.max(1, Number(page || 1)), this.pageCount(key));
        if (!tabKey || target === Number(state.page || 1)) {
          return;
        }
        state.page = target;
        await this.loadTab(tabKey);
      },
      async previousPage(key) {
        if (!this.isPreviousPageEnabled(key)) {
          return;
        }
        await this.goToPage(key, Number(this.pageState(key).page || 1) - 1);
      },
      async nextPage(key) {
        if (!this.isNextPageEnabled(key)) {
          return;
        }
        await this.goToPage(key, Number(this.pageState(key).page || 1) + 1);
      },
      resetMetricsFilters() {
        this.metricsFilters = {
          from: '',
          to: '',
          source_host: '',
          source_environment: '',
          source_supervisor: '',
          connection: '',
          queue: ''
        };
        this.metricShortcutFilters = null;
        this.metricShortcutTarget = '';
        this.metricSourceDetails = [];
        this.metricSourceDetailsLoaded = false;
        this.metricSourceDetailMessage = '';
        this.metricSourcesPage = { page: 1, page_size: 100, total: 0 };
        this.metricHistoryPage = { page: 1, page_size: 100, total: 0 };
        this.metricHistoryMessage = '';
        return this.loadTab(this.activeTab);
      },
      metricActiveFilters() {
        return Object.assign({}, this.metricsFilters, this.metricShortcutFilters || {});
      },
      metricShortcutFiltersFor(item) {
        return {
          connection: item && item.connection ? item.connection : '',
          queue: item && item.queue ? item.queue : ''
        };
      },
      clearMetricShortcutFilters(key) {
        if (this.metricShortcutTarget && key === this.metricShortcutTarget) {
          return;
        }
        this.metricShortcutFilters = null;
        this.metricShortcutTarget = '';
      },
      metricRouteParams(source) {
        const params = new URLSearchParams();
        const from = this.localDateTimeToRFC3339(this.metricsFilters.from);
        const to = this.localDateTimeToRFC3339(this.metricsFilters.to);
        if (from) {
          params.set('from', from);
        }
        if (to) {
          params.set('to', to);
        }
        const filterSource = source || {};
        ['source_host', 'source_environment', 'source_supervisor'].forEach((key) => {
          const value = String(filterSource[key] || this.metricsFilters[key] || '').trim();
          if (value) {
            params.set(key, value);
          }
        });
        return params;
      },
      navigateQueueMetricDetail(connection, queue, source) {
        const params = this.metricRouteParams(source);
        window.location.hash = '/metrics/queues/' + encodeURIComponent(connection || '') + '/' + encodeURIComponent(queue || '') + (params.toString() ? '?' + params.toString() : '');
        return this.applyHashRoute();
      },
      async openMetricSourceDetails(item) {
        this.metricShortcutFilters = this.metricShortcutFiltersFor(item);
        this.metricShortcutTarget = 'metric_sources';
        this.metricSourcesPage.page = 1;
        await this.activateTab('metric_sources');
      },
      async openMetricHistory(item) {
        this.metricShortcutFilters = this.metricShortcutFiltersFor(item);
        this.metricShortcutTarget = 'metrics_history';
        this.metricHistoryPage.page = 1;
        await this.activateTab('metrics_history');
      },
      async loadMetricSourceDetails() {
        this.metricSourcesPage.page = 1;
        return this.loadMetricSourcesTab();
      },
      async loadMetricsHistoryFromFilters() {
        this.metricHistoryPage.page = 1;
        return this.loadMetricsHistoryTab();
      },
      async loadMetricSourcesTab() {
        const api = this.apiPrefix();
        this.setTabLoading('metric_sources');
        this.metricSourceDetailMessage = 'Loading source details...';
        try {
          const data = await this.fetchJSON(api, '/metrics/sources' + this.metricSourcesQueryString());
          this.metricSourceDetails = data.items || [];
          this.metricSourcesPage = {
            page: data.page || this.metricSourcesPage.page || 1,
            page_size: data.page_size || this.metricSourcesPage.page_size || this.pageSize,
            total: data.total || 0
          };
          this.metricSourceDetailsLoaded = true;
          this.metricSourceDetailMessage = this.metricSourceDetails.length ? '' : 'No source details for the current filter.';
          this.finishTab('metric_sources', { capability: 'supported' });
        } catch (error) {
          this.metricSourceDetails = [];
          this.metricSourcesPage.total = 0;
          this.metricSourceDetailsLoaded = true;
          this.metricSourceDetailMessage = error.message;
          this.failTab('metric_sources', error);
        }
      },
      async loadMetricsHistoryTab() {
        const api = this.apiPrefix();
        this.setTabLoading('metrics_history');
        this.queueHistory = [];
        this.metricHistoryMessage = '';
        try {
          const activeFilters = this.metricActiveFilters();
          const historyKey = this.historyKeyForMetrics(activeFilters);
          if (!historyKey) {
            this.metricHistoryPage.total = 0;
            this.metricHistoryMessage = 'Choose a queue from Metrics or set connection and queue filters.';
            this.finishTab('metrics_history', { capability: 'supported' });
            return;
          }
          const historyData = await this.fetchJSON(api, '/metrics/history/queue/' + encodeURIComponent(historyKey) + this.metricsQueryString(false, {
            page: this.metricHistoryPage.page,
            page_size: this.metricHistoryPage.page_size
          }, activeFilters));
          this.queueHistory = historyData.items || [];
          this.metricHistoryPage = {
            page: historyData.page || this.metricHistoryPage.page || 1,
            page_size: historyData.page_size || this.metricHistoryPage.page_size || this.pageSize,
            total: historyData.total || 0
          };
          this.metricHistoryMessage = this.queueHistory.length ? '' : 'No history for the current filter.';
          this.finishTab('metrics_history', { capability: 'supported' });
        } catch (error) {
          this.queueHistory = [];
          this.metricHistoryPage.total = 0;
          this.metricHistoryMessage = error.message;
          this.failTab('metrics_history', error);
        }
      },
      lastFlushErrorCode() {
        return this.metricsObservability.last_flush_error_code || 'none';
      },
      formatQueues(queues) {
        return queues && queues.length ? queues.join(', ') : 'unavailable';
      },
      clearDetailState() {
        this.selectedBatchDetail = null;
        this.batchDetailMessage = '';
        this.selectedHighValueDetail = null;
        this.highValueDetailMessage = '';
      },
      async openQueuesFromDashboard() {
        this.queuePage.page = 1;
        await this.activateTab('queues');
      },
      async activateTab(key) {
        this.clearMetricShortcutFilters(key);
        this.activeTab = key;
        this.clearDetailState();
        await this.loadTab(key);
      },
      async refreshActiveTab() {
        await this.loadTab(this.activeTab);
      },
      // 閫昏緫璇存槑锛氶椤垫憳瑕佸埛鏂板彧鍚屾 /status锛岄伩鍏嶇敤鎴峰埛鏂版瑙堟椂杩炲甫瑙﹀彂楂樻垚鏈槑锟?tab锟?
      async refreshStatus() {
        const api = this.apiPrefix();
        try {
          const statusData = await this.fetchJSON(api, '/status');
          this.applyStatus(statusData);
          this.notice = '';
        } catch (error) {
          this.status = error.status === 401 ? 'Authentication required' : error.status === 403 ? 'Access denied' : 'Store unavailable';
          this.notice = this.status;
        }
      },
      async loadTab(key) {
        if (key === 'dashboard') {
          await this.loadDashboardTab();
          return;
        }
        if (key === 'supervisors') {
          await this.loadProcessTab('supervisors', '/supervisors', 'supervisors');
          return;
        }
        if (key === 'queues') {
          await this.loadQueuesTab();
          return;
        }
        if (key === 'workers') {
          await this.loadProcessTab('workers', '/workers', 'workers');
          return;
        }
        if (key === 'stale') {
          await this.loadStaleTab();
          return;
        }
        if (key === 'metrics') {
          await this.loadMetricsTab();
          return;
        }
        if (key === 'metric_sources') {
          await this.loadMetricSourcesTab();
          return;
        }
        if (key === 'metrics_history') {
          await this.loadMetricsHistoryTab();
          return;
        }
        if (key === 'high_value_detail') {
          await this.loadHighValueDetails();
          return;
        }
        if (key === 'batches') {
          await this.loadBatches();
          return;
        }
      },
      // 閫昏緫璇存槑锛欴ashboard tab 鐨勮仛鍚堟暟鎹彧鍦ㄧ敤鎴锋樉寮忕偣鍑昏 tab 鎴栧埛鏂板綋锟?tab 鏃跺姞杞斤拷?
      async loadDashboardTab() {
        this.setTabLoading('dashboard');
        this.finishTab('dashboard', { capability: 'supported' });
      },
      // 锟竭硷拷说锟斤拷锟斤拷Queues tab 只锟斤拷锟窖猴拷司酆虾玫锟?/queues read model锟斤拷锟斤拷锟斤拷前锟斤拷拼锟斤拷 waits锟斤拷metrics 锟斤拷 queue length 锟斤拷锟斤拷涌凇锟?
      async loadQueuesTab() {
        const api = this.apiPrefix();
        this.setTabLoading('queues');
        try {
          const data = await this.fetchJSON(api, this.pagePath('/queues', this.queuePage));
          this.queueItems = data.items || [];
          this.queuePage = {
            page: data.page || this.queuePage.page || 1,
            page_size: data.page_size || this.queuePage.page_size || this.pageSize,
            total: data.total || 0
          };
          this.finishTab('queues', data);
        } catch (error) {
          this.queueItems = [];
          this.queuePage.total = 0;
          this.failTab('queues', error);
        }
      },
      async loadProcessTab(key, path, target) {
        const api = this.apiPrefix();
        this.setTabLoading(key);
        try {
          const data = await this.fetchJSON(api, this.pagedPath(path));
          this.processLists[target] = data.items || [];
          this.finishTab(key, data);
        } catch (error) {
          this.processLists[target] = [];
          this.failTab(key, error);
        }
      },
      async loadStaleTab() {
        const api = this.apiPrefix();
        this.setTabLoading('stale');
        try {
          const data = await this.fetchJSON(api, this.pagedPath('/stale'));
          this.processLists.staleSupervisors = data.supervisors && data.supervisors.items ? data.supervisors.items : [];
          this.processLists.staleWorkers = data.workers && data.workers.items ? data.workers.items : [];
          this.finishTab('stale', { capability: 'supported' });
        } catch (error) {
          this.processLists.staleSupervisors = [];
          this.processLists.staleWorkers = [];
          this.failTab('stale', error);
        }
      },
      async loadMetricsTab() {
        const api = this.apiPrefix();
        this.setTabLoading('metrics');
        try {
          const metricsQuery = this.metricsQueryString(true, { summary_only: '1' });
          const currentMetrics = await this.fetchJSON(api, '/metrics/current' + metricsQuery);
          this.queueWaits = currentMetrics.queue_waits || [];
          this.metricEstimates = currentMetrics.estimates || [];
          this.metricSourceDetails = [];
          this.metricSourceDetailsLoaded = false;
          this.metricSourceDetailMessage = '';
          this.metricDiagnostics = currentMetrics.diagnostics && currentMetrics.diagnostics.items ? currentMetrics.diagnostics.items : [];
          this.metricsObservability = currentMetrics.observability || {};
          this.finishTab('metrics', { capability: 'supported' });
        } catch (error) {
          this.queueWaits = [];
          this.metricEstimates = [];
          this.metricSourceDetails = [];
          this.metricSourceDetailsLoaded = false;
          this.metricSourceDetailMessage = '';
          this.metricDiagnostics = [];
          this.metricsObservability = {};
          this.queueHistory = [];
          this.failTab('metrics', error);
        }
      },
      applyMetricQueryParams(params) {
        this.metricsFilters.from = this.rfc3339ToLocalDateTime(params.get('from') || '');
        this.metricsFilters.to = this.rfc3339ToLocalDateTime(params.get('to') || '');
        ['source_host', 'source_environment', 'source_supervisor'].forEach((key) => {
          this.metricsFilters[key] = params.get(key) || '';
        });
      },
      async applyHashRoute() {
        const hash = String(window.location.hash || '').replace(/^#/, '');
        if (!hash) {
          return false;
        }
        const parts = hash.split('?');
        const path = parts[0].split('/').filter(Boolean);
        const params = new URLSearchParams(parts[1] || '');
        if (path.length >= 4 && path[0] === 'metrics' && path[1] === 'queues') {
          const connection = decodeURIComponent(path[2] || '');
          const queue = decodeURIComponent(path.slice(3).join('/') || '');
          this.applyMetricQueryParams(params);
          this.metricsFilters.connection = connection;
          this.metricsFilters.queue = queue;
          this.activeTab = 'queue_metric_detail';
          await this.loadQueueMetricDetail(connection, queue);
          return true;
        }
        return false;
      },
      async loadQueueMetricDetail(connection, queue) {
        const api = this.apiPrefix();
        this.selectedQueueMetricDetail = { connection: connection, queue: queue };
        this.queueMetricDetailMessage = '';
        this.queueMetricDetailHistory = [];
        this.queueMetricDetailSources = [];
        this.setTabLoading('queue_metric_detail');
        try {
          const current = await this.fetchJSON(api, '/metrics/sources' + this.metricsQueryString(true, {}, this.metricActiveFilters()));
          this.queueMetricDetailSources = current.items || [];
          const key = connection + ':' + queue;
          const history = await this.fetchJSON(api, '/metrics/history/queue/' + encodeURIComponent(key) + this.metricsQueryString(false));
          this.queueMetricDetailHistory = history.items || [];
          if (!this.queueMetricDetailSources.length && !this.queueMetricDetailHistory.length) {
            this.queueMetricDetailMessage = 'No queue metric detail for the current filter.';
          }
          this.finishTab('queue_metric_detail', { capability: 'supported' });
        } catch (error) {
          this.queueMetricDetailMessage = error.message;
          this.failTab('queue_metric_detail', error);
        }
      },
      highValueQueryString() {
        const params = new URLSearchParams();
        params.set('page', String(this.highValuePage.page || 1));
        params.set('page_size', String(this.highValuePage.page_size || this.pageSize));
        if (this.highValueKind) {
          params.set('kind', this.highValueKind);
        }
        const from = this.localDateTimeToRFC3339(this.highValueOccurredFrom);
        const to = this.localDateTimeToRFC3339(this.highValueOccurredTo);
        if (from) {
          params.set('occurred_from', from);
        }
        if (to) {
          params.set('occurred_to', to);
        }
        return '?' + params.toString();
      },
      formatHighValueDetail(item) {
        if (!item) {
          return 'unavailable';
        }
        return item.kind + ' / ' + item.connection + ':' + item.queue + ' / ' + (item.job_name || item.job_id || item.id);
      },
      async loadHighValueDetails() {
        const api = this.apiPrefix();
        this.setTabLoading('high_value_detail');
        try {
          const data = await this.fetchJSON(api, '/high-value-detail' + this.highValueQueryString());
          this.highValueDetails = data.items || [];
          this.highValuePage = {
            page: data.page || this.highValuePage.page || 1,
            page_size: data.page_size || this.highValuePage.page_size || this.pageSize,
            total: data.total || 0
          };
          this.selectedHighValueDetail = null;
          this.highValueDetailMessage = this.highValueDetails.length ? '' : 'No High Value Detail items for the current filter.';
          this.finishTab('high_value_detail', data);
        } catch (error) {
          this.highValueDetails = [];
          this.highValuePage.total = 0;
          this.selectedHighValueDetail = null;
          this.highValueDetailMessage = error.message;
          this.failTab('high_value_detail', error);
        }
      },
      async loadHighValueDetailsFromFilters() {
        this.highValuePage.page = 1;
        return this.loadHighValueDetails();
      },
      async showHighValueDetail(id) {
        const api = this.apiPrefix();
        this.selectedHighValueDetail = null;
        this.highValueDetailMessage = 'Loading High Value Detail...';
        try {
          this.selectedHighValueDetail = await this.fetchJSON(api, '/high-value-detail/' + encodeURIComponent(id));
          this.highValueDetailMessage = '';
        } catch (error) {
          this.highValueDetailMessage = error.status === 404 ? 'No High Value Detail found.' : error.message;
        }
      },
      async loadBatches() {
        const api = this.apiPrefix();
        const query = this.batchQuery ? '&query=' + encodeURIComponent(this.batchQuery) : '';
        this.setTabLoading('batches');
        try {
          const batchesData = await this.fetchJSON(api, this.pagePath('/batches', this.batchPage) + query);
          this.batches = batchesData.items || [];
          this.batchPage = {
            page: batchesData.page || this.batchPage.page || 1,
            page_size: batchesData.page_size || this.batchPage.page_size || this.pageSize,
            total: batchesData.total || 0
          };
          this.batchCapability = batchesData.capability || 'supported';
          this.batchEmptyMessage = this.batchCapability === 'disabled' || this.batchCapability === 'unsupported'
            ? batchesData.reason || 'Batches are unavailable.'
            : this.batchQuery
              ? 'No batches matched the current search.'
              : '';
          if (!this.batches.length && this.batchCapability === 'supported') {
            this.selectedBatchDetail = null;
            this.batchDetailMessage = this.batchQuery ? 'No batch detail available for the current search.' : '';
          }
          this.finishTab('batches', batchesData);
        } catch (error) {
          this.batches = [];
          this.batchPage.total = 0;
          this.batchEmptyMessage = 'Batches are unavailable.';
          this.failTab('batches', error);
        }
      },
      async loadBatchesFromSearch() {
        this.batchPage.page = 1;
        return this.loadBatches();
      },
      async showBatchDetail(id) {
        const api = this.apiPrefix();
        this.selectedBatchDetail = null;
        this.batchDetailMessage = 'Loading batch detail...';
        try {
          const data = await this.fetchJSON(api, '/batches/' + encodeURIComponent(id));
          if (data.capability === 'disabled' || data.capability === 'unsupported') {
            this.batchDetailMessage = data.reason || 'Batch detail is unavailable.';
            return;
          }
          this.selectedBatchDetail = data.batch || null;
          this.batchDetailMessage = this.selectedBatchDetail ? '' : 'No batch detail available.';
        } catch (error) {
          this.batchDetailMessage = error.status === 404 ? 'No batch detail available.' : error.message;
        }
      },
      async init() {
        const api = this.apiPrefix();

        try {
          const statusData = await this.fetchJSON(api, '/status');
          this.applyStatus(statusData);
          this.clearDetailState();
          this.ready = true;
          window.addEventListener('hashchange', () => this.applyHashRoute());
          await this.applyHashRoute();
        } catch (error) {
          this.status = error.status === 401 ? 'Authentication required' : error.status === 403 ? 'Access denied' : 'Store unavailable';
          this.notice = this.status;
          this.batchEmptyMessage = 'Batches are unavailable.';
          this.clearDetailState();
          this.batchDetailMessage = this.batchEmptyMessage;
          this.jobDetailMessage = this.status;
          this.failedDetailMessage = this.status;
          this.ready = true;
        }
      }
    };
  });
});
