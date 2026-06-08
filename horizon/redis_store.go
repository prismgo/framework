package horizon

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	encodingpkg "github.com/prismgo/framework/encoding"
	redisfacade "github.com/prismgo/framework/redis"
	"github.com/redis/go-redis/v9"
)

// RedisOptions 保存 Horizon Redis Store 使用的共享连接名。
//
// 设计思路：Horizon Store 只读取 database.redis.{HORIZON_CONNECTION} 对应的连接参数，不读取 queue/cache/session
// 的连接名或 prefix，避免 Horizon 状态被其他子系统配置切换意外影响。
type RedisOptions struct {
	// Connection 是 database.redis 中的连接名称；空值使用 Redis manager 默认连接。
	Connection string
}

// RedisStore 使用独立 key schema 保存 Horizon 运行时状态。
//
// 需求背景：Horizon 需要跨进程可见的 supervisor/worker heartbeat 与控制标记，Redis 是生产推荐 Store。
// Store entity record 从 issue 05 开始通过 horizon.encoding 指定的 Payload Encoding 持久化；Redis
// 原生索引和控制字段仍使用原有 string/hash/set/zset，方便命令和运维工具定位。
type RedisStore struct {
	// client 是当前 Store 独占使用的 Redis client；命令层不得直接创建或访问它。
	client *redis.Client
	// options 保存 Horizon prefix 与 heartbeat TTL。
	options StoreOptions
	// codec 编解码 Redis Store 中保存的 Horizon record bytes。
	//
	// 设计原因：codec 是 RedisStore 的内部实现细节，不暴露 Encoding() 公共方法；调用方只通过
	// Config.Encoding/StoreOptions.Encoding 选择持久化格式。
	codec encodingcontract.Codec
}

// acquireHeartbeatLeaseScript 用单条 Redis Lua 脚本完成租约 SET NX、索引写入和 heartbeat 实体写入。
//
// 设计原因：Redis pipeline 只能减少 RTT，不能避免两个进程同时通过“读取冲突列表”后都写 heartbeat。
// 租约 key 使用 PX 与 heartbeat TTL 同步过期；后续 HeartbeatMaster/HeartbeatSupervisor 会续写租约，
// 因此进程崩溃后无需显式释放即可按 TTL 恢复可启动状态。
var acquireHeartbeatLeaseScript = redis.NewScript(`
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[3]) then
	redis.call("SADD", KEYS[2], ARGV[1])
	redis.call("SET", KEYS[3], ARGV[2], "PX", ARGV[3])
	return 1
end
return 0
`)

// NewRedisStore 创建生产推荐的 Redis Horizon Store。
//
// 参数说明：redisOptions.Connection 来自 database.redis.{HORIZON_CONNECTION}；storeOptions 来自 Horizon Config。
// 返回值不会自动 fallback 到 memory，连接可用性由 DefaultStoreFactory.ResolveStore 通过 Ping 验证。
func NewRedisStore(redisOptions RedisOptions, storeOptions StoreOptions) (*RedisStore, error) {
	connection := strings.TrimSpace(redisOptions.Connection)
	client, err := redisfacade.Client(connection)
	if err != nil {
		return nil, err
	}
	typed, ok := client.(*redis.Client)
	if !ok {
		return nil, fmt.Errorf("horizon: redis connection %q is %T, want *redis.Client", connection, client)
	}
	return NewRedisStoreFromClient(typed, storeOptions), nil
}

// NewRedisStoreFromClient 允许测试复用 miniredis client。
//
// 使用方式：生产路径通常使用 NewRedisStore；测试路径传入已创建的 Redis client 来验证 key schema 和 TTL。
// 参数说明：options.Encoding 为空时使用 msgpack；显式 json 时保持旧 Redis record 的 JSON bytes 形态。
func NewRedisStoreFromClient(client *redis.Client, options StoreOptions) *RedisStore {
	options = normalizeStoreOptions(options)
	return &RedisStore{client: client, options: options, codec: horizonStoreCodec(options.Encoding)}
}

// AcquireMasterLease 原子声明 Redis 中当前 host/environment 的 master 租约。
func (s *RedisStore) AcquireMasterLease(ctx context.Context, state MasterState) (bool, error) {
	id := strings.TrimSpace(state.ID)
	if id == "" {
		return false, fmt.Errorf("horizon: master id is required")
	}
	state.ID = id
	body, err := s.encodeRecord(state)
	if err != nil {
		return false, err
	}
	key := s.masterLeaseKey(state.Host, state.Environment)
	ok, err := acquireHeartbeatLeaseScript.Run(ctx, s.client, []string{key, s.mastersKey(), s.masterKey(id)}, id, body, s.heartbeatTTLMillis()).Int()
	if err != nil {
		return false, err
	}
	return ok == 1, nil
}

// HeartbeatSupervisor 写入 supervisor heartbeat，并把 supervisor 名称加入固定索引 key。
//
// 参数说明：ctx 透传 Redis 操作上下文；state.Name 必填，state.LastHeartbeatAt 由调用方提供，便于测试控制时间。
// HeartbeatMaster 写入 master heartbeat，并把 master ID 加入固定索引 key。
//
// 参数说明：state.ID 必填；实体 key 带 heartbeat TTL，索引 key 用于跨进程列表读取。
func (s *RedisStore) HeartbeatMaster(ctx context.Context, state MasterState) error {
	id := strings.TrimSpace(state.ID)
	if id == "" {
		return fmt.Errorf("horizon: master id is required")
	}
	state.ID = id
	body, err := s.encodeRecord(state)
	if err != nil {
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.SAdd(ctx, s.mastersKey(), id)
	pipe.Set(ctx, s.masterKey(id), body, s.options.HeartbeatTTL)
	pipe.Set(ctx, s.masterLeaseKey(state.Host, state.Environment), id, s.options.HeartbeatTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// AcquireSupervisorLease 原子声明 Redis 中当前 host/environment/name 的 supervisor 租约。
func (s *RedisStore) AcquireSupervisorLease(ctx context.Context, state SupervisorState) (bool, error) {
	name := strings.TrimSpace(state.Name)
	if name == "" {
		return false, fmt.Errorf("horizon: supervisor name is required")
	}
	state.Name = name
	identity := supervisorInstanceID(state)
	body, err := s.encodeRecord(state)
	if err != nil {
		return false, err
	}
	key := s.supervisorLeaseKey(state.Name, state.Host, state.Environment)
	ok, err := acquireHeartbeatLeaseScript.Run(ctx, s.client, []string{key, s.supervisorsKey(), s.supervisorInstanceKey(identity)}, identity, body, s.heartbeatTTLMillis()).Int()
	if err != nil {
		return false, err
	}
	return ok == 1, nil
}

// Master 读取单个 master，并在读取时按 heartbeat TTL 派生 stale 状态。
func (s *RedisStore) Master(ctx context.Context, id string, now time.Time) (MasterState, bool, error) {
	state, ok, err := s.readMaster(ctx, strings.TrimSpace(id))
	if err != nil || !ok {
		return MasterState{}, ok, err
	}
	return masterWithDerivedStatus(state, s.options.HeartbeatTTL, now), true, nil
}

// Masters 按索引读取全部 master，并清理实体 key 已过期的残留索引。
func (s *RedisStore) Masters(ctx context.Context, now time.Time) ([]MasterState, error) {
	ids, err := s.client.SMembers(ctx, s.mastersKey()).Result()
	if err != nil {
		return nil, err
	}
	items := make([]MasterState, 0, len(ids))
	for _, id := range ids {
		state, ok, err := s.readMaster(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			_ = s.client.SRem(ctx, s.mastersKey(), id).Err()
			continue
		}
		items = append(items, masterWithDerivedStatus(state, s.options.HeartbeatTTL, now))
	}
	sortMasterStates(items)
	return items, nil
}

func (s *RedisStore) HeartbeatSupervisor(ctx context.Context, state SupervisorState) error {
	name := strings.TrimSpace(state.Name)
	if name == "" {
		return fmt.Errorf("horizon: supervisor name is required")
	}
	state.Name = name
	identity := supervisorInstanceID(state)
	body, err := s.encodeRecord(state)
	if err != nil {
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.SAdd(ctx, s.supervisorsKey(), identity)
	pipe.Set(ctx, s.supervisorInstanceKey(identity), body, s.options.HeartbeatTTL)
	// 兼容单实体读取接口；列表读取与真实实例归属都以 instance key 为准。
	pipe.Set(ctx, s.supervisorKey(name), body, s.options.HeartbeatTTL)
	// 语义说明：lease value 必须与抢租阶段一致，始终保存实例 identity，
	// 这样排障、所有权判断和后续冲突解释读取到的都是稳定语义。
	pipe.Set(ctx, s.supervisorLeaseKey(state.Name, state.Host, state.Environment), identity, s.options.HeartbeatTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// HeartbeatWorker 写入 worker heartbeat，并把 worker ID 加入固定索引 key。
//
// 逻辑说明：实体 key 带 heartbeat TTL，索引 key 允许残留，读/list/trim 会过滤或清理过期实体。
func (s *RedisStore) HeartbeatWorker(ctx context.Context, state WorkerState) error {
	id := strings.TrimSpace(state.ID)
	if id == "" {
		return fmt.Errorf("horizon: worker id is required")
	}
	state.ID = id
	identity := workerInstanceID(state)
	body, err := s.encodeRecord(state)
	if err != nil {
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.SAdd(ctx, s.workersKey(), identity)
	pipe.Set(ctx, s.workerInstanceKey(identity), body, s.options.HeartbeatTTL)
	// 保留 ID 维度 legacy key 作兼容别名；同 slot 多机实例由 instance key 表达。
	pipe.Set(ctx, s.workerKey(id), body, s.options.HeartbeatTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// Supervisor 读取单个 supervisor，并在读取时派生 stale/pause/terminating 状态。
//
// 参数说明：name 是 supervisor 名称；now 是状态派生时间，测试可传入固定时间。
func (s *RedisStore) Supervisor(ctx context.Context, name string, now time.Time) (SupervisorState, bool, error) {
	name = strings.TrimSpace(name)
	identities, err := s.client.SMembers(ctx, s.supervisorsKey()).Result()
	if err != nil {
		return SupervisorState{}, false, err
	}
	var state SupervisorState
	ok := false
	for _, identity := range identities {
		candidate, found, err := s.readSupervisorInstance(ctx, identity)
		if err != nil {
			return SupervisorState{}, false, err
		}
		if !found {
			_ = s.client.SRem(ctx, s.supervisorsKey(), identity).Err()
			continue
		}
		if candidate.Name != name {
			continue
		}
		if !ok || candidate.LastHeartbeatAt.After(state.LastHeartbeatAt) {
			state = candidate
			ok = true
		}
	}
	if !ok {
		return SupervisorState{}, false, nil
	}
	control, err := s.Control(ctx)
	if err != nil {
		return SupervisorState{}, false, err
	}
	return supervisorWithDerivedStatus(state, control, s.options.HeartbeatTTL, now), true, nil
}

// Supervisors 按索引读取所有 supervisor，忽略并清理实体 key 已过期的残留索引。
//
// 设计原因：Redis entity key 使用 TTL 自动过期，set 索引不会同步过期，因此 list 必须具备过滤能力。
func (s *RedisStore) Supervisors(ctx context.Context, now time.Time) ([]SupervisorState, error) {
	names, err := s.client.SMembers(ctx, s.supervisorsKey()).Result()
	if err != nil {
		return nil, err
	}
	control, err := s.Control(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]SupervisorState, 0, len(names))
	for _, identity := range names {
		state, ok, err := s.readSupervisorInstance(ctx, identity)
		if err != nil {
			return nil, err
		}
		if !ok {
			_ = s.client.SRem(ctx, s.supervisorsKey(), identity).Err()
			continue
		}
		items = append(items, supervisorWithDerivedStatus(state, control, s.options.HeartbeatTTL, now))
	}
	sortSupervisorStates(items)
	return items, nil
}

// Worker 读取单个 worker，并在读取时派生 stale/pause/terminating 状态。
func (s *RedisStore) Worker(ctx context.Context, id string, now time.Time) (WorkerState, bool, error) {
	id = strings.TrimSpace(id)
	identities, err := s.client.SMembers(ctx, s.workersKey()).Result()
	if err != nil {
		return WorkerState{}, false, err
	}
	var state WorkerState
	ok := false
	for _, identity := range identities {
		candidate, found, err := s.readWorkerInstance(ctx, identity)
		if err != nil {
			return WorkerState{}, false, err
		}
		if !found {
			_ = s.client.SRem(ctx, s.workersKey(), identity).Err()
			continue
		}
		if candidate.ID != id {
			continue
		}
		if !ok || candidate.LastHeartbeatAt.After(state.LastHeartbeatAt) {
			state = candidate
			ok = true
		}
	}
	if !ok {
		return WorkerState{}, false, nil
	}
	control, err := s.Control(ctx)
	if err != nil {
		return WorkerState{}, false, err
	}
	return workerWithDerivedStatus(state, control, s.options.HeartbeatTTL, now), true, nil
}

// Workers 按索引读取所有 worker，忽略并清理实体 key 已过期的残留索引。
func (s *RedisStore) Workers(ctx context.Context, now time.Time) ([]WorkerState, error) {
	ids, err := s.client.SMembers(ctx, s.workersKey()).Result()
	if err != nil {
		return nil, err
	}
	control, err := s.Control(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]WorkerState, 0, len(ids))
	for _, identity := range ids {
		state, ok, err := s.readWorkerInstance(ctx, identity)
		if err != nil {
			return nil, err
		}
		if !ok {
			_ = s.client.SRem(ctx, s.workersKey(), identity).Err()
			continue
		}
		items = append(items, workerWithDerivedStatus(state, control, s.options.HeartbeatTTL, now))
	}
	sortWorkerStates(items)
	return items, nil
}

// Control 读取 Horizon 控制标记。
//
// 设计思路：global_paused 与 terminate_requested_at 放在 control hash，paused supervisor 名称放在独立 set，
// 对应 issue 02 固定 key schema。
func (s *RedisStore) Control(ctx context.Context) (ControlState, error) {
	values, err := s.client.HGetAll(ctx, s.controlKey()).Result()
	if err != nil {
		return ControlState{}, err
	}
	paused, err := s.client.SMembers(ctx, s.pausedSupervisorsKey()).Result()
	if err != nil {
		return ControlState{}, err
	}
	control := ControlState{PausedSupervisors: make(map[string]bool, len(paused))}
	control.GlobalPaused = values["global_paused"] == "1"
	control.TerminateShouldWait = values["terminate_should_wait"] == "1"
	if raw := values["terminate_requested_at"]; raw != "" {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, raw); parseErr == nil {
			control.TerminateRequestedAt = parsed
		}
	}
	for _, name := range paused {
		control.PausedSupervisors[name] = true
	}
	return control, nil
}

// SetGlobalPaused 写入或清除全局暂停标记。
func (s *RedisStore) SetGlobalPaused(ctx context.Context, paused bool) error {
	value := "0"
	if paused {
		value = "1"
	}
	return s.client.HSet(ctx, s.controlKey(), "global_paused", value).Err()
}

// SetSupervisorPaused 写入或清除指定 supervisor 的暂停标记。
//
// 参数说明：name 是 supervisor 名称，空值视为调用方错误并返回显式错误。
func (s *RedisStore) SetSupervisorPaused(ctx context.Context, name string, paused bool) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("horizon: supervisor name is required")
	}
	if paused {
		return s.client.SAdd(ctx, s.pausedSupervisorsKey(), name).Err()
	}
	return s.client.SRem(ctx, s.pausedSupervisorsKey(), name).Err()
}

// RequestTerminate 写入一次性 terminate 请求时间。
//
// 逻辑说明：horizon:continue 不会清除该字段，后续 supervisor 启动流程可显式调用 ClearTerminateRequest。
func (s *RedisStore) RequestTerminate(ctx context.Context, at time.Time, wait bool) error {
	if at.IsZero() {
		at = time.Now()
	}
	waitValue := "0"
	if wait {
		waitValue = "1"
	}
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, s.controlKey(), "terminate_requested_at", at.Format(time.RFC3339Nano))
	pipe.HSet(ctx, s.controlKey(), "terminate_should_wait", waitValue)
	_, err := pipe.Exec(ctx)
	return err
}

// ClearTerminateRequest 清除 terminate 请求，供后续 supervisor 启动流程使用。
func (s *RedisStore) ClearTerminateRequest(ctx context.Context) error {
	return s.client.HDel(ctx, s.controlKey(), "terminate_requested_at", "terminate_should_wait").Err()
}

// Trim 清理已经 stale 的 supervisor/worker 实体和索引。
func (s *RedisStore) Trim(ctx context.Context, now time.Time) error {
	masters, err := s.Masters(ctx, now)
	if err != nil {
		return err
	}
	for _, master := range masters {
		if master.Status == MasterStale {
			if err := s.client.SRem(ctx, s.mastersKey(), master.ID).Err(); err != nil {
				return err
			}
			if err := s.client.Del(ctx, s.masterKey(master.ID), s.masterLeaseKey(master.Host, master.Environment)).Err(); err != nil {
				return err
			}
		}
	}
	supervisors, err := s.Supervisors(ctx, now)
	if err != nil {
		return err
	}
	for _, supervisor := range supervisors {
		if supervisor.Status == SupervisorStale {
			identity := supervisorInstanceID(supervisor)
			if err := s.client.SRem(ctx, s.supervisorsKey(), identity, supervisor.Name).Err(); err != nil {
				return err
			}
			if err := s.client.Del(ctx, s.supervisorInstanceKey(identity), s.supervisorKey(supervisor.Name), s.supervisorLeaseKey(supervisor.Name, supervisor.Host, supervisor.Environment)).Err(); err != nil {
				return err
			}
		}
	}
	workers, err := s.Workers(ctx, now)
	if err != nil {
		return err
	}
	for _, worker := range workers {
		if worker.Status == WorkerStale {
			identity := workerInstanceID(worker)
			if err := s.client.SRem(ctx, s.workersKey(), identity, worker.ID).Err(); err != nil {
				return err
			}
			if err := s.client.Del(ctx, s.workerInstanceKey(identity), s.workerKey(worker.ID)).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// StatusSnapshot 读取控制标记、supervisor 和 worker 状态，并派生全局 status。
func (s *RedisStore) StatusSnapshot(ctx context.Context, now time.Time) (StatusSnapshot, error) {
	control, err := s.Control(ctx)
	if err != nil {
		return StatusSnapshot{}, err
	}
	supervisors, err := s.Supervisors(ctx, now)
	if err != nil {
		return StatusSnapshot{}, err
	}
	workers, err := s.Workers(ctx, now)
	if err != nil {
		return StatusSnapshot{}, err
	}
	return deriveStatusSnapshot(control, supervisors, workers), nil
}

// ClearMetrics 清理事件派生 metrics，不影响 heartbeat、control flags 或 queue 数据。
func (s *RedisStore) ClearMetrics(ctx context.Context) error {
	return s.client.Del(ctx, s.eventMetricWindowsKey(), s.eventMetricRollupWindowsKey()).Err()
}

// AppendEventMetricWindows 追加 event_metrics window 数据，并按 retention 清理旧 window。
//
// 设计思路：每个 window 保存为独立 JSON，索引用 sorted set 按事件窗口时间排序；
// 这允许多个进程和多个 flush 批次追加同一时间段的数据，而不是互相覆盖全局 snapshot。
func (s *RedisStore) AppendEventMetricWindows(ctx context.Context, windows []EventMetricWindow, retention time.Duration) error {
	if len(windows) == 0 {
		return nil
	}
	now := time.Now().UTC()
	// 以数据窗口中最晚的 WindowEnd 作为 retention 截断参考，避免测试或延迟
	// 场景下过去的事件窗口被 wall clock 错误修剪。
	trimRef := time.Time{}
	for _, window := range windows {
		if !window.WindowEnd.IsZero() && window.WindowEnd.After(trimRef) {
			trimRef = window.WindowEnd
		}
	}
	if trimRef.IsZero() {
		trimRef = now
	}
	pipe := s.client.TxPipeline()
	for index, window := range windows {
		if window.FlushAt.IsZero() {
			window.FlushAt = now
		}
		id := eventMetricWindowID(window, index)
		body, err := s.encodeRecord(window)
		if err != nil {
			return err
		}
		ttl := time.Duration(0)
		if retention > 0 {
			ttl = retention
		}
		scoreAt := window.WindowStart
		if scoreAt.IsZero() {
			scoreAt = window.FlushAt
		}
		// score 使用事件窗口时间；flushAt 只作为诊断字段，不能改变窗口归属。
		pipe.Set(ctx, s.eventMetricWindowKey(id), body, ttl)
		pipe.ZAdd(ctx, s.eventMetricWindowsKey(), redis.Z{Score: float64(scoreAt.UnixNano()), Member: id})
	}
	for index, window := range queueEventMetricRollups(windows, now) {
		id := eventMetricWindowID(window, index)
		body, err := s.encodeRecord(window)
		if err != nil {
			return err
		}
		ttl := time.Duration(0)
		if retention > 0 {
			ttl = retention
		}
		scoreAt := window.WindowStart
		if scoreAt.IsZero() {
			scoreAt = window.FlushAt
		}
		pipe.Set(ctx, s.eventMetricRollupWindowKey(id), body, ttl)
		pipe.ZAdd(ctx, s.eventMetricRollupWindowsKey(), redis.Z{Score: float64(scoreAt.UnixNano()), Member: id})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	if retention > 0 {
		cutoff := trimRef.Add(-retention)
		if err := s.trimEventMetricWindows(ctx, cutoff); err != nil {
			return err
		}
		return s.trimEventMetricRollupWindows(ctx, cutoff)
	}
	return nil
}

// EventMetricWindows 返回按事件 window 开始时间倒序排列的追加 event_metrics windows。
//
// 查询边界：Redis 按 sorted set 分批扫描保留窗口，再在内存中做来源维度精确过滤和 read-repair。
// Total 表示完整保留集合内的匹配数，避免过滤结果落在当前页候选之后时被漏报。
func (s *RedisStore) EventMetricWindows(ctx context.Context, query EventMetricWindowQuery) (PageEnvelope[EventMetricWindow], error) {
	query = normalizeEventMetricWindowQuery(query)
	items := make([]EventMetricWindow, 0, query.Page.PageSize)
	offset := int64(0)
	for {
		// 来源/queue 过滤无法用当前 sorted set score 表达，必须扫完整保留集合才能给出真实 Total。
		ids, err := s.eventMetricWindowCandidateIDs(ctx, query, offset, maxPageSize)
		if err != nil {
			return PageEnvelope[EventMetricWindow]{}, err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			body, err := s.client.Get(ctx, s.eventMetricWindowKey(id)).Bytes()
			if errors.Is(err, redis.Nil) {
				_ = s.client.ZRem(ctx, s.eventMetricWindowsKey(), id).Err()
				continue
			}
			if err != nil {
				return PageEnvelope[EventMetricWindow]{}, err
			}
			var window EventMetricWindow
			if err := s.decodeRecord(body, &window); err != nil {
				_ = s.client.ZRem(ctx, s.eventMetricWindowsKey(), id).Err()
				_ = s.client.Del(ctx, s.eventMetricWindowKey(id)).Err()
				continue
			}
			if eventMetricWindowMatchesQuery(window, query) {
				items = append(items, window)
			}
		}
		if len(ids) < maxPageSize {
			break
		}
		offset += int64(len(ids))
	}
	sortEventMetricWindows(items)
	return PageEnvelope[EventMetricWindow]{
		Items:    cloneEventMetricWindows(pageSlice(items, query.Page)),
		Total:    len(items),
		Page:     query.Page.Page,
		PageSize: query.Page.PageSize,
	}, nil
}

// EventMetricRollupWindows 返回 queue 级聚合窗口，不读取 raw source windows。
//
// 读取策略：rollup 使用独立 zset 和实体 key，按窗口开始时间裁剪候选，再按 query 精确过滤
// connection/queue/时间范围。这样 summary 查询只遍历 rollup 集合；raw source windows 的数量
// 不会影响 `/metrics/current` 默认 summary 和 `/queues` 的首屏聚合成本。
func (s *RedisStore) EventMetricRollupWindows(ctx context.Context, query EventMetricWindowQuery) ([]EventMetricWindow, error) {
	query = normalizeEventMetricWindowQuery(query)
	items := make([]EventMetricWindow, 0)
	offset := int64(0)
	for {
		ids, err := s.eventMetricRollupCandidateIDs(ctx, query, offset, maxPageSize)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			body, err := s.client.Get(ctx, s.eventMetricRollupWindowKey(id)).Bytes()
			if errors.Is(err, redis.Nil) {
				_ = s.client.ZRem(ctx, s.eventMetricRollupWindowsKey(), id).Err()
				continue
			}
			if err != nil {
				return nil, err
			}
			var window EventMetricWindow
			if err := s.decodeRecord(body, &window); err != nil {
				_ = s.client.ZRem(ctx, s.eventMetricRollupWindowsKey(), id).Err()
				_ = s.client.Del(ctx, s.eventMetricRollupWindowKey(id)).Err()
				continue
			}
			if eventMetricWindowMatchesQuery(window, query) {
				items = append(items, window)
			}
		}
		if len(ids) < maxPageSize {
			break
		}
		offset += int64(len(ids))
	}
	sortEventMetricWindows(items)
	return cloneEventMetricWindows(items), nil
}

func (s *RedisStore) eventMetricRollupCandidateIDs(ctx context.Context, query EventMetricWindowQuery, offset int64, count int64) ([]string, error) {
	if !query.To.IsZero() {
		return s.client.ZRevRangeByScore(ctx, s.eventMetricRollupWindowsKey(), &redis.ZRangeBy{
			Min:    "-inf",
			Max:    strconv.FormatInt(query.To.Add(-time.Nanosecond).UnixNano(), 10),
			Offset: offset,
			Count:  count,
		}).Result()
	}
	return s.client.ZRevRange(ctx, s.eventMetricRollupWindowsKey(), offset, offset+count-1).Result()
}

// eventMetricWindowCandidateIDs 按事件窗口开始时间裁剪 Redis 候选集合。
//
// 设计原因：Redis event_metrics 当前以 WindowStart 作为 sorted set score；当查询给出 To 时，
// 可以先用 score < To 裁掉不可能重叠的未来窗口，再读取 JSON 做 WindowEnd > From 和来源过滤。
// 设计边界：来源维度仍在读取实体后精确匹配，避免为每个维度建立多套索引；候选数量按请求页窗口
// 有界扩大到 maxPageSize，防止无过滤场景扫描全历史。
func (s *RedisStore) eventMetricWindowCandidateIDs(ctx context.Context, query EventMetricWindowQuery, offset int64, count int64) ([]string, error) {
	if !query.To.IsZero() {
		return s.client.ZRevRangeByScore(ctx, s.eventMetricWindowsKey(), &redis.ZRangeBy{
			Max:    "(" + strconv.FormatInt(query.To.UnixNano(), 10),
			Min:    "-inf",
			Offset: offset,
			Count:  count,
		}).Result()
	}
	return s.client.ZRevRange(ctx, s.eventMetricWindowsKey(), offset, offset+count-1).Result()
}

// SaveQueueLengthSnapshot 保存最近一次队列长度采样结果。
//
// 逻辑说明：queue length 是 issue 04 的独立模型，固定写入 {prefix}:metrics:queue_lengths，
// 不复用事件派生 metrics snapshot/counters，便于后续 purge 策略分别演进。
func (s *RedisStore) SaveQueueLengthSnapshot(ctx context.Context, snapshot QueueLengthSnapshot) error {
	body, err := s.encodeRecord(cloneQueueLengthSnapshot(snapshot))
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.queueLengthSnapshotKey(), body, 0).Err()
}

// QueueLengthSnapshot 读取最近一次队列长度采样结果；key 不存在时返回零值快照。
func (s *RedisStore) QueueLengthSnapshot(ctx context.Context) (QueueLengthSnapshot, error) {
	body, err := s.client.Get(ctx, s.queueLengthSnapshotKey()).Bytes()
	if errors.Is(err, redis.Nil) {
		return QueueLengthSnapshot{}, nil
	}
	if err != nil {
		return QueueLengthSnapshot{}, err
	}
	var snapshot QueueLengthSnapshot
	if err := s.decodeRecord(body, &snapshot); err != nil {
		return QueueLengthSnapshot{}, err
	}
	return cloneQueueLengthSnapshot(snapshot), nil
}

// SaveBatchSummary 保存 BatchEvent 派生出的只读批次摘要，并维护按创建时间排序的索引。
func (s *RedisStore) SaveBatchSummary(ctx context.Context, summary BatchSummary) error {
	id := strings.TrimSpace(summary.ID)
	if id == "" {
		return fmt.Errorf("horizon: batch id is required")
	}
	summary.ID = id
	if summary.UpdatedAt.IsZero() {
		summary.UpdatedAt = time.Now().UTC()
	}
	body, err := s.encodeRecord(summary)
	if err != nil {
		return err
	}
	scoreAt := summary.CreatedAt
	if scoreAt.IsZero() {
		scoreAt = summary.UpdatedAt
	}
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, s.batchKey(id), body, 0)
	pipe.ZAdd(ctx, s.batchesKey(), redis.Z{Score: float64(scoreAt.UnixNano()), Member: id})
	_, err = pipe.Exec(ctx)
	return err
}

// SaveBatchSummaries 批量保存 BatchEvent 摘要，并按 batch_summary_retention 裁剪旧数据。
func (s *RedisStore) SaveBatchSummaries(ctx context.Context, items []BatchSummary, retention time.Duration) error {
	for _, item := range items {
		if err := s.SaveBatchSummary(ctx, item); err != nil {
			return err
		}
	}
	if retention <= 0 {
		return nil
	}
	// 以新数据中最晚的 retention time 作为截断参考，避免 wall clock 误修剪过去数据。
	trimRef := time.Now().UTC()
	for _, item := range items {
		if rt := batchSummaryRetentionTime(item); rt.After(trimRef) {
			trimRef = rt
		}
	}
	return s.trimBatchSummaries(ctx, trimRef.Add(-retention))
}

// Batches 按 ID/name 搜索 Redis 中的批次安全摘要；损坏 JSON 会被跳过以保护 Dashboard。
func (s *RedisStore) Batches(ctx context.Context, query string) ([]BatchSummary, error) {
	ids, err := s.client.ZRevRange(ctx, s.batchesKey(), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	query = strings.ToLower(strings.TrimSpace(query))
	out := make([]BatchSummary, 0, len(ids))
	for _, id := range ids {
		summary, ok, err := s.Batch(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			_ = s.client.ZRem(ctx, s.batchesKey(), id).Err()
			continue
		}
		if batchMatchesQuery(summary, query) {
			out = append(out, summary)
		}
	}
	sortBatchSummaries(out)
	return cloneBatchSummaries(out), nil
}

// BatchesPage 按有界 Redis sorted-set 窗口读取批次摘要。
//
// 逻辑说明：空查询只读取当前分页窗口；query/search 也只承诺在该有界窗口内按 ID/name 匹配，
// 不扫描全部历史批次，避免 Dashboard 搜索触发无边界 Redis 读取。
func (s *RedisStore) BatchesPage(ctx context.Context, query string, page PageRequest) (PageEnvelope[BatchSummary], error) {
	start := int64(pageStart(page))
	end := start + int64(page.PageSize) - 1
	ids, err := s.client.ZRevRange(ctx, s.batchesKey(), start, end).Result()
	if err != nil {
		return PageEnvelope[BatchSummary]{}, err
	}
	total64, err := s.client.ZCard(ctx, s.batchesKey()).Result()
	if err != nil {
		return PageEnvelope[BatchSummary]{}, err
	}
	query = strings.ToLower(strings.TrimSpace(query))
	items := make([]BatchSummary, 0, len(ids))
	for _, id := range ids {
		summary, ok, err := s.Batch(ctx, id)
		if err != nil {
			return PageEnvelope[BatchSummary]{}, err
		}
		if !ok {
			_ = s.client.ZRem(ctx, s.batchesKey(), id).Err()
			continue
		}
		if batchMatchesQuery(summary, query) {
			items = append(items, summary)
		}
	}
	total := int(total64)
	if query != "" {
		total = len(items)
	}
	return PageEnvelope[BatchSummary]{
		Items:    cloneBatchSummaries(items),
		Total:    total,
		Page:     page.Page,
		PageSize: page.PageSize,
	}, nil
}

// Batch 读取单个批次安全摘要；不存在或单条 JSON 损坏时返回 ok=false。
func (s *RedisStore) Batch(ctx context.Context, id string) (BatchSummary, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return BatchSummary{}, false, nil
	}
	body, err := s.client.Get(ctx, s.batchKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return BatchSummary{}, false, nil
	}
	if err != nil {
		return BatchSummary{}, false, err
	}
	var summary BatchSummary
	if err := s.decodeRecord(body, &summary); err != nil {
		return BatchSummary{}, false, nil
	}
	return summary, true, nil
}

// SaveHighValueDetails 保存 failed/poison/slow job 的可丢弃安全诊断摘要。
func (s *RedisStore) SaveHighValueDetails(ctx context.Context, details []HighValueJobDetail, retention time.Duration) error {
	if len(details) == 0 {
		return nil
	}
	now := time.Now().UTC()
	pipe := s.client.TxPipeline()
	for _, detail := range details {
		id := strings.TrimSpace(detail.ID)
		if id == "" {
			continue
		}
		detail.ID = id
		if detail.OccurredAt.IsZero() {
			detail.OccurredAt = now
		}
		body, err := s.encodeRecord(detail)
		if err != nil {
			return err
		}
		ttl := time.Duration(0)
		if retention > 0 {
			ttl = retention
		}
		pipe.Set(ctx, s.highValueDetailKey(id), body, ttl)
		pipe.ZAdd(ctx, s.highValueDetailsKey(), redis.Z{Score: float64(detail.OccurredAt.UnixNano()), Member: id})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	if retention > 0 {
		return s.trimHighValueDetails(ctx, now.Add(-retention))
	}
	return nil
}

func (s *RedisStore) HighValueDetails(ctx context.Context, query HighValueDetailQuery) (PageEnvelope[HighValueJobDetail], error) {
	query = normalizeHighValueDetailQuery(query)
	items := make([]HighValueJobDetail, 0, query.Page.PageSize)
	offset := int64(0)
	for {
		// kind/time 过滤需要基于完整诊断保留集合，否则较旧的匹配明细会被第一页候选遮住。
		ids, err := s.highValueDetailCandidateIDs(ctx, query, offset, maxPageSize)
		if err != nil {
			return PageEnvelope[HighValueJobDetail]{}, err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			detail, ok, err := s.HighValueDetail(ctx, id)
			if err != nil {
				return PageEnvelope[HighValueJobDetail]{}, err
			}
			if !ok {
				_ = s.client.ZRem(ctx, s.highValueDetailsKey(), id).Err()
				continue
			}
			if highValueDetailMatchesQuery(detail, query) {
				items = append(items, detail)
			}
		}
		if len(ids) < maxPageSize {
			break
		}
		offset += int64(len(ids))
	}
	sortHighValueJobDetails(items)
	return PageEnvelope[HighValueJobDetail]{
		Items:    cloneHighValueJobDetails(pageSlice(items, query.Page)),
		Total:    len(items),
		Page:     query.Page.Page,
		PageSize: query.Page.PageSize,
	}, nil
}

func (s *RedisStore) highValueDetailCandidateIDs(ctx context.Context, query HighValueDetailQuery, offset int64, count int64) ([]string, error) {
	if !query.OccurredTo.IsZero() {
		return s.client.ZRevRangeByScore(ctx, s.highValueDetailsKey(), &redis.ZRangeBy{
			Max:    "(" + strconv.FormatInt(query.OccurredTo.UnixNano(), 10),
			Min:    "-inf",
			Offset: offset,
			Count:  count,
		}).Result()
	}
	return s.client.ZRevRange(ctx, s.highValueDetailsKey(), offset, offset+count-1).Result()
}

func (s *RedisStore) HighValueDetail(ctx context.Context, id string) (HighValueJobDetail, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return HighValueJobDetail{}, false, nil
	}
	body, err := s.client.Get(ctx, s.highValueDetailKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return HighValueJobDetail{}, false, nil
	}
	if err != nil {
		return HighValueJobDetail{}, false, err
	}
	var detail HighValueJobDetail
	if err := s.decodeRecord(body, &detail); err != nil {
		return HighValueJobDetail{}, false, nil
	}
	return detail, true, nil
}

func (s *RedisStore) SaveObservabilityDiagnostics(ctx context.Context, diagnostics []ObservabilityDiagnostic, retention time.Duration) error {
	if len(diagnostics) == 0 {
		return nil
	}
	now := time.Now().UTC()
	pipe := s.client.TxPipeline()
	for index, diagnostic := range diagnostics {
		if diagnostic.Count <= 0 {
			continue
		}
		if diagnostic.ObservedAt.IsZero() {
			diagnostic.ObservedAt = now
		}
		id := observabilityDiagnosticID(diagnostic, index)
		body, err := s.encodeRecord(diagnostic)
		if err != nil {
			return err
		}
		ttl := time.Duration(0)
		if retention > 0 {
			ttl = retention
		}
		pipe.Set(ctx, s.observabilityDiagnosticKey(id), body, ttl)
		pipe.ZAdd(ctx, s.observabilityDiagnosticsKey(), redis.Z{Score: float64(diagnostic.ObservedAt.UnixNano()), Member: id})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	if retention > 0 {
		return s.trimObservabilityDiagnostics(ctx, now.Add(-retention))
	}
	return nil
}

func (s *RedisStore) ObservabilityDiagnostics(ctx context.Context, page PageRequest) (PageEnvelope[ObservabilityDiagnostic], error) {
	ids, err := s.client.ZRevRange(ctx, s.observabilityDiagnosticsKey(), int64(pageStart(page)), int64(pageStart(page)+page.PageSize-1)).Result()
	if err != nil {
		return PageEnvelope[ObservabilityDiagnostic]{}, err
	}
	total64, err := s.client.ZCard(ctx, s.observabilityDiagnosticsKey()).Result()
	if err != nil {
		return PageEnvelope[ObservabilityDiagnostic]{}, err
	}
	items := make([]ObservabilityDiagnostic, 0, len(ids))
	for _, id := range ids {
		body, err := s.client.Get(ctx, s.observabilityDiagnosticKey(id)).Bytes()
		if errors.Is(err, redis.Nil) {
			_ = s.client.ZRem(ctx, s.observabilityDiagnosticsKey(), id).Err()
			continue
		}
		if err != nil {
			return PageEnvelope[ObservabilityDiagnostic]{}, err
		}
		var diagnostic ObservabilityDiagnostic
		if err := s.decodeRecord(body, &diagnostic); err != nil {
			_ = s.client.ZRem(ctx, s.observabilityDiagnosticsKey(), id).Err()
			_ = s.client.Del(ctx, s.observabilityDiagnosticKey(id)).Err()
			continue
		}
		items = append(items, diagnostic)
	}
	return PageEnvelope[ObservabilityDiagnostic]{
		Items:    cloneObservabilityDiagnostics(items),
		Total:    int(total64),
		Page:     page.Page,
		PageSize: page.PageSize,
	}, nil
}

func (s *RedisStore) trimHighValueDetails(ctx context.Context, cutoff time.Time) error {
	ids, err := s.client.ZRangeByScore(ctx, s.highValueDetailsKey(), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(cutoff.UnixNano(), 10)}).Result()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.highValueDetailsKey(), stringSliceToAny(ids)...)
	for _, id := range ids {
		pipe.Del(ctx, s.highValueDetailKey(id))
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) trimObservabilityDiagnostics(ctx context.Context, cutoff time.Time) error {
	ids, err := s.client.ZRangeByScore(ctx, s.observabilityDiagnosticsKey(), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(cutoff.UnixNano(), 10)}).Result()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.observabilityDiagnosticsKey(), stringSliceToAny(ids)...)
	for _, id := range ids {
		pipe.Del(ctx, s.observabilityDiagnosticKey(id))
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) trimEventMetricWindows(ctx context.Context, cutoff time.Time) error {
	ids, err := s.client.ZRangeByScore(ctx, s.eventMetricWindowsKey(), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(cutoff.UnixNano(), 10)}).Result()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.eventMetricWindowsKey(), stringSliceToAny(ids)...)
	for _, id := range ids {
		pipe.Del(ctx, s.eventMetricWindowKey(id))
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) trimEventMetricRollupWindows(ctx context.Context, cutoff time.Time) error {
	ids, err := s.client.ZRangeByScore(ctx, s.eventMetricRollupWindowsKey(), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(cutoff.UnixNano(), 10)}).Result()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.eventMetricRollupWindowsKey(), stringSliceToAny(ids)...)
	for _, id := range ids {
		pipe.Del(ctx, s.eventMetricRollupWindowKey(id))
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) trimBatchSummaries(ctx context.Context, cutoff time.Time) error {
	ids, err := s.client.ZRangeByScore(ctx, s.batchesKey(), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(cutoff.UnixNano(), 10)}).Result()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.batchesKey(), stringSliceToAny(ids)...)
	for _, id := range ids {
		pipe.Del(ctx, s.batchKey(id))
	}
	_, err = pipe.Exec(ctx)
	return err
}

// RecordOrphanProcess 记录 master 下 orphan worker PID 的首次发现时间。
//
// 逻辑说明：Redis 使用 sorted set 保存 pid -> first_seen_unix_nano，便于按 age 查询需要二次终止的 orphan。
func (s *RedisStore) RecordOrphanProcess(ctx context.Context, masterID string, pid int, firstSeenAt time.Time) error {
	masterID = strings.TrimSpace(masterID)
	if masterID == "" {
		return fmt.Errorf("horizon: master id is required")
	}
	if pid <= 0 {
		return fmt.Errorf("horizon: orphan pid must be positive")
	}
	if firstSeenAt.IsZero() {
		firstSeenAt = time.Now().UTC()
	}
	return s.client.ZAddArgs(ctx, s.orphanProcessesKey(masterID), redis.ZAddArgs{
		NX: true,
		Members: []redis.Z{{
			Score:  float64(firstSeenAt.UnixNano()),
			Member: strconv.Itoa(pid),
		}},
	}).Err()
}

// OrphanProcesses 列出指定 master 下的 orphan process tracking 记录。
func (s *RedisStore) OrphanProcesses(ctx context.Context, masterID string) ([]OrphanProcess, error) {
	return s.readOrphanProcesses(ctx, masterID, &redis.ZRangeBy{})
}

// OrphanProcessesOlderThan 按 first_seen age 查询 orphan process tracking 记录。
func (s *RedisStore) OrphanProcessesOlderThan(ctx context.Context, masterID string, age time.Duration, now time.Time) ([]OrphanProcess, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := now.Add(-age).UnixNano()
	return s.readOrphanProcesses(ctx, masterID, &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(cutoff, 10)})
}

// ForgetOrphanProcess 删除指定 master 下的 orphan process tracking 记录。
func (s *RedisStore) ForgetOrphanProcess(ctx context.Context, masterID string, pid int) error {
	return s.client.ZRem(ctx, s.orphanProcessesKey(masterID), strconv.Itoa(pid)).Err()
}

func (s *RedisStore) readOrphanProcesses(ctx context.Context, masterID string, by *redis.ZRangeBy) ([]OrphanProcess, error) {
	masterID = strings.TrimSpace(masterID)
	if masterID == "" {
		return nil, nil
	}
	var values []redis.Z
	var err error
	if by == nil || (by.Min == "" && by.Max == "") {
		values, err = s.client.ZRangeWithScores(ctx, s.orphanProcessesKey(masterID), 0, -1).Result()
	} else {
		values, err = s.client.ZRangeByScoreWithScores(ctx, s.orphanProcessesKey(masterID), by).Result()
	}
	if err != nil {
		return nil, err
	}
	out := make([]OrphanProcess, 0, len(values))
	for _, value := range values {
		pid, err := strconv.Atoi(fmt.Sprint(value.Member))
		if err != nil || pid <= 0 {
			continue
		}
		out = append(out, OrphanProcess{
			MasterID:    masterID,
			PID:         pid,
			FirstSeenAt: time.Unix(0, int64(value.Score)).UTC(),
		})
	}
	return out, nil
}

// encodeRecord 使用当前 Horizon Payload Encoding 编码 Redis entity record。
//
// 需求背景：RedisStore 中 master/supervisor/worker heartbeat、metrics window、queue length、
// batch summary、high-value detail 和 diagnostics 都属于 Horizon Store records，应统一通过同一个
// horizon.encoding 编码；control hash、索引和 orphan pid sorted set 不经过该 helper。
//
// 参数说明：value 是即将写入 Redis entity key 的稳定 record 结构体。
func (s *RedisStore) encodeRecord(value any) ([]byte, error) {
	return s.codecOrDefault().Marshal(value)
}

// decodeRecord 使用当前 Horizon Payload Encoding 解码 Redis entity record。
//
// 逻辑说明：默认 msgpack 不做 JSON fallback，旧 JSON record 会按普通解码错误处理；列表读取路径会按
// 既有 read-repair 规则跳过或清理损坏的单条观测 record，单实体读取则把错误返回给调用方。
//
// 参数说明：body 是 Redis entity key 中保存的原始 bytes；value 必须是可写目标指针。
func (s *RedisStore) decodeRecord(body []byte, value any) error {
	return s.codecOrDefault().Unmarshal(body, value)
}

func (s *RedisStore) heartbeatTTLMillis() int64 {
	ms := int64(s.options.HeartbeatTTL / time.Millisecond)
	if ms < 1 {
		// Redis PX 不能接受 0；极小 TTL 仍要形成有效租约而不是让启动路径报协议错误。
		return 1
	}
	return ms
}

// codecOrDefault 返回 RedisStore 当前使用的 Payload Encoding codec。
//
// 设计思路：正常构造路径一定会设置 codec；保留 nil fallback 只服务于零值 RedisStore 或测试替身，
// 不作为配置错误的静默降级入口。严格配置错误已在 LoadConfigFrom/NewManager 中暴露。
func (s *RedisStore) codecOrDefault() encodingcontract.Codec {
	if s != nil && s.codec != nil {
		return s.codec
	}
	return encodingpkg.Msgpack()
}

// horizonStoreCodec 将 StoreOptions.Encoding 解析为 RedisStore 内部 codec。
//
// 逻辑说明：直接构造 RedisStore 的低层 API 不返回 error，为保持既有签名只在这里兜底 msgpack；
// 应用严格装配路径必须先经过 LoadConfigFrom 或 NewManager，因此非法 horizon.encoding 会在启动
// 边界返回配置错误，而不是依赖此低层 helper。
//
// 参数说明：name 来自 Config.Encoding 或测试显式传入的 StoreOptions.Encoding。
func horizonStoreCodec(name string) encodingcontract.Codec {
	codec, err := encodingpkg.ResolveWithDefault(encodingpkg.NameMsgpack, name)
	if err != nil {
		return encodingpkg.Msgpack()
	}
	return codec
}

// readMaster 从实体 key 读取原始 master 状态。
func (s *RedisStore) readMaster(ctx context.Context, id string) (MasterState, bool, error) {
	if id == "" {
		return MasterState{}, false, nil
	}
	body, err := s.client.Get(ctx, s.masterKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return MasterState{}, false, nil
	}
	if err != nil {
		return MasterState{}, false, err
	}
	var state MasterState
	if err := s.decodeRecord(body, &state); err != nil {
		return MasterState{}, false, err
	}
	return state, true, nil
}

func (s *RedisStore) readSupervisorInstance(ctx context.Context, identity string) (SupervisorState, bool, error) {
	if identity == "" {
		return SupervisorState{}, false, nil
	}
	body, err := s.client.Get(ctx, s.supervisorInstanceKey(identity)).Bytes()
	if errors.Is(err, redis.Nil) {
		return SupervisorState{}, false, nil
	}
	if err != nil {
		return SupervisorState{}, false, err
	}
	var state SupervisorState
	if err := s.decodeRecord(body, &state); err != nil {
		return SupervisorState{}, false, err
	}
	return state, true, nil
}

func (s *RedisStore) readWorkerInstance(ctx context.Context, identity string) (WorkerState, bool, error) {
	if identity == "" {
		return WorkerState{}, false, nil
	}
	body, err := s.client.Get(ctx, s.workerInstanceKey(identity)).Bytes()
	if errors.Is(err, redis.Nil) {
		return WorkerState{}, false, nil
	}
	if err != nil {
		return WorkerState{}, false, err
	}
	var state WorkerState
	if err := s.decodeRecord(body, &state); err != nil {
		return WorkerState{}, false, err
	}
	return state, true, nil
}

// supervisorsKey 返回 supervisor 名称索引 key。
// mastersKey 返回 master ID 索引 key，固定为 {prefix}:masters。
func (s *RedisStore) mastersKey() string { return s.prefix() + ":masters" }

// masterKey 返回单个 master JSON 实体 key，固定为 {prefix}:master:{id}。
func (s *RedisStore) masterKey(id string) string {
	return s.prefix() + ":master:" + strings.TrimSpace(id)
}

func (s *RedisStore) masterLeaseKey(host string, environment string) string {
	return s.prefix() + ":lease:master:" + strings.TrimSpace(host) + ":" + strings.TrimSpace(environment)
}

func (s *RedisStore) supervisorsKey() string { return s.prefix() + ":supervisors" }

// supervisorKey 返回单个 supervisor JSON 实体 key。
func (s *RedisStore) supervisorKey(name string) string {
	return s.prefix() + ":supervisor:" + strings.TrimSpace(name)
}

func (s *RedisStore) supervisorInstanceKey(identity string) string {
	// identity 内含 NUL 分隔符，key path 使用 URL-safe base64 避免生成不可读/不可复制的 Redis key。
	return s.prefix() + ":supervisor_instance:" + encodeRedisIdentity(identity)
}

func (s *RedisStore) supervisorLeaseKey(name string, host string, environment string) string {
	return s.prefix() + ":lease:supervisor:" + strings.TrimSpace(host) + ":" + strings.TrimSpace(environment) + ":" + strings.TrimSpace(name)
}

// workersKey 返回 worker ID 索引 key。
func (s *RedisStore) workersKey() string { return s.prefix() + ":workers" }

// workerKey 返回单个 worker JSON 实体 key。
func (s *RedisStore) workerKey(id string) string {
	return s.prefix() + ":worker:" + strings.TrimSpace(id)
}

func (s *RedisStore) workerInstanceKey(identity string) string {
	// 与 supervisor 实例 key 一致，worker 实例身份不直接拼到 Redis key path。
	return s.prefix() + ":worker_instance:" + encodeRedisIdentity(identity)
}

// controlKey 返回全局控制 hash key。
func (s *RedisStore) controlKey() string { return s.prefix() + ":control" }

// pausedSupervisorsKey 返回 supervisor pause 标记 set key。
func (s *RedisStore) pausedSupervisorsKey() string { return s.prefix() + ":control:supervisors" }

// queueLengthSnapshotKey 返回队列长度快照 JSON key，固定独立于事件派生 metrics keys。
func (s *RedisStore) queueLengthSnapshotKey() string { return s.prefix() + ":metrics:queue_lengths" }

func (s *RedisStore) eventMetricWindowsKey() string {
	return s.prefix() + ":event_metrics:windows"
}

// eventMetricWindowKey 返回单个 event_metrics window JSON key。
func (s *RedisStore) eventMetricWindowKey(id string) string {
	return s.prefix() + ":event_metrics:window:" + strings.TrimSpace(id)
}

func (s *RedisStore) eventMetricRollupWindowsKey() string {
	return s.prefix() + ":event_metrics:rollups"
}

func (s *RedisStore) eventMetricRollupWindowKey(id string) string {
	return s.prefix() + ":event_metrics:rollup:" + strings.TrimSpace(id)
}

// batchesKey 返回批次摘要索引 sorted set。
func (s *RedisStore) batchesKey() string { return s.prefix() + ":batches" }

// batchKey 返回单个批次安全摘要 JSON key。
func (s *RedisStore) batchKey(id string) string {
	return s.prefix() + ":batch:" + strings.TrimSpace(id)
}

func (s *RedisStore) highValueDetailsKey() string {
	return s.prefix() + ":high_value_details"
}

func (s *RedisStore) highValueDetailKey(id string) string {
	return s.prefix() + ":high_value_detail:" + strings.TrimSpace(id)
}

func (s *RedisStore) observabilityDiagnosticsKey() string {
	return s.prefix() + ":observability_diagnostics"
}

func (s *RedisStore) observabilityDiagnosticKey(id string) string {
	return s.prefix() + ":observability_diagnostic:" + strings.TrimSpace(id)
}

// orphanProcessesKey 返回 Laravel ProcessRepository 风格的 master 维度 orphan PID sorted set。
func (s *RedisStore) orphanProcessesKey(masterID string) string {
	return s.prefix() + ":processes:orphans:" + strings.TrimSpace(masterID)
}

// prefix 规范化 Horizon Redis key 前缀，避免调用方传入多余冒号。
func (s *RedisStore) prefix() string { return strings.Trim(s.options.Prefix, ":") }

func encodeRedisIdentity(identity string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(identity))
}

func stringSliceToAny(items []string) []any {
	out := make([]any, len(items))
	for i, item := range items {
		out[i] = item
	}
	return out
}

func observabilityDiagnosticID(item ObservabilityDiagnostic, index int) string {
	at := item.ObservedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return strings.TrimSpace(item.Reason) + ":" + strconv.FormatInt(at.UnixNano(), 10) + ":" + strconv.Itoa(index)
}

// eventMetricWindowID 返回单个 Redis event_metrics window 的实体 key 后缀。
//
// 设计原因：多个 Horizon 实例可能在相同事件窗口、相同 supervisor/connection/queue/job 下 flush。
// ID 必须包含来源维度和 metrics_window，避免 Redis Set 覆盖另一实例的分片；index 只解决同一
// flush batch 内完全相同来源窗口的稳定唯一性。
func eventMetricWindowID(item EventMetricWindow, index int) string {
	// 同一 flush 可包含同一 job/window 的多个来源维度，index 保证批内 ID 稳定唯一。
	parts := []string{
		item.WindowStart.Format(time.RFC3339Nano),
		item.WindowEnd.Format(time.RFC3339Nano),
		item.FlushAt.Format(time.RFC3339Nano),
		strconv.FormatInt(eventMetricMetricsWindowMS(item.MetricsWindowMS, item.WindowStart, item.WindowEnd), 10),
		strings.TrimSpace(item.SourcePrefix),
		strings.TrimSpace(item.SourceHost),
		strings.TrimSpace(item.SourceEnvironment),
		strings.TrimSpace(item.SourceSupervisor),
		strings.TrimSpace(item.Connection),
		strings.TrimSpace(item.Queue),
		strings.TrimSpace(item.JobName),
		strconv.Itoa(index),
	}
	return strings.Join(parts, ":")
}
