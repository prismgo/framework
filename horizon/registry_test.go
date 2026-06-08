package horizon

import "testing"

func TestCommandFactoriesRegisterIntegrationRuntimeCommands(t *testing.T) {
	// 需求背景：Laravel config contract 已经提供真实 Store 驱动的运行时命令，因此 CommandFactories 需要注册这些命令，
	// 但仍不能注册尚未实现的 worker/install/listen/publish 命令。
	factories := CommandFactories()
	names := make(map[string]bool, len(factories))
	for _, factory := range factories {
		names[factory().Definition().Name] = true
	}
	for _, want := range []string{
		"horizon",
		"horizon:list",
		"horizon:status",
		"horizon:supervisors",
		"horizon:supervisor-status",
		"horizon:supervisor",
		"horizon:work",
		"horizon:pause",
		"horizon:continue",
		"horizon:pause-supervisor",
		"horizon:continue-supervisor",
		"horizon:terminate",
		"horizon:timeout",
		"horizon:stale",
		"horizon:snapshot",
		"horizon:clear-metrics",
		"horizon:clear",
		"horizon:forget",
		"horizon:purge",
		"horizon:install",
		"horizon:listen",
	} {
		if !names[want] {
			t.Fatalf("missing command factory: %s", want)
		}
	}
	for _, forbidden := range []string{"horizon:publish"} {
		if names[forbidden] {
			t.Fatalf("unexpected command registered: %s", forbidden)
		}
	}
}

func TestCommandConfigViewClonesHorizonConfig(t *testing.T) {
	// 设计原因：horizon:list 使用 cmd 包的展示模型，转换时必须复制 map/slice，避免输出层修改核心 Config。
	cfg := Config{
		Environment: "local",
		Store:       "memory",
		Connection:  "default",
		Prefix:      "demo",
		Supervisors: map[string]SupervisorConfig{
			"supervisor-default": {
				Name:          "supervisor-default",
				Connection:    "redis",
				Queues:        []string{"default"},
				Balance:       BalanceAuto,
				MinProcesses:  1,
				MaxProcesses:  2,
				Backoff:       []int{1, 5},
				StopWhenEmpty: true,
			},
		},
	}

	view := toCommandConfigView(cfg)
	view.Supervisors["supervisor-default"].Queues[0] = "changed"
	view.Supervisors["supervisor-default"].Backoff[0] = 99

	// Trim 字段已移除，仅验证 Supervisors 的深拷贝
	if cfg.Supervisors["supervisor-default"].Queues[0] != "default" {
		t.Fatal("queue slice was not cloned")
	}
	if cfg.Supervisors["supervisor-default"].Backoff[0] != 1 {
		t.Fatal("backoff slice was not cloned")
	}
}

func TestCloneIntMapHandlesEmptyInput(t *testing.T) {
	// 测试目的：trim 配置允许为空，空输入在命令展示模型中保持 nil，避免制造无意义字段。
	if cloneIntMap(nil) != nil {
		t.Fatal("nil input should return nil")
	}
}

func TestCloneIntMapNonEmptyAndMutationIsolation(t *testing.T) {
	// 需求背景：cloneIntMap 用于命令展示模型中对配置数据的浅拷贝，防止输出层
	// 修改核心 Config。本测试覆盖非空克隆和 mutation isolation。

	// 非空 map 克隆 + 值正确性
	input := map[string]int{"a": 1, "b": 2, "c": 3}
	cloned := cloneIntMap(input)
	if len(cloned) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(cloned))
	}
	for k, v := range input {
		if cloned[k] != v {
			t.Fatalf("cloned[%s] = %d, want %d", k, cloned[k], v)
		}
	}

	// 修改克隆不影响原 map
	cloned["a"] = 999
	if input["a"] != 1 {
		t.Fatal("modifying clone mutated the original map")
	}

	// 修改原 map 不影响克隆
	input["b"] = 888
	if cloned["b"] != 2 {
		t.Fatal("modifying original after clone mutated the cloned map")
	}

	// 空非 nil map → 返回 nil
	if cloneIntMap(map[string]int{}) != nil {
		t.Fatal("empty non-nil map should return nil")
	}
}
