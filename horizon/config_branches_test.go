package horizon

import (
	"reflect"
	"testing"
)

func TestConfigParsingHelperBranches(t *testing.T) {
	// 需求背景：配置可能来自 .env、手写 map 或测试 fake，输入类型不完全一致。本测试覆盖
	// queue、balance 和数字解析的兼容分支，保证宽输入最终收敛为严格输出。
	queueCases := []struct {
		name  string
		value any
		want  []string
	}{
		{name: "string", value: "default, emails, default", want: []string{"default", "emails"}},
		{name: "string slice", value: []string{"high", " low "}, want: []string{"high", "low"}},
		{name: "any slice", value: []any{"critical", "bulk"}, want: []string{"critical", "bulk"}},
		{name: "single scalar", value: 12, want: []string{"12"}},
	}
	for _, tc := range queueCases {
		t.Run("queue "+tc.name, func(t *testing.T) {
			if got := parseQueues(tc.value); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("queues = %#v, want %#v", got, tc.want)
			}
		})
	}

	if got := normalizeBalance("simple"); got != BalanceSimple {
		t.Fatalf("simple balance = %q", got)
	}
	if got := normalizeBalance("false"); got != BalanceFalse {
		t.Fatalf("false balance = %q", got)
	}
	if got := normalizeBalance("unknown"); got != "" {
		t.Fatalf("invalid balance = %q", got)
	}

	intCases := []any{int64(3), float64(4), "5", nil}
	for _, value := range intCases {
		if _, ok := parseNonNegativeInt(value); !ok {
			t.Fatalf("expected %T %v to parse as non-negative int", value, value)
		}
	}
	for _, value := range []any{int64(-1), float64(-1), "bad", struct{}{}} {
		if _, ok := parseNonNegativeInt(value); ok {
			t.Fatalf("expected %T %v to be rejected", value, value)
		}
	}
}

func TestBackoffAndBoolParsingBranches(t *testing.T) {
	// 需求背景：backoff 和 stop_when_empty 是容易被写成字符串的配置项。本测试覆盖常见合法写法，
	// 同时验证非法项会返回配置错误，避免 worker 使用错误重试或退出策略。
	backoffCases := []struct {
		name  string
		value any
		want  []int
	}{
		{name: "int slice", value: []int{1, 2}, want: []int{1, 2}},
		{name: "any slice", value: []any{"3", 4}, want: []int{3, 4}},
		{name: "string", value: "5, 6", want: []int{5, 6}},
		{name: "single", value: 7, want: []int{7}},
	}
	for _, tc := range backoffCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseBackoffField("supervisor-default", tc.value)
			if err != nil {
				t.Fatalf("parseBackoffField returned error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("backoff = %#v, want %#v", got, tc.want)
			}
		})
	}
	for _, value := range []any{[]int{-1}, []any{"bad"}, "1,bad", -1} {
		if _, err := parseBackoffField("supervisor-default", value); err == nil {
			t.Fatalf("expected backoff value %#v to be rejected", value)
		}
	}

	for _, value := range []any{true, "true", "1", "yes", "on"} {
		got, err := boolField("supervisor-default", value)
		if err != nil || !got {
			t.Fatalf("expected %#v to parse as true, got %v err %v", value, got, err)
		}
	}
	for _, value := range []any{nil, false, "", "false", "0", "no", "off"} {
		got, err := boolField("supervisor-default", value)
		if err != nil || got {
			t.Fatalf("expected %#v to parse as false, got %v err %v", value, got, err)
		}
	}
	if _, err := boolField("supervisor-default", 1); err == nil {
		t.Fatal("expected non-bool scalar to be rejected")
	}
}

func TestCompatibilityConfigHelperBranches(t *testing.T) {
	// 需求背景：historical scenario 11 新增的兼容配置 helper 需要覆盖空值、标量、非法值和路径规整分支，
	// 避免后续 Dashboard/API/runtime 各自实现不同默认值。
	cfg := Config{}
	if cfg.DashboardPath() != "/horizon" || cfg.APIPrefix() != "/horizon/api" {
		t.Fatalf("zero config paths were not normalized: %q %q", cfg.DashboardPath(), cfg.APIPrefix())
	}
	if normalizePath("/ops/horizon/") != "ops/horizon" || normalizePath(" ") != "horizon" {
		t.Fatal("normalizePath did not trim slashes or apply fallback")
	}
	if !firstBool(true, false) || !firstBool("yes", false) || firstBool("off", true) {
		t.Fatal("firstBool did not parse common boolean values")
	}
	if !firstBool("bad", true) || firstBool(nil, false) {
		t.Fatal("firstBool did not honor fallback branches")
	}
	if got := parseStringList(7); !reflect.DeepEqual(got, []string{"7"}) {
		t.Fatalf("scalar string list = %#v", got)
	}
	if got := parseStringList(nil); got != nil {
		t.Fatalf("nil string list = %#v", got)
	}
	waits := parseWaits(map[string]any{" redis:default ": "30", "bad": "soon", "": 1})
	if waits["redis:default"] != 30 || len(waits) != 1 {
		t.Fatalf("waits parse = %#v", waits)
	}
	if _, err := selectEnvironmentConfig("qa", nil); err != nil {
		t.Fatalf("empty environments should preserve legacy light-load behavior: %v", err)
	}
	if !strIs("prod-*", "prod-cn") || strIs("prod-*", "qa") || !strIs("*-cn", "prod-cn") {
		t.Fatal("strIs wildcard matching failed")
	}
}
