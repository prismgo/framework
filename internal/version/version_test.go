package version

import (
	"reflect"
	"testing"
)

func TestAtLeast(t *testing.T) {
	tests := []struct {
		name    string
		version string
		target  string
		want    bool
	}{
		{"equal versions", "8.0.11", "8.0.11", true},
		{"greater major", "9.0.0", "8.0.11", true},
		{"greater minor", "8.1.0", "8.0.11", true},
		{"greater patch", "8.0.32", "8.0.11", true},
		{"less major", "7.0.0", "8.0.11", false},
		{"less minor", "8.0.0", "8.0.11", false},
		{"less patch", "8.0.9", "8.0.11", false},
		{"double digit major", "10.0.0", "8.0.11", true},
		{"double digit minor", "8.10.0", "8.0.11", true},
		{"double digit patch", "8.0.100", "8.0.11", true},
		{"version with suffix", "8.0.32-0ubuntu0.20.04.1", "8.0.11", true},
		{"single digit patch with suffix", "8.0.9-0ubuntu0.20.04.1", "8.0.11", false},
		{"single digit patch with log suffix", "8.0.9-log", "8.0.11", false},
		{"empty version", "", "8.0.11", false},
		{"empty target", "8.0.32", "", true},
		{"both empty", "", "", true},
		{"invalid version format", "invalid", "8.0.11", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AtLeast(tt.version, tt.target)
			if got != tt.want {
				t.Errorf("AtLeast(%q, %q) = %v, want %v", tt.version, tt.target, got, tt.want)
			}
		})
	}
}

func TestParseParts(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    []int
	}{
		{"simple version", "8.0.32", []int{8, 0, 32}},
		{"version with ubuntu suffix", "8.0.32-0ubuntu0.20.04.1", []int{8, 0, 32}},
		{"version with log suffix", "8.0.10-log", []int{8, 0, 10}},
		{"single digit patch with suffix", "8.0.9-0ubuntu", []int{8, 0, 9}},
		{"single digit patch with log", "5.7.9-something", []int{5, 7, 9}},
		{"invalid format", "invalid", []int{}},
		{"empty string", "", []int{}},
		{"version with dots only", "1.2.3.4", []int{1, 2, 3, 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseParts(tt.version)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseParts(%q) = %v, want %v", tt.version, got, tt.want)
			}
		})
	}
}

// TestAtLeast_EdgeCases 验证 AtLeast 函数的边界情况处理
func TestAtLeast_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		version string
		target  string
		want    bool
	}{
		// 负号开头：- 被当作后缀分隔符，parseParts 返回空切片
		{"negative leading version", "-1.0.0", "8.0.11", false},
		{"negative leading target", "8.0.11", "-1.0.0", true},

		// 前导零：strconv.Atoi 正常解析
		{"leading zeros equal", "08.00.011", "8.0.11", true},
		{"leading zeros greater", "08.01.00", "8.0.11", true},

		// target 带后缀
		{"target with rc suffix", "8.0.32", "8.0.11-rc1", true},
		{"target with beta suffix", "8.0.10", "8.0.11-beta", false},

		// 双方都带后缀
		{"both with suffix equal", "8.0.11-ubuntu", "8.0.11-rc1", true},
		{"both with suffix greater", "8.0.32-ubuntu", "8.0.11-rc1", true},
		{"both with suffix less", "8.0.9-ubuntu", "8.0.11-rc1", false},

		// 单段版本
		{"single segment equal", "8", "8", true},
		{"single segment vs multi equal", "8", "8.0.0", true},
		{"single segment vs multi greater", "9", "8.0.11", true},
		{"single segment vs multi less", "7", "8.0.11", false},

		// 全无效段
		{"all invalid segments version", "a.b.c", "8.0.11", false},
		{"all invalid segments target", "8.0.11", "a.b.c", true},
		{"both all invalid", "a.b.c", "x.y.z", true},

		// 混合有效/无效段：无效段被跳过
		{"mixed valid invalid version", "8.0.x.11", "8.0.11", true},
		{"mixed valid invalid target", "8.0.11", "8.0.x.11", true},

		// 前导/尾随点
		{"leading dot version", ".8.0.11", "8.0.11", true},
		{"trailing dot version", "8.0.11.", "8.0.11", true},
		{"leading dot target", "8.0.11", ".8.0.11", true},
		{"trailing dot target", "8.0.11", "8.0.11.", true},

		// 连续点
		{"double dots version", "8..0.11", "8.0.11", true},
		{"double dots target", "8.0.11", "8..0.11", true},

		// 全零版本
		{"all zeros equal", "0.0.0", "0.0.0", true},
		{"all zeros less", "0.0.0", "0.0.1", false},
		{"all zeros greater", "0.0.1", "0.0.0", true},

		// 多后缀：只截断第一个 -
		{"multi hyphen version", "8.0.11-beta-1", "8.0.11", true},
		{"multi hyphen target", "8.0.11", "8.0.11-beta-1", true},

		// 超长段数
		{"five segments equal", "8.0.11.0.0", "8.0.11", true},
		{"five segments greater", "8.0.11.0.1", "8.0.11", true},
		{"five segments less", "8.0.10.99.99", "8.0.11", false},

		// 含空格：Atoi 失败，段被跳过
		{"spaces in version", " 8 . 0 . 11 ", "8.0.11", false},
		{"spaces in target", "8.0.11", " 8 . 0 . 11 ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AtLeast(tt.version, tt.target)
			if got != tt.want {
				t.Errorf("AtLeast(%q, %q) = %v, want %v", tt.version, tt.target, got, tt.want)
			}
		})
	}
}

// TestParseParts_EdgeCases 验证 parseParts 函数的边界情况处理
func TestParseParts_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    []int
	}{
		// 负号开头
		{"negative leading", "-1.0.0", []int{}},

		// 前导零
		{"leading zeros", "08.00.011", []int{8, 0, 11}},

		// 单段
		{"single segment", "8", []int{8}},

		// 全无效段
		{"all invalid", "a.b.c", []int{}},

		// 混合有效/无效
		{"mixed valid invalid", "8.0.x.11", []int{8, 0, 11}},

		// 前导/尾随点
		{"leading dot", ".8.0.11", []int{8, 0, 11}},
		{"trailing dot", "8.0.11.", []int{8, 0, 11}},

		// 连续点
		{"double dots", "8..0.11", []int{8, 0, 11}},

		// 全零
		{"all zeros", "0.0.0", []int{0, 0, 0}},

		// 多后缀
		{"multi hyphen", "8.0.11-beta-1", []int{8, 0, 11}},

		// 超长段数
		{"five segments", "1.2.3.4.5", []int{1, 2, 3, 4, 5}},

		// 含空格
		{"spaces", " 8 . 0 . 11 ", []int{}},

		// 只有点
		{"only dots", "...", []int{}},

		// 只有后缀
		{"only suffix", "-ubuntu", []int{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseParts(tt.version)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseParts(%q) = %v, want %v", tt.version, got, tt.want)
			}
		})
	}
}

// TestAtLeast_UsesSlicesCompare 验证 AtLeast 函数使用 slices.Compare 进行比较
// 这个测试确保版本比较逻辑与补齐长度后的 slices.Compare 行为一致
func TestAtLeast_UsesSlicesCompare(t *testing.T) {
	tests := []struct {
		name    string
		version string
		target  string
		want    bool
	}{
		{"equal versions", "8.0.11", "8.0.11", true},
		{"greater version", "8.0.32", "8.0.11", true},
		{"less version", "8.0.9", "8.0.11", false},
		{"greater major", "9.0.0", "8.0.11", true},
		{"less major", "7.0.0", "8.0.11", false},
		{"different lengths equal", "8.0", "8.0.0", true},
		{"different lengths greater", "8.1", "8.0.11", true},
		{"different lengths less", "8.0", "8.0.11", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AtLeast(tt.version, tt.target)
			if got != tt.want {
				t.Errorf("AtLeast(%q, %q) = %v, want %v", tt.version, tt.target, got, tt.want)
			}
		})
	}
}
