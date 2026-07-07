// Package version 提供版本号解析和比较工具。
package version

import (
	"slices"
	"strconv"
	"strings"
)

// AtLeast 比较版本号字符串，判断 version 是否大于等于 target。
// 支持带后缀的版本号（如 "8.0.32-0ubuntu0.20.04.1"），先截断 "-" 后缀，再按 "." 分割逐段比较数字。
func AtLeast(version, target string) bool {
	if target == "" {
		return true
	}
	if version == "" {
		return false
	}

	vParts := parseParts(version)
	tParts := parseParts(target)

	// 补齐长度，使缺少的段视为 0
	maxLen := max(len(vParts), len(tParts))
	for len(vParts) < maxLen {
		vParts = append(vParts, 0)
	}
	for len(tParts) < maxLen {
		tParts = append(tParts, 0)
	}

	return slices.Compare(vParts, tParts) >= 0
}

// parseParts 解析版本号字符串，提取数字部分。
// 先截断 "-" 后缀（如 Debian/Ubuntu 包后缀），再按 "." 分割提取数字。
// 例如 "8.0.32-0ubuntu" -> [8, 0, 32]
func parseParts(version string) []int {
	// 截断 Debian/Ubuntu 后缀（如 "-0ubuntu0.20.04.1" 或 "-log"）
	if idx := strings.IndexByte(version, '-'); idx >= 0 {
		version = version[:idx]
	}

	parts := strings.Split(version, ".")
	nums := make([]int, 0, len(parts))
	for _, part := range parts {
		num, err := strconv.Atoi(part)
		if err != nil {
			continue
		}
		nums = append(nums, num)
	}
	return nums
}
