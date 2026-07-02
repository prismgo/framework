package route

import (
	"regexp"
	"testing"
)

// TestCompileConstraints 测试 compileConstraints 函数的各种边界条件
// 需求背景：Low #6 - compileConstraints 缺少直接单元测试，需要覆盖空 map、nil 输入、特殊正则字符等边界条件
func TestCompileConstraints(t *testing.T) {
	tests := []struct {
		name        string
		constraints map[string]string
		wantNil     bool
		wantLen     int
		wantKeys    []string
	}{
		{
			name:        "nil map",
			constraints: nil,
			wantNil:     true,
		},
		{
			name:        "empty map",
			constraints: map[string]string{},
			wantNil:     true,
		},
		{
			name: "single valid constraint",
			constraints: map[string]string{
				"id": "[0-9]+",
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"id"},
		},
		{
			name: "multiple valid constraints",
			constraints: map[string]string{
				"id":   "[0-9]+",
				"slug": "[a-z-]+",
			},
			wantNil:  false,
			wantLen:  2,
			wantKeys: []string{"id", "slug"},
		},
		{
			name: "skip empty expression",
			constraints: map[string]string{
				"id":   "[0-9]+",
				"name": "",
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"id"},
		},
		{
			name: "skip wildcard expression",
			constraints: map[string]string{
				"id":       "[0-9]+",
				"filepath": ".*",
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"id"},
		},
		{
			name: "constraint without anchors",
			constraints: map[string]string{
				"id": "[0-9]+",
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"id"},
		},
		{
			name: "constraint with start anchor only",
			constraints: map[string]string{
				"id": "^[0-9]+",
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"id"},
		},
		{
			name: "constraint with end anchor only",
			constraints: map[string]string{
				"id": "[0-9]+$",
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"id"},
		},
		{
			name: "constraint with both anchors",
			constraints: map[string]string{
				"id": "^[0-9]+$",
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"id"},
		},
		{
			name: "invalid regex is silently skipped",
			constraints: map[string]string{
				"id":    "[0-9]+",
				"invalid": "[unclosed",
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"id"},
		},
		{
			name: "special regex characters",
			constraints: map[string]string{
				"special": `[a-z]+\.[0-9]+`,
			},
			wantNil:  false,
			wantLen:  1,
			wantKeys: []string{"special"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compileConstraints(tt.constraints)

			if tt.wantNil {
				if got != nil {
					t.Errorf("compileConstraints() = %v, want nil", got)
				}
				return
			}

			if got == nil {
				t.Fatalf("compileConstraints() = nil, want non-nil")
			}

			if len(got) != tt.wantLen {
				t.Errorf("compileConstraints() returned %d constraints, want %d", len(got), tt.wantLen)
			}

			for _, key := range tt.wantKeys {
				if _, ok := got[key]; !ok {
					t.Errorf("compileConstraints() missing key %q", key)
				}
			}

			// 验证编译后的正则表达式能够正确匹配
			for key, re := range got {
				if re == nil {
					t.Errorf("compileConstraints()[%q] = nil, want non-nil regexp", key)
				}
			}
		})
	}
}

// TestCompileConstraintsAnchorNormalization 测试 compileConstraints 是否正确添加锚点
func TestCompileConstraintsAnchorNormalization(t *testing.T) {
	tests := []struct {
		name      string
		expr      string
		wantMatch string
		wantFail  string
	}{
		{
			name:      "no anchors added",
			expr:      "[0-9]+",
			wantMatch: "123",
			wantFail:  "abc123def",
		},
		{
			name:      "start anchor only added",
			expr:      "^[0-9]+",
			wantMatch: "123",
			wantFail:  "abc123",
		},
		{
			name:      "end anchor only added",
			expr:      "[0-9]+$",
			wantMatch: "123",
			wantFail:  "123abc",
		},
		{
			name:      "both anchors preserved",
			expr:      "^[0-9]+$",
			wantMatch: "123",
			wantFail:  "abc123def",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			constraints := map[string]string{"test": tt.expr}
			compiled := compileConstraints(constraints)

			re := compiled["test"]
			if re == nil {
				t.Fatalf("compileConstraints() returned nil regexp")
			}

			if !re.MatchString(tt.wantMatch) {
				t.Errorf("regexp should match %q, but didn't", tt.wantMatch)
			}

			if re.MatchString(tt.wantFail) {
				t.Errorf("regexp should not match %q, but did", tt.wantFail)
			}
		})
	}
}

// TestCloneCompiledConstraints 测试 cloneCompiledConstraints 函数的深拷贝行为
// 需求背景：Low #6 - cloneCompiledConstraints 缺少直接单元测试
func TestCloneCompiledConstraints(t *testing.T) {
	tests := []struct {
		name    string
		src     map[string]*regexp.Regexp
		wantNil bool
		wantLen int
	}{
		{
			name:    "nil map",
			src:     nil,
			wantNil: true,
		},
		{
			name:    "empty map",
			src:     map[string]*regexp.Regexp{},
			wantNil: true,
		},
		{
			name: "single entry",
			src: map[string]*regexp.Regexp{
				"id": regexp.MustCompile(`^[0-9]+$`),
			},
			wantNil: false,
			wantLen: 1,
		},
		{
			name: "multiple entries",
			src: map[string]*regexp.Regexp{
				"id":   regexp.MustCompile(`^[0-9]+$`),
				"slug": regexp.MustCompile(`^[a-z-]+$`),
			},
			wantNil: false,
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cloneCompiledConstraints(tt.src)

			if tt.wantNil {
				if got != nil {
					t.Errorf("cloneCompiledConstraints() = %v, want nil", got)
				}
				return
			}

			if got == nil {
				t.Fatalf("cloneCompiledConstraints() = nil, want non-nil")
			}

			if len(got) != tt.wantLen {
				t.Errorf("cloneCompiledConstraints() returned %d entries, want %d", len(got), tt.wantLen)
			}

			// 验证克隆的 regexp 与源相同
			for key, srcRe := range tt.src {
				gotRe, ok := got[key]
				if !ok {
					t.Errorf("clone missing key %q", key)
					continue
				}
				if gotRe != srcRe {
					t.Errorf("clone[%q] = %p, want %p (same pointer)", key, gotRe, srcRe)
				}
			}

			// 验证深拷贝：修改源 map 不应影响克隆
			if tt.src != nil {
				originalLen := len(tt.src)
				tt.src["new"] = regexp.MustCompile(`^test$`)
				if len(got) != originalLen {
					t.Errorf("clone was affected by source modification: got %d entries, want %d", len(got), originalLen)
				}
			}
		})
	}
}

// TestCloneCompiledConstraintsIndependence 验证克隆的 map 是独立的
func TestCloneCompiledConstraintsIndependence(t *testing.T) {
	src := map[string]*regexp.Regexp{
		"id": regexp.MustCompile(`^[0-9]+$`),
	}

	clone1 := cloneCompiledConstraints(src)
	clone2 := cloneCompiledConstraints(src)

	// 修改 clone1 不应影响 clone2
	clone1["new"] = regexp.MustCompile(`^test$`)

	if _, ok := clone2["new"]; ok {
		t.Error("modifying clone1 affected clone2")
	}

	if len(clone2) != 1 {
		t.Errorf("clone2 has %d entries, want 1", len(clone2))
	}
}
