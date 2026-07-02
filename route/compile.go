package route

import (
	"regexp"
	"strings"
)

var paramPattern = regexp.MustCompile(`^\{([A-Za-z_][A-Za-z0-9_]*)(\?)?\}$`)

// compilePaths 将 URI 模板编译为 Gin 路由路径列表。
//
// 设计说明：可选参数仅支持尾部省略。例如 /a/{b?}/{c?} 生成 /a、/a/:b、/a/:b/:c，
// 不会生成跳过中间参数的组合（如 /a/:c）。这样可以避免语义不合理的路径匹配。
func compilePaths(uri string, constraints map[string]string) []string {
	uri = joinPaths("/", uri)
	if uri == "/" {
		return []string{"/"}
	}
	segments := strings.Split(strings.Trim(uri, "/"), "/")

	// 找出尾部连续可选参数的起始位置
	optionalStart := len(segments)
	for i := len(segments) - 1; i >= 0; i-- {
		matches := paramPattern.FindStringSubmatch(segments[i])
		if matches == nil || matches[2] != "?" {
			break
		}
		optionalStart = i
	}

	// 构建非可选部分的路径（必须全部包含）
	basePaths := []string{""}
	for i := 0; i < optionalStart; i++ {
		segment := segments[i]
		matches := paramPattern.FindStringSubmatch(segment)
		if matches == nil {
			// 静态段
			for j := range basePaths {
				basePaths[j] = basePaths[j] + "/" + segment
			}
		} else {
			// 必选参数
			name := matches[1]
			compiled := ":" + name
			if i == len(segments)-1 && constraints[name] == ".*" {
				compiled = "*" + name
			}
			for j := range basePaths {
				basePaths[j] = basePaths[j] + "/" + compiled
			}
		}
	}

	// 为尾部可选参数生成组合（仅支持尾部省略）
	paths := make([]string, len(basePaths))
	copy(paths, basePaths)
	prevBatch := basePaths

	for i := optionalStart; i < len(segments); i++ {
		segment := segments[i]
		matches := paramPattern.FindStringSubmatch(segment)
		name := matches[1]
		compiled := ":" + name
		if i == len(segments)-1 && constraints[name] == ".*" {
			compiled = "*" + name
		}

		var currentBatch []string
		for _, path := range prevBatch {
			currentBatch = append(currentBatch, path+"/"+compiled)
		}
		paths = append(paths, currentBatch...)
		prevBatch = currentBatch
	}

	// 处理空路径
	for i, path := range paths {
		if path == "" {
			paths[i] = "/"
		}
	}

	return paths
}

// compileConstraints 预编译约束正则表达式
func compileConstraints(constraints map[string]string) map[string]*regexp.Regexp {
	if len(constraints) == 0 {
		return nil
	}
	compiled := make(map[string]*regexp.Regexp, len(constraints))
	for param, expr := range constraints {
		if expr == "" || expr == ".*" {
			continue
		}
		// 确保正则表达式有 ^ 和 $ 锚点
		if !strings.HasPrefix(expr, "^") {
			expr = "^" + expr
		}
		if !strings.HasSuffix(expr, "$") {
			expr += "$"
		}
		if re, err := regexp.Compile(expr); err == nil {
			compiled[param] = re
		}
	}
	return compiled
}

// cloneCompiledConstraints 深拷贝预编译的正则表达式映射
func cloneCompiledConstraints(src map[string]*regexp.Regexp) map[string]*regexp.Regexp {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]*regexp.Regexp, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func requireResourceParam(resource string) string {
	resource = strings.Trim(resource, "/")
	if resource == "" {
		panic("route: resource name is required")
	}
	parts := strings.Split(resource, ".")
	last := parts[len(parts)-1]
	last = strings.TrimSuffix(last, "s")
	if last == "" {
		last = parts[len(parts)-1]
	}
	return last
}

func resourceURI(name string) string {
	return "/" + strings.ReplaceAll(strings.Trim(name, "/"), ".", "/")
}

func resourceMemberURI(name string, param string) string {
	return strings.TrimRight(resourceURI(name), "/") + "/{" + param + "}"
}

func resourceNamePrefix(name string) string {
	return strings.Trim(strings.ReplaceAll(name, "/", "."), ".")
}
