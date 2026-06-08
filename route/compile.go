package route

import (
	"regexp"
	"strings"
)

var paramPattern = regexp.MustCompile(`^\{([A-Za-z_][A-Za-z0-9_]*)(\?)?\}$`)

func compilePaths(uri string, constraints map[string]string) []string {
	uri = joinPaths("/", uri)
	if uri == "/" {
		return []string{"/"}
	}
	segments := strings.Split(strings.Trim(uri, "/"), "/")
	paths := []string{""}
	for index, segment := range segments {
		matches := paramPattern.FindStringSubmatch(segment)
		if matches == nil {
			for i := range paths {
				paths[i] += "/" + segment
			}
			continue
		}

		name := matches[1]
		optional := matches[2] == "?"
		compiled := ":" + name
		if index == len(segments)-1 && constraints[name] == ".*" {
			compiled = "*" + name
		}

		next := make([]string, 0, len(paths)*2)
		for _, path := range paths {
			if optional {
				next = append(next, path)
			}
			next = append(next, path+"/"+compiled)
		}
		paths = next
	}
	for i, path := range paths {
		if path == "" {
			paths[i] = "/"
		}
	}
	return paths
}

func matchesConstraint(expr, value string) bool {
	if expr == "" || expr == ".*" {
		return true
	}
	if !strings.HasPrefix(expr, "^") {
		expr = "^" + expr
	}
	if !strings.HasSuffix(expr, "$") {
		expr += "$"
	}
	ok, err := regexp.MatchString(expr, value)
	return err == nil && ok
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
