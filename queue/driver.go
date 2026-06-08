package queue

import (
	"strings"
)

func normalizeDriverName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func cloneConnectionConfig(spec ConnectionConfig) ConnectionConfig {
	spec.Options = cloneAnyMap(spec.Options)
	return spec
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
