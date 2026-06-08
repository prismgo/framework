package make

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"unicode"
)

var validGoIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type normalizedName struct {
	Directories []string
	FileName    string
	PackageName string
	TypeName    string
}

func normalizeName(input string, spec artifactSpec) (normalizedName, error) {
	cleaned := strings.TrimSpace(input)
	cleaned = strings.ReplaceAll(cleaned, "\\", "/")
	cleaned = strings.ReplaceAll(cleaned, ".", "/")
	cleaned = strings.TrimPrefix(cleaned, "App/")
	cleaned = strings.TrimPrefix(cleaned, "app/")
	cleaned = strings.TrimPrefix(cleaned, "Models/")
	cleaned = strings.TrimPrefix(cleaned, "models/")
	if cleaned == "" {
		return normalizedName{}, fmt.Errorf("generator name is required")
	}
	if strings.HasPrefix(cleaned, "/") || path.IsAbs(cleaned) {
		return normalizedName{}, fmt.Errorf("illegal path %q: absolute paths are not supported", input)
	}

	rawParts := strings.Split(cleaned, "/")
	parts := make([]string, 0, len(rawParts))
	for _, raw := range rawParts {
		part := strings.TrimSpace(raw)
		if part == "" || part == "." || part == ".." {
			return normalizedName{}, fmt.Errorf("illegal path %q: empty or traversal segments are not supported", input)
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return normalizedName{}, fmt.Errorf("generator name is required")
	}

	base := parts[len(parts)-1]
	typeName := exportedTypeName(base, spec.Suffix)
	if !validGoIdentifier.MatchString(typeName) {
		return normalizedName{}, fmt.Errorf("illegal Go identifier %q from %q", typeName, input)
	}

	dirs := make([]string, 0, len(parts)-1)
	for _, part := range parts[:len(parts)-1] {
		dir := snakeCase(part)
		if dir == "" || strings.Contains(dir, "..") {
			return normalizedName{}, fmt.Errorf("illegal path %q", input)
		}
		dirs = append(dirs, dir)
	}

	packageName := spec.Package
	if len(dirs) > 0 {
		packageName = dirs[len(dirs)-1]
	}
	if packageName == "" || !validGoIdentifier.MatchString(packageName) {
		return normalizedName{}, fmt.Errorf("illegal Go package %q from %q", packageName, input)
	}

	return normalizedName{
		Directories: dirs,
		FileName:    snakeCase(base),
		PackageName: packageName,
		TypeName:    typeName,
	}, nil
}

func exportedTypeName(value string, suffix string) string {
	words := splitWords(value)
	if len(words) == 0 {
		return ""
	}
	name := strings.Join(words, "")
	if suffix != "" && !strings.HasSuffix(name, suffix) {
		name += suffix
	}
	return name
}

func snakeCase(value string) string {
	words := splitWords(value)
	lower := make([]string, 0, len(words))
	for _, word := range words {
		lower = append(lower, strings.ToLower(word))
	}
	return strings.Join(lower, "_")
}

func splitWords(value string) []string {
	var words []string
	var current []rune
	runes := []rune(strings.TrimSpace(value))
	flush := func() {
		if len(current) == 0 {
			return
		}
		word := string(current)
		words = append(words, strings.ToUpper(word[:1])+strings.ToLower(word[1:]))
		current = nil
	}
	for i, r := range runes {
		if r == '_' || r == '-' || unicode.IsSpace(r) {
			flush()
			continue
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			current = append(current, r)
			continue
		}
		if unicode.IsUpper(r) && len(current) > 0 {
			prev := current[len(current)-1]
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || nextLower {
				flush()
			}
		}
		current = append(current, r)
	}
	flush()
	return words
}
