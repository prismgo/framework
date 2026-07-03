package translation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/prismgo/framework/exception"
	pathutil "github.com/prismgo/framework/internal/path"
)

const vendorDir = "vendor"

var validIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

func isValidIdentifier(s string) bool {
	return s != "" && validIdentifier.MatchString(s)
}

type FileLoader struct {
	paths     []string
	jsonPaths []string
	hints     map[string]string

	mu         sync.RWMutex
	loaded     map[string]map[string]map[string]map[string]any
	loadedJSON map[string]map[string]any
}

func NewFileLoader() *FileLoader {
	return &FileLoader{
		hints:      make(map[string]string),
		loaded:     make(map[string]map[string]map[string]map[string]any),
		loadedJSON: make(map[string]map[string]any),
	}
}

func (f *FileLoader) Load(locale, group, namespace string) (map[string]any, error) {
	if !isValidIdentifier(locale) {
		return nil, errors.New("invalid locale format")
	}

	if namespace == "" {
		namespace = defaultNamespace
	}

	if group == "" {
		return f.loadJSON(locale)
	}

	if !isValidIdentifier(group) {
		return nil, errors.New("invalid group format")
	}

	if namespace == defaultNamespace {
		return f.loadGroup(locale, group)
	}

	return f.loadNamespacedGroup(locale, group, namespace)
}

func (f *FileLoader) loadGroup(locale, group string) (map[string]any, error) {
	f.mu.RLock()
	if cached, ok := f.loaded[defaultNamespace]; ok {
		if localeData, ok := cached[locale]; ok {
			if groupData, ok := localeData[group]; ok {
				f.mu.RUnlock()
				return groupData, nil
			}
		}
	}
	f.mu.RUnlock()

	merged := make(map[string]any)
	for _, path := range f.getAllPaths() {
		filePath := filepath.Join(path, locale, group+".json")
		if data, err := f.loadFile(filePath); err == nil {
			for k, v := range data {
				merged[k] = v
			}
		}
	}

	f.mu.Lock()
	if f.loaded[defaultNamespace] == nil {
		f.loaded[defaultNamespace] = make(map[string]map[string]map[string]any)
	}
	if f.loaded[defaultNamespace][locale] == nil {
		f.loaded[defaultNamespace][locale] = make(map[string]map[string]any)
	}
	f.loaded[defaultNamespace][locale][group] = merged
	f.mu.Unlock()

	return merged, nil
}

func (f *FileLoader) loadNamespacedGroup(locale, group, namespace string) (map[string]any, error) {
	f.mu.RLock()
	if cached, ok := f.loaded[namespace]; ok {
		if localeData, ok := cached[locale]; ok {
			if groupData, ok := localeData[group]; ok {
				f.mu.RUnlock()
				return groupData, nil
			}
		}
	}
	f.mu.RUnlock()

	merged := make(map[string]any)

	f.mu.RLock()
	hint, hintExists := f.hints[namespace]
	f.mu.RUnlock()

	if hintExists {
		filePath := filepath.Join(hint, locale, group+".json")
		if data, err := f.loadFile(filePath); err == nil {
			for k, v := range data {
				merged[k] = v
			}
		}
	}

	for _, path := range f.getAllPaths() {
		vendorPath := filepath.Join(path, vendorDir, namespace, locale, group+".json")
		if data, err := f.loadFile(vendorPath); err == nil {
			for k, v := range data {
				if _, exists := merged[k]; !exists {
					merged[k] = v
				}
			}
		}
	}

	f.mu.Lock()
	if f.loaded[namespace] == nil {
		f.loaded[namespace] = make(map[string]map[string]map[string]any)
	}
	if f.loaded[namespace][locale] == nil {
		f.loaded[namespace][locale] = make(map[string]map[string]any)
	}
	f.loaded[namespace][locale][group] = merged
	f.mu.Unlock()

	return merged, nil
}

func (f *FileLoader) loadJSON(locale string) (map[string]any, error) {
	f.mu.RLock()
	if cached, ok := f.loadedJSON[locale]; ok {
		f.mu.RUnlock()
		return cached, nil
	}
	f.mu.RUnlock()

	merged := make(map[string]any)

	for _, jsonPath := range f.getAllJSONPaths() {
		filePath := filepath.Join(jsonPath, locale+".json")
		data, err := f.loadFile(filePath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				exception.Report(context.Background(), fmt.Errorf("translation: load JSON file %s: %w", filePath, err), nil)
			}
			continue
		}
		for k, v := range data {
			merged[k] = v
		}
	}

	for _, path := range f.getAllPaths() {
		filePath := filepath.Join(path, locale+".json")
		data, err := f.loadFile(filePath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				exception.Report(context.Background(), fmt.Errorf("translation: load JSON file %s: %w", filePath, err), nil)
			}
			continue
		}
		for k, v := range data {
			if _, exists := merged[k]; !exists {
				merged[k] = v
			}
		}
	}

	f.mu.Lock()
	f.loadedJSON[locale] = merged
	f.mu.Unlock()

	return merged, nil
}

func (f *FileLoader) loadFile(path string) (map[string]any, error) {
	path = pathutil.Clean(path)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (f *FileLoader) AddNamespace(namespace, hint string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hints[namespace] = pathutil.Clean(hint)
}

func (f *FileLoader) AddPath(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cleanPath := pathutil.Clean(path)
	for _, p := range f.paths {
		if p == cleanPath {
			return
		}
	}
	f.paths = append(f.paths, cleanPath)
}

func (f *FileLoader) AddJSONPath(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cleanPath := pathutil.Clean(path)
	for _, p := range f.jsonPaths {
		if p == cleanPath {
			return
		}
	}
	f.jsonPaths = append(f.jsonPaths, cleanPath)
}

func (f *FileLoader) Namespaces() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make(map[string]string, len(f.hints))
	for k, v := range f.hints {
		result[k] = v
	}
	return result
}

func (f *FileLoader) Paths() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]string, len(f.paths))
	copy(result, f.paths)
	return result
}

func (f *FileLoader) JSONPaths() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]string, len(f.jsonPaths))
	copy(result, f.jsonPaths)
	return result
}

func (f *FileLoader) getAllPaths() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// 返回切片副本以避免数据竞争
	result := make([]string, len(f.paths))
	copy(result, f.paths)
	return result
}

func (f *FileLoader) getAllJSONPaths() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// 返回切片副本以避免数据竞争
	result := make([]string, len(f.jsonPaths))
	copy(result, f.jsonPaths)
	return result
}
