package translation

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFileLoaderJSONParseError(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "translation_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if removeErr := os.RemoveAll(tmpDir); removeErr != nil {
			t.Errorf("Failed to remove temp dir: %v", removeErr)
		}
	}()

	// 创建无效的 JSON 文件
	invalidJSON := `{invalid json}`
	jsonPath := filepath.Join(tmpDir, "en.json")
	if err := os.WriteFile(jsonPath, []byte(invalidJSON), 0644); err != nil {
		t.Fatalf("Failed to write invalid JSON file: %v", err)
	}

	loader := NewFileLoader()
	loader.AddJSONPath(tmpDir)

	// 任务 2：JSON 解析错误应该返回错误，而不是吞掉
	_, err = loader.Load("en", "", "")
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestFileLoaderEmptyGroup(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "translation_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if removeErr := os.RemoveAll(tmpDir); removeErr != nil {
			t.Errorf("Failed to remove temp dir: %v", removeErr)
		}
	}()

	loader := NewFileLoader()
	loader.AddPath(tmpDir)

	// 任务 3：group 为空时应该静默返回空 map，而不是返回错误
	result, err := loader.Load("en", "", "")
	if err != nil {
		t.Errorf("Expected no error for empty group, got: %v", err)
	}
	if result == nil {
		t.Error("Expected empty map for empty group, got nil")
	}
	if len(result) != 0 {
		t.Errorf("Expected empty map, got %d items", len(result))
	}
}

func TestFileLoaderConcurrentHintsAccess(t *testing.T) {
	loader := NewFileLoader()

	var wg sync.WaitGroup
	const goroutines = 10
	const iterations = 100

	// 并发添加 namespace
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				loader.AddNamespace(fmt.Sprintf("ns_%d_%d", id, j), "/hint")
			}
		}(i)
	}

	// 并发加载 namespaced group
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = loader.Load("en", "messages", fmt.Sprintf("ns_%d_%d", id, j))
			}
		}(i)
	}

	wg.Wait()
}
