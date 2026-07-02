package translation

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
)

// setupTranslationTestContainer 创建测试所需的容器环境，包含 logger 和 exception handler。
func setupTranslationTestContainer(t *testing.T) {
	t.Helper()

	registry := container.NewContainer()
	container.SetProvider(func() *container.Container {
		return registry
	})
	t.Cleanup(func() {
		container.SetProvider(nil)
	})

	// 创建并绑定 logger manager
	mgr, err := logger.NewManager(logger.Config{
		Default: "null",
		Channels: map[string]logger.ChannelOptions{
			"null": {Driver: "null"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", mgr); err != nil {
		t.Fatalf("Failed to bind logger manager: %v", err)
	}

	// 创建并绑定 exception handler
	handler := exception.New()
	if err := registry.Instance("exception.handler", handler); err != nil {
		t.Fatalf("Failed to bind exception handler: %v", err)
	}
}

func TestFileLoaderJSONParseError(t *testing.T) {
	setupTranslationTestContainer(t)

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

	// JSON 解析错误应该通过 exception.Report 上报，而不是返回错误
	result, err := loader.Load("en", "", "")
	if err != nil {
		t.Errorf("Expected no error returned, got: %v", err)
	}
	if result == nil {
		t.Error("Expected empty map, got nil")
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
