package queue

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestArchitectureRemovesLegacyQueueSurfaces 用文本守卫覆盖本地 runtime retry contract 的删除型验收项。
//
// 需求背景：本次重构要求删除旧 public API、重复 DTO 和重复 codec；这些问题多数会在
// 编译层继续“可用”，因此需要一条明确的架构回归测试阻止旧入口被悄悄带回。
func TestArchitectureRemovesLegacyQueueSurfaces(t *testing.T) {
	root := filepath.Clean(".")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "DispatchEnvelope")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "installImmediateProcessor")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "queueProcessor")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "processor(")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "prepareAdvancedDispatch")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "encodeEnvelope")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "envelopeFromPayload")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "payloadForEnvelope")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "normalizeOptions")
	assertFileNotContains(t, filepath.Join(root, "manager.go"), "func (m *Manager) Extend(")
	assertFileNotContains(t, filepath.Join(root, "facade.go"), "func DispatchEnvelope(")
	assertFileNotContains(t, filepath.Join(root, "facade.go"), "Resolve().Extend(")
	assertFileNotContains(t, filepath.Join(root, "types.go"), "type Envelope =")
	assertFileNotContains(t, filepath.Join(root, "types.go"), "type PendingJob =")
	assertFileNotContains(t, filepath.Join(root, "types.go"), "type Payload =")
	assertFileNotContains(t, filepath.Join(root, "state_batch.go"), "type BatchStatus =")
	assertPathMissing(t, filepath.Join(root, "payload_encoder.go"))
	assertPathMissing(t, filepath.Join(root, "internal", "envelopecodec"))
	assertFileNotContains(t, filepath.Join(root, "rabbitmq", "types.go"), "type Envelope struct")
	assertFileNotContains(t, filepath.Join(root, "rabbitmq", "types.go"), "type PendingJob struct")
	assertPathMissing(t, filepath.Join(root, "rabbitmq", "json_codec.go"))
	assertFileNotContains(t, filepath.Join(root, "rabbitmq", "reserved_job.go"), "toRuntimeEnvelope")
	assertFileNotContains(t, filepath.Join(root, "rabbitmq", "reserved_job.go"), "fromRuntimeEnvelope")
	assertFileNotContains(t, filepath.Join(root, "redis", "reserved_job.go"), "func (j *RedisReservedJob) Envelope()")
	assertFileNotContains(t, filepath.Join(root, "rabbitmq", "reserved_job.go"), "func (j *RabbitMQReservedJob) Envelope()")
	assertPathMissing(t, filepath.Join(root, "processor.go"))
	assertFileNotContains(t, filepath.Join(root, "job_runner.go"), "type Processor")
	assertFileContains(t, filepath.Join(root, "job_runner.go"), "type JobRunner")
	assertFileContains(t, filepath.Join(root, "failure_handler.go"), "type FailureHandler")
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("%s should not exist", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", path, err)
	}
}

func assertFileNotContains(t *testing.T, path string, pattern string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if strings.Contains(string(body), pattern) {
		t.Fatalf("%s still contains %q", path, pattern)
	}
}

func assertFileContains(t *testing.T, path string, pattern string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(body), pattern) {
		t.Fatalf("%s should contain %q", path, pattern)
	}
}
