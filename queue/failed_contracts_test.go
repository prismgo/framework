package queue

import (
	"testing"

	eventcontract "github.com/prismgo/framework/contracts/event"
	"github.com/prismgo/framework/queue/payload"
)

var _ FailedStore = NewMemoryFailedStore()

// TestJobFailedName 验证失败任务生命周期事件由 queue.JobFailed 承担。
//
// 需求背景：payload.FailedJob 只负责 failed store 的持久化快照，事件层使用
// queue.JobFailed 包装该 DTO，避免归档结构和事件契约混在同一个类型上。
func TestJobFailedName(t *testing.T) {
	if got := (JobFailed{}).Name(); got != EventJobFailed {
		t.Fatalf("queue.JobFailed.Name() = %q, want %q", got, EventJobFailed)
	}
}

func TestPayloadFailedJobIsOnlyFailedStoreDTO(t *testing.T) {
	if _, ok := any(payload.FailedJob{}).(Event); ok {
		t.Fatal("payload.FailedJob must not implement queue.Event")
	}
	if _, ok := any(payload.FailedJob{}).(eventcontract.Event); ok {
		t.Fatal("payload.FailedJob must not implement event.Event")
	}
}

// TestJobFailedEmbedsPayloadFailedJob 验证事件 wrapper 不复制失败归档字段。
//
// 设计思路：监听器读取失败事件时仍可通过嵌入字段访问 FailedJob DTO 内容；
// 归档存储、查询和编解码继续消费 payload.FailedJob 本体。
func TestJobFailedEmbedsPayloadFailedJob(t *testing.T) {
	failed := JobFailed{FailedJob: payload.FailedJob{
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-1",
		JobName:    "ExampleJob",
		Error:      "boom",
		Tags:       []string{"critical"},
	}}

	var _ Event = failed
	var _ eventcontract.Event = failed

	if failed.Connection != "redis" || failed.Queue != "default" || failed.JobID != "job-1" || failed.JobName != "ExampleJob" {
		t.Fatalf("queue.JobFailed should expose embedded FailedJob fields, got %+v", failed)
	}
	if len(failed.Tags) != 1 || failed.Tags[0] != "critical" || failed.Error != "boom" {
		t.Fatalf("queue.JobFailed should preserve embedded FailedJob detail, got %+v", failed)
	}
}
