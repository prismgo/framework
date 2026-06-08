package helper

import (
	"reflect"
	"testing"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

func TestNormalizeQueues(t *testing.T) {
	tests := []struct {
		name         string
		queues       []string
		defaultQueue string
		want         []string
	}{
		{name: "empty list falls back to default", queues: nil, defaultQueue: "default", want: []string{"default"}},
		{name: "blank list falls back to default", queues: []string{"", " \t "}, defaultQueue: "default", want: []string{"default"}},
		{name: "blank entries are removed", queues: []string{" ", "jobs", ""}, defaultQueue: "default", want: []string{"jobs"}},
		{name: "duplicates keep first occurrence", queues: []string{"high", "low", "high"}, defaultQueue: "default", want: []string{"high", "low"}},
		{name: "default queue is trimmed", queues: nil, defaultQueue: "  fallback  ", want: []string{"fallback"}},
		{name: "blank default queue uses default", queues: nil, defaultQueue: " ", want: []string{"default"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 逻辑说明：直接验证公共内部 helper 的输入输出，driver 和 worker 只负责传入默认队列。
			got := NormalizeQueues(tt.queues, tt.defaultQueue)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("NormalizeQueues()=%#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNormalizePopWaitMode(t *testing.T) {
	if got := NormalizePopWaitMode(nil); got != queuecontract.PopWaitAvailable {
		t.Fatalf("empty wait mode=%v, want PopWaitAvailable", got)
	}
	if got := NormalizePopWaitMode([]queuecontract.PopWaitMode{queuecontract.PopNoWait}); got != queuecontract.PopNoWait {
		t.Fatalf("explicit wait mode=%v, want PopNoWait", got)
	}
}
