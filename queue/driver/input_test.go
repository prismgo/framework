package driver

import (
	"reflect"
	"testing"

	queuecontract "github.com/prismgo/framework/contracts/queue"
)

func TestNormalizeQueuesAndPopWaitMode(t *testing.T) {
	want := []string{"high", "low"}
	if got := NormalizeQueues([]string{" high ", "", "low", "high"}, "default"); !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeQueues() = %#v, want %#v", got, want)
	}
	if got := NormalizeQueues(nil, " fallback "); !reflect.DeepEqual(got, []string{"fallback"}) {
		t.Fatalf("NormalizeQueues() fallback = %#v, want %#v", got, []string{"fallback"})
	}
	if got := NormalizePopWaitMode(nil); got != queuecontract.PopWaitAvailable {
		t.Fatalf("NormalizePopWaitMode(nil) = %v, want %v", got, queuecontract.PopWaitAvailable)
	}
	if got := NormalizePopWaitMode([]queuecontract.PopWaitMode{queuecontract.PopNoWait}); got != queuecontract.PopNoWait {
		t.Fatalf("NormalizePopWaitMode(explicit) = %v, want %v", got, queuecontract.PopNoWait)
	}
}
