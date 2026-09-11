package driver

import (
	"errors"
	"testing"
)

func TestQueueDriverSentinelsRemainMatchableWhenWrapped(t *testing.T) {
	if !errors.Is(errors.Join(errors.New("adapter context"), ErrEmpty), ErrEmpty) {
		t.Fatal("wrapped empty error should match ErrEmpty")
	}
	if !errors.Is(errors.Join(errors.New("decode context"), ErrPoisonEnvelope), ErrPoisonEnvelope) {
		t.Fatal("wrapped poison error should match ErrPoisonEnvelope")
	}
}
