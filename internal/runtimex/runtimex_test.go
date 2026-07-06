package runtimex_test

import (
	"testing"

	"github.com/prismgo/framework/internal/runtimex"
)

func TestGoroutineID_ReturnsPositiveID(t *testing.T) {
	id := runtimex.GoroutineID()
	if id == 0 {
		t.Fatal("GoroutineID() returned 0, expected a positive goroutine ID")
	}
}

func TestGoroutineID_UniquePerGoroutine(t *testing.T) {
	ch := make(chan uint64, 1)
	go func() {
		ch <- runtimex.GoroutineID()
	}()
	id2 := <-ch
	id1 := runtimex.GoroutineID()
	if id1 == id2 {
		t.Fatalf("main goroutine ID %d == child goroutine ID %d, expected different IDs", id1, id2)
	}
}
