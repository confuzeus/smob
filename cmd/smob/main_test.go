package main

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestAwaitShutdown proves the two-signal contract: the first signal cancels the
// command context and keeps waiting, the second returns exit code 1 immediately.
func TestAwaitShutdown(t *testing.T) {
	sigs := make(chan os.Signal, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan int, 1)
	go func() { done <- awaitShutdown(sigs, cancel) }()

	sigs <- os.Interrupt
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("first signal did not cancel the command context")
	}
	select {
	case code := <-done:
		t.Fatalf("awaitShutdown returned %d after one signal, want it to keep waiting", code)
	default:
	}

	sigs <- syscall.SIGTERM
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("exit code after the second signal = %d, want 1", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second signal did not return an exit code")
	}
}
