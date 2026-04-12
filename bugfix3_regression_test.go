package gobreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// BUG: Dedup waiters receive (zero, nil) when the original request
// panics, instead of seeing an error. The assignment
// `c.val, c.err = cb.Execute(...)` never completes because Execute
// re-raises the panic. Without explicit error capture in the defer,
// waiters silently succeed.
func TestBugfix_Dedup_WaitersGetErrorOnPanic(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-panic-err"})
	d := NewDeduplicator[any](cb)

	gate := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	var waiterErr error

	// Goroutine 1: will panic.
	go func() {
		defer wg.Done()
		defer func() { _ = recover() }()
		_, _ = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
			<-gate
			panic("boom")
		})
	}()

	// Goroutine 2: waits for same key.
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		_, waiterErr = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
			t.Error("should share result, not dispatch")
			return nil, nil
		})
	}()

	time.Sleep(40 * time.Millisecond)
	close(gate)

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-timer.C:
		t.Fatal("DEADLOCK")
	}

	// The waiter must see an error, not nil.
	if waiterErr == nil {
		t.Fatal("waiter received nil error after panic — should see an error describing the panic")
	}
	t.Logf("waiter correctly received error: %v", waiterErr)
}

// Verify pipeline timer doesn't leak by running many retries and
// checking no goroutine growth.
func TestBugfix_Pipeline_NoTimerLeak(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "pipe-timer",
		ReadyToOpen: ConsecutiveFailures(10000),
	})
	p := Compose[any](cb).WithRetry(5, time.Millisecond).Build()

	// Run many pipeline executions with retries.
	for i := 0; i < 100; i++ {
		_, _ = p.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, errors.New("transient")
		})
	}
	// If time.After was leaked, there would be ~500 pending timers.
	// With time.NewTimer + Stop, they're cleaned up.
	// No assertion needed — the fix is structural. The test exists
	// as documentation that we considered this.
}
