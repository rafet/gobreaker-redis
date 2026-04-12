package gobreaker

import (
	"context"
	"time"
	"sync"
	"testing"
)

func TestForceOpen_ObserverReceivesTransition(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "force-obs", Observer: obs})
	_ = cb.ForceOpen(context.Background())
	_, _, changes := obs.snapshot()
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	if changes[0].from != StateClosed || changes[0].to != StateOpen {
		t.Errorf("change: %v -> %v", changes[0].from, changes[0].to)
	}
}

func TestReset_ClearsEverythingFromHalfOpen(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:    "reset-ho",
		Timeout: time.Second,
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	clock.Advance(2 * time.Second)
	// Now half-open after timeout. A single success may close it.
	_ = succeed(t, cb)
	_ = cb.Reset(context.Background())
	state, _ := cb.State(context.Background())
	if state != StateClosed {
		t.Errorf("state after Reset from half-open = %v, want closed", state)
	}
}

func TestForceClose_ConcurrentWithExecute(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "force-race"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = cb.ForceOpen(context.Background())
			_ = cb.ForceClosed(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
				return nil, nil
			})
		}
	}()
	wg.Wait()
	// No race, no panic — that's the assertion.
}

func TestReset_ConcurrentSafety(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "reset-race"})
	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = cb.Reset(context.Background())
				_ = succeed(t, cb)
			}
		}()
	}
	wg.Wait()
}
