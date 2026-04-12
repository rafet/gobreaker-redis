package gobreaker

import (
	"context"
	"sync"
	"testing"
	"time"
)

// BUG1: Dedup ExecuteDedup panic → deadlock.
// Before the fix, a panic in the wrapped function caused wg.Done()
// to be skipped, deadlocking all waiting goroutines forever.
func TestBugfix_Dedup_PanicDoesNotDeadlock(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-panic"})
	d := NewDeduplicator[any](cb)

	gate := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: will panic.
	go func() {
		defer wg.Done()
		defer func() { _ = recover() }()
		_, _ = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
			<-gate
			panic("boom")
		})
	}()

	// Goroutine 2: waits for the same key. Before the fix, this
	// would deadlock because wg.Done() was never called after the panic.
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		// Give goroutine 1 time to register the key.
		time.Sleep(20 * time.Millisecond)
		_, _ = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
			t.Error("second call should share the first's result, not dispatch")
			return nil, nil
		})
		close(done)
	}()

	// Release the panic.
	time.Sleep(30 * time.Millisecond)
	close(gate)

	// If the bug is present, this will hang forever. Use a timeout
	// as a safety net.
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
		// OK: goroutine 2 completed.
	case <-timer.C:
		t.Fatal("DEADLOCK: goroutine 2 is stuck waiting on wg.Wait() because panic skipped wg.Done()")
	}

	wg.Wait()

	// Verify the key was cleaned up.
	d.mu.Lock()
	n := len(d.in)
	d.mu.Unlock()
	if n != 0 {
		t.Errorf("dedup in-flight map has %d entries after panic, want 0 (key should be cleaned up)", n)
	}
}

// BUG2: ForceOpen/ForceClosed generic path did not fire OnStateChange.
func TestBugfix_ForceOpen_GenericPath_FiresCallback(t *testing.T) {
	var transitions int
	cb, err := New[any](context.Background(), Settings{
		Name:           "force-generic",
		Store:          failingStore{},
		OnStoreFailure: FallbackToLocal,
		OnStateChange: func(_ string, _, _ State, _ Counts) {
			transitions++
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// This breaker uses the generic (non-fast) path because the store
	// is not a *LocalStore (it's failingStore with FallbackToLocal).
	_ = cb.ForceOpen(context.Background())
	if transitions != 1 {
		t.Errorf("transitions = %d, want 1 (generic-path ForceOpen should fire OnStateChange)", transitions)
	}
}

// BUG3: ForceOpen generic-path closure read cb.settings.Timeout
// without lock, racing with concurrent UpdateSettings.
func TestBugfix_ForceOpen_GenericPath_NoSettingsRace(t *testing.T) {
	cb, err := New[any](context.Background(), Settings{
		Name:           "force-race",
		Store:          failingStore{},
		OnStoreFailure: FallbackToLocal,
	})
	if err != nil {
		t.Fatal(err)
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
			cb.UpdateSettings(func(s *Settings) {
				s.Timeout = time.Duration(i) * time.Millisecond
			})
		}
	}()
	wg.Wait()
	// No race detector error = pass.
}

// BUG4: fireStateChangesSlice read cb.settings without lock.
// Now it takes Settings as a parameter, captured under lock.
func TestBugfix_FireStateChangesSlice_NoSettingsRace(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fire-race"})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = failBreaker(t, cb)
			_ = cb.Reset(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			cb.UpdateSettings(func(s *Settings) {
				s.Timeout = time.Duration(i) * time.Millisecond
			})
		}
	}()
	wg.Wait()
}
