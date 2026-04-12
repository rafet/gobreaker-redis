package gobreaker

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// adversarial_test.go contains tests designed to break things — not
// normal usage patterns but hostile edge cases that an attacker or a
// very unlucky production event might produce.

// TestAdversarial_ExecuteDuringForceOpenLoop verifies that rapid
// ForceOpen/ForceClosed cycling during Execute doesn't corrupt state.
func TestAdversarial_ExecuteDuringForceOpenLoop(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "adv-force"})
	var wg sync.WaitGroup
	wg.Add(3)

	// Writer 1: Execute loop
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
				return nil, nil
			})
		}
	}()

	// Writer 2: ForceOpen/Close cycle
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_ = cb.ForceOpen(context.Background())
			_ = cb.ForceClosed(context.Background())
		}
	}()

	// Writer 3: UpdateSettings
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			cb.UpdateSettings(func(s *Settings) {
				s.Timeout = time.Duration(i+1) * time.Second
			})
		}
	}()

	wg.Wait()

	// Verify state is internally consistent.
	state, err := cb.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !state.IsValid() {
		t.Errorf("invalid state: %v", state)
	}
	c, _ := cb.Counts(context.Background())
	sum := c.TotalSuccesses + c.TotalFailures + c.TotalExclusions + c.InFlights
	if sum > c.Requests {
		t.Errorf("outcome sum %d > Requests %d", sum, c.Requests)
	}
}

// TestAdversarial_DedupWithPanicAndSuccess verifies that a panic in
// one dedup call doesn't corrupt the next one on the same key.
func TestAdversarial_DedupWithPanicAndSuccess(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "adv-dedup"})
	d := NewDeduplicator[any](cb)

	// First call: panic.
	func() {
		defer func() { _ = recover() }()
		_, _ = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
			panic("boom")
		})
	}()

	// Key should be cleaned up. Second call should work normally.
	result, err := d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("second call after panic: err = %v", err)
	}
	if result != "ok" {
		t.Errorf("result = %v, want ok", result)
	}
}

// TestAdversarial_GroupReapDuringEveryOperation verifies that Reap
// doesn't corrupt the map when called during Get, Execute, Delete.
func TestAdversarial_GroupReapDuringEveryOperation(t *testing.T) {
	g, _ := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "adv-group"},
		MaxIdle:  time.Millisecond,
		MaxSize:  5,
	})
	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = g.Get(context.Background(), string(rune('a'+(i%26))))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = g.Execute(context.Background(), string(rune('a'+(i%26))), func(_ context.Context) (any, error) {
				return nil, nil
			})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			g.Delete(string(rune('a' + (i % 26))))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			g.Reap()
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	// No panic, no race = pass.
}

// TestAdversarial_PipelineTimeoutShorterThanRetryDelay verifies that
// the timeout cuts off retries cleanly.
func TestAdversarial_PipelineTimeoutShorterThanRetryDelay(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "adv-pipe",
		ReadyToOpen: ConsecutiveFailures(10000),
	})
	var calls int64
	p := Compose[any](cb).
		WithTimeout(50 * time.Millisecond).
		WithRetry(100, 200*time.Millisecond). // retryDelay > timeout
		Build()

	_, err := p.Execute(context.Background(), func(ctx context.Context) (any, error) {
		atomic.AddInt64(&calls, 1)
		return nil, errors.New("fail")
	})
	if err == nil {
		t.Error("expected error")
	}
	got := atomic.LoadInt64(&calls)
	// Timeout should cut off after 1-2 attempts (50ms timeout < 200ms retry delay).
	if got > 3 {
		t.Errorf("calls = %d, timeout should have cut retries short", got)
	}
}

// TestAdversarial_HedgeWithZeroDelay verifies Hedge with 0 delay
// dispatches both requests immediately.
func TestAdversarial_HedgeWithZeroDelay(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "adv-hedge"})
	var calls int64
	_, _ = Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			select {
			case <-time.After(50 * time.Millisecond):
				return "ok", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		HedgeDelay(0), // immediate hedge
	)
	got := atomic.LoadInt64(&calls)
	if got < 2 {
		t.Errorf("calls = %d with HedgeDelay(0), want >= 2", got)
	}
}

// TestAdversarial_LatencyTrackerOverflow verifies the tracker handles
// extreme latency values without overflow or panic.
func TestAdversarial_LatencyTrackerOverflow(t *testing.T) {
	lt := NewLatencyTracker(10)
	extremes := []time.Duration{
		0,
		time.Nanosecond,
		time.Hour * 24 * 365, // 1 year
		time.Duration(1<<62),  // near max int64
		-time.Second,          // negative
	}
	for _, d := range extremes {
		lt.OnOutcome("x", OutcomeSuccess, d) // must not panic
	}
	// Percentile must not panic.
	_ = lt.Percentile(0.99)
	_ = lt.Percentile(0)
	_ = lt.Percentile(1)
}

// TestAdversarial_StoreClosedDuringExecute verifies that closing the
// LocalStore while Execute is in progress doesn't panic.
func TestAdversarial_StoreClosedDuringExecute(t *testing.T) {
	store := NewLocalStore()
	cb, _ := New[any](context.Background(), Settings{
		Name:  "adv-close",
		Store: store,
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
				return nil, nil
			})
		}
	}()
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		_ = store.Close()
	}()
	wg.Wait()
	// No panic = pass. Errors are expected.
}

// TestAdversarial_RapidStateTransitions trips and recovers the
// breaker as fast as possible and verifies state consistency.
func TestAdversarial_RapidStateTransitions(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:        "adv-rapid",
		Timeout:     time.Millisecond,
		ReadyToOpen: ConsecutiveFailures(1),
	})
	for i := 0; i < 1000; i++ {
		// Trip.
		_ = failBreaker(t, cb)
		// Recover.
		clock.Advance(2 * time.Millisecond)
		_ = succeed(t, cb)
	}
	// State must be valid.
	state, _ := cb.State(context.Background())
	if !state.IsValid() {
		t.Errorf("invalid state after 1000 rapid transitions: %v", state)
	}
}

// TestAdversarial_GoroutineCount verifies that after heavy concurrent
// usage, no goroutines are leaked.
func TestAdversarial_GoroutineCount(t *testing.T) {
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	cb, _ := newTestBreaker(t, Settings{Name: "adv-goroutine"})
	d := NewDeduplicator[any](cb)

	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
					return nil, nil
				})
				_, _ = Hedge(context.Background(), cb,
					func(_ context.Context) (any, error) { return nil, nil },
					HedgeDelay(time.Microsecond),
				)
			}
		}()
	}
	wg.Wait()

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	after := runtime.NumGoroutine()

	if delta := after - before; delta > 5 {
		t.Errorf("goroutine leak: before=%d after=%d delta=%d", before, after, delta)
	}
}

// TestAdversarial_SettingsNilPredicates_NoPanicOnConstruct verifies
// that passing nil for predicate fields doesn't panic during New —
// defaults should be applied.
func TestAdversarial_SettingsNilPredicates(t *testing.T) {
	cb, err := New[any](context.Background(), Settings{
		Name:          "adv-nil",
		ReadyToOpen:   nil,
		ReadyToClose:  nil,
		ReadyToReopen: nil,
		IsSuccessful:  nil,
		IsExcluded:    nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Execute should work with defaults.
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, nil
	})
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, errors.New("fail")
	})
}
