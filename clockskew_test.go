package gobreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// clockskew_test.go contains tests that simulate non-monotonic clocks
// and inter-process clock skew. Distributed circuit breakers depend on
// time.Now() agreeing across replicas; if it disagrees by enough, the
// state machine can transition incorrectly. These tests pin down the
// behavior under each anomaly so future regressions are caught.

// skewClock is a controllable clock that can be moved forward AND
// backward. It is used to simulate NTP step-changes and inter-replica
// skew where the apparent time on one node is ahead or behind the
// shared store's notion of time.
type skewClock struct {
	mu sync.Mutex
	t  time.Time
}

func newSkewClock(start time.Time) *skewClock {
	return &skewClock{t: start}
}

func (c *skewClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *skewClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func (c *skewClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestClockSkew_BackwardJumpDoesNotPanicOpenState verifies that the
// breaker survives a backward time step (NTP correction) while in the
// open state. The expiry was set with the old clock; the new clock
// observes a "now" that is BEFORE the expiry, which should keep the
// breaker open without crashing or producing nonsensical state.
func TestClockSkew_BackwardJumpDoesNotPanicOpenState(t *testing.T) {
	clock := newSkewClock(time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC))
	store := NewLocalStore()
	store.setClock(clock.Now)

	cb, err := New[any](context.Background(), Settings{
		Name:        "skew-back",
		Store:       store,
		Timeout:     time.Minute,
		ReadyToOpen: ConsecutiveFailures(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.setClock(clock.Now)

	// Trip the breaker.
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, errors.New("trip")
	})
	if state := stateOf(t, cb); state != StateOpen {
		t.Fatalf("setup: state = %v, want open", state)
	}

	// Step time backwards by 5 minutes (NTP correction).
	clock.Set(clock.Now().Add(-5 * time.Minute))

	// State observation should still be open — the expiry is now in
	// the future even though we just stepped backwards. Crucially:
	// no panic, no transition.
	if state := stateOf(t, cb); state != StateOpen {
		t.Errorf("after backward step: state = %v, want open", state)
	}
	// Execute should reject as well.
	_, err = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		t.Error("wrapped function should not run after backward step")
		return nil, nil
	})
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("Execute err = %v, want ErrOpenState", err)
	}
}

// TestClockSkew_ForwardJumpEntersHalfOpen verifies the dual: a
// forward time jump that crosses the open expiry causes the next
// admission to transition into half-open, which is the desired
// behavior (the breaker recovers as if the timeout had naturally
// elapsed).
func TestClockSkew_ForwardJumpEntersHalfOpen(t *testing.T) {
	clock := newSkewClock(time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC))
	store := NewLocalStore()
	store.setClock(clock.Now)

	cb, err := New[any](context.Background(), Settings{
		Name:        "skew-fwd",
		Store:       store,
		Timeout:     time.Minute,
		ReadyToOpen: ConsecutiveFailures(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.setClock(clock.Now)

	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, errors.New("trip")
	})
	if state := stateOf(t, cb); state != StateOpen {
		t.Fatalf("setup: state = %v", state)
	}

	// Jump forward by 1 hour. This should leave us well past the
	// 1-minute Timeout.
	clock.Advance(time.Hour)

	// Next admission attempt should succeed (and return to closed
	// because the default ReadyToClose fires on a single success
	// when HalfOpenMaxInFlights == 1).
	if _, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return "ok", nil
	}); err != nil {
		t.Errorf("post-jump Execute: %v", err)
	}
	if state := stateOf(t, cb); state != StateClosed {
		t.Errorf("after recovery: state = %v, want closed", state)
	}
}

// TestClockSkew_NowEqualsExpiryEntersHalfOpen verifies the boundary
// where now == expiry. The state machine uses `!now.Before(expiry)`
// (so equality is treated as "expired"). This documents that choice
// and guards against an off-by-one mutation.
func TestClockSkew_NowEqualsExpiryEntersHalfOpen(t *testing.T) {
	clock := newSkewClock(time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC))
	store := NewLocalStore()
	store.setClock(clock.Now)

	cb, err := New[any](context.Background(), Settings{
		Name:        "skew-eq",
		Store:       store,
		Timeout:     time.Minute,
		ReadyToOpen: ConsecutiveFailures(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.setClock(clock.Now)

	tripTime := clock.Now()
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, errors.New("trip")
	})
	// Move clock to exactly the expiry instant.
	clock.Set(tripTime.Add(time.Minute))

	if _, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return "ok", nil
	}); err != nil {
		t.Errorf("at expiry: Execute = %v, want success (boundary inclusive)", err)
	}
}

// TestClockSkew_NowOneNanoBeforeExpiryStaysOpen is the symmetric
// boundary check: one nanosecond before expiry must remain open.
func TestClockSkew_NowOneNanoBeforeExpiryStaysOpen(t *testing.T) {
	clock := newSkewClock(time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC))
	store := NewLocalStore()
	store.setClock(clock.Now)

	cb, err := New[any](context.Background(), Settings{
		Name:        "skew-pre",
		Store:       store,
		Timeout:     time.Minute,
		ReadyToOpen: ConsecutiveFailures(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.setClock(clock.Now)

	tripTime := clock.Now()
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, errors.New("trip")
	})
	// Move clock to exactly one nanosecond before expiry.
	clock.Set(tripTime.Add(time.Minute - time.Nanosecond))

	_, err = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		t.Error("wrapped function should not run one ns before expiry")
		return nil, nil
	})
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("err = %v, want ErrOpenState", err)
	}
}

// TestClockSkew_IntervalRolloverWithBackwardJump verifies the closed
// state's interval-based generation rollover survives a backward jump.
// The interval should NOT roll over until the clock once again exceeds
// the original expiry.
func TestClockSkew_IntervalRolloverWithBackwardJump(t *testing.T) {
	clock := newSkewClock(time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC))
	store := NewLocalStore()
	store.setClock(clock.Now)

	cb, err := New[any](context.Background(), Settings{
		Name:     "skew-interval",
		Store:    store,
		Interval: 10 * time.Second,
		// Never trip during this test.
		ReadyToOpen: ConsecutiveFailures(1 << 30),
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.setClock(clock.Now)

	// Three failures before the backward jump.
	for i := 0; i < 3; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, errors.New("counted but not tripping")
		})
	}
	c, _ := cb.Counts(context.Background())
	if c.ConsecutiveFailures != 3 {
		t.Fatalf("setup: consec failures = %d", c.ConsecutiveFailures)
	}

	// Step backward by 5 seconds. The interval expiry is now in the
	// future, so no rollover should happen.
	clock.Advance(-5 * time.Second)
	if _, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return "ok", nil
	}); err != nil {
		t.Fatal(err)
	}
	c, _ = cb.Counts(context.Background())
	if c.ConsecutiveFailures != 0 {
		t.Errorf("after success: consec failures = %d, want 0", c.ConsecutiveFailures)
	}
	// And the rollover should not have fired.
	if c.TotalSuccesses != 1 {
		t.Errorf("TotalSuccesses = %d, want 1 (no premature rollover)", c.TotalSuccesses)
	}
}

// TestClockSkew_GenerationStartInFutureDoesNotCorrupt verifies that a
// snapshot whose GenerationStart appears to be in the future relative
// to now() is handled gracefully. This can happen when a replica
// reads a snapshot written by another replica whose clock is ahead.
func TestClockSkew_GenerationStartInFutureDoesNotCorrupt(t *testing.T) {
	clock := newSkewClock(time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC))
	store := NewLocalStore()
	store.setClock(clock.Now)

	cb, err := New[any](context.Background(), Settings{
		Name:        "skew-future",
		Store:       store,
		Timeout:     time.Minute,
		ReadyToOpen: ConsecutiveFailures(5),
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.setClock(clock.Now)

	// Run a normal request to materialize the snapshot.
	if _, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return "ok", nil
	}); err != nil {
		t.Fatal(err)
	}

	// Step the clock backwards by an hour, simulating a replica
	// reading a snapshot from a peer whose clock is ahead.
	clock.Advance(-time.Hour)

	// Subsequent operations should still work without panic and
	// without producing weird state.
	for i := 0; i < 5; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			return "ok", nil
		})
	}

	// State should still be closed; counts should reflect the
	// operations.
	if state := stateOf(t, cb); state != StateClosed {
		t.Errorf("state = %v, want closed", state)
	}
	c, _ := cb.Counts(context.Background())
	if c.TotalSuccesses < 5 {
		t.Errorf("TotalSuccesses = %d, want >= 5", c.TotalSuccesses)
	}
}
