package gobreaker

import (
	"context"
	"errors"
	"testing"
)

func TestForceOpen_FromClosed(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "force-open"})
	if err := cb.ForceOpen(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ := cb.State(context.Background())
	if state != StateOpen {
		t.Errorf("state = %v, want open", state)
	}
	// Requests should be rejected.
	_, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		t.Error("should not run")
		return nil, nil
	})
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("err = %v, want ErrOpenState", err)
	}
}

func TestForceOpen_AlreadyOpen(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "force-open-2"})
	_ = cb.ForceOpen(context.Background())
	if err := cb.ForceOpen(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ := cb.State(context.Background())
	if state != StateOpen {
		t.Errorf("state = %v, want open", state)
	}
}

func TestForceClosed_FromOpen(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "force-close"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	if stateOf(t, cb) != StateOpen {
		t.Fatal("setup: expected open")
	}
	if err := cb.ForceClosed(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ := cb.State(context.Background())
	if state != StateClosed {
		t.Errorf("state = %v, want closed", state)
	}
	// Requests should pass through again.
	if err := succeed(t, cb); err != nil {
		t.Errorf("Execute after ForceClosed: %v", err)
	}
}

func TestForceClosed_AlreadyClosed(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "force-close-2"})
	if err := cb.ForceClosed(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReset_ClearsCountsAndState(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "reset"})
	// Accumulate some counts.
	for i := 0; i < 3; i++ {
		_ = failBreaker(t, cb)
	}
	c, _ := cb.Counts(context.Background())
	if c.ConsecutiveFailures != 3 {
		t.Fatalf("setup: ConsecutiveFailures = %d", c.ConsecutiveFailures)
	}

	if err := cb.Reset(context.Background()); err != nil {
		t.Fatal(err)
	}

	state, _ := cb.State(context.Background())
	if state != StateClosed {
		t.Errorf("state after Reset = %v, want closed", state)
	}
	c, _ = cb.Counts(context.Background())
	if c != (Counts{}) {
		t.Errorf("Counts after Reset = %+v, want zero", c)
	}
}

func TestReset_FromOpen(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "reset-open"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	if stateOf(t, cb) != StateOpen {
		t.Fatal("setup: expected open")
	}
	if err := cb.Reset(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ := cb.State(context.Background())
	if state != StateClosed {
		t.Errorf("state = %v, want closed", state)
	}
	// Should be able to execute immediately.
	if err := succeed(t, cb); err != nil {
		t.Errorf("Execute after Reset: %v", err)
	}
}

func TestForceOpen_FiresOnStateChange(t *testing.T) {
	var changes int
	cb, _ := newTestBreaker(t, Settings{
		Name: "force-callback",
		OnStateChange: func(_ string, _, _ State, _ Counts) {
			changes++
		},
	})
	_ = cb.ForceOpen(context.Background())
	if changes != 1 {
		t.Errorf("OnStateChange called %d times, want 1", changes)
	}
	// Forcing same state again should NOT fire callback.
	_ = cb.ForceOpen(context.Background())
	if changes != 1 {
		t.Errorf("OnStateChange called %d times after same-state force, want 1", changes)
	}
}

func TestForceClosed_FiresOnStateChange(t *testing.T) {
	var changes int
	cb, _ := newTestBreaker(t, Settings{
		Name: "force-callback-2",
		OnStateChange: func(_ string, _, _ State, _ Counts) {
			changes++
		},
	})
	_ = cb.ForceOpen(context.Background())
	_ = cb.ForceClosed(context.Background())
	if changes != 2 {
		t.Errorf("OnStateChange called %d times, want 2 (open + close)", changes)
	}
}

func TestReset_DoesNotFireCallbackIfAlreadyClosed(t *testing.T) {
	var changes int
	cb, _ := newTestBreaker(t, Settings{
		Name: "reset-no-cb",
		OnStateChange: func(_ string, _, _ State, _ Counts) {
			changes++
		},
	})
	_ = cb.Reset(context.Background())
	// Reset from closed → closed: same state, no callback.
	if changes != 0 {
		t.Errorf("OnStateChange called %d times on Reset(closed→closed), want 0", changes)
	}
}
