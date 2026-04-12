package gobreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// orderingObserver records the sequence of Observer method names to
// verify the call order invariant: OnRequest → OnOutcome → (optional)
// OnStateChange.
type orderingObserver struct {
	mu    sync.Mutex
	calls []string
}

func (o *orderingObserver) OnRequest(_ string, _ bool, _ State) {
	o.mu.Lock()
	o.calls = append(o.calls, "request")
	o.mu.Unlock()
}

func (o *orderingObserver) OnOutcome(_ string, _ Outcome, _ time.Duration) {
	o.mu.Lock()
	o.calls = append(o.calls, "outcome")
	o.mu.Unlock()
}

func (o *orderingObserver) OnStateChange(_ string, _, _ State, _ Counts) {
	o.mu.Lock()
	o.calls = append(o.calls, "statechange")
	o.mu.Unlock()
}

func (o *orderingObserver) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.calls...)
}

func TestObserverOrder_SuccessPath(t *testing.T) {
	obs := &orderingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "order-ok", Observer: obs})
	_ = succeed(t, cb)
	got := obs.snapshot()
	want := []string{"request", "outcome"}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestObserverOrder_FailureTrip(t *testing.T) {
	obs := &orderingObserver{}
	cb, _ := newTestBreaker(t, Settings{
		Name:        "order-trip",
		Observer:    obs,
		ReadyToOpen: ConsecutiveFailures(1),
	})
	_ = failBreaker(t, cb)
	got := obs.snapshot()
	// Expected: request → outcome → statechange (closed→open)
	want := []string{"request", "outcome", "statechange"}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestObserverOrder_RejectionPath(t *testing.T) {
	obs := &orderingObserver{}
	cb, _ := newTestBreaker(t, Settings{
		Name:        "order-reject",
		Observer:    obs,
		ReadyToOpen: ConsecutiveFailures(1),
	})
	_ = failBreaker(t, cb) // trip
	obs.mu.Lock()
	obs.calls = nil // reset
	obs.mu.Unlock()

	_ = succeed(t, cb) // rejected
	got := obs.snapshot()
	// Rejection: only OnRequest(admitted=false), no OnOutcome.
	want := []string{"request"}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

func TestObserverOrder_Pipeline(t *testing.T) {
	obs := &orderingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "order-pipe", Observer: obs})
	p := Compose[any](cb).Build()
	_, _ = p.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, nil
	})
	got := obs.snapshot()
	// Pipeline wraps Execute, so order should be: request → outcome.
	if len(got) < 2 || got[0] != "request" || got[1] != "outcome" {
		t.Errorf("pipeline observer order = %v, want [request outcome ...]", got)
	}
}

func TestObserverOrder_Hedge_PrimaryFast(t *testing.T) {
	obs := &orderingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "order-hedge", Observer: obs})
	_, _ = Hedge(context.Background(), cb,
		func(_ context.Context) (any, error) { return nil, nil },
		HedgeDelay(time.Hour),
	)
	got := obs.snapshot()
	// Primary finishes fast, no hedge dispatched.
	if len(got) < 2 || got[0] != "request" || got[1] != "outcome" {
		t.Errorf("hedge observer order = %v, want [request outcome]", got)
	}
}

func TestObserverOrder_ForceOpen(t *testing.T) {
	obs := &orderingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "order-force", Observer: obs})
	_ = cb.ForceOpen(context.Background())
	got := obs.snapshot()
	// ForceOpen fires OnStateChange.
	if len(got) != 1 || got[0] != "statechange" {
		t.Errorf("ForceOpen observer = %v, want [statechange]", got)
	}
}

func TestObserverOrder_ExclusionPath(t *testing.T) {
	obs := &orderingObserver{}
	cb, _ := newTestBreaker(t, Settings{
		Name:       "order-excl",
		Observer:   obs,
		IsExcluded: IgnoreContextErrors,
	})
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, context.Canceled
	})
	got := obs.snapshot()
	// request → outcome (exclusion, not statechange)
	want := []string{"request", "outcome"}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

var _ = errors.New // keep import
