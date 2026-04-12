package gobreaker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestOutcomeString(t *testing.T) {
	cases := map[Outcome]string{
		OutcomeSuccess:   "success",
		OutcomeFailure:   "failure",
		OutcomeExclusion: "exclusion",
		OutcomeRejected:  "rejected",
		Outcome(99):      "unknown",
	}
	for o, want := range cases {
		if got := o.String(); got != want {
			t.Errorf("Outcome(%d).String() = %q, want %q", o, got, want)
		}
	}
}

func TestNopObserverImplementsObserver(t *testing.T) {
	// Compile-time assertion: NopObserver satisfies Observer.
	var _ Observer = NopObserver{}

	// Run-time check: NopObserver methods must be safe to call and
	// must not panic.
	var n NopObserver
	n.OnRequest("x", true, StateClosed)
	n.OnOutcome("x", OutcomeSuccess, time.Millisecond)
	n.OnStateChange("x", StateClosed, StateOpen, Counts{})
}

// recordingObserver captures every event so tests can assert on them.
type recordingObserver struct {
	mu       sync.Mutex
	requests []recordedRequest
	outcomes []recordedOutcome
	changes  []recordedChange
}

type recordedRequest struct {
	name     string
	admitted bool
	state    State
}

type recordedOutcome struct {
	name    string
	outcome Outcome
	latency time.Duration
}

type recordedChange struct {
	name   string
	from   State
	to     State
	counts Counts
}

func (o *recordingObserver) OnRequest(name string, admitted bool, state State) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, recordedRequest{name, admitted, state})
}

func (o *recordingObserver) OnOutcome(name string, outcome Outcome, latency time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.outcomes = append(o.outcomes, recordedOutcome{name, outcome, latency})
}

func (o *recordingObserver) OnStateChange(name string, from, to State, c Counts) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.changes = append(o.changes, recordedChange{name, from, to, c})
}

func (o *recordingObserver) snapshot() ([]recordedRequest, []recordedOutcome, []recordedChange) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]recordedRequest(nil), o.requests...),
		append([]recordedOutcome(nil), o.outcomes...),
		append([]recordedChange(nil), o.changes...)
}

func TestObserverReceivesAdmissionAndOutcome(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "obs", Observer: obs})
	if err := succeed(t, cb); err != nil {
		t.Fatal(err)
	}
	reqs, outs, _ := obs.snapshot()
	if len(reqs) != 1 || !reqs[0].admitted {
		t.Errorf("requests = %+v, want 1 admitted", reqs)
	}
	if len(outs) != 1 || outs[0].outcome != OutcomeSuccess {
		t.Errorf("outcomes = %+v, want 1 success", outs)
	}
}

func TestObserverReceivesFailureOutcome(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "obs-fail", Observer: obs})
	_ = failBreaker(t, cb)
	_, outs, _ := obs.snapshot()
	if len(outs) != 1 || outs[0].outcome != OutcomeFailure {
		t.Errorf("outcomes = %+v, want 1 failure", outs)
	}
}

func TestObserverReceivesExclusionOutcome(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{
		Name:       "obs-excl",
		IsExcluded: IgnoreContextErrors,
		Observer:   obs,
	})
	_, _ = cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
		return nil, context.Canceled
	})
	_, outs, _ := obs.snapshot()
	if len(outs) != 1 || outs[0].outcome != OutcomeExclusion {
		t.Errorf("outcomes = %+v, want 1 exclusion", outs)
	}
}

func TestObserverReceivesStateChange(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "obs-trip", Observer: obs})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	_, _, changes := obs.snapshot()
	if len(changes) != 1 {
		t.Fatalf("changes = %+v, want 1", changes)
	}
	if changes[0].from != StateClosed || changes[0].to != StateOpen {
		t.Errorf("change = %v->%v, want closed->open", changes[0].from, changes[0].to)
	}
	if changes[0].counts.ConsecutiveFailures != 5 {
		t.Errorf("change.counts.ConsecutiveFailures = %d, want 5", changes[0].counts.ConsecutiveFailures)
	}
}

func TestObserverRejectionLogged(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "obs-reject", Observer: obs})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	// Reset accumulated events; we only care about the next call.
	obs.mu.Lock()
	obs.requests = nil
	obs.outcomes = nil
	obs.mu.Unlock()

	_ = succeed(t, cb) // will be rejected
	reqs, outs, _ := obs.snapshot()
	if len(reqs) != 1 || reqs[0].admitted {
		t.Errorf("requests = %+v, want 1 not-admitted", reqs)
	}
	if len(outs) != 0 {
		t.Errorf("outcomes = %+v, want 0 (rejected requests have no outcome)", outs)
	}
}
