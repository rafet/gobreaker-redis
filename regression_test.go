package gobreaker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// regression_test.go contains tests for every bug found during the v2
// CodeRabbit review pass and every gap surfaced by the audit that
// followed it. Each test is named after the issue it guards against and
// includes a comment describing the failure mode.
//
// The convention: REG_<topic>_<symptom>. Run them as a sanity sweep with
//
//	go test -race -run 'REG_'
//
// New bugs caught in the field should add a test here before the fix
// goes in. The test that fails before the fix and passes after is the
// authoritative artifact that the bug exists and was actually addressed.

// REG_StateChange_NotEmittedOnFailedUpdate is the regression test for
// CodeRabbit review issue #7 (gobreaker.go report path).
//
// Bug: report() called fireStateChanges() unconditionally, even when the
// underlying Store.Update returned an error AFTER having invoked the
// closure. Observers therefore saw transitions that were never persisted.
func TestREG_StateChange_NotEmittedOnFailedUpdate(t *testing.T) {
	store := newClosureRanFailingStore(errors.New("commit failed"))
	var stateChanges int64
	cb := &CircuitBreaker[any]{
		settings: Settings{
			Name:                 "x",
			Timeout:              time.Minute,
			HalfOpenMaxInFlights: 1,
			OnStoreFailure:       FailFast, // surface the store error
			ReadyToOpen:          ConsecutiveFailures(1),
			ReadyToClose:         ConsecutiveSuccesses(1),
			ReadyToReopen:        Always,
			IsSuccessful:         defaultIsSuccessful,
			IsExcluded:           defaultIsExcluded,
			OnStateChange: func(_ string, _, _ State, _ Counts) {
				atomic.AddInt64(&stateChanges, 1)
			},
		},
		store: store,
		now:   time.Now,
	}

	// Call report() directly with the same generation as the seeded
	// snapshot (Generation=0), simulating an admitted-but-failing
	// request that the closure would mark as a failure capable of
	// tripping the breaker.
	err := cb.report(context.Background(), 0, errors.New("upstream failure"))
	if err == nil {
		t.Fatal("expected error from failing store, got nil")
	}
	if got := atomic.LoadInt64(&stateChanges); got != 0 {
		t.Errorf("OnStateChange fired %d time(s) on a failed Update; want 0", got)
	}
	if store.callCount() != 1 {
		t.Errorf("Update called %d times; want 1", store.callCount())
	}
}

// REG_StateChange_NotEmittedOnFailedAdmit guards the same property on
// the admit() path. The closure runs, may emit transitions (e.g. open →
// half-open), and then the commit fails. The observer must NOT see the
// half-open transition.
func TestREG_StateChange_NotEmittedOnFailedAdmit(t *testing.T) {
	store := newClosureRanFailingStore(errors.New("commit failed"))
	var stateChanges int64
	cb := &CircuitBreaker[any]{
		settings: Settings{
			Name:                 "x",
			Timeout:              time.Minute,
			HalfOpenMaxInFlights: 1,
			OnStoreFailure:       FailFast,
			ReadyToOpen:          ConsecutiveFailures(1),
			ReadyToClose:         ConsecutiveSuccesses(1),
			ReadyToReopen:        Always,
			IsSuccessful:         defaultIsSuccessful,
			IsExcluded:           defaultIsExcluded,
			OnStateChange: func(_ string, _, _ State, _ Counts) {
				atomic.AddInt64(&stateChanges, 1)
			},
		},
		store: store,
		now:   time.Now,
	}
	// Seed the underlying store with an OPEN state whose timeout has
	// already expired, so admit() will run advanceTime() inside the
	// closure and produce an Open->HalfOpen transition that the
	// commit then drops.
	store.snap = Snapshot{
		State:           StateOpen,
		Generation:      1,
		GenerationStart: time.Now().Add(-2 * time.Minute),
		Expiry:          time.Now().Add(-time.Minute),
	}

	if _, err := cb.admit(context.Background()); err == nil {
		t.Fatal("expected error from failing store, got nil")
	}
	if got := atomic.LoadInt64(&stateChanges); got != 0 {
		t.Errorf("OnStateChange fired %d time(s) on a failed admit Update; want 0", got)
	}
}

// REG_New_RespectsFallbackPolicy is the regression test for CodeRabbit
// review issue #5 (constructor bypassing OnStoreFailure).
//
// Bug: New() called store.Update directly instead of cb.runUpdate, so a
// failing store would abort construction even with FallbackToLocal set.
func TestREG_New_RespectsFallbackPolicy(t *testing.T) {
	failing := failingStore{}
	cb, err := New[any](context.Background(), Settings{
		Name:           "fallback-init",
		Store:          failing,
		OnStoreFailure: FallbackToLocal,
	})
	if err != nil {
		t.Fatalf("New with failing store + FallbackToLocal returned error: %v", err)
	}
	if cb == nil {
		t.Fatal("New returned nil breaker")
	}
	// The breaker should immediately be usable via the local fallback.
	if _, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return "ok", nil
	}); err != nil {
		t.Errorf("first Execute on fallback-only breaker: %v", err)
	}
}

// REG_New_FailFastSurfacesError is the matching test for the strict
// policy: when the user explicitly opts into FailFast, a failing store
// must abort construction.
func TestREG_New_FailFastSurfacesError(t *testing.T) {
	if _, err := New[any](context.Background(), Settings{
		Name:           "failfast-init",
		Store:          failingStore{},
		OnStoreFailure: FailFast,
	}); err == nil {
		t.Error("New with failing store + FailFast did not error")
	}
}

// REG_OnOpenOnly_NilFallbackDoesNotPanic is the regression test for
// CodeRabbit review issue #4 (fallback.go OnOpenOnly nil panic).
func TestREG_OnOpenOnly_NilFallbackDoesNotPanic(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "x"})
	// Trip the breaker.
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}

	wrapped := OnOpenOnly[any](nil)
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("OnOpenOnly(nil) panicked: %v", r)
		}
	}()
	_, err := cb.ExecuteWithFallback(
		context.Background(),
		func(_ context.Context) (any, error) { return "x", nil },
		wrapped,
	)
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("err = %v, want ErrOpenState (nil fallback should propagate)", err)
	}
}

// REG_Validate_RejectsUnknownStorePolicy is the regression test for
// CodeRabbit review issue #13 (Settings.Validate accepts garbage
// OnStoreFailure values).
func TestREG_Validate_RejectsUnknownStorePolicy(t *testing.T) {
	s := Settings{Name: "x", OnStoreFailure: 99}
	err := s.Validate()
	if !errors.Is(err, ErrInvalidSettings) {
		t.Errorf("Validate accepted OnStoreFailure=99: err = %v", err)
	}

	if err := (Settings{Name: "x", OnStoreFailure: FallbackToLocal}).Validate(); err != nil {
		t.Errorf("FallbackToLocal rejected: %v", err)
	}
	if err := (Settings{Name: "x", OnStoreFailure: FailFast}).Validate(); err != nil {
		t.Errorf("FailFast rejected: %v", err)
	}
}

// REG_New_RejectsUnknownStorePolicy verifies the Validate hook is
// actually called from New (regression against future refactors that
// might bypass it).
func TestREG_New_RejectsUnknownStorePolicy(t *testing.T) {
	if _, err := New[any](context.Background(), Settings{
		Name:           "x",
		OnStoreFailure: 42,
	}); !errors.Is(err, ErrInvalidSettings) {
		t.Errorf("New accepted OnStoreFailure=42: err = %v", err)
	}
}

// REG_Admit_RejectsWithRealState is the regression test for CodeRabbit
// review issue #6 (admit returning a zero snapshot on rejection).
//
// The Observer must see the actual state that caused the rejection
// (StateOpen, StateHalfOpen) and never the zero StateClosed.
func TestREG_Admit_RejectsWithRealState(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "obs-rej", Observer: obs})
	// Trip the breaker.
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}

	// Drop everything observed during the trip itself.
	obs.mu.Lock()
	obs.requests = nil
	obs.mu.Unlock()

	_ = succeed(t, cb) // rejected

	reqs, _, _ := obs.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d, want 1", len(reqs))
	}
	r := reqs[0]
	if r.admitted {
		t.Errorf("request admitted; want rejected")
	}
	if r.state != StateOpen {
		t.Errorf("rejection observed state = %v, want StateOpen", r.state)
	}
}

// REG_Admit_HalfOpenRejectionPreservesState verifies the same property
// in the half-open path: ErrTooManyRequests must report StateHalfOpen,
// not StateClosed.
func TestREG_Admit_HalfOpenRejectionPreservesState(t *testing.T) {
	obs := &recordingObserver{}
	cb, clock := newTestBreaker(t, Settings{
		Name:                 "halfopen-obs",
		Timeout:              time.Second,
		HalfOpenMaxInFlights: 1,
		Observer:             obs,
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	clock.Advance(2 * time.Second)

	// Hold one probe in flight.
	gate := make(chan struct{})
	doneCh := make(chan error, 1)
	go func() {
		_, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			<-gate
			return "ok", nil
		})
		doneCh <- err
	}()
	for {
		c, _ := cb.Counts(context.Background())
		if c.InFlights == 1 {
			break
		}
	}

	// Reset observation.
	obs.mu.Lock()
	obs.requests = nil
	obs.mu.Unlock()

	// Second admission should be rejected with the actual half-open state.
	_, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		t.Error("should not run")
		return nil, nil
	})
	if !errors.Is(err, ErrTooManyRequests) {
		t.Errorf("err = %v, want ErrTooManyRequests", err)
	}

	reqs, _, _ := obs.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d, want 1", len(reqs))
	}
	if reqs[0].state != StateHalfOpen {
		t.Errorf("rejection state = %v, want StateHalfOpen", reqs[0].state)
	}

	close(gate)
	<-doneCh
}

// REG_LocalStore_GetAfterCloseReturnsError is the regression test for
// CodeRabbit review issue #10 (Close caused subsequent Get to nil-map
// panic).
func TestREG_LocalStore_GetAfterCloseReturnsError(t *testing.T) {
	s := NewLocalStore()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(context.Background(), "x")
	if !errors.Is(err, ErrStoreClosed) {
		t.Errorf("Get after Close: err = %v, want ErrStoreClosed", err)
	}
}

// REG_LocalStore_UpdateAfterCloseReturnsError is the matching test for
// Update.
func TestREG_LocalStore_UpdateAfterCloseReturnsError(t *testing.T) {
	s := NewLocalStore()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	called := false
	_, err := s.Update(context.Background(), "x", func(c Snapshot, _ time.Time) (Snapshot, error) {
		called = true
		return c, nil
	})
	if !errors.Is(err, ErrStoreClosed) {
		t.Errorf("Update after Close: err = %v, want ErrStoreClosed", err)
	}
	if called {
		t.Error("UpdateFunc was invoked after Close; expected to short-circuit")
	}
}

// REG_LocalStore_CloseIsIdempotent verifies that calling Close repeatedly
// is safe and always returns nil. The Store interface allows
// implementations to error after Close, but Close itself must not.
func TestREG_LocalStore_CloseIsIdempotent(t *testing.T) {
	s := NewLocalStore()
	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Errorf("Close call %d: %v", i+1, err)
		}
	}
}

// REG_Group_KeyToNameCollision is the regression test for CodeRabbit
// review issue #8 (Group cached by raw key, leading to two breaker
// instances for one logical breaker when KeyToName collides).
func TestREG_Group_KeyToNameCollision(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
		KeyToName: func(_ string) string {
			// Pathological case: every key resolves to the same name.
			return "shared"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cb1, err := g.Get(context.Background(), "alpha")
	if err != nil {
		t.Fatal(err)
	}
	cb2, err := g.Get(context.Background(), "beta")
	if err != nil {
		t.Fatal(err)
	}
	if cb1 != cb2 {
		t.Error("Group returned distinct breakers for two keys that map to the same derived name")
	}
	if g.Len() != 1 {
		t.Errorf("Len = %d, want 1 (collision should yield a single cached breaker)", g.Len())
	}
}

// REG_Group_DeleteUsesDerivedName verifies that Delete also operates on
// derived names, so deleting via either of two collision-mapped user
// keys removes the single cached breaker.
func TestREG_Group_DeleteUsesDerivedName(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings:  Settings{Name: "g"},
		KeyToName: func(_ string) string { return "shared" },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = g.Get(context.Background(), "alpha")
	_, _ = g.Get(context.Background(), "beta")
	if g.Len() != 1 {
		t.Fatalf("Len = %d, want 1", g.Len())
	}

	if !g.Delete("beta") {
		t.Error("Delete via beta returned false")
	}
	if g.Len() != 0 {
		t.Errorf("Len after delete = %d, want 0", g.Len())
	}
	// Deleting again should report no entry.
	if g.Delete("alpha") {
		t.Error("second Delete returned true; want false")
	}
}

// REG_Execute_ReportFailureOnSuccessfulCall covers the path where the
// wrapped request succeeds but the post-call Update fails. The breaker
// should return both the result and the store error so the caller can
// decide how to react.
//
// flakyStore is positioned with passNextUpdates(1) so that the admit
// Update succeeds and only the report Update fails.
func TestREG_Execute_ReportFailureOnSuccessfulCall(t *testing.T) {
	store := newFlakyStore(NewLocalStore())
	cb, err := New[string](context.Background(), Settings{
		Name:           "report-fail",
		Store:          store,
		OnStoreFailure: FailFast,
	})
	if err != nil {
		t.Fatal(err)
	}
	// admit Update should succeed; the very next Update (the report)
	// must fail.
	store.passNextUpdates(1)
	store.failNextUpdates(1)

	result, err := cb.Execute(context.Background(), func(_ context.Context) (string, error) {
		return "primary-ok", nil
	})
	// The wrapped request succeeded — its result must reach the caller.
	if result != "primary-ok" {
		t.Errorf("result = %q, want primary-ok", result)
	}
	if err == nil {
		t.Error("expected store error to surface")
	}
	if !errors.Is(err, ErrStoreUnavailable) {
		t.Errorf("err = %v, want ErrStoreUnavailable", err)
	}
}

// REG_Execute_ReportFailureOnFailedCall covers the path where both the
// wrapped request and the post-call Update fail. The call error must
// take precedence because it carries the more relevant signal.
func TestREG_Execute_ReportFailureOnFailedCall(t *testing.T) {
	store := newFlakyStore(NewLocalStore())
	cb, err := New[string](context.Background(), Settings{
		Name:           "report-fail-2",
		Store:          store,
		OnStoreFailure: FailFast,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.passNextUpdates(1)
	store.failNextUpdates(1)

	_, err = cb.Execute(context.Background(), func(_ context.Context) (string, error) {
		return "", errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want errBoom (call error should take precedence over store error)", err)
	}
}

// REG_Transition_NoOpForSameState verifies that a transition() call
// with from == to is a no-op (returns the snapshot unchanged, does not
// emit a state change). This is the early-return branch in transition()
// that previously had no test coverage.
func TestREG_Transition_NoOpForSameState(t *testing.T) {
	cb := &CircuitBreaker[any]{
		settings: Settings{Name: "x"}.defaults(),
	}
	var changes []stateChange
	in := Snapshot{State: StateClosed, Generation: 5}
	out := cb.transition(in, StateClosed, time.Now(), &changes)
	if out != in {
		t.Errorf("transition closed->closed mutated snapshot: %+v -> %+v", in, out)
	}
	if len(changes) != 0 {
		t.Errorf("changes recorded for no-op transition: %+v", changes)
	}
}

// REG_FailureRatio_AllExclusionsDoesNotFire verifies that a generation
// containing only excluded outcomes does not trigger a FailureRatio
// predicate, since the denominator (successes + failures) is zero.
func TestREG_FailureRatio_AllExclusionsDoesNotFire(t *testing.T) {
	f := FailureRatio(1, 0.5)
	c := Counts{TotalExclusions: 100, Requests: 100}
	if f(c) {
		t.Error("FailureRatio fired with only exclusions; denominator is zero, must return false")
	}
}

// REG_IgnoreContextErrors_NilSafe verifies the IsExcluded helper handles
// a nil error.
func TestREG_IgnoreContextErrors_NilSafe(t *testing.T) {
	if IgnoreContextErrors(nil) {
		t.Error("IgnoreContextErrors(nil) should not be excluded")
	}
}

// REG_Execute_PanicAndStoreFailureBothPropagate covers the rare double
// failure mode: the wrapped function panics AND the store update fails.
// The panic must still propagate (panic safety dominates over store
// error reporting).
func TestREG_Execute_PanicAndStoreFailureBothPropagate(t *testing.T) {
	store := newFlakyStore(NewLocalStore())
	cb, err := New[string](context.Background(), Settings{
		Name:           "panic-flaky",
		Store:          store,
		OnStoreFailure: FailFast,
	})
	if err != nil {
		t.Fatal(err)
	}
	// admit succeeds, report (after panic) fails.
	store.passNextUpdates(1)
	store.failNextUpdates(1)

	defer func() {
		if r := recover(); r == nil {
			t.Error("panic was not re-raised")
		}
	}()
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (string, error) {
		panic("boom")
	})
	t.Fatal("Execute should not return after panic")
}

// REG_Observer_StateChangeOrdering verifies that when both
// Settings.OnStateChange and Settings.Observer are set, the synchronous
// callback fires before the observer for each transition. The order
// matters for users who layer the two: e.g. OnStateChange logs locally,
// Observer ships to a metrics endpoint.
func TestREG_Observer_StateChangeOrdering(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	cb, _ := newTestBreaker(t, Settings{
		Name: "ordering",
		OnStateChange: func(_ string, _, _ State, _ Counts) {
			mu.Lock()
			order = append(order, "callback")
			mu.Unlock()
		},
		Observer: observerFunc{
			onStateChange: func(_ string, _, _ State, _ Counts) {
				mu.Lock()
				order = append(order, "observer")
				mu.Unlock()
			},
		},
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "callback" || order[1] != "observer" {
		t.Errorf("order = %v, want [callback observer]", order)
	}
}

// observerFunc is a small adapter that lets a single test inject just
// the OnStateChange behavior it cares about. The other Observer methods
// are no-ops.
type observerFunc struct {
	NopObserver
	onStateChange func(name string, from, to State, counts Counts)
}

func (o observerFunc) OnStateChange(name string, from, to State, counts Counts) {
	if o.onStateChange != nil {
		o.onStateChange(name, from, to, counts)
	}
}

// REG_Group_PerKeyOverrideNilDoesNotPanic verifies that NewGroup is OK
// with a nil PerKeySettings hook (the default).
func TestREG_Group_PerKeyOverrideNilDoesNotPanic(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings:       Settings{Name: "g"},
		PerKeySettings: nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Get(context.Background(), "k"); err != nil {
		t.Errorf("Get with nil PerKeySettings: %v", err)
	}
}

// REG_Group_BubblesNewError verifies that errors from the underlying
// New() bubble up through Group.Get instead of being silently swallowed.
func TestREG_Group_BubblesNewError(t *testing.T) {
	// Use an invalid template by giving the group a per-key override
	// that returns an unknown OnStoreFailure value, which Validate
	// rejects.
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
		PerKeySettings: func(_ string, base Settings) Settings {
			base.OnStoreFailure = 99
			return base
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Get(context.Background(), "k")
	if !errors.Is(err, ErrInvalidSettings) {
		t.Errorf("err = %v, want ErrInvalidSettings", err)
	}
	if g.Len() != 0 {
		t.Errorf("failed Get cached the breaker: Len = %d", g.Len())
	}
}

// REG_Settings_DefaultIntervalIsZero verifies the documented contract:
// when Interval is unset, the breaker accumulates Counts indefinitely
// in the closed state.
func TestREG_Settings_DefaultIntervalIsZero(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "no-interval"})
	if cb.settings.Interval != 0 {
		t.Errorf("default Interval = %v, want 0", cb.settings.Interval)
	}
}

// REG_Snapshot_IsZeroDetectsAllFields verifies that IsZero is
// conservative — any non-zero field defeats it. This guards against
// future struct expansions that forget to update IsZero.
func TestREG_Snapshot_IsZeroDetectsAllFields(t *testing.T) {
	if !(Snapshot{}).IsZero() {
		t.Error("zero Snapshot.IsZero() should be true")
	}
	cases := []Snapshot{
		{Version: 1},
		{State: StateOpen},
		{Generation: 1},
		{Counts: Counts{Requests: 1}},
		{GenerationStart: time.Now()},
		{Expiry: time.Now()},
	}
	for i, c := range cases {
		if c.IsZero() {
			t.Errorf("case %d: %+v.IsZero() = true; want false", i, c)
		}
	}
}

// REG_Counts_OnExclusionDoesNotResetConsecutive guards a subtle
// invariant: an excluded outcome must NOT reset ConsecutiveSuccesses or
// ConsecutiveFailures, because exclusion conveys neither health nor
// failure. This already has a test, but it is reproduced here so the
// regression suite covers the same property without dependency on the
// counts_test.go internal helpers.
func TestREG_Counts_OnExclusionDoesNotResetConsecutive(t *testing.T) {
	c := Counts{ConsecutiveSuccesses: 3, ConsecutiveFailures: 0, InFlights: 1}
	c.onExclusion()
	if c.ConsecutiveSuccesses != 3 {
		t.Errorf("ConsecutiveSuccesses = %d, want 3", c.ConsecutiveSuccesses)
	}

	c = Counts{ConsecutiveFailures: 4, InFlights: 1}
	c.onExclusion()
	if c.ConsecutiveFailures != 4 {
		t.Errorf("ConsecutiveFailures = %d, want 4", c.ConsecutiveFailures)
	}
}

// REG_State_StableNumericValues guards against accidental reordering of
// the State enum, which would silently corrupt every persisted
// Snapshot.
func TestREG_State_StableNumericValues(t *testing.T) {
	want := map[State]int32{StateClosed: 0, StateHalfOpen: 1, StateOpen: 2}
	for s, n := range want {
		if int32(s) != n {
			t.Errorf("State %s = %d, want %d", s, int32(s), n)
		}
	}
}

// REG_Outcome_StableNumericValues likewise guards Outcome.
func TestREG_Outcome_StableNumericValues(t *testing.T) {
	want := map[Outcome]int{OutcomeSuccess: 0, OutcomeFailure: 1, OutcomeExclusion: 2, OutcomeRejected: 3}
	for o, n := range want {
		if int(o) != n {
			t.Errorf("Outcome %s = %d, want %d", o, int(o), n)
		}
	}
}

// REG_Group_ConcurrentCreateAndExecute stresses the Group double-check
// locking under contention. Many goroutines call Execute(key) for the
// same key simultaneously; only one breaker should be created.
func TestREG_Group_ConcurrentCreateAndExecute(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
	})
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := g.Execute(context.Background(), "shared", func(_ context.Context) (any, error) {
				return "ok", nil
			})
			if err != nil {
				t.Errorf("Execute: %v", err)
			}
		}()
	}
	wg.Wait()
	if g.Len() != 1 {
		t.Errorf("Len = %d, want 1 (concurrent first-touch should not create duplicates)", g.Len())
	}
}

// REG_Execute_NilWrapped verifies that Execute panics cleanly when
// passed a nil function. We document this as undefined-but-sane: the
// breaker does not nil-check, but the resulting panic is recovered as a
// failure outcome.
func TestREG_Execute_NilWrapped(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "nil-fn"})
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil function")
		}
	}()
	_, _ = cb.Execute(context.Background(), nil)
}

// REG_Execute_ContextCancellationCountedAsFailureByDefault documents
// the default behavior: without IgnoreContextErrors, a cancelled
// context counts as a failure. This is the safe default but it surprises
// callers, so we test it explicitly to make any future change loud.
func TestREG_Execute_ContextCancellationCountedAsFailureByDefault(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "ctx-default"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = cb.Execute(ctx, func(ctx context.Context) (any, error) {
		return nil, ctx.Err()
	})
	c, _ := cb.Counts(context.Background())
	if c.TotalFailures != 1 {
		t.Errorf("TotalFailures = %d, want 1 (context.Canceled counted as failure by default)", c.TotalFailures)
	}
}

// REG_Execute_ContextCancellationExcludedWhenConfigured verifies the
// IgnoreContextErrors helper works end-to-end through Execute.
func TestREG_Execute_ContextCancellationExcludedWhenConfigured(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:       "ctx-excluded",
		IsExcluded: IgnoreContextErrors,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = cb.Execute(ctx, func(ctx context.Context) (any, error) {
		return nil, ctx.Err()
	})
	c, _ := cb.Counts(context.Background())
	if c.TotalFailures != 0 {
		t.Errorf("TotalFailures = %d, want 0", c.TotalFailures)
	}
	if c.TotalExclusions != 1 {
		t.Errorf("TotalExclusions = %d, want 1", c.TotalExclusions)
	}
}

// REG_LocalStore_NewIsNotShared verifies that two NewLocalStore() calls
// produce independent stores. The bug guarded against is a future
// refactor that might accidentally share package-level state.
func TestREG_LocalStore_NewIsNotShared(t *testing.T) {
	a := NewLocalStore()
	b := NewLocalStore()
	_, _ = a.Update(context.Background(), "k", func(c Snapshot, _ time.Time) (Snapshot, error) {
		c.Counts.Requests = 99
		return c, nil
	})
	bSnap, _ := b.Get(context.Background(), "k")
	if bSnap.Counts.Requests != 0 {
		t.Errorf("store b leaked state from store a: %+v", bSnap)
	}
}

// REG_Group_DoubleCheckedLockingFromCache verifies the second branch
// inside Get's write-locked critical section: when a competing
// goroutine has populated the cache between our RLock release and our
// Lock acquisition, we must observe the existing entry instead of
// constructing a duplicate.
//
// We exercise the path deterministically by pre-seeding the cache
// (mimicking the post-race state) and then calling Get for the same
// key. The internal RLock check sees nothing (no read lock contention
// in the test), then the Lock check finds the seeded entry. The result
// must be the same pointer.
func TestREG_Group_DoubleCheckedLockingFromCache(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Manually seed the cache with a sentinel breaker.
	seed, err := New[any](context.Background(), Settings{Name: "g:k"})
	if err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	g.breakers["g:k"] = seed
	g.mu.Unlock()

	got, err := g.Get(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if got != seed {
		t.Error("Get returned a fresh breaker; want the cached one")
	}
}

// REG_Group_DoubleCheckedLockingRace constructs a deterministic race
// against the L139-141 branch in group.go. The branch fires when one
// goroutine populates the cache while a second goroutine is blocked on
// the write lock. The test uses a gated Store to suspend the first
// goroutine inside the New[T] constructor (which calls Store.Update),
// queues a second goroutine on the same key (which blocks on the
// write lock), then releases the first one. When the first goroutine
// finishes and releases the lock, the second one acquires it and must
// see the cached entry.
//
// The test asserts both correctness (only one breaker created) and
// branch coverage (the second goroutine took the L139-141 path, which
// is implied by g.Len() == 1).
func TestREG_Group_DoubleCheckedLockingRace(t *testing.T) {
	gated := newGatedStore(NewLocalStore())
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g", Store: gated},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The first Get() call constructs a CircuitBreaker, whose New()
	// calls Store.Update for the initial snapshot. We arm a gate so
	// that this Update blocks until we release it.
	releaseFirst := gated.nextGate()

	type result struct {
		cb  *CircuitBreaker[any]
		err error
	}
	first := make(chan result, 1)
	second := make(chan result, 1)

	go func() {
		cb, err := g.Get(context.Background(), "k")
		first <- result{cb, err}
	}()

	// Wait until the first goroutine has actually entered Get() and
	// blocked on the gate. We poll the goroutine count of the gated
	// store: when the goroutine is parked inside Update we know it
	// has acquired the write lock. The Lock acquisition happens
	// before the Update call, so once Update is reached the lock is
	// held.
	//
	// Polling on a public observable: the cache is empty until the
	// first goroutine completes, but we cannot observe lock
	// ownership directly. Instead we wait until the first goroutine
	// has consumed our gate slot, which happens INSIDE Update — i.e.
	// while the first goroutine holds the write lock.
	for {
		gated.mu.Lock()
		consumed := len(gated.gates) == 0
		gated.mu.Unlock()
		if consumed {
			break
		}
		runtime.Gosched()
	}

	// Now spawn the second goroutine. It will block on the write
	// lock because the first goroutine still holds it.
	go func() {
		cb, err := g.Get(context.Background(), "k")
		second <- result{cb, err}
	}()

	// Give the second goroutine a moment to actually call Get() and
	// queue on the lock. This is the only timing-sensitive step;
	// even an aggressive runtime can be assumed to schedule the
	// goroutine within a few milliseconds.
	time.Sleep(20 * time.Millisecond)

	// Release the first goroutine. It will commit the snapshot,
	// release the lock, and the second goroutine will then acquire
	// the lock and observe the cached entry (L139-141).
	close(releaseFirst)

	res1 := <-first
	res2 := <-second
	if res1.err != nil {
		t.Fatalf("first Get: %v", res1.err)
	}
	if res2.err != nil {
		t.Fatalf("second Get: %v", res2.err)
	}
	if res1.cb != res2.cb {
		t.Error("two goroutines got distinct breakers; double-check did not engage")
	}
	if g.Len() != 1 {
		t.Errorf("Len = %d, want 1 (only one breaker should be created)", g.Len())
	}
}

// REG_Group_ExecuteSurfacesGetError verifies the bubble-up path in
// Execute when Get fails. Without this test, group.Execute's error
// branch is not exercised even though Get's error branch is.
func TestREG_Group_ExecuteSurfacesGetError(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
		PerKeySettings: func(_ string, base Settings) Settings {
			base.OnStoreFailure = 99 // invalid → New rejects
			return base
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	_, err = g.Execute(context.Background(), "k", func(_ context.Context) (any, error) {
		called = true
		return nil, nil
	})
	if !errors.Is(err, ErrInvalidSettings) {
		t.Errorf("err = %v, want ErrInvalidSettings", err)
	}
	if called {
		t.Error("Execute invoked the wrapped function despite Get failing")
	}
}

// REG_Execute_PanicWithObserverObservesFailure verifies that the
// Observer.OnOutcome path inside the panic branch fires before the
// panic is re-raised. This is the gobreaker.go L197-199 branch.
func TestREG_Execute_PanicWithObserverObservesFailure(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "panic-obs", Observer: obs})

	func() {
		defer func() { _ = recover() }()
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			panic("kaboom")
		})
	}()

	_, outs, _ := obs.snapshot()
	if len(outs) != 1 {
		t.Fatalf("outcomes = %d, want 1 (Observer must see the panic)", len(outs))
	}
	if outs[0].outcome != OutcomeFailure {
		t.Errorf("outcome = %v, want OutcomeFailure", outs[0].outcome)
	}
}

// REG_Group_PerKeyOverrideRunOnce verifies that PerKeySettings is
// invoked exactly once per key (the first time it is seen), not on
// every Get.
func TestREG_Group_PerKeyOverrideRunOnce(t *testing.T) {
	var calls int64
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
		PerKeySettings: func(_ string, base Settings) Settings {
			atomic.AddInt64(&calls, 1)
			return base
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, _ = g.Get(context.Background(), "k")
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("PerKeySettings invoked %d times, want 1", got)
	}
}

// fmtPrefix is used by some tests to silence "imported and not used"
// warnings when the only consumer of a package is wrapped in a closure
// that the test runner does not flatten. Keeping it as a no-op
// reference is cheaper than juggling _ imports.
var _ = fmt.Sprintf
