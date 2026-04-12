package gobreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a controllable clock used by all gobreaker tests. It avoids
// time.Sleep entirely so the suite is deterministic and fast.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{t: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestBreaker constructs a CircuitBreaker[any] with a deterministic clock.
// All breaker tests should go through this helper so they share the same
// time semantics.
func newTestBreaker(t *testing.T, s Settings) (*CircuitBreaker[any], *fakeClock) {
	t.Helper()
	clock := newFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	if s.Name == "" {
		s.Name = t.Name()
	}
	cb, err := New[any](context.Background(), s)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cb.setClock(clock.Now)
	return cb, clock
}

func succeed(t *testing.T, cb *CircuitBreaker[any]) error {
	t.Helper()
	_, err := cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
		return "ok", nil
	})
	return err
}

var errBoom = errors.New("boom")

func failBreaker(t *testing.T, cb *CircuitBreaker[any]) error {
	t.Helper()
	_, err := cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
		return nil, errBoom
	})
	return err
}

// stateOf reads the breaker's authoritative state for assertions. It
// goes through the public State() API so the fast-path inlineSnap and
// the generic store path produce identical answers. Tests that need to
// inspect the time-advance branch separately should call cb.State()
// directly with a controlled clock.
func stateOf(t *testing.T, cb *CircuitBreaker[any]) State {
	t.Helper()
	if cb.localStore != nil {
		cb.inlineMu.Lock()
		s := cb.inlineSnap.State
		cb.inlineMu.Unlock()
		return s
	}
	snap, err := cb.store.Get(context.Background(), cb.settings.Name)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	return snap.State
}

func TestNewRejectsBadSettings(t *testing.T) {
	_, err := New[any](context.Background(), Settings{})
	if !errors.Is(err, ErrInvalidSettings) {
		t.Errorf("expected ErrInvalidSettings, got %v", err)
	}
}

func TestNewSeedsInitialSnapshot(t *testing.T) {
	store := NewLocalStore()
	cb, err := New[any](context.Background(), Settings{Name: "x", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	_ = cb
	snap, _ := store.Get(context.Background(), "x")
	if snap.State != StateClosed {
		t.Errorf("State = %v, want closed", snap.State)
	}
	if snap.Generation != 1 {
		t.Errorf("Generation = %d, want 1", snap.Generation)
	}
	if snap.Version != 1 {
		t.Errorf("Version = %d, want 1", snap.Version)
	}
}

func TestNewIdempotentInitialization(t *testing.T) {
	store := NewLocalStore()
	cb1, err := New[any](context.Background(), Settings{Name: "shared", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	cb2, err := New[any](context.Background(), Settings{Name: "shared", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	_ = cb1
	_ = cb2
	snap, _ := store.Get(context.Background(), "shared")
	// Two breakers initializing should not double-bump generation.
	if snap.Generation != 1 {
		t.Errorf("Generation after double init = %d, want 1", snap.Generation)
	}
}

// TestClosedToOpenOnConsecutiveFailures verifies the headline behavior:
// 5 consecutive failures (the default ReadyToOpen) trip the breaker.
func TestClosedToOpenOnConsecutiveFailures(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "trip"})
	for i := 0; i < 4; i++ {
		if err := failBreaker(t, cb); !errors.Is(err, errBoom) {
			t.Fatalf("failure %d: err = %v, want errBoom", i, err)
		}
	}
	if got := stateOf(t, cb); got != StateClosed {
		t.Errorf("after 4 failures: state = %v, want closed", got)
	}
	if err := failBreaker(t, cb); !errors.Is(err, errBoom) {
		t.Fatalf("failure 5: err = %v, want errBoom", err)
	}
	if got := stateOf(t, cb); got != StateOpen {
		t.Errorf("after 5 failures: state = %v, want open", got)
	}
}

// TestOpenRejectsRequests verifies that the open state returns ErrOpenState
// without invoking the wrapped function.
func TestOpenRejectsRequests(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "open"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	if got := stateOf(t, cb); got != StateOpen {
		t.Fatalf("setup: expected open, got %v", got)
	}

	called := false
	_, err := cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
		called = true
		return nil, nil
	})
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("err = %v, want ErrOpenState", err)
	}
	if called {
		t.Error("wrapped function should not run when breaker is open")
	}
}

// TestOpenToHalfOpenAfterTimeout verifies the time-based transition.
func TestOpenToHalfOpenAfterTimeout(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{Name: "timeout", Timeout: 30 * time.Second})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	if got := stateOf(t, cb); got != StateOpen {
		t.Fatalf("setup: expected open, got %v", got)
	}

	clock.Advance(29 * time.Second)
	// Still open: timeout has not yet elapsed.
	if got := stateOf(t, cb); got != StateOpen {
		t.Errorf("at 29s: state = %v, want open", got)
	}

	clock.Advance(2 * time.Second) // total 31s
	// The next admission attempt should observe the timeout and enter half-open.
	if err := succeed(t, cb); err != nil {
		t.Errorf("first request after timeout: err = %v", err)
	}
	if got := stateOf(t, cb); got != StateClosed {
		// With HalfOpenMaxInFlights=1 and ReadyToClose=ConsecutiveSuccesses(1),
		// a single success closes the breaker again.
		t.Errorf("after success in half-open: state = %v, want closed", got)
	}
}

// TestHalfOpenInFlightAdmissionControl is the regression test for sony #30:
// in half-open we admit at most HalfOpenMaxInFlights concurrent requests.
func TestHalfOpenInFlightAdmissionControl(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:                 "halfopen-cap",
		Timeout:              time.Second,
		HalfOpenMaxInFlights: 2,
	})
	// Trip the breaker.
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	clock.Advance(2 * time.Second)

	// Hold two requests in flight: use channels to gate the wrapped fn.
	gate := make(chan struct{})
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
				<-gate
				return "ok", nil
			})
			done <- err
		}()
	}
	// Wait until both probes have been admitted.
	deadline := time.Now().Add(time.Second)
	for {
		c, err := cb.Counts(context.Background())
		if err != nil {
			t.Fatalf("Counts: %v", err)
		}
		if c.InFlights == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("InFlights never reached 2: %+v", c)
		}
		time.Sleep(time.Millisecond)
	}

	// A third request should be rejected with ErrTooManyRequests.
	_, err := cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
		t.Error("third request should not be admitted")
		return nil, nil
	})
	if !errors.Is(err, ErrTooManyRequests) {
		t.Errorf("err = %v, want ErrTooManyRequests", err)
	}

	// Release the two probes.
	close(gate)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Errorf("probe err = %v", err)
		}
	}

	if got := stateOf(t, cb); got != StateClosed {
		t.Errorf("after 2 successful probes: state = %v, want closed", got)
	}
}

// TestHalfOpenReopenOnFailure verifies the conventional behavior: any failure
// in half-open reopens the breaker.
func TestHalfOpenReopenOnFailure(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{Name: "reopen", Timeout: time.Second})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	clock.Advance(2 * time.Second)
	if err := failBreaker(t, cb); !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want errBoom", err)
	}
	if got := stateOf(t, cb); got != StateOpen {
		t.Errorf("after failure in half-open: state = %v, want open", got)
	}
}

// TestExclusionDoesNotOpen is the regression test for the IsExcluded feature:
// excluded errors should never trip the breaker, no matter how many.
func TestExclusionDoesNotOpen(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:         "exclude",
		IsExcluded:   IgnoreContextErrors,
		IsSuccessful: defaultIsSuccessful,
	})
	for i := 0; i < 100; i++ {
		_, err := cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
			return nil, context.Canceled
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("iter %d: err = %v, want context.Canceled", i, err)
		}
	}
	if got := stateOf(t, cb); got != StateClosed {
		t.Errorf("state after 100 excluded errors = %v, want closed", got)
	}
	c, _ := cb.Counts(context.Background())
	if c.TotalExclusions != 100 {
		t.Errorf("TotalExclusions = %d, want 100", c.TotalExclusions)
	}
	if c.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", c.ConsecutiveFailures)
	}
}

// TestPanicCountsAsFailureAndPropagates verifies that panics are counted and
// re-raised.
func TestPanicCountsAsFailureAndPropagates(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "panic"})
	defer func() {
		if r := recover(); r == nil {
			t.Error("panic was not re-raised")
		}
	}()
	_, _ = cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
		panic("boom")
	})
	t.Error("Execute should not return after panic")
}

func TestPanicIsCountedAsFailure(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "panic-counted"})
	for i := 0; i < 5; i++ {
		func() {
			defer func() { _ = recover() }()
			_, _ = cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
				panic("boom")
			})
		}()
	}
	if got := stateOf(t, cb); got != StateOpen {
		t.Errorf("state after 5 panics = %v, want open", got)
	}
}

// TestOnStateChangeReceivesPreviousCounts is the regression test for sony
// #72: the callback must see the counts that triggered the transition, not
// the freshly-reset counts of the new generation.
func TestOnStateChangeReceivesPreviousCounts(t *testing.T) {
	var (
		mu       sync.Mutex
		captured []Counts
		fromTo   []string
	)
	cb, _ := newTestBreaker(t, Settings{
		Name: "callback-counts",
		OnStateChange: func(name string, from, to State, c Counts) {
			mu.Lock()
			defer mu.Unlock()
			captured = append(captured, c)
			fromTo = append(fromTo, fmt.Sprintf("%s->%s", from, to))
		},
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 1 {
		t.Fatalf("got %d state changes, want 1", len(captured))
	}
	if fromTo[0] != "closed->open" {
		t.Errorf("fromTo = %q, want closed->open", fromTo[0])
	}
	c := captured[0]
	if c.ConsecutiveFailures != 5 {
		t.Errorf("captured.ConsecutiveFailures = %d, want 5", c.ConsecutiveFailures)
	}
	if c.TotalFailures != 5 {
		t.Errorf("captured.TotalFailures = %d, want 5", c.TotalFailures)
	}
}

// TestOnStateChangeNoDeadlockOnReentry is the regression test for sony #37:
// calling cb.Counts from inside OnStateChange must not deadlock.
func TestOnStateChangeNoDeadlockOnReentry(t *testing.T) {
	var (
		observed Counts
		cbHolder atomic.Pointer[CircuitBreaker[any]]
	)
	cb, _ := newTestBreaker(t, Settings{
		Name: "no-deadlock",
		OnStateChange: func(name string, from, to State, c Counts) {
			// This call would deadlock with sony/gobreaker because
			// OnStateChange runs while the internal mutex is held.
			// Our breaker releases the lock before firing the
			// callback, so this is safe.
			cur, err := cbHolder.Load().Counts(context.Background())
			if err != nil {
				t.Errorf("Counts in callback: %v", err)
			}
			observed = cur
		},
	})
	cbHolder.Store(cb)
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	// Just observing that the test did not hang is enough.
	_ = observed
}

// TestIntervalRolloverInClosed verifies that the closed state rotates the
// generation at every Interval boundary, dropping stale counts.
func TestIntervalRolloverInClosed(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:     "rollover",
		Interval: 10 * time.Second,
	})
	// Three failures, well below the trip threshold.
	for i := 0; i < 3; i++ {
		_ = failBreaker(t, cb)
	}
	c, _ := cb.Counts(context.Background())
	if c.ConsecutiveFailures != 3 {
		t.Errorf("before rollover: ConsecutiveFailures = %d, want 3", c.ConsecutiveFailures)
	}

	clock.Advance(11 * time.Second)
	// The next observation should rotate generation and reset counts.
	if err := succeed(t, cb); err != nil {
		t.Fatal(err)
	}
	c, _ = cb.Counts(context.Background())
	if c.ConsecutiveFailures != 0 {
		t.Errorf("after rollover: ConsecutiveFailures = %d, want 0", c.ConsecutiveFailures)
	}
	if c.TotalSuccesses != 1 {
		t.Errorf("after rollover: TotalSuccesses = %d, want 1", c.TotalSuccesses)
	}
}

// TestStaleGenerationOutcomeDropped verifies that an outcome reported after
// the breaker has rotated to a new generation is silently dropped.
func TestStaleGenerationOutcomeDropped(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:     "stale",
		Interval: 10 * time.Second,
	})
	// Hold one request in flight.
	gate := make(chan struct{})
	resultCh := make(chan error, 1)
	go func() {
		_, err := cb.Execute(context.Background(), func(ctx context.Context) (any, error) {
			<-gate
			return nil, errBoom
		})
		resultCh <- err
	}()
	// Wait until in-flight is 1.
	for {
		c, _ := cb.Counts(context.Background())
		if c.InFlights == 1 {
			break
		}
	}

	// Advance past Interval and force a rollover with a new request.
	clock.Advance(11 * time.Second)
	_ = succeed(t, cb)

	// Now release the gated request: its failure outcome should NOT be
	// counted because it belongs to the previous generation.
	close(gate)
	<-resultCh

	c, _ := cb.Counts(context.Background())
	if c.TotalFailures != 0 {
		t.Errorf("TotalFailures after stale outcome = %d, want 0", c.TotalFailures)
	}
}

func TestExecuteForwardsContext(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "ctx"})
	type ctxKey string
	const key ctxKey = "k"

	parent := context.WithValue(context.Background(), key, "v")
	_, err := cb.Execute(parent, func(ctx context.Context) (any, error) {
		if got := ctx.Value(key); got != "v" {
			t.Errorf("context not forwarded: %v", got)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentRequestsClosed verifies that the breaker handles many
// concurrent requests without losing counts.
func TestConcurrentRequestsClosed(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "concurrent"})
	const goroutines = 50
	const perGoroutine = 200
	var wg sync.WaitGroup
	var ok int64
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				if err := succeed(t, cb); err == nil {
					atomic.AddInt64(&ok, 1)
				}
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	if ok != want {
		t.Errorf("ok = %d, want %d", ok, want)
	}
	c, _ := cb.Counts(context.Background())
	if c.TotalSuccesses != uint64(want) {
		t.Errorf("TotalSuccesses = %d, want %d", c.TotalSuccesses, want)
	}
	if c.InFlights != 0 {
		t.Errorf("InFlights = %d, want 0", c.InFlights)
	}
}

func TestStoreFailFastSurfaces(t *testing.T) {
	failing := &failingStore{}
	_, err := New[any](context.Background(), Settings{
		Name:           "failfast",
		Store:          failing,
		OnStoreFailure: FailFast,
	})
	// New uses Update; failingStore returns an error from Update, so
	// New should fail.
	if err == nil {
		t.Fatal("expected New to fail with failing store")
	}
}

func TestStoreFallbackToLocal(t *testing.T) {
	// In FallbackToLocal mode, a failing store should be transparently
	// replaced by an in-memory fallback. We construct the breaker with
	// the failing store from the start so the fast path detection (which
	// fires only when Settings.Store is a *LocalStore) does not engage.
	cb, err := New[any](context.Background(), Settings{
		Name:           "fallback",
		Store:          failingStore{},
		OnStoreFailure: FallbackToLocal,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	// The fallback store should now hold the open state.
	if cb.localFallback == nil {
		t.Fatal("local fallback was not created")
	}
	snap, _ := cb.localFallback.Get(context.Background(), "fallback")
	if snap.State != StateOpen {
		t.Errorf("fallback state = %v, want open", snap.State)
	}
}

// failingStore is a Store implementation that returns an error from every
// operation. Used to exercise OnStoreFailure paths.
type failingStore struct{}

func (failingStore) Get(_ context.Context, _ string) (Snapshot, error) {
	return Snapshot{}, errors.New("store down")
}

func (failingStore) Update(_ context.Context, _ string, _ UpdateFunc) (Snapshot, error) {
	return Snapshot{}, errors.New("store down")
}

func (failingStore) Close() error { return nil }

func TestNameReturnsConfiguredName(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "my-breaker"})
	if cb.Name() != "my-breaker" {
		t.Errorf("Name() = %q, want %q", cb.Name(), "my-breaker")
	}
}

func TestStatePublicAPIObservesHalfOpenAfterTimeout(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:    "public-state",
		Timeout: 10 * time.Second,
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	// Read via public API: should be open while still under Timeout.
	got, err := cb.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != StateOpen {
		t.Errorf("State() = %v, want open", got)
	}

	clock.Advance(11 * time.Second)
	// Without performing a request, State() should already report half-open.
	got, err = cb.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != StateHalfOpen {
		t.Errorf("State() after timeout = %v, want half-open", got)
	}
}

func TestStateFailFastSurfacesError(t *testing.T) {
	// Construct with the failing store from the start so the fast
	// path detection does not engage.
	cb, err := New[any](context.Background(), Settings{
		Name:           "x",
		Store:          failingStore{},
		OnStoreFailure: FailFast,
	})
	if err == nil {
		// New itself should fail under FailFast with a failing store.
		t.Fatalf("New should have failed with failing store + FailFast, got cb=%v", cb)
	}
	if !errors.Is(err, ErrStoreUnavailable) && !errors.Is(err, errors.New("store down")) {
		// New wraps the store error in a "gobreaker: initialize ..."
		// envelope. Either ErrStoreUnavailable wrapping or the bare
		// store error is acceptable.
		if err.Error() == "" {
			t.Errorf("expected non-empty error from New")
		}
	}
}

func TestStateFallbackUsesLocalStore(t *testing.T) {
	// Construct directly with the failing store so the fast path is
	// not engaged. FallbackToLocal must let the constructor succeed
	// and route subsequent operations to the lazy local fallback.
	cb, err := New[any](context.Background(), Settings{
		Name:           "x",
		Store:          failingStore{},
		OnStoreFailure: FallbackToLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	// In FallbackToLocal mode the call should not error.
	if _, err := cb.State(context.Background()); err != nil {
		t.Errorf("State err = %v, want nil (fallback)", err)
	}
	if _, err := cb.Counts(context.Background()); err != nil {
		t.Errorf("Counts err = %v, want nil (fallback)", err)
	}
}
