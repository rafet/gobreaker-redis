// Package gobreaker implements a distributed-first circuit breaker.
//
// The package separates two concerns that are usually conflated in
// circuit-breaker libraries:
//
//   - The state machine, implemented in this file as a pure transformation
//     from one Snapshot to the next. The state machine is deterministic and
//     has no I/O.
//   - The persistence backend, defined by the Store interface. Two
//     implementations ship with the module: LocalStore (in-memory,
//     goroutine-safe, in this package) and RespStore (Redis/Valkey/KeyDB/
//     DragonflyDB, in the respstore subpackage).
//
// This separation makes the breaker correct by construction across both
// single-process and multi-process deployments: the state machine is the
// same in both cases, only the Store changes.
//
// # Quick start
//
//	cb, err := gobreaker.New[*http.Response](ctx, gobreaker.Settings{
//	    Name:    "user-service",
//	    Timeout: 30 * time.Second,
//	})
//	if err != nil { /* ... */ }
//
//	resp, err := cb.Execute(ctx, func(ctx context.Context) (*http.Response, error) {
//	    return http.DefaultClient.Do(req.WithContext(ctx))
//	})
//
// See the package examples and docs/DESIGN.md for the rationale behind the
// state-machine choices.
package gobreaker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CircuitBreaker is a state machine that prevents sending requests likely to
// fail. It is parameterised on the return type of the protected request so
// callers do not need to perform an interface{} type assertion.
//
// CircuitBreakers are safe for concurrent use by multiple goroutines.
type CircuitBreaker[T any] struct {
	settings Settings
	store    Store

	// localFallback is a single-process LocalStore used when the primary
	// store is unreachable and OnStoreFailure is FallbackToLocal. It is
	// lazily populated; nil until first failure.
	mu            sync.Mutex
	localFallback *LocalStore

	// now is the clock function. Overridable for tests via setClock.
	now func() time.Time
}

// New constructs a new CircuitBreaker. It calls Settings.Validate, applies
// defaults, and ensures that an initial Snapshot exists in the Store.
//
// New does not take ownership of Settings.Store: the caller is responsible
// for closing it. If Settings.Store is nil, a fresh LocalStore is created
// and owned by the breaker (Close will release it).
func New[T any](ctx context.Context, settings Settings) (*CircuitBreaker[T], error) {
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	settings = settings.defaults()

	store := settings.Store
	if store == nil {
		store = NewLocalStore()
		settings.Store = store
	}

	cb := &CircuitBreaker[T]{
		settings: settings,
		store:    store,
		now:      time.Now,
	}

	// Materialize an initial snapshot if none exists. We route this
	// through runUpdate (rather than store.Update directly) so that the
	// configured OnStoreFailure policy applies during construction too —
	// otherwise a transient backend outage would abort breaker creation
	// even when FallbackToLocal is set.
	if _, err := cb.runUpdate(ctx, cb.initialize); err != nil {
		return nil, fmt.Errorf("gobreaker: initialize %q: %w", settings.Name, err)
	}

	return cb, nil
}

// initialize is the UpdateFunc used by New. It leaves an existing snapshot
// untouched and only initializes a fresh one.
func (cb *CircuitBreaker[T]) initialize(current Snapshot, now time.Time) (Snapshot, error) {
	if !current.IsZero() {
		return current, nil
	}
	return Snapshot{
		State:           StateClosed,
		Generation:      1,
		GenerationStart: now,
		Expiry:          cb.closedExpiry(now),
	}, nil
}

// Name returns the breaker's name.
func (cb *CircuitBreaker[T]) Name() string {
	return cb.settings.Name
}

// State returns the current state of the breaker as observed by the Store.
// It performs a read against the Store and may incur a network round-trip
// for distributed implementations.
func (cb *CircuitBreaker[T]) State(ctx context.Context) (State, error) {
	snap, err := cb.loadSnapshot(ctx)
	if err != nil {
		return StateClosed, err
	}
	// Apply time-based transitions (open → half-open) without persisting.
	// This gives a "current" view without taking a write.
	now := cb.now()
	if snap.State == StateOpen && !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
		return StateHalfOpen, nil
	}
	return snap.State, nil
}

// Counts returns a snapshot of the current Counts as observed by the Store.
// Like State, it performs a read against the Store.
func (cb *CircuitBreaker[T]) Counts(ctx context.Context) (Counts, error) {
	snap, err := cb.loadSnapshot(ctx)
	if err != nil {
		return Counts{}, err
	}
	return snap.Counts, nil
}

// Execute runs req if the breaker admits it, then reports the outcome back
// to the breaker. The state machine guarantees:
//
//   - In the closed state, every request is admitted. The outcome may
//     trigger a transition to open via ReadyToOpen.
//   - In the open state, no request is admitted: Execute returns
//     ErrOpenState immediately. After Settings.Timeout has elapsed, the
//     next admission attempt transitions the breaker to half-open.
//   - In the half-open state, at most HalfOpenMaxInFlights requests are
//     admitted concurrently. Excess admissions return ErrTooManyRequests.
//     Successful outcomes feed ReadyToClose; failures feed ReadyToReopen.
//
// The supplied context is forwarded to req. If req panics, the panic is
// recovered, recorded as a failure, and re-raised after the breaker state is
// updated.
func (cb *CircuitBreaker[T]) Execute(ctx context.Context, req func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	admittedSnap, err := cb.admit(ctx)
	if err != nil {
		// Admission was refused (or the store failed). Report the
		// rejection to the observer if there is one. We pass the
		// observed state where possible: admittedSnap.State is set
		// when admit returned ErrOpenState/ErrTooManyRequests; for
		// other errors it is the zero state, which the observer can
		// interpret as "unknown".
		if cb.settings.Observer != nil {
			cb.settings.Observer.OnRequest(cb.settings.Name, false, admittedSnap.State)
		}
		return zero, err
	}
	if cb.settings.Observer != nil {
		cb.settings.Observer.OnRequest(cb.settings.Name, true, admittedSnap.State)
	}

	// At this point the request is admitted. We must report an outcome no
	// matter how req returns, including panics.
	var (
		result   T
		callErr  error
		panicVal any
	)

	start := cb.now()
	func() {
		defer func() {
			panicVal = recover()
		}()
		result, callErr = req(ctx)
	}()
	latency := cb.now().Sub(start)

	if panicVal != nil {
		// Treat the panic as a failure outcome before re-raising.
		_ = cb.report(ctx, admittedSnap.Generation, fmt.Errorf("gobreaker: request panicked: %v", panicVal))
		if cb.settings.Observer != nil {
			cb.settings.Observer.OnOutcome(cb.settings.Name, OutcomeFailure, latency)
		}
		panic(panicVal)
	}

	if cb.settings.Observer != nil {
		cb.settings.Observer.OnOutcome(cb.settings.Name, classifyOutcome(cb.settings, callErr), latency)
	}

	if err := cb.report(ctx, admittedSnap.Generation, callErr); err != nil {
		// Reporting failed (e.g. store unreachable). The protected
		// request itself succeeded — return its result and surface the
		// store error.
		if callErr == nil {
			return result, err
		}
		// If both the call and the report failed, the call error is
		// more relevant to the user.
		return result, callErr
	}

	return result, callErr
}

// classifyOutcome maps a request error to its Outcome label using the
// breaker's IsExcluded and IsSuccessful predicates.
func classifyOutcome(s Settings, err error) Outcome {
	if s.IsExcluded(err) {
		return OutcomeExclusion
	}
	if s.IsSuccessful(err) {
		return OutcomeSuccess
	}
	return OutcomeFailure
}

// admit is the read-modify-write step that decides whether to admit a new
// request. It runs as an UpdateFunc against the Store so distributed
// implementations can serialize admission across processes.
func (cb *CircuitBreaker[T]) admit(ctx context.Context) (Snapshot, error) {
	var (
		admitted     bool
		admitErr     error
		stateChanges []stateChange
	)

	snap, storeErr := cb.runUpdate(ctx, func(current Snapshot, now time.Time) (Snapshot, error) {
		// Reset per-call accumulators (the closure may run multiple
		// times if the store retries on conflict).
		admitted = false
		admitErr = nil
		stateChanges = stateChanges[:0]

		next := current
		if next.IsZero() {
			next = Snapshot{
				State:           StateClosed,
				Generation:      1,
				GenerationStart: now,
				Expiry:          cb.closedExpiry(now),
			}
		}

		// Apply any pending time-based transitions before deciding
		// admission.
		next = cb.advanceTime(next, now, &stateChanges)

		switch next.State {
		case StateOpen:
			admitErr = ErrOpenState
			return next, nil

		case StateHalfOpen:
			if next.Counts.InFlights >= cb.settings.HalfOpenMaxInFlights {
				admitErr = ErrTooManyRequests
				return next, nil
			}

		case StateClosed:
			// always admitted
		}

		next.Counts.onRequest()
		admitted = true
		return next, nil
	})

	if storeErr != nil {
		return Snapshot{}, storeErr
	}

	cb.fireStateChanges(stateChanges)

	if !admitted {
		// Return the snapshot we observed even on rejection so the
		// caller (and the Observer) can see the real state that
		// caused the rejection — typically StateOpen or StateHalfOpen.
		// Returning the zero Snapshot here would surface a misleading
		// "closed" state to OnRequest hooks.
		return snap, admitErr
	}
	return snap, nil
}

// report applies the outcome of an admitted request to the breaker state.
// generationAtAdmit is the generation that was current when the request was
// admitted; if the breaker has since rotated to a new generation (via state
// change or interval rollover), the outcome is discarded — it would be
// counted against a stale window.
func (cb *CircuitBreaker[T]) report(ctx context.Context, generationAtAdmit uint64, callErr error) error {
	var stateChanges []stateChange

	_, storeErr := cb.runUpdate(ctx, func(current Snapshot, now time.Time) (Snapshot, error) {
		stateChanges = stateChanges[:0]

		next := current
		next = cb.advanceTime(next, now, &stateChanges)

		// If the generation moved on while the request was in flight,
		// the in-flight slot has already been reset by the rollover —
		// just drop the outcome.
		if next.Generation != generationAtAdmit {
			return next, nil
		}

		switch {
		case cb.settings.IsExcluded(callErr):
			next.Counts.onExclusion()

		case cb.settings.IsSuccessful(callErr):
			next.Counts.onSuccess()
			if next.State == StateHalfOpen && cb.settings.ReadyToClose(next.Counts) {
				next = cb.transition(next, StateClosed, now, &stateChanges)
			}

		default:
			next.Counts.onFailure()
			switch next.State {
			case StateClosed:
				if cb.settings.ReadyToOpen(next.Counts) {
					next = cb.transition(next, StateOpen, now, &stateChanges)
				}
			case StateHalfOpen:
				if cb.settings.ReadyToReopen(next.Counts) {
					next = cb.transition(next, StateOpen, now, &stateChanges)
				}
			}
		}

		return next, nil
	})

	// Only publish state changes if the underlying Update actually
	// committed. Store implementations are allowed to invoke the closure
	// (and therefore append to stateChanges) even on a path that
	// ultimately returns an error — for example a CAS retry that exhausts
	// its budget. Emitting OnStateChange on such a path would report
	// transitions that were never persisted.
	if storeErr == nil {
		cb.fireStateChanges(stateChanges)
	}
	return storeErr
}

// stateChange records a transition that should be reported via OnStateChange
// after the Store update completes. Counts is captured immediately before
// the transition (i.e. the counts that triggered it), matching the v3 spec
// proposed by sony but not yet shipped.
type stateChange struct {
	from   State
	to     State
	counts Counts
}

// advanceTime applies time-based transitions (open → half-open after
// Timeout, closed → closed at Interval boundary). It does not perform
// outcome-driven transitions; those are handled in report.
func (cb *CircuitBreaker[T]) advanceTime(snap Snapshot, now time.Time, changes *[]stateChange) Snapshot {
	switch snap.State {
	case StateOpen:
		if !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
			snap = cb.transition(snap, StateHalfOpen, now, changes)
		}
	case StateClosed:
		if !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
			// Closed → closed: just rotate the generation, do not
			// fire OnStateChange (no actual state change).
			snap.Generation++
			snap.Counts.reset()
			snap.GenerationStart = now
			snap.Expiry = cb.closedExpiry(now)
		}
	}
	return snap
}

// transition records a state change in changes and returns a snapshot in the
// new generation with reset Counts and recomputed Expiry.
func (cb *CircuitBreaker[T]) transition(snap Snapshot, to State, now time.Time, changes *[]stateChange) Snapshot {
	if snap.State == to {
		return snap
	}
	*changes = append(*changes, stateChange{
		from:   snap.State,
		to:     to,
		counts: snap.Counts,
	})

	snap.State = to
	snap.Generation++
	snap.Counts.reset()
	snap.GenerationStart = now

	switch to {
	case StateClosed:
		snap.Expiry = cb.closedExpiry(now)
	case StateOpen:
		snap.Expiry = now.Add(cb.settings.Timeout)
	case StateHalfOpen:
		snap.Expiry = time.Time{}
	}
	return snap
}

// closedExpiry returns the expiry for the closed state given now and
// Settings.Interval. Zero Interval means "no expiry".
func (cb *CircuitBreaker[T]) closedExpiry(now time.Time) time.Time {
	if cb.settings.Interval <= 0 {
		return time.Time{}
	}
	return now.Add(cb.settings.Interval)
}

// fireStateChanges invokes OnStateChange and Observer.OnStateChange for
// each recorded transition. It is called after the Store Update completes
// so callbacks run without the breaker (or the Store) holding any locks.
// Callbacks are free to call back into cb.State / cb.Counts without
// deadlocking — fixing sony/gobreaker #37.
func (cb *CircuitBreaker[T]) fireStateChanges(changes []stateChange) {
	if len(changes) == 0 {
		return
	}
	for _, ch := range changes {
		if cb.settings.OnStateChange != nil {
			cb.settings.OnStateChange(cb.settings.Name, ch.from, ch.to, ch.counts)
		}
		if cb.settings.Observer != nil {
			cb.settings.Observer.OnStateChange(cb.settings.Name, ch.from, ch.to, ch.counts)
		}
	}
}

// loadSnapshot reads the current snapshot from the Store, transparently
// falling back to the local store on Store error if the policy permits.
func (cb *CircuitBreaker[T]) loadSnapshot(ctx context.Context) (Snapshot, error) {
	snap, err := cb.store.Get(ctx, cb.settings.Name)
	if err == nil {
		return snap, nil
	}
	if cb.settings.OnStoreFailure == FailFast {
		return Snapshot{}, fmt.Errorf("%w: %w", ErrStoreUnavailable, err)
	}
	return cb.localFallbackStore().Get(ctx, cb.settings.Name)
}

// runUpdate dispatches an UpdateFunc to the Store, transparently falling
// back to the local store on Store error if the policy permits.
func (cb *CircuitBreaker[T]) runUpdate(ctx context.Context, fn UpdateFunc) (Snapshot, error) {
	snap, err := cb.store.Update(ctx, cb.settings.Name, fn)
	if err == nil {
		return snap, nil
	}
	if cb.settings.OnStoreFailure == FailFast {
		return Snapshot{}, fmt.Errorf("%w: %w", ErrStoreUnavailable, err)
	}
	return cb.localFallbackStore().Update(ctx, cb.settings.Name, fn)
}

// localFallbackStore returns the lazily-allocated local fallback store.
func (cb *CircuitBreaker[T]) localFallbackStore() *LocalStore {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.localFallback == nil {
		cb.localFallback = NewLocalStore()
	}
	return cb.localFallback
}

// setClock replaces the breaker's clock function. Tests use it directly via
// the package-internal helper. The replacement is also propagated to the
// underlying LocalStore if there is one.
func (cb *CircuitBreaker[T]) setClock(now func() time.Time) {
	cb.now = now
	if ls, ok := cb.store.(*LocalStore); ok {
		ls.setClock(now)
	}
}
