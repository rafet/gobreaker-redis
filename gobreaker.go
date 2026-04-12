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

	// localStore is set to a non-nil value if and only if Settings.Store
	// is a *LocalStore. When set, Execute takes a closure-free fast path
	// that bypasses the Store interface dispatch and directly mutates
	// the in-memory snapshot. This eliminates per-call closure
	// allocations that would otherwise dominate the hot path.
	//
	// The fast path is observable only as a performance characteristic:
	// it produces identical state transitions and identical observer
	// events as the generic store path.
	localStore *LocalStore

	// inlineSnap is the breaker's authoritative snapshot when running
	// on the fast path. It is protected by inlineMu and synchronized
	// with localStore on every read/write so the public Get/Update
	// surface of the LocalStore continues to reflect the current
	// state. The cached pointer here exists purely to avoid the map
	// lookup that LocalStore.Update would otherwise perform on every
	// request.
	inlineMu   sync.Mutex
	inlineSnap Snapshot

	// localFallback is a single-process LocalStore used when the primary
	// store is unreachable and OnStoreFailure is FallbackToLocal. It is
	// lazily populated; nil until first failure.
	mu            sync.RWMutex
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
	if ls, ok := store.(*LocalStore); ok {
		cb.localStore = ls
	}

	// Materialize an initial snapshot if none exists. We route this
	// through runUpdate (rather than store.Update directly) so that the
	// configured OnStoreFailure policy applies during construction too —
	// otherwise a transient backend outage would abort breaker creation
	// even when FallbackToLocal is set.
	snap, err := cb.runUpdate(ctx, cb.initialize)
	if err != nil {
		return nil, fmt.Errorf("gobreaker: initialize %q: %w", settings.Name, err)
	}

	// Cache the initial snapshot in inlineSnap so the fast path does
	// not need to do a map lookup on the very first call. Subsequent
	// fast-path calls keep this cache in sync.
	if cb.localStore != nil {
		cb.inlineSnap = snap
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

// State returns the current state of the breaker. For LocalStore-backed
// breakers (the fast path) this is an in-memory read with no I/O. For
// distributed Stores it performs a read against the Store and may incur
// a network round-trip.
func (cb *CircuitBreaker[T]) State(ctx context.Context) (State, error) {
	if cb.localStore != nil {
		cb.inlineMu.Lock()
		state := cb.inlineSnap.State
		expiry := cb.inlineSnap.Expiry
		cb.inlineMu.Unlock()
		now := cb.now()
		if state == StateOpen && !expiry.IsZero() && !now.Before(expiry) {
			return StateHalfOpen, nil
		}
		return state, nil
	}
	snap, err := cb.loadSnapshot(ctx)
	if err != nil {
		return StateClosed, err
	}
	now := cb.now()
	if snap.State == StateOpen && !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
		return StateHalfOpen, nil
	}
	return snap.State, nil
}

// Counts returns a snapshot of the current Counts. For LocalStore-backed
// breakers it is an in-memory read.
func (cb *CircuitBreaker[T]) Counts(ctx context.Context) (Counts, error) {
	if cb.localStore != nil {
		cb.inlineMu.Lock()
		c := cb.inlineSnap.Counts
		cb.inlineMu.Unlock()
		return c, nil
	}
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
	if cb.localStore != nil {
		return cb.executeFast(ctx, req)
	}
	return cb.executeStore(ctx, req)
}

// executeStore is the generic Execute implementation that goes through the
// Store interface. It is used whenever the breaker's Store is anything other
// than a *LocalStore.
func (cb *CircuitBreaker[T]) executeStore(ctx context.Context, req func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	// Snapshot settings once for the entire call so concurrent
	// UpdateSettings cannot race with our field reads.
	settings := cb.loadSettings()

	admittedSnap, err := cb.admit(ctx, settings)
	if err != nil {
		if settings.Observer != nil {
			settings.Observer.OnRequest(settings.Name, false, admittedSnap.State)
		}
		return zero, err
	}
	if settings.Observer != nil {
		settings.Observer.OnRequest(settings.Name, true, admittedSnap.State)
	}

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
		_ = cb.report(ctx, settings, admittedSnap.Generation, fmt.Errorf("gobreaker: request panicked: %v", panicVal))
		if settings.Observer != nil {
			settings.Observer.OnOutcome(settings.Name, OutcomeFailure, latency)
		}
		panic(panicVal)
	}

	if settings.Observer != nil {
		settings.Observer.OnOutcome(settings.Name, classifyOutcome(settings, callErr), latency)
	}

	if err := cb.report(ctx, settings, admittedSnap.Generation, callErr); err != nil {
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

// executeFast is the closure-free Execute implementation that runs when the
// breaker's Store is a *LocalStore. It produces identical observable behavior
// to executeStore (state transitions, observer events, OnStateChange
// callbacks) but avoids the per-call closure allocations that the Store
// interface requires.
//
// The structure is otherwise the same: an admit phase that decides whether
// to run the request, then the request itself, then a report phase that
// updates state. Both phases acquire cb.inlineMu directly and operate on
// cb.inlineSnap by pointer, eliminating the per-call map lookup that the
// LocalStore.Update path would otherwise perform.
func (cb *CircuitBreaker[T]) executeFast(ctx context.Context, req func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	// ============ admit phase ============
	cb.inlineMu.Lock()
	now := time.Now()
	if cb.localStore.now != nil {
		now = cb.localStore.now()
	}

	// Snapshot settings-dependent flags under the lock so concurrent
	// UpdateSettings calls cannot race with our reads.
	settings := cb.settings

	snap := &cb.inlineSnap

	// Apply time-based transitions before deciding admission.
	var admitChange *stateChange
	switch snap.State {
	case StateOpen:
		if !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
			admitChange = &stateChange{from: StateOpen, to: StateHalfOpen, counts: snap.Counts}
			snap.State = StateHalfOpen
			snap.Generation++
			snap.Counts.reset()
			snap.GenerationStart = now
			snap.Expiry = time.Time{}
		}
	case StateClosed:
		if !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
			snap.Generation++
			snap.Counts.reset()
			snap.GenerationStart = now
			snap.Expiry = cb.closedExpiry(now)
		}
	}

	var admitErr error
	switch snap.State {
	case StateOpen:
		admitErr = ErrOpenState
	case StateHalfOpen:
		if snap.Counts.InFlights >= settings.HalfOpenMaxInFlights {
			admitErr = ErrTooManyRequests
		} else if settings.HalfOpenAdmission != nil {
			elapsed := now.Sub(snap.GenerationStart)
			if !settings.HalfOpenAdmission.Admit(elapsed) {
				admitErr = ErrTooManyRequests
			}
		}
	}
	if admitErr != nil {
		snap.Version++
		stateAtReject := snap.State
		cb.syncToLocalStoreLocked()
		cb.inlineMu.Unlock()
		if admitChange != nil && (settings.OnStateChange != nil || settings.Observer != nil) {
			cb.fireStateChangesSlice(settings, admitChange)
		}
		if settings.Observer != nil {
			settings.Observer.OnRequest(settings.Name, false, stateAtReject)
		}
		return zero, admitErr
	}

	snap.Counts.onRequest()
	admittedGen := snap.Generation
	admittedState := snap.State
	snap.Version++
	cb.inlineMu.Unlock()

	if admitChange != nil && (settings.OnStateChange != nil || settings.Observer != nil) {
		cb.fireStateChangesSlice(settings, admitChange)
	}
	if settings.Observer != nil {
		settings.Observer.OnRequest(settings.Name, true, admittedState)
	}

	// ============ run the request ============
	var (
		result   T
		callErr  error
		panicVal any
	)
	// Latency timing is only needed when an Observer is registered.
	// Skipping the two cb.now() calls in the no-observer case (the
	// vast majority of single-process production usage) shaves 60+ns
	// off the hot path because time.Now()/runtime.walltime dominates
	// the wall-clock cost on most platforms.
	var start time.Time
	hasObserver := settings.Observer != nil
	if hasObserver {
		start = cb.now()
	}
	func() {
		defer func() {
			panicVal = recover()
		}()
		result, callErr = req(ctx)
	}()
	var latency time.Duration
	if hasObserver {
		latency = cb.now().Sub(start)
	}

	if panicVal != nil {
		// Treat the panic as a failure and re-raise. We do not call
		// the report phase for the panic path; instead we apply the
		// failure inline and re-raise.
		cb.reportFastInline(admittedGen, fmt.Errorf("gobreaker: request panicked: %v", panicVal))
		if hasObserver {
			settings.Observer.OnOutcome(settings.Name, OutcomeFailure, latency)
		}
		panic(panicVal)
	}

	if hasObserver {
		settings.Observer.OnOutcome(settings.Name, classifyOutcome(settings, callErr), latency)
	}

	cb.reportFastInline(admittedGen, callErr)
	return result, callErr
}

// reportFastInline applies the outcome of an admitted request to the
// in-memory snapshot. It is the closure-free counterpart to
// CircuitBreaker.report. Concurrency is provided by cb.inlineMu.
//
// Transitions captured during the call are reported via fireStateChanges
// after the lock is released, mirroring the executeStore guarantee that
// callbacks never run while the breaker is locked.
func (cb *CircuitBreaker[T]) reportFastInline(admittedGen uint64, callErr error) {
	cb.inlineMu.Lock()
	now := time.Now()
	if cb.localStore.now != nil {
		now = cb.localStore.now()
	}

	// Capture settings under lock so concurrent UpdateSettings cannot race.
	settings := cb.settings

	snap := &cb.inlineSnap

	// Apply time-based transitions first.
	var ch1 *stateChange
	switch snap.State {
	case StateOpen:
		if !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
			ch1 = &stateChange{from: StateOpen, to: StateHalfOpen, counts: snap.Counts}
			snap.State = StateHalfOpen
			snap.Generation++
			snap.Counts.reset()
			snap.GenerationStart = now
			snap.Expiry = time.Time{}
		}
	case StateClosed:
		if !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
			snap.Generation++
			snap.Counts.reset()
			snap.GenerationStart = now
			snap.Expiry = cb.closedExpiry(now)
		}
	}

	if snap.Generation != admittedGen {
		// The breaker rolled over while the request was in flight.
		// The outcome belongs to a stale generation; drop it.
		snap.Version++
		cb.syncToLocalStoreLocked()
		cb.inlineMu.Unlock()
		if ch1 != nil {
			cb.fireStateChangesSlice(settings, ch1)
		}
		return
	}

	var ch2 *stateChange
	switch {
	case settings.IsExcluded(callErr):
		snap.Counts.onExclusion()
	case settings.IsSuccessful(callErr):
		snap.Counts.onSuccess()
		switch snap.State {
		case StateClosed:
			// Evaluate ReadyToOpen even on success. This enables
			// latency-aware predicates that trip on slow-but-successful
			// requests (e.g. LatencyP99Above). Traditional failure-count
			// predicates like ConsecutiveFailures(5) naturally return
			// false here because ConsecutiveFailures == 0 after a
			// success, so this is backward-compatible.
			if settings.ReadyToOpen(snap.Counts) {
				ch2 = &stateChange{from: snap.State, to: StateOpen, counts: snap.Counts}
				snap.State = StateOpen
				snap.Generation++
				snap.Counts.reset()
				snap.GenerationStart = now
				snap.Expiry = now.Add(settings.Timeout)
			}
		case StateHalfOpen:
			if settings.ReadyToClose(snap.Counts) {
				ch2 = &stateChange{from: snap.State, to: StateClosed, counts: snap.Counts}
				snap.State = StateClosed
				snap.Generation++
				snap.Counts.reset()
				snap.GenerationStart = now
				snap.Expiry = cb.closedExpiry(now)
			}
		}
	default:
		snap.Counts.onFailure()
		switch snap.State {
		case StateClosed:
			if settings.ReadyToOpen(snap.Counts) {
				ch2 = &stateChange{from: snap.State, to: StateOpen, counts: snap.Counts}
				snap.State = StateOpen
				snap.Generation++
				snap.Counts.reset()
				snap.GenerationStart = now
				snap.Expiry = now.Add(settings.Timeout)
			}
		case StateHalfOpen:
			if settings.ReadyToReopen(snap.Counts) {
				ch2 = &stateChange{from: snap.State, to: StateOpen, counts: snap.Counts}
				snap.State = StateOpen
				snap.Generation++
				snap.Counts.reset()
				snap.GenerationStart = now
				snap.Expiry = now.Add(settings.Timeout)
			}
		}
	}

	snap.Version++
	cb.inlineMu.Unlock()

	if ch1 != nil {
		cb.fireStateChangesSlice(settings, ch1)
	}
	if ch2 != nil {
		cb.fireStateChangesSlice(settings, ch2)
	}
}

// syncToLocalStoreLocked publishes the in-memory inlineSnap into the
// underlying LocalStore so the public Get/Update surface keeps
// observing the same state. The caller must hold cb.inlineMu.
//
// We deliberately use a separate mutex (LocalStore.mu) here even though
// it makes the lock ordering subtle: callers that mutate the
// LocalStore directly via Update will see the inlineSnap value after
// our publish. The two locks are never held in inverse order so there
// is no deadlock potential.
func (cb *CircuitBreaker[T]) syncToLocalStoreLocked() {
	cb.localStore.mu.Lock()
	if !cb.localStore.closed {
		cb.localStore.data[cb.settings.Name] = cb.inlineSnap
	}
	cb.localStore.mu.Unlock()
}

// fireStateChangesSlice is a single-stateChange convenience over
// fireStateChanges. It exists so the fast path can pass a *stateChange
// without allocating a backing slice on the heap on the steady-state
// success path (where no transition occurs and the function is never
// called).
func (cb *CircuitBreaker[T]) fireStateChangesSlice(s Settings, ch *stateChange) {
	if s.OnStateChange != nil {
		s.OnStateChange(s.Name, ch.from, ch.to, ch.counts)
	}
	if s.Observer != nil {
		s.Observer.OnStateChange(s.Name, ch.from, ch.to, ch.counts)
	}
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
func (cb *CircuitBreaker[T]) admit(ctx context.Context, settings Settings) (Snapshot, error) {
	var (
		admitted     bool
		admitErr     error
		stateChanges []stateChange
	)

	snap, storeErr := cb.runUpdateWith(ctx, settings, func(current Snapshot, now time.Time) (Snapshot, error) {
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
				Expiry:          closedExpiryFor(now, settings.Interval),
			}
		}

		// Apply any pending time-based transitions before deciding
		// admission.
		next = advanceTime(next, now, settings, &stateChanges)

		switch next.State {
		case StateOpen:
			admitErr = ErrOpenState
			return next, nil

		case StateHalfOpen:
			if next.Counts.InFlights >= settings.HalfOpenMaxInFlights {
				admitErr = ErrTooManyRequests
				return next, nil
			}
			if settings.HalfOpenAdmission != nil {
				elapsed := now.Sub(next.GenerationStart)
				if !settings.HalfOpenAdmission.Admit(elapsed) {
					admitErr = ErrTooManyRequests
					return next, nil
				}
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

	fireStateChanges(settings, stateChanges)

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
func (cb *CircuitBreaker[T]) report(ctx context.Context, settings Settings, generationAtAdmit uint64, callErr error) error {
	var stateChanges []stateChange

	_, storeErr := cb.runUpdateWith(ctx, settings, func(current Snapshot, now time.Time) (Snapshot, error) {
		stateChanges = stateChanges[:0]

		next := current
		next = advanceTime(next, now, settings, &stateChanges)

		// If the generation moved on while the request was in flight,
		// the in-flight slot has already been reset by the rollover —
		// just drop the outcome.
		if next.Generation != generationAtAdmit {
			return next, nil
		}

		switch {
		case settings.IsExcluded(callErr):
			next.Counts.onExclusion()

		case settings.IsSuccessful(callErr):
			next.Counts.onSuccess()
			switch next.State {
			case StateClosed:
				if settings.ReadyToOpen(next.Counts) {
					next = transition(next, StateOpen, now, settings, &stateChanges)
				}
			case StateHalfOpen:
				if settings.ReadyToClose(next.Counts) {
					next = transition(next, StateClosed, now, settings, &stateChanges)
				}
			}

		default:
			next.Counts.onFailure()
			switch next.State {
			case StateClosed:
				if settings.ReadyToOpen(next.Counts) {
					next = transition(next, StateOpen, now, settings, &stateChanges)
				}
			case StateHalfOpen:
				if settings.ReadyToReopen(next.Counts) {
					next = transition(next, StateOpen, now, settings, &stateChanges)
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
		fireStateChanges(settings, stateChanges)
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
func advanceTime(snap Snapshot, now time.Time, settings Settings, changes *[]stateChange) Snapshot {
	switch snap.State {
	case StateOpen:
		if !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
			snap = transition(snap, StateHalfOpen, now, settings, changes)
		}
	case StateClosed:
		if !snap.Expiry.IsZero() && !now.Before(snap.Expiry) {
			// Closed → closed: just rotate the generation, do not
			// fire OnStateChange (no actual state change).
			snap.Generation++
			snap.Counts.reset()
			snap.GenerationStart = now
			snap.Expiry = closedExpiryFor(now, settings.Interval)
		}
	}
	return snap
}

// transition records a state change in changes and returns a snapshot in the
// new generation with reset Counts and recomputed Expiry.
func transition(snap Snapshot, to State, now time.Time, settings Settings, changes *[]stateChange) Snapshot {
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
		snap.Expiry = closedExpiryFor(now, settings.Interval)
	case StateOpen:
		snap.Expiry = now.Add(settings.Timeout)
	case StateHalfOpen:
		snap.Expiry = time.Time{}
	}
	return snap
}

// closedExpiry returns the expiry for the closed state. Zero Interval
// means "no expiry". The interval parameter is passed explicitly to
// avoid reading cb.settings outside a lock — callers must capture the
// interval from a settings snapshot taken under the appropriate lock.
func closedExpiryFor(now time.Time, interval time.Duration) time.Time {
	if interval <= 0 {
		return time.Time{}
	}
	return now.Add(interval)
}

// closedExpiry is the legacy helper that reads interval from
// cb.settings. It is ONLY safe to call from code paths that hold
// cb.inlineMu (the fast path) or cb.mu.RLock (the generic path).
func (cb *CircuitBreaker[T]) closedExpiry(now time.Time) time.Time {
	return closedExpiryFor(now, cb.settings.Interval)
}

// fireStateChanges invokes OnStateChange and Observer.OnStateChange for
// each recorded transition. It is called after the Store Update completes
// so callbacks run without the breaker (or the Store) holding any locks.
// Callbacks are free to call back into cb.State / cb.Counts without
// deadlocking — fixing sony/gobreaker #37.
func fireStateChanges(s Settings, changes []stateChange) {
	if len(changes) == 0 {
		return
	}
	for _, ch := range changes {
		if s.OnStateChange != nil {
			s.OnStateChange(s.Name, ch.from, ch.to, ch.counts)
		}
		if s.Observer != nil {
			s.Observer.OnStateChange(s.Name, ch.from, ch.to, ch.counts)
		}
	}
}

// loadSnapshot reads the current snapshot from the Store, transparently
// falling back to the local store on Store error if the policy permits.
func (cb *CircuitBreaker[T]) loadSnapshot(ctx context.Context) (Snapshot, error) {
	settings := cb.loadSettings()
	return cb.loadSnapshotWith(ctx, settings)
}

// loadSnapshotWith is like loadSnapshot but uses a pre-captured Settings
// to avoid reading cb.settings without a lock.
func (cb *CircuitBreaker[T]) loadSnapshotWith(ctx context.Context, settings Settings) (Snapshot, error) {
	snap, err := cb.store.Get(ctx, settings.Name)
	if err == nil {
		return snap, nil
	}
	if settings.OnStoreFailure == FailFast {
		return Snapshot{}, fmt.Errorf("%w: %w", ErrStoreUnavailable, err)
	}
	return cb.localFallbackStore().Get(ctx, settings.Name)
}

// runUpdate dispatches an UpdateFunc to the Store, transparently falling
// back to the local store on Store error if the policy permits.
func (cb *CircuitBreaker[T]) runUpdate(ctx context.Context, fn UpdateFunc) (Snapshot, error) {
	settings := cb.loadSettings()
	return cb.runUpdateWith(ctx, settings, fn)
}

// runUpdateWith is like runUpdate but uses a pre-captured Settings to
// avoid reading cb.settings without a lock.
func (cb *CircuitBreaker[T]) runUpdateWith(ctx context.Context, settings Settings, fn UpdateFunc) (Snapshot, error) {
	snap, err := cb.store.Update(ctx, settings.Name, fn)
	if err == nil {
		return snap, nil
	}
	if settings.OnStoreFailure == FailFast {
		return Snapshot{}, fmt.Errorf("%w: %w", ErrStoreUnavailable, err)
	}
	return cb.localFallbackStore().Update(ctx, settings.Name, fn)
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

// loadSettings returns a race-free copy of the breaker's Settings.
// On the fast path (localStore != nil), settings are captured under
// inlineMu at the start of each call, so this helper is only needed
// by the generic executeStore / admit / report code paths.
func (cb *CircuitBreaker[T]) loadSettings() Settings {
	cb.mu.RLock()
	s := cb.settings
	cb.mu.RUnlock()
	return s
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
