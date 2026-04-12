package gobreaker

import (
	"context"
	"time"
)

// ForceOpen forces the breaker into the open state. All subsequent
// Execute calls will return ErrOpenState until the Timeout elapses
// (transitioning to half-open) or ForceClose / Reset is called.
//
// This is the operational "kill switch" for maintenance windows and
// incident response: it immediately stops all traffic to the protected
// upstream without waiting for failures to accumulate.
//
// ForceOpen writes through to the Store (if distributed) so all
// replicas sharing the same breaker name observe the forced state.
func (cb *CircuitBreaker[T]) ForceOpen(ctx context.Context) error {
	if cb.localStore != nil {
		return cb.forceStateFast(StateOpen)
	}
	return cb.forceStateStore(ctx, StateOpen)
}

// ForceClosed forces the breaker into the closed state. The breaker
// resumes admitting all traffic immediately, resetting Counts and
// starting a fresh generation.
//
// Use this when you have external evidence that the upstream is
// healthy and the breaker's open state is a false positive — for
// example, after a deployment that fixed the root cause.
func (cb *CircuitBreaker[T]) ForceClosed(ctx context.Context) error {
	if cb.localStore != nil {
		return cb.forceStateFast(StateClosed)
	}
	return cb.forceStateStore(ctx, StateClosed)
}

// Reset zeroes all Counts and returns the breaker to the closed state
// regardless of its current state. It starts a fresh generation.
//
// Reset is more aggressive than ForceClosed: it always resets Counts
// even if the breaker is already closed. Use it for testing, flaky
// state recovery, or after a schema migration that invalidates the
// existing Snapshot.
func (cb *CircuitBreaker[T]) Reset(ctx context.Context) error {
	if cb.localStore != nil {
		return cb.forceStateFast(StateClosed)
	}
	return cb.forceStateStore(ctx, StateClosed)
}

// forceStateStore is the generic (non-fast) implementation of
// ForceOpen/ForceClosed/Reset. It captures settings under the
// breaker's mutex to avoid races with concurrent UpdateSettings,
// then runs the state change through the Store, and fires the
// OnStateChange callback afterward.
func (cb *CircuitBreaker[T]) forceStateStore(ctx context.Context, target State) error {
	// Capture settings race-free. The closure below runs under the
	// Store's internal lock (different from cb.mu), so we must NOT
	// read cb.settings inside the closure.
	cb.mu.Lock()
	settings := cb.settings
	cb.mu.Unlock()

	var change *stateChange

	_, err := cb.runUpdateWith(ctx, settings, func(current Snapshot, now time.Time) (Snapshot, error) {
		change = nil // reset on retry
		if current.State == target && target != StateClosed {
			return current, nil
		}
		from := current.State
		current.State = target
		current.Generation++
		current.Counts.reset()
		current.GenerationStart = now
		switch target {
		case StateClosed:
			current.Expiry = closedExpiryFor(now, settings.Interval)
		case StateOpen:
			current.Expiry = now.Add(settings.Timeout)
		case StateHalfOpen:
			current.Expiry = time.Time{}
		}
		if from != target {
			change = &stateChange{from: from, to: target, counts: Counts{}}
		}
		return current, nil
	})
	if err != nil {
		return err
	}

	// Fire callback AFTER the Store update committed — matching the
	// contract that callbacks never run while the breaker holds a lock.
	if change != nil {
		if settings.OnStateChange != nil {
			settings.OnStateChange(settings.Name, change.from, change.to, change.counts)
		}
		if settings.Observer != nil {
			settings.Observer.OnStateChange(settings.Name, change.from, change.to, change.counts)
		}
	}
	return nil
}

// forceStateFast is the inlineSnap version of ForceOpen/ForceClosed/Reset.
func (cb *CircuitBreaker[T]) forceStateFast(target State) error {
	cb.inlineMu.Lock()
	now := time.Now()
	if cb.localStore.now != nil {
		now = cb.localStore.now()
	}
	// Capture settings under lock (same lock as UpdateSettings fast path).
	settings := cb.settings

	snap := &cb.inlineSnap
	if snap.State == target && target != StateClosed {
		cb.inlineMu.Unlock()
		return nil
	}
	from := snap.State
	snap.State = target
	snap.Generation++
	snap.Counts.reset()
	snap.GenerationStart = now
	switch target {
	case StateClosed:
		snap.Expiry = cb.closedExpiry(now)
	case StateOpen:
		snap.Expiry = now.Add(settings.Timeout)
	case StateHalfOpen:
		snap.Expiry = time.Time{}
	}
	snap.Version++
	cb.inlineMu.Unlock()

	if from != target {
		if settings.OnStateChange != nil {
			settings.OnStateChange(settings.Name, from, target, Counts{})
		}
		if settings.Observer != nil {
			settings.Observer.OnStateChange(settings.Name, from, target, Counts{})
		}
	}
	return nil
}
