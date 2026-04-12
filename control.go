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
	_, err := cb.runUpdate(ctx, func(current Snapshot, now time.Time) (Snapshot, error) {
		if current.State == StateOpen {
			return current, nil
		}
		current.State = StateOpen
		current.Generation++
		current.Counts.reset()
		current.GenerationStart = now
		current.Expiry = now.Add(cb.settings.Timeout)
		return current, nil
	})
	return err
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
	_, err := cb.runUpdate(ctx, func(current Snapshot, now time.Time) (Snapshot, error) {
		if current.State == StateClosed {
			return current, nil
		}
		current.State = StateClosed
		current.Generation++
		current.Counts.reset()
		current.GenerationStart = now
		current.Expiry = cb.closedExpiry(now)
		return current, nil
	})
	return err
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
	_, err := cb.runUpdate(ctx, func(_ Snapshot, now time.Time) (Snapshot, error) {
		return Snapshot{
			State:           StateClosed,
			Generation:      1,
			GenerationStart: now,
			Expiry:          cb.closedExpiry(now),
		}, nil
	})
	return err
}

// forceStateFast is the inlineSnap version of ForceOpen/ForceClosed/Reset.
func (cb *CircuitBreaker[T]) forceStateFast(target State) error {
	cb.inlineMu.Lock()
	now := time.Now()
	if cb.localStore.now != nil {
		now = cb.localStore.now()
	}
	snap := &cb.inlineSnap
	if snap.State == target && target != StateClosed {
		// Already in the target state; nothing to do (except Reset
		// which always resets even if already closed).
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
		snap.Expiry = now.Add(cb.settings.Timeout)
	case StateHalfOpen:
		snap.Expiry = time.Time{}
	}
	snap.Version++
	cb.inlineMu.Unlock()

	if from != target && (cb.settings.OnStateChange != nil || cb.settings.Observer != nil) {
		ch := &stateChange{from: from, to: target, counts: Counts{}}
		cb.fireStateChangesSlice(ch)
	}
	return nil
}
