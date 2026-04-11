package gobreaker

import (
	"context"
	"errors"
)

// FallbackFunc is invoked by ExecuteWithFallback whenever the primary
// request would otherwise return an error. It receives the original error
// (which may be ErrOpenState, ErrTooManyRequests, or any error from the
// wrapped request) and may return a recovered value, propagate the error,
// or return a new error of its own.
//
// The fallback runs OUTSIDE the breaker's accounting: its outcome does not
// feed back into Counts. This is a deliberate choice — fallbacks are usually
// cheap recovery paths (cached values, default responses) whose health is
// not informative about the protected service.
type FallbackFunc[T any] func(ctx context.Context, err error) (T, error)

// ExecuteWithFallback runs req under the breaker. If req returns an error,
// or if the breaker rejects the request, fallback is invoked with that
// error and its result is returned to the caller.
//
// Common patterns:
//
//	// Return a cached value when the upstream is open
//	resp, err := cb.ExecuteWithFallback(ctx,
//	    func(ctx context.Context) (*User, error) { return fetchUser(ctx, id) },
//	    func(ctx context.Context, _ error) (*User, error) { return cache.Get(id), nil },
//	)
//
//	// Only fall back when the breaker is open, propagate real errors
//	resp, err := cb.ExecuteWithFallback(ctx, primary, gobreaker.OnOpenOnly[*User](fallback))
//
// The fallback is invoked synchronously on the calling goroutine.
func (cb *CircuitBreaker[T]) ExecuteWithFallback(
	ctx context.Context,
	req func(ctx context.Context) (T, error),
	fallback FallbackFunc[T],
) (T, error) {
	result, err := cb.Execute(ctx, req)
	if err == nil || fallback == nil {
		return result, err
	}
	return fallback(ctx, err)
}

// OnOpenOnly wraps a FallbackFunc so it runs only when the breaker rejected
// the request (ErrOpenState or ErrTooManyRequests). For any other error
// from the wrapped request, the original error is propagated unchanged.
//
// Use this when the fallback represents a "circuit-open contingency" rather
// than a general failure handler — for example, returning a stale cached
// value only because the breaker has decided the upstream is unhealthy.
func OnOpenOnly[T any](fallback FallbackFunc[T]) FallbackFunc[T] {
	return func(ctx context.Context, err error) (T, error) {
		if errors.Is(err, ErrOpenState) || errors.Is(err, ErrTooManyRequests) {
			return fallback(ctx, err)
		}
		var zero T
		return zero, err
	}
}
