package gobreaker

import "errors"

// Errors returned by CircuitBreaker.Execute and the Store interface.
var (
	// ErrOpenState is returned by Execute when the breaker is in the open
	// state and rejects the request without invoking it.
	ErrOpenState = errors.New("gobreaker: circuit is open")

	// ErrTooManyRequests is returned by Execute when the breaker is in the
	// half-open state and the in-flight request count has reached the
	// HalfOpenMaxInFlights limit. Half-open semantics are configurable via
	// ReadyToClose/ReadyToReopen, but in-flight admission control is the
	// only fixed safety rail: it prevents a stampede during recovery.
	ErrTooManyRequests = errors.New("gobreaker: too many in-flight requests in half-open state")

	// ErrInvalidSettings is returned by New when the supplied Settings are
	// internally inconsistent (e.g. negative durations, missing Name).
	ErrInvalidSettings = errors.New("gobreaker: invalid settings")

	// ErrStoreUnavailable is returned by Execute when the breaker's Store
	// could not be reached and Settings.OnStoreFailure is FailFast.
	ErrStoreUnavailable = errors.New("gobreaker: shared store is unavailable")

	// ErrSnapshotConflict is returned by a Store implementation when an
	// optimistic update fails because the snapshot was modified concurrently.
	// Stores are expected to retry internally; this error escapes only when
	// the retry budget is exhausted.
	ErrSnapshotConflict = errors.New("gobreaker: snapshot conflict")
)
