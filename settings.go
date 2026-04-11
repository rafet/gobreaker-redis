package gobreaker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// StoreFailurePolicy controls what happens when the breaker's Store cannot be
// reached. The choice is a fundamental availability/correctness tradeoff and
// must be made consciously per breaker.
type StoreFailurePolicy int

const (
	// FallbackToLocal degrades the breaker to single-process mode using a
	// local in-memory snapshot when the Store is unreachable. The breaker
	// continues to admit and reject requests according to its state machine
	// but loses cross-process coordination until the Store recovers.
	//
	// This is the default. It prioritizes availability of the protected
	// service over strict correctness of the distributed state.
	FallbackToLocal StoreFailurePolicy = 0

	// FailFast causes Execute to return ErrStoreUnavailable immediately
	// whenever the Store cannot be reached. The protected request is NOT
	// invoked. Use this when stale or divergent breaker state is more
	// dangerous than rejecting traffic.
	FailFast StoreFailurePolicy = 1
)

// Settings configures a CircuitBreaker. Most fields are optional and have
// sensible defaults; only Name is required.
//
// The state machine has three customizable transitions, each driven by its
// own predicate:
//
//	closed   --ReadyToOpen-->   open
//	open     --(after Timeout)-->   half-open
//	half-open --ReadyToClose-->  closed
//	half-open --ReadyToReopen--> open
//
// This is a deliberate departure from sony/gobreaker, which conflates the
// half-open success threshold with MaxRequests and uses an unconfigurable
// "any failure reopens" rule. Splitting the three predicates makes the
// half-open phase customizable without ambiguity (see issues #30, #53, #63
// in sony/gobreaker).
type Settings struct {
	// Name identifies the breaker in logs, metrics, and Store keys. Must
	// be non-empty. Two breakers in the same process or sharing the same
	// Store must have distinct names.
	Name string

	// Interval is the cyclic period of the closed state. At every Interval
	// boundary the breaker resets Counts to zero, starting a fresh
	// generation. If Interval is zero, Counts accumulate indefinitely
	// while the breaker remains closed (and reset only on state
	// transitions).
	Interval time.Duration

	// Timeout is the period the breaker remains in the open state before
	// transitioning to half-open. If zero, defaults to 60 seconds.
	// Negative values are rejected by Validate.
	Timeout time.Duration

	// HalfOpenMaxInFlights bounds the number of requests admitted
	// concurrently in the half-open state. This is the only safety rail
	// that is not user-configurable as a predicate: it prevents a stampede
	// during recovery. If zero, defaults to 1 (single-probe semantics
	// matching most circuit-breaker descriptions in the literature).
	HalfOpenMaxInFlights uint64

	// ReadyToOpen is evaluated after every failed request in the closed
	// state. If it returns true, the breaker transitions to open. If nil,
	// defaults to ConsecutiveFailures(5).
	ReadyToOpen ReadyToFunc

	// ReadyToClose is evaluated after every successful request in the
	// half-open state. If it returns true, the breaker transitions to
	// closed. If nil, defaults to ConsecutiveSuccesses(HalfOpenMaxInFlights),
	// which means "close after observing as many consecutive successes as
	// the maximum number of probes we allow concurrently".
	ReadyToClose ReadyToFunc

	// ReadyToReopen is evaluated after every failed request in the
	// half-open state. If it returns true, the breaker transitions back to
	// open. If nil, defaults to Always (any failure reopens). This is the
	// conventional behavior; relaxing it lets the breaker tolerate
	// transient blips during recovery, at the cost of admitting more
	// requests to a service that may still be unhealthy.
	ReadyToReopen ReadyToFunc

	// IsSuccessful classifies the error returned by a request as a
	// success or failure. If nil, any non-nil error is treated as a
	// failure.
	IsSuccessful func(err error) bool

	// IsExcluded classifies certain errors as outcomes that should not
	// affect the breaker state at all (neither success nor failure).
	// Typical examples are context.Canceled and context.DeadlineExceeded
	// originating from the caller, which say nothing about the health of
	// the protected service. If nil, no errors are excluded.
	IsExcluded func(err error) bool

	// OnStateChange is called whenever the breaker transitions between
	// states. It receives a snapshot of Counts as observed immediately
	// before the transition (so callers can see what triggered it) and is
	// invoked WITHOUT the breaker's internal lock held. This means it is
	// safe to call back into the breaker (e.g. cb.Counts()) from inside
	// the callback — a long-standing footgun in sony/gobreaker (issue #37).
	//
	// The callback runs synchronously on the goroutine that triggered the
	// transition. Long-running work should be dispatched to a separate
	// goroutine to avoid stalling request handling.
	OnStateChange func(name string, from State, to State, counts Counts)

	// Store is the persistence backend for the breaker's snapshot. If
	// nil, a fresh LocalStore is created and used. Pass the same Store
	// instance to multiple breakers to share keyspace; the Name field
	// disambiguates breakers within a Store.
	Store Store

	// OnStoreFailure controls behavior when Store operations fail.
	// Defaults to FallbackToLocal.
	OnStoreFailure StoreFailurePolicy

	// Observer, if non-nil, receives a structured event stream from the
	// breaker (admissions, outcomes, state changes). It is the integration
	// hook for metrics and tracing libraries; see the Observer interface
	// documentation for guarantees and constraints.
	Observer Observer
}

// defaults populates zero-valued fields with their defaults. It returns a
// fresh Settings with the same Name and Store; the receiver is not modified.
func (s Settings) defaults() Settings {
	out := s
	if out.Timeout <= 0 {
		out.Timeout = 60 * time.Second
	}
	if out.HalfOpenMaxInFlights == 0 {
		out.HalfOpenMaxInFlights = 1
	}
	if out.ReadyToOpen == nil {
		out.ReadyToOpen = ConsecutiveFailures(5)
	}
	if out.ReadyToClose == nil {
		out.ReadyToClose = ConsecutiveSuccesses(out.HalfOpenMaxInFlights)
	}
	if out.ReadyToReopen == nil {
		out.ReadyToReopen = Always
	}
	if out.IsSuccessful == nil {
		out.IsSuccessful = defaultIsSuccessful
	}
	if out.IsExcluded == nil {
		out.IsExcluded = defaultIsExcluded
	}
	return out
}

// Validate checks Settings for internal consistency. It is called by New
// before any state is created.
func (s Settings) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidSettings)
	}
	if s.Interval < 0 {
		return fmt.Errorf("%w: Interval must be non-negative", ErrInvalidSettings)
	}
	if s.Timeout < 0 {
		return fmt.Errorf("%w: Timeout must be non-negative", ErrInvalidSettings)
	}
	return nil
}

func defaultIsSuccessful(err error) bool {
	return err == nil
}

func defaultIsExcluded(_ error) bool {
	return false
}

// IgnoreContextErrors is a convenience IsExcluded function that excludes
// context.Canceled and context.DeadlineExceeded errors (and any error that
// wraps them). It is the recommended setting whenever the breaker wraps a
// request whose context is controlled by the caller.
func IgnoreContextErrors(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
