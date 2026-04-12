package gobreaker

import (
	"context"
	"time"
)

// Snapshot is the complete persisted state of a CircuitBreaker at a single
// point in time. Snapshots are versioned: every successful Update increments
// Version, allowing optimistic concurrency control across distributed
// processes that share a Store.
//
// The zero value of Snapshot represents a freshly created breaker in the
// closed state with no history. Stores must return the zero value (with
// Version == 0) for keys that do not yet exist; they must NOT return an error.
type Snapshot struct {
	// Version is incremented by the Store on every successful write. It is
	// used to detect concurrent modifications. A value of 0 means the
	// snapshot is fresh and has never been persisted.
	Version uint64

	// State is the current breaker state.
	State State

	// Generation is incremented every time the breaker enters a new
	// generation (state transition or interval rollover). It is exposed so
	// in-flight requests from a previous generation can detect that they
	// must not update Counts.
	Generation uint64

	// Counts is the request statistics accumulated since the start of the
	// current generation.
	Counts Counts

	// GenerationStart is the wall-clock time at which the current
	// generation began. Used together with Settings.Interval to detect
	// closed-state rollover.
	GenerationStart time.Time

	// Expiry is the wall-clock time at which the current state expires:
	//   - StateClosed: end of the current Interval (zero if Interval == 0)
	//   - StateOpen:   end of the Timeout, after which the breaker becomes
	//                  half-open
	//   - StateHalfOpen: zero (no automatic expiry)
	Expiry time.Time
}

// IsZero reports whether s is the zero value (a freshly created breaker that
// has never been persisted).
func (s Snapshot) IsZero() bool {
	return s.Version == 0 && s.State == StateClosed && s.Generation == 0 &&
		s.Counts == (Counts{}) && s.GenerationStart.IsZero() && s.Expiry.IsZero()
}

// UpdateFunc is a pure transformation from one snapshot to the next, given
// the current wall-clock time. It must be deterministic and side-effect free
// because Store implementations may invoke it multiple times in the presence
// of concurrent modifications.
//
// If the function returns an error, the Update is aborted and the error is
// surfaced to the caller. Counts and other fields in the returned snapshot
// are written verbatim — the function is responsible for incrementing
// Generation, resetting Counts, and recomputing Expiry as needed.
type UpdateFunc func(current Snapshot, now time.Time) (Snapshot, error)

// Store persists CircuitBreaker snapshots and provides atomic read-modify-write
// semantics. Implementations must be safe for concurrent use by multiple
// goroutines and, in the case of distributed implementations, by multiple
// processes sharing the same backing store.
//
// Two implementations are provided in this module:
//   - LocalStore (in this package): goroutine-safe in-memory store, suitable
//     for single-process use, tests, and applications that do not need
//     cross-process state sharing.
//   - RespStore (in package github.com/rafet/gobreaker-redis/v2/respstore):
//     RESP-protocol store backed by go-redis/v9, compatible with Redis,
//     Valkey, KeyDB, and DragonflyDB.
type Store interface {
	// Get returns the current snapshot for key. If the key does not exist,
	// Get returns the zero Snapshot and a nil error.
	Get(ctx context.Context, key string) (Snapshot, error)

	// Update atomically reads the current snapshot for key, applies fn to
	// produce a new snapshot, and writes the new snapshot back. If the
	// store detects a concurrent modification it must retry by re-reading
	// and re-invoking fn until success or until its retry budget is
	// exhausted, in which case it returns ErrSnapshotConflict.
	//
	// fn may be invoked multiple times for a single Update call and must
	// therefore be free of observable side effects.
	//
	// On success, the returned snapshot has its Version field set to the
	// value that was actually persisted.
	Update(ctx context.Context, key string, fn UpdateFunc) (Snapshot, error)

	// Close releases any resources held by the store. After Close, all
	// other methods may return an error.
	Close() error
}
