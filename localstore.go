package gobreaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrStoreClosed is returned by LocalStore methods when called after Close.
// It satisfies the Store contract that "after Close, all other methods may
// return an error" by returning a stable, errors.Is-comparable sentinel
// instead of panicking on a nil map.
var ErrStoreClosed = errors.New("gobreaker: store is closed")

// LocalStore is an in-memory Store implementation that is safe for use by
// multiple goroutines within a single process. It is the default Store used
// by New when Settings.Store is nil.
//
// LocalStore is intentionally simple: it serializes all access to a given key
// behind a sync.Mutex. For single-process use this is sufficient and avoids
// the latency of a network round-trip. Tests and applications that need to
// fake a distributed store can use LocalStore directly.
type LocalStore struct {
	mu     sync.Mutex
	data   map[string]Snapshot
	closed bool
	now    func() time.Time // overridable for tests; nil means time.Now
}

// NewLocalStore returns a new, empty LocalStore.
func NewLocalStore() *LocalStore {
	return &LocalStore{data: make(map[string]Snapshot)}
}

// Get returns the current snapshot for key. If key does not exist, returns
// the zero Snapshot and a nil error. After Close, Get returns ErrStoreClosed.
func (s *LocalStore) Get(_ context.Context, key string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Snapshot{}, ErrStoreClosed
	}
	return s.data[key], nil
}

// Update atomically reads the current snapshot for key, applies fn, and
// writes the new snapshot back. Because LocalStore serializes access behind a
// mutex, fn is invoked exactly once per Update call. After Close, Update
// returns ErrStoreClosed without invoking fn.
func (s *LocalStore) Update(_ context.Context, key string, fn UpdateFunc) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Snapshot{}, ErrStoreClosed
	}

	now := time.Now()
	if s.now != nil {
		now = s.now()
	}

	current := s.data[key]
	next, err := fn(current, now)
	if err != nil {
		return Snapshot{}, err
	}

	next.Version = current.Version + 1
	s.data[key] = next
	return next, nil
}

// Close releases the in-memory state. Subsequent Get/Update calls return
// ErrStoreClosed. Close is idempotent: calling it multiple times is safe
// and always returns nil.
func (s *LocalStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.data = nil
	return nil
}

// setClock replaces the internal clock function for tests. It is unexported
// to keep the public API minimal; tests in this package use it directly.
func (s *LocalStore) setClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}
