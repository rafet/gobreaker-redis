package gobreaker

import (
	"context"
	"sync"
	"time"
)

// LocalStore is an in-memory Store implementation that is safe for use by
// multiple goroutines within a single process. It is the default Store used
// by New when Settings.Store is nil.
//
// LocalStore is intentionally simple: it serializes all access to a given key
// behind a sync.RWMutex. For single-process use this is sufficient and avoids
// the latency of a network round-trip. Tests and applications that need to
// fake a distributed store can use LocalStore directly.
type LocalStore struct {
	mu   sync.Mutex
	data map[string]Snapshot
	now  func() time.Time // overridable for tests; nil means time.Now
}

// NewLocalStore returns a new, empty LocalStore.
func NewLocalStore() *LocalStore {
	return &LocalStore{data: make(map[string]Snapshot)}
}

// Get returns the current snapshot for key. If key does not exist, returns
// the zero Snapshot and a nil error.
func (s *LocalStore) Get(_ context.Context, key string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[key], nil
}

// Update atomically reads the current snapshot for key, applies fn, and
// writes the new snapshot back. Because LocalStore serializes access behind a
// mutex, fn is invoked exactly once per Update call.
func (s *LocalStore) Update(_ context.Context, key string, fn UpdateFunc) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

// Close is a no-op for LocalStore.
func (s *LocalStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
