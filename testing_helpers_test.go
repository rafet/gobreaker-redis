package gobreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// This file contains test helpers shared across the package's _test.go
// files. They are NOT exported. Putting them in a single file makes the
// failure-injection arsenal easy to discover, audit, and extend when a new
// failure mode needs to be tested.

// closureRanFailingStore is a Store implementation that runs the
// UpdateFunc closure (so any state-machine logic inside the closure
// observes a fresh Snapshot and produces output) and THEN returns an
// error from Update. This simulates the realistic distributed-store
// failure mode where the read succeeds, the closure mutates state, but
// the commit (the CAS write) fails.
//
// The standard failingStore fails IMMEDIATELY, before the closure runs,
// so it cannot exercise any code path that depends on the closure having
// produced output before the error surfaces. closureRanFailingStore is the
// helper that lets us test bugs like "fireStateChanges fires after a
// failed Update" — see the regression test for issue #7 in the v2 review.
type closureRanFailingStore struct {
	mu        sync.Mutex
	snap      Snapshot
	updates   int64
	commitErr error
}

func newClosureRanFailingStore(commitErr error) *closureRanFailingStore {
	if commitErr == nil {
		commitErr = errors.New("closureRanFailingStore: commit failed")
	}
	return &closureRanFailingStore{commitErr: commitErr}
}

func (s *closureRanFailingStore) Get(_ context.Context, _ string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap, nil
}

func (s *closureRanFailingStore) Update(_ context.Context, _ string, fn UpdateFunc) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	atomic.AddInt64(&s.updates, 1)

	// Pretend the read succeeded, run the closure (which is allowed to
	// produce a transition), then fail the commit.
	_, err := fn(s.snap, time.Now())
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{}, s.commitErr
}

func (s *closureRanFailingStore) Close() error { return nil }

func (s *closureRanFailingStore) callCount() int64 {
	return atomic.LoadInt64(&s.updates)
}

// flakyStore wraps another Store with explicit "skip then fail then
// pass" semantics. It is the standard helper for tests that need to
// position a failure at a specific call in a sequence: e.g. "the first
// Update should succeed (admit), the second should fail (report)".
//
// Counters are decremented per call. Once both skip and fail counters
// reach zero, all subsequent calls are delegated to the inner store
// transparently. Updates and Gets are tracked independently.
type flakyStore struct {
	inner       Store
	mu          sync.Mutex
	skipUpdates int // first N Update calls pass through
	failUpdates int // next N Update calls fail
	skipGets    int
	failGets    int
}

func newFlakyStore(inner Store) *flakyStore {
	return &flakyStore{inner: inner}
}

// failNextUpdates arms the store so that the next n Update calls fail
// after passing the previous skip Updates. Existing armed counters are
// replaced.
func (s *flakyStore) failNextUpdates(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failUpdates = n
}

// passNextUpdates instructs the store to pass the next n Update calls
// through to the inner store before any failures take effect.
func (s *flakyStore) passNextUpdates(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.skipUpdates = n
}

// failNextGets arms the store so that the next n Get calls fail. It is
// retained as part of the helper API for symmetry with failNextUpdates;
// reach for it whenever a future test needs to inject Get failures.
//
//nolint:unused // part of the documented helper API
func (s *flakyStore) failNextGets(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failGets = n
}

func (s *flakyStore) Get(ctx context.Context, key string) (Snapshot, error) {
	s.mu.Lock()
	if s.skipGets > 0 {
		s.skipGets--
		s.mu.Unlock()
		return s.inner.Get(ctx, key)
	}
	if s.failGets > 0 {
		s.failGets--
		s.mu.Unlock()
		return Snapshot{}, errors.New("flakyStore: get failed")
	}
	s.mu.Unlock()
	return s.inner.Get(ctx, key)
}

func (s *flakyStore) Update(ctx context.Context, key string, fn UpdateFunc) (Snapshot, error) {
	s.mu.Lock()
	if s.skipUpdates > 0 {
		s.skipUpdates--
		s.mu.Unlock()
		return s.inner.Update(ctx, key, fn)
	}
	if s.failUpdates > 0 {
		s.failUpdates--
		s.mu.Unlock()
		return Snapshot{}, errors.New("flakyStore: update failed")
	}
	s.mu.Unlock()
	return s.inner.Update(ctx, key, fn)
}

func (s *flakyStore) Close() error { return s.inner.Close() }

// gatedStore wraps another Store and pauses every Update call on a
// channel until released. It is the helper used to construct
// deterministic races inside Group.Get's double-checked locking
// section: blocking the constructor's first Store.Update lets a second
// goroutine queue up on the write lock, then releasing the gate lets
// the first goroutine commit, releasing the lock, at which point the
// second goroutine acquires the lock and observes the cached entry
// (the L139-141 branch in group.go).
type gatedStore struct {
	inner Store
	mu    sync.Mutex
	gates []chan struct{}
}

func newGatedStore(inner Store) *gatedStore {
	return &gatedStore{inner: inner}
}

// nextGate returns a channel that the next Update call will block on.
// Each call to nextGate adds one gate; gates are consumed in FIFO order
// by Update calls.
func (s *gatedStore) nextGate() chan struct{} {
	gate := make(chan struct{})
	s.mu.Lock()
	s.gates = append(s.gates, gate)
	s.mu.Unlock()
	return gate
}

func (s *gatedStore) Get(ctx context.Context, key string) (Snapshot, error) {
	return s.inner.Get(ctx, key)
}

func (s *gatedStore) Update(ctx context.Context, key string, fn UpdateFunc) (Snapshot, error) {
	s.mu.Lock()
	var gate chan struct{}
	if len(s.gates) > 0 {
		gate = s.gates[0]
		s.gates = s.gates[1:]
	}
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return s.inner.Update(ctx, key, fn)
}

func (s *gatedStore) Close() error { return s.inner.Close() }
