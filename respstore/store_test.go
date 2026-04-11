package respstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// newTestStore returns a Store backed by an in-process miniredis. The
// returned cleanup function closes both the store and the miniredis server.
func newTestStore(t *testing.T, opts ...Option) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := New(client, opts...)
	return store, mr
}

func TestStoreGetMissingKey(t *testing.T) {
	store, _ := newTestStore(t)
	snap, err := store.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Get on missing key: %v", err)
	}
	if !snap.IsZero() {
		t.Errorf("Get on missing key returned %+v, want zero", snap)
	}
}

func TestStoreUpdateInitializes(t *testing.T) {
	store, mr := newTestStore(t)
	snap, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, now time.Time) (gobreaker.Snapshot, error) {
		if !c.IsZero() {
			t.Errorf("expected zero current, got %+v", c)
		}
		return gobreaker.Snapshot{
			State:           gobreaker.StateClosed,
			Generation:      1,
			GenerationStart: now,
		}, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if snap.Version != 1 {
		t.Errorf("Version = %d, want 1", snap.Version)
	}
	// Verify the key actually exists in miniredis with the expected fields.
	key := store.Key("x")
	if !mr.Exists(key) {
		t.Errorf("key %q does not exist", key)
	}
	if got := mr.HGet(key, "v"); got != "1" {
		t.Errorf("v field = %q, want 1", got)
	}
	if got := mr.HGet(key, "s"); got != "0" {
		t.Errorf("s field = %q, want 0 (closed)", got)
	}
}

func TestStoreUpdateRoundTrip(t *testing.T) {
	store, _ := newTestStore(t)

	original := gobreaker.Snapshot{
		State:           gobreaker.StateOpen,
		Generation:      42,
		GenerationStart: time.Date(2030, 1, 2, 3, 4, 5, 6, time.UTC),
		Expiry:          time.Date(2030, 1, 2, 3, 14, 5, 6, time.UTC),
		Counts: gobreaker.Counts{
			Requests:             100,
			InFlights:            5,
			TotalSuccesses:       60,
			TotalFailures:        30,
			TotalExclusions:      5,
			ConsecutiveSuccesses: 0,
			ConsecutiveFailures:  10,
		},
	}

	if _, err := store.Update(context.Background(), "trip", func(_ gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return original, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	loaded, err := store.Get(context.Background(), "trip")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Version is set by the store, not by us — strip it for comparison.
	if loaded.Version != 1 {
		t.Errorf("Version = %d, want 1", loaded.Version)
	}
	loaded.Version = 0

	// Snapshot is compared with == so the timezone field of time.Time
	// participates. The store always normalizes to UTC, so normalize the
	// expected value too.
	original.GenerationStart = original.GenerationStart.UTC()
	original.Expiry = original.Expiry.UTC()
	if loaded != original {
		t.Errorf("round trip mismatch:\n got=%+v\nwant=%+v", loaded, original)
	}
}

func TestStoreUpdateIncrementsVersion(t *testing.T) {
	store, _ := newTestStore(t)
	noop := func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) { return c, nil }
	for i := uint64(1); i <= 5; i++ {
		snap, err := store.Update(context.Background(), "x", noop)
		if err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
		if snap.Version != i {
			t.Errorf("Update %d: Version = %d, want %d", i, snap.Version, i)
		}
	}
}

func TestStoreUpdatePropagatesUserError(t *testing.T) {
	store, _ := newTestStore(t)
	want := errors.New("nope")
	_, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return gobreaker.Snapshot{}, want
	})
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
}

// TestStoreCASRetryOnConflict simulates two contending writers and verifies
// that the store retries the user's UpdateFunc instead of dropping a write.
//
// The first writer's UpdateFunc is intercepted: on its first invocation we
// secretly mutate the key from a goroutine, simulating a concurrent process
// committing a different snapshot. The store must observe the version
// mismatch, retry, and produce a final snapshot whose Counts include both
// updates.
func TestStoreCASRetryOnConflict(t *testing.T) {
	store, mr := newTestStore(t)

	// Seed an initial snapshot at version 1.
	if _, err := store.Update(context.Background(), "race", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		c.State = gobreaker.StateClosed
		c.Counts.Requests = 100
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}

	var calls int32
	_, err := store.Update(context.Background(), "race", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			// Concurrent writer commits before us: bump
			// requests by 7 directly through the same store.
			_, err := store.Update(context.Background(), "race", func(c2 gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
				c2.Counts.Requests += 7
				return c2, nil
			})
			if err != nil {
				return gobreaker.Snapshot{}, err
			}
		}
		c.Counts.Requests++
		return c, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if atomic.LoadInt32(&calls) < 2 {
		t.Errorf("UpdateFunc was called %d times, expected at least 2 (CAS should retry)", calls)
	}

	loaded, _ := store.Get(context.Background(), "race")
	// Final state: 100 (seed) + 7 (concurrent) + 1 (our update) = 108
	if loaded.Counts.Requests != 108 {
		t.Errorf("Requests = %d, want 108", loaded.Counts.Requests)
	}
	// Version: seed (1) + concurrent (2) + retry final (3) = 3
	if loaded.Version != 3 {
		t.Errorf("Version = %d, want 3", loaded.Version)
	}
	_ = mr
}

func TestStoreCASBudgetExhausted(t *testing.T) {
	store, _ := newTestStore(t, WithMaxRetries(2))
	if _, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}

	// Force every CAS attempt to fail by mutating the key from inside the
	// UpdateFunc, simulating perpetual contention.
	_, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		_, _ = store.Update(context.Background(), "x", func(c2 gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
			c2.Counts.Requests++
			return c2, nil
		})
		return c, nil
	})
	if !errors.Is(err, gobreaker.ErrSnapshotConflict) {
		t.Errorf("err = %v, want ErrSnapshotConflict", err)
	}
}

func TestStoreTTLRefreshedOnUpdate(t *testing.T) {
	store, mr := newTestStore(t, WithTTL(5*time.Second))

	if _, err := store.Update(context.Background(), "ttl", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}

	key := store.Key("ttl")
	ttl := mr.TTL(key)
	if ttl <= 0 || ttl > 5*time.Second {
		t.Errorf("TTL = %v, want in (0, 5s]", ttl)
	}

	// Advance miniredis clock by 4s. TTL should still be present (not expired).
	mr.FastForward(4 * time.Second)
	if !mr.Exists(key) {
		t.Error("key expired prematurely")
	}

	// Update again — TTL should be refreshed.
	if _, err := store.Update(context.Background(), "ttl", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(4 * time.Second)
	if !mr.Exists(key) {
		t.Error("key expired even though TTL should have been refreshed")
	}
}

func TestStoreTTLNoneByDefault(t *testing.T) {
	store, mr := newTestStore(t)
	if _, err := store.Update(context.Background(), "no-ttl", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}
	key := store.Key("no-ttl")
	if ttl := mr.TTL(key); ttl != 0 {
		t.Errorf("TTL = %v, want 0 (no expiry)", ttl)
	}
}

func TestStoreKeyPrefix(t *testing.T) {
	store, mr := newTestStore(t, WithKeyPrefix("myapp"))
	if _, err := store.Update(context.Background(), "svc", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists("myapp:svc") {
		t.Errorf("expected key myapp:svc, found: %v", mr.Keys())
	}
}

func TestStoreCorruptSnapshotRejected(t *testing.T) {
	store, mr := newTestStore(t)
	// Manually write garbage into the snapshot fields.
	mr.HSet(store.Key("bad"), "v", "1", "s", "not-a-number")
	_, err := store.Get(context.Background(), "bad")
	if err == nil {
		t.Error("expected error on corrupt snapshot")
	}
}

func TestStoreInvalidStateValueRejected(t *testing.T) {
	store, mr := newTestStore(t)
	// State 99 is not a valid State value.
	mr.HSet(store.Key("bad"), "v", "1", "s", "99", "g", "1")
	_, err := store.Get(context.Background(), "bad")
	if err == nil {
		t.Error("expected error on invalid state")
	}
}

func TestStoreConcurrentUpdates(t *testing.T) {
	store, _ := newTestStore(t)

	// Seed.
	if _, err := store.Update(context.Background(), "concurrent", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}

	// 10 × 30 = 300 updates is enough to exercise CAS retries through
	// miniredis without saturating the default retry budget. Real-world
	// breakers see far less contention than this because each breaker
	// owns its own key.
	const goroutines = 10
	const perGoroutine = 30
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				_, err := store.Update(context.Background(), "concurrent", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
					c.Counts.Requests++
					return c, nil
				})
				if err != nil {
					t.Errorf("Update: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	loaded, _ := store.Get(context.Background(), "concurrent")
	want := uint64(goroutines * perGoroutine)
	if loaded.Counts.Requests != want {
		t.Errorf("Requests = %d, want %d (CAS atomicity broken — lost updates)", loaded.Counts.Requests, want)
	}
	// Version = seed(1) + each successful update(want) = want+1
	if loaded.Version != want+1 {
		t.Errorf("Version = %d, want %d", loaded.Version, want+1)
	}
}

func TestStoreClose(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := New(client)
	if err := store.Close(); err != nil {
		t.Errorf("Close (non-owning): %v", err)
	}
	// Close should not have shut down the client we passed in.
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Errorf("client closed unexpectedly: %v", err)
	}
	_ = client.Close()
}

func TestStoreNewWithAddressOwnsClient(t *testing.T) {
	mr := miniredis.RunT(t)
	store, err := NewWithAddress(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// After Close, the store's client should be unusable.
	if _, err := store.Get(context.Background(), "x"); err == nil {
		t.Error("expected error after Close, got nil")
	}
}

func TestStoreNewWithAddressEmpty(t *testing.T) {
	if _, err := NewWithAddress(""); err == nil {
		t.Error("expected error for empty address")
	}
}

// TestStoreEndToEndWithCircuitBreaker is the integration test that proves the
// store works as a drop-in replacement for LocalStore against the real
// CircuitBreaker state machine.
func TestStoreEndToEndWithCircuitBreaker(t *testing.T) {
	store, _ := newTestStore(t)

	cb, err := gobreaker.New[any](context.Background(), gobreaker.Settings{
		Name:    "integration",
		Store:   store,
		Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	errBoom := errors.New("boom")
	for i := 0; i < 5; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, errBoom
		})
	}

	state, err := cb.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state != gobreaker.StateOpen {
		t.Errorf("after 5 failures: state = %v, want open", state)
	}

	// New requests should be rejected.
	_, err = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		t.Error("should not run")
		return nil, nil
	})
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Errorf("err = %v, want ErrOpenState", err)
	}
}

// TestStoreCrossInstanceCoordination is the headline distributed test: two
// independently constructed CircuitBreaker instances sharing the same
// RespStore must observe each other's state changes.
func TestStoreCrossInstanceCoordination(t *testing.T) {
	store, _ := newTestStore(t)

	mkBreaker := func() *gobreaker.CircuitBreaker[any] {
		cb, err := gobreaker.New[any](context.Background(), gobreaker.Settings{
			Name:    "shared",
			Store:   store,
			Timeout: 30 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		return cb
	}

	cbA := mkBreaker()
	cbB := mkBreaker()

	// Trip through cbA.
	errBoom := errors.New("boom")
	for i := 0; i < 5; i++ {
		_, _ = cbA.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, errBoom
		})
	}

	// cbB must observe the open state without doing any work itself.
	state, err := cbB.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state != gobreaker.StateOpen {
		t.Errorf("cbB.State = %v, want open (cross-instance state should be visible)", state)
	}

	// And cbB must reject new requests.
	_, err = cbB.Execute(context.Background(), func(_ context.Context) (any, error) {
		t.Error("cbB should not invoke req")
		return nil, nil
	})
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Errorf("cbB.Execute err = %v, want ErrOpenState", err)
	}
}
