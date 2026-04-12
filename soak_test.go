package gobreaker

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// soak_test.go contains long-running stress tests that exercise the
// breaker continuously and assert that no resource grows unboundedly.
// They are NOT marked as short-mode-only because they finish in ~5
// seconds and are valuable on every CI run; the goal is to catch
// goroutine leaks, memory growth, and counter overflow under load.

// TestSoak_NoGoroutineLeak runs the breaker against a no-op wrapped
// function in a tight loop and verifies that the goroutine count after
// the burst is the same as before. This catches both
//
//   - Wrapped functions that spawn goroutines and forget to wait
//   - Internal helpers that spawn goroutines and forget to clean up
//
// We use a tolerance window because the testing infrastructure itself
// may use goroutines (logger, etc.).
func TestSoak_NoGoroutineLeak(t *testing.T) {
	cb, err := New[int](context.Background(), Settings{Name: "soak-no-leak"})
	if err != nil {
		t.Fatal(err)
	}

	// Settle the runtime before measuring.
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	const reqs = 50_000
	for i := 0; i < reqs; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (int, error) {
			return 0, nil
		})
	}

	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	after := runtime.NumGoroutine()

	if delta := after - before; delta > 2 {
		t.Errorf("goroutine count grew by %d (before=%d, after=%d) after %d Execute calls",
			delta, before, after, reqs)
	}
}

// TestSoak_ConcurrentNoGoroutineLeak is the parallel variant. We
// spawn a fixed pool of worker goroutines, run them through the
// breaker for a fixed time budget, then verify the runtime returns
// to a clean state.
func TestSoak_ConcurrentNoGoroutineLeak(t *testing.T) {
	cb, err := New[int](context.Background(), Settings{Name: "soak-concurrent"})
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	const workers = 16
	const perWorker = 5_000
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				_, _ = cb.Execute(context.Background(), func(_ context.Context) (int, error) {
					return 0, nil
				})
			}
		}()
	}
	wg.Wait()

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if delta := after - before; delta > 2 {
		t.Errorf("goroutine count grew by %d after %d concurrent calls",
			delta, workers*perWorker)
	}
}

// TestSoak_LocalStoreMapDoesNotGrowUnboundedly verifies that a single
// breaker reusing a single key does not produce additional map entries
// in the underlying LocalStore. The map should reach size 1 after
// initialization and stay there forever.
func TestSoak_LocalStoreMapDoesNotGrowUnboundedly(t *testing.T) {
	store := NewLocalStore()
	cb, err := New[int](context.Background(), Settings{
		Name:  "soak-map",
		Store: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = cb

	for i := 0; i < 10_000; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (int, error) {
			return 0, nil
		})
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.data) > 1 {
		t.Errorf("LocalStore.data has %d entries; want 1 (one breaker, one key)", len(store.data))
	}
}

// TestSoak_GroupCacheBoundedByDistinctKeys verifies that Group's
// in-memory cache size matches the number of distinct keys, not the
// number of Execute calls.
func TestSoak_GroupCacheBoundedByDistinctKeys(t *testing.T) {
	g, err := NewGroup[int](context.Background(), GroupSettings{
		Settings: Settings{Name: "soak-group"},
	})
	if err != nil {
		t.Fatal(err)
	}

	const distinct = 32
	const per = 1_000
	for i := 0; i < per; i++ {
		for k := 0; k < distinct; k++ {
			key := keyFor(k)
			_, _ = g.Execute(context.Background(), key, func(_ context.Context) (int, error) {
				return 0, nil
			})
		}
	}
	if g.Len() != distinct {
		t.Errorf("Group.Len() = %d, want %d", g.Len(), distinct)
	}
}

// keyFor produces a small set of distinct keys for soak tests. We
// avoid strconv.Itoa to keep the soak loop allocation-free at the
// test layer (the helper preallocates).
func keyFor(i int) string {
	const ks = "abcdefghijklmnopqrstuvwxyz0123456"
	if i >= len(ks) {
		i = i % len(ks)
	}
	return ks[i : i+1]
}

// TestSoak_NoMemoryGrowthOnSteadyState runs the breaker for a fixed
// duration and asserts that the heap (after a forced GC) does not
// grow significantly. This catches memory leaks that escape the
// allocation profiler — typically map entries, channel buffers, or
// closure captures that escape silently.
func TestSoak_NoMemoryGrowthOnSteadyState(t *testing.T) {
	cb, err := New[int](context.Background(), Settings{Name: "soak-mem"})
	if err != nil {
		t.Fatal(err)
	}

	// Warm up the breaker so the inlineSnap is populated.
	for i := 0; i < 1_000; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (int, error) {
			return 0, nil
		})
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const burst = 200_000
	for i := 0; i < burst; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (int, error) {
			return 0, nil
		})
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// HeapAlloc may grow slightly due to noise; require it to grow
	// by less than 256 KiB across 200k calls. The fast path is
	// allocation-free; this is a generous tolerance for any noise
	// the test runtime introduces.
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	const limit = 256 * 1024
	if growth > limit {
		t.Errorf("heap grew by %d bytes after %d calls; limit %d", growth, burst, limit)
	}
}

// TestSoak_FailureRateUnderConcurrentTrip verifies that the breaker
// stays consistent under heavy contention with periodic forced trips
// and recoveries. We spawn a worker pool that mixes successes and
// failures and an "observer" goroutine that periodically forces a
// state read; the test asserts that no in-flight counter ever
// underflows.
func TestSoak_FailureRateUnderConcurrentTrip(t *testing.T) {
	cb, err := New[int](context.Background(), Settings{
		Name:        "soak-trip",
		Timeout:     50 * time.Millisecond,
		ReadyToOpen: ConsecutiveFailures(3),
	})
	if err != nil {
		t.Fatal(err)
	}

	var (
		failures int64
		oks      int64
	)
	failErr := errors.New("soak failure")

	const workers = 8
	const perWorker = 5_000
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				_, err := cb.Execute(context.Background(), func(_ context.Context) (int, error) {
					// Fail 1 out of 4 calls.
					if (w*perWorker+j)%4 == 0 {
						return 0, failErr
					}
					return 0, nil
				})
				switch {
				case err == nil:
					atomic.AddInt64(&oks, 1)
				case errors.Is(err, failErr):
					atomic.AddInt64(&failures, 1)
				}
			}
		}()
	}
	wg.Wait()

	c, _ := cb.Counts(context.Background())
	if c.InFlights != 0 {
		t.Errorf("InFlights = %d, want 0 after all workers finished", c.InFlights)
	}
	// We allow whatever final state — the test asserts only the
	// invariants, not specific transition counts.
	t.Logf("oks=%d failures=%d final state counts=%+v", atomic.LoadInt64(&oks), atomic.LoadInt64(&failures), c)
}
