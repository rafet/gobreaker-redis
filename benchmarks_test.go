package gobreaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// benchmarks_test.go contains the internal microbenchmarks for the
// CircuitBreaker hot paths. They are run with:
//
//	go test -bench=. -benchmem -run=^$ ./...
//
// The numbers establish the baseline against which the cross-library
// comparison in benchmarks/ measures us. Improvements should land here
// first; the comparison directory is a derivative.

var errBench = errors.New("benchmark error")

// helper: a no-op function used as the wrapped request body. Most
// benchmarks measure overhead beyond this call.
func benchNoopOK(_ context.Context) (int, error)   { return 0, nil }
func benchNoopFail(_ context.Context) (int, error) { return 0, errBench }

// BenchmarkExecute_Closed_Success measures the cost of a single
// successful Execute call against a closed breaker backed by
// LocalStore. This is the most common production scenario.
func BenchmarkExecute_Closed_Success(b *testing.B) {
	cb, err := New[int](context.Background(), Settings{Name: "bench"})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(ctx, benchNoopOK)
	}
}

// BenchmarkExecute_Closed_Failure measures the cost when the wrapped
// request returns a counted failure but the breaker stays closed.
func BenchmarkExecute_Closed_Failure(b *testing.B) {
	cb, err := New[int](context.Background(), Settings{
		Name:        "bench-fail",
		ReadyToOpen: ConsecutiveFailures(uint64(b.N) + 1), // never trips
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(ctx, benchNoopFail)
	}
}

// BenchmarkExecute_Open_Reject measures the rejection fast-path. This
// is what callers see when the breaker is doing its job and shielding
// the upstream.
func BenchmarkExecute_Open_Reject(b *testing.B) {
	cb, err := New[int](context.Background(), Settings{
		Name:        "bench-open",
		ReadyToOpen: ConsecutiveFailures(1),
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	// Trip the breaker.
	_, _ = cb.Execute(ctx, benchNoopFail)
	state, _ := cb.State(ctx)
	if state != StateOpen {
		b.Fatalf("setup: state = %v, want open", state)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(ctx, benchNoopOK)
	}
}

// BenchmarkExecute_Closed_Success_Parallel measures throughput under
// concurrent load on a closed breaker. The "parallel" loop divides
// b.N across GOMAXPROCS goroutines.
func BenchmarkExecute_Closed_Success_Parallel(b *testing.B) {
	cb, err := New[int](context.Background(), Settings{Name: "bench-par"})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Execute(ctx, benchNoopOK)
		}
	})
}

// BenchmarkExecute_Open_Reject_Parallel measures the open-state
// rejection fast-path under concurrent load. This is the
// most-relevant number for "how much overhead does the breaker add
// when it is shielding traffic".
func BenchmarkExecute_Open_Reject_Parallel(b *testing.B) {
	cb, err := New[int](context.Background(), Settings{
		Name:        "bench-open-par",
		ReadyToOpen: ConsecutiveFailures(1),
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	_, _ = cb.Execute(ctx, benchNoopFail)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Execute(ctx, benchNoopOK)
		}
	})
}

// BenchmarkAdmit isolates the admission half of Execute (the
// before-call work).
func BenchmarkAdmit(b *testing.B) {
	cb, err := New[int](context.Background(), Settings{Name: "bench-admit"})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.admit(ctx, cb.settings)
		// Reset in-flight: we are not exercising report() here so
		// the in-flight counter would otherwise grow unbounded.
		cb.store.Update(ctx, "bench-admit", func(c Snapshot, _ time.Time) (Snapshot, error) {
			c.Counts.InFlights = 0
			return c, nil
		})
	}
}

// BenchmarkLocalStore_Update isolates the in-memory store path with
// a no-op closure. This is the cheapest possible Update.
func BenchmarkLocalStore_Update(b *testing.B) {
	s := NewLocalStore()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Update(ctx, "k", func(c Snapshot, _ time.Time) (Snapshot, error) {
			return c, nil
		})
	}
}

// BenchmarkGroup_Execute_Cached measures the per-key Group dispatch
// when the breaker is already cached. This is the cost of the
// double-checked locking lookup plus a normal Execute.
func BenchmarkGroup_Execute_Cached(b *testing.B) {
	g, err := NewGroup[int](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	// Warm the cache.
	_, _ = g.Get(ctx, "k")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = g.Execute(ctx, "k", benchNoopOK)
	}
}

// BenchmarkGroup_Execute_Cached_Parallel is the parallel variant.
func BenchmarkGroup_Execute_Cached_Parallel(b *testing.B) {
	g, err := NewGroup[int](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	_, _ = g.Get(ctx, "k")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = g.Execute(ctx, "k", benchNoopOK)
		}
	})
}
