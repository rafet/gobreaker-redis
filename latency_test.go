package gobreaker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestLatencyTracker_RecordsOutcomes(t *testing.T) {
	lt := NewLatencyTracker(100)
	lt.OnOutcome("x", OutcomeSuccess, 10*time.Millisecond)
	lt.OnOutcome("x", OutcomeFailure, 20*time.Millisecond)
	lt.OnOutcome("x", OutcomeExclusion, 30*time.Millisecond)
	if lt.Len() != 3 {
		t.Errorf("Len = %d, want 3", lt.Len())
	}
}

func TestLatencyTracker_IgnoresRejections(t *testing.T) {
	lt := NewLatencyTracker(100)
	lt.OnOutcome("x", OutcomeRejected, 0)
	lt.OnOutcome("x", OutcomeRejected, 0)
	if lt.Len() != 0 {
		t.Errorf("Len = %d, want 0 (rejections excluded)", lt.Len())
	}
}

func TestLatencyTracker_ResetsOnStateChange(t *testing.T) {
	lt := NewLatencyTracker(100)
	lt.OnOutcome("x", OutcomeSuccess, 10*time.Millisecond)
	lt.OnOutcome("x", OutcomeSuccess, 20*time.Millisecond)
	lt.OnStateChange("x", StateClosed, StateOpen, Counts{})
	if lt.Len() != 0 {
		t.Errorf("Len after state change = %d, want 0", lt.Len())
	}
}

func TestLatencyTracker_Percentile(t *testing.T) {
	lt := NewLatencyTracker(100)
	for i := 1; i <= 100; i++ {
		lt.OnOutcome("x", OutcomeSuccess, time.Duration(i)*time.Millisecond)
	}
	p99 := lt.Percentile(0.99)
	if p99 < 98*time.Millisecond || p99 > 100*time.Millisecond {
		t.Errorf("P99 = %v, want ~99ms", p99)
	}
}

func TestLatencyTracker_P99Above_Fires(t *testing.T) {
	lt := NewLatencyTracker(100)
	pred := lt.P99Above(50 * time.Millisecond)

	// Below min samples: should not fire.
	if pred(Counts{}) {
		t.Error("P99Above fired with empty buffer")
	}

	// Add 100 slow requests.
	for i := 0; i < 100; i++ {
		lt.OnOutcome("x", OutcomeSuccess, 100*time.Millisecond)
	}
	if !pred(Counts{}) {
		t.Error("P99Above did not fire with P99=100ms > threshold=50ms")
	}
}

func TestLatencyTracker_P99Above_DoesNotFireBelowThreshold(t *testing.T) {
	lt := NewLatencyTracker(100)
	pred := lt.P99Above(200 * time.Millisecond)

	for i := 0; i < 100; i++ {
		lt.OnOutcome("x", OutcomeSuccess, 100*time.Millisecond)
	}
	if pred(Counts{}) {
		t.Error("P99Above fired with P99=100ms < threshold=200ms")
	}
}

func TestLatencyTracker_P99Above_MinSamples(t *testing.T) {
	lt := NewLatencyTracker(100)
	pred := lt.P99Above(10*time.Millisecond, 50) // require 50 samples

	for i := 0; i < 49; i++ {
		lt.OnOutcome("x", OutcomeSuccess, 100*time.Millisecond)
	}
	if pred(Counts{}) {
		t.Error("should not fire below minSamples")
	}
	lt.OnOutcome("x", OutcomeSuccess, 100*time.Millisecond)
	if !pred(Counts{}) {
		t.Error("should fire at exactly minSamples")
	}
}

func TestLatencyTracker_P50Above(t *testing.T) {
	lt := NewLatencyTracker(100)
	pred := lt.P50Above(50 * time.Millisecond)

	for i := 0; i < 100; i++ {
		lt.OnOutcome("x", OutcomeSuccess, 100*time.Millisecond)
	}
	if !pred(Counts{}) {
		t.Error("P50Above did not fire")
	}
}

func TestLatencyTracker_P95Above(t *testing.T) {
	lt := NewLatencyTracker(100)
	pred := lt.P95Above(90 * time.Millisecond)

	for i := 1; i <= 100; i++ {
		lt.OnOutcome("x", OutcomeSuccess, time.Duration(i)*time.Millisecond)
	}
	if !pred(Counts{}) {
		t.Error("P95Above did not fire (P95 ~= 95ms > 90ms)")
	}
}

func TestLatencyTracker_PercentileAbove(t *testing.T) {
	lt := NewLatencyTracker(100)
	pred := lt.PercentileAbove(0.75, 70*time.Millisecond)

	for i := 1; i <= 100; i++ {
		lt.OnOutcome("x", OutcomeSuccess, time.Duration(i)*time.Millisecond)
	}
	if !pred(Counts{}) {
		t.Error("PercentileAbove(0.75, 70ms) did not fire (P75 ~= 75ms > 70ms)")
	}
}

// TestLatencyTracker_EndToEnd_TripsBreaker wires a LatencyTracker into
// a real CircuitBreaker and verifies that slow requests trip it.
func TestLatencyTracker_EndToEnd_TripsBreaker(t *testing.T) {
	tracker := NewLatencyTracker(100)

	clock := newFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	store := NewLocalStore()
	store.setClock(clock.Now)

	cb, err := New[any](context.Background(), Settings{
		Name:     "latency-e2e",
		Store:    store,
		Observer: tracker,
		ReadyToOpen: Or(
			ConsecutiveFailures(100),            // normal threshold (high)
			tracker.P99Above(50*time.Millisecond, 10), // latency threshold (low)
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.setClock(clock.Now)

	// Simulate 10 slow successful requests (not failures!). The clock
	// must advance INSIDE the wrapped function so that the
	// latency = cb.now().Sub(start) measurement sees a nonzero delta.
	for i := 0; i < 10; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			clock.Advance(100 * time.Millisecond)
			return nil, nil
		})
	}

	// The breaker should trip based on latency, not failures.
	state, _ := cb.State(context.Background())
	if state != StateOpen {
		t.Errorf("state = %v, want open (latency-tripped)", state)
	}
}

// TestLatencyTracker_EndToEnd_SlowDoesNotTripBelowMinSamples verifies
// the minSamples guard prevents false trips during warmup.
func TestLatencyTracker_EndToEnd_NoFalseTrip(t *testing.T) {
	tracker := NewLatencyTracker(100)

	cb, err := New[any](context.Background(), Settings{
		Name:     "latency-safe",
		Observer: tracker,
		ReadyToOpen: tracker.P99Above(50*time.Millisecond, 20), // need 20 samples
	})
	if err != nil {
		t.Fatal(err)
	}

	// Only 5 slow requests — below minSamples.
	for i := 0; i < 5; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, nil
		})
	}

	state, _ := cb.State(context.Background())
	if state != StateClosed {
		t.Errorf("state = %v, want closed (below minSamples)", state)
	}
}

// TestLatencyTracker_ConcurrentSafe verifies the tracker under
// concurrent Observer calls.
func TestLatencyTracker_ConcurrentSafe(t *testing.T) {
	lt := NewLatencyTracker(1000)
	pred := lt.P99Above(time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				lt.OnOutcome("x", OutcomeSuccess, 10*time.Millisecond)
				_ = pred(Counts{})
				_ = lt.Percentile(0.5)
			}
		}()
	}
	wg.Wait()
	if lt.Len() > 1000 {
		t.Errorf("Len = %d, want <= 1000 (capacity)", lt.Len())
	}
}

// TestLatencyTracker_ImplementsObserver is a compile-time assertion.
func TestLatencyTracker_ImplementsObserver(t *testing.T) {
	var _ Observer = (*LatencyTracker)(nil)
}

func TestLatencyTracker_OrComposition(t *testing.T) {
	lt := NewLatencyTracker(100)
	pred := Or(
		ConsecutiveFailures(5),
		lt.P99Above(50 * time.Millisecond),
	)

	// Latency alone should fire.
	for i := 0; i < 100; i++ {
		lt.OnOutcome("x", OutcomeSuccess, 100*time.Millisecond)
	}
	if !pred(Counts{ConsecutiveFailures: 0}) {
		t.Error("Or(failures, latency) did not fire on latency alone")
	}

	// Reset and test failures alone.
	lt.OnStateChange("x", StateClosed, StateOpen, Counts{})
	if !pred(Counts{ConsecutiveFailures: 5}) {
		t.Error("Or(failures, latency) did not fire on failures alone")
	}
}

func BenchmarkLatencyTracker_OnOutcome(b *testing.B) {
	lt := NewLatencyTracker(1000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lt.OnOutcome("x", OutcomeSuccess, time.Millisecond)
	}
}

func BenchmarkLatencyTracker_P99Above(b *testing.B) {
	lt := NewLatencyTracker(1000)
	for i := 0; i < 1000; i++ {
		lt.OnOutcome("x", OutcomeSuccess, time.Duration(i)*time.Microsecond)
	}
	pred := lt.P99Above(time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pred(Counts{})
	}
}
