// Package benchmarks compares the steady-state performance of seven
// circuit-breaker libraries on identical micro-workloads. The
// libraries are pulled in by this isolated module so the main module
// is not polluted with their transitive dependencies.
//
// Run with:
//
//	go test -run=^$ -bench=. -benchmem -benchtime=2s ./...
//
// The wrapped function is a no-op closure that returns either nil or
// a sentinel error. The intent is to measure the breaker overhead
// itself, not the cost of the wrapped work.
package benchmarks

import (
	"context"
	"errors"
	"testing"
	"time"

	cep21 "github.com/cep21/circuit/v4"
	failsafe "github.com/failsafe-go/failsafe-go"
	failsafecb "github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/exaring/hoglet"
	mercari "github.com/mercari/go-circuitbreaker"
	rafet "github.com/rafet/gobreaker-redis/v2"
	rubyist "github.com/rubyist/circuitbreaker"
	sonyv1 "github.com/sony/gobreaker"
	sonyv2 "github.com/sony/gobreaker/v2"
)

var (
	benchErr = errors.New("benchmark error")
	bgCtx    = context.Background()
)

// ============================================================================
// rafet/gobreaker-redis (this package)
// ============================================================================

func BenchmarkRafet_Closed_Success(b *testing.B) {
	cb, _ := rafet.New[int](bgCtx, rafet.Settings{Name: "rafet"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(bgCtx, func(_ context.Context) (int, error) { return 0, nil })
	}
}

func BenchmarkRafet_Closed_Failure(b *testing.B) {
	cb, _ := rafet.New[int](bgCtx, rafet.Settings{
		Name:        "rafet-fail",
		ReadyToOpen: rafet.ConsecutiveFailures(uint64(b.N) + 1),
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(bgCtx, func(_ context.Context) (int, error) { return 0, benchErr })
	}
}

func BenchmarkRafet_Open_Reject(b *testing.B) {
	cb, _ := rafet.New[int](bgCtx, rafet.Settings{
		Name:        "rafet-open",
		ReadyToOpen: rafet.ConsecutiveFailures(1),
	})
	_, _ = cb.Execute(bgCtx, func(_ context.Context) (int, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(bgCtx, func(_ context.Context) (int, error) { return 0, nil })
	}
}

func BenchmarkRafet_Closed_Success_Parallel(b *testing.B) {
	cb, _ := rafet.New[int](bgCtx, rafet.Settings{Name: "rafet-par"})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Execute(bgCtx, func(_ context.Context) (int, error) { return 0, nil })
		}
	})
}

func BenchmarkRafet_Open_Reject_Parallel(b *testing.B) {
	cb, _ := rafet.New[int](bgCtx, rafet.Settings{
		Name:        "rafet-open-par",
		ReadyToOpen: rafet.ConsecutiveFailures(1),
	})
	_, _ = cb.Execute(bgCtx, func(_ context.Context) (int, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Execute(bgCtx, func(_ context.Context) (int, error) { return 0, nil })
		}
	})
}

// ============================================================================
// sony/gobreaker v1 — the classic
// ============================================================================

func BenchmarkSonyV1_Closed_Success(b *testing.B) {
	cb := sonyv1.NewCircuitBreaker(sonyv1.Settings{Name: "sony1"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(func() (interface{}, error) { return 0, nil })
	}
}

func BenchmarkSonyV1_Closed_Failure(b *testing.B) {
	cb := sonyv1.NewCircuitBreaker(sonyv1.Settings{
		Name: "sony1-fail",
		ReadyToTrip: func(c sonyv1.Counts) bool {
			return c.ConsecutiveFailures > uint32(b.N)+1
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(func() (interface{}, error) { return 0, benchErr })
	}
}

func BenchmarkSonyV1_Open_Reject(b *testing.B) {
	cb := sonyv1.NewCircuitBreaker(sonyv1.Settings{
		Name:        "sony1-open",
		ReadyToTrip: func(c sonyv1.Counts) bool { return c.ConsecutiveFailures >= 1 },
	})
	_, _ = cb.Execute(func() (interface{}, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(func() (interface{}, error) { return 0, nil })
	}
}

func BenchmarkSonyV1_Closed_Success_Parallel(b *testing.B) {
	cb := sonyv1.NewCircuitBreaker(sonyv1.Settings{Name: "sony1-par"})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Execute(func() (interface{}, error) { return 0, nil })
		}
	})
}

func BenchmarkSonyV1_Open_Reject_Parallel(b *testing.B) {
	cb := sonyv1.NewCircuitBreaker(sonyv1.Settings{
		Name:        "sony1-open-par",
		ReadyToTrip: func(c sonyv1.Counts) bool { return c.ConsecutiveFailures >= 1 },
	})
	_, _ = cb.Execute(func() (interface{}, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Execute(func() (interface{}, error) { return 0, nil })
		}
	})
}

// ============================================================================
// sony/gobreaker v2 — generics rewrite
// ============================================================================

func BenchmarkSonyV2_Closed_Success(b *testing.B) {
	cb := sonyv2.NewCircuitBreaker[int](sonyv2.Settings{Name: "sony2"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(func() (int, error) { return 0, nil })
	}
}

func BenchmarkSonyV2_Closed_Failure(b *testing.B) {
	cb := sonyv2.NewCircuitBreaker[int](sonyv2.Settings{
		Name: "sony2-fail",
		ReadyToTrip: func(c sonyv2.Counts) bool {
			return c.ConsecutiveFailures > uint32(b.N)+1
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(func() (int, error) { return 0, benchErr })
	}
}

func BenchmarkSonyV2_Open_Reject(b *testing.B) {
	cb := sonyv2.NewCircuitBreaker[int](sonyv2.Settings{
		Name:        "sony2-open",
		ReadyToTrip: func(c sonyv2.Counts) bool { return c.ConsecutiveFailures >= 1 },
	})
	_, _ = cb.Execute(func() (int, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Execute(func() (int, error) { return 0, nil })
	}
}

func BenchmarkSonyV2_Closed_Success_Parallel(b *testing.B) {
	cb := sonyv2.NewCircuitBreaker[int](sonyv2.Settings{Name: "sony2-par"})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Execute(func() (int, error) { return 0, nil })
		}
	})
}

func BenchmarkSonyV2_Open_Reject_Parallel(b *testing.B) {
	cb := sonyv2.NewCircuitBreaker[int](sonyv2.Settings{
		Name:        "sony2-open-par",
		ReadyToTrip: func(c sonyv2.Counts) bool { return c.ConsecutiveFailures >= 1 },
	})
	_, _ = cb.Execute(func() (int, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Execute(func() (int, error) { return 0, nil })
		}
	})
}

// ============================================================================
// mercari/go-circuitbreaker
// ============================================================================

func BenchmarkMercari_Closed_Success(b *testing.B) {
	cb := mercari.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Do(bgCtx, func() (interface{}, error) { return 0, nil })
	}
}

func BenchmarkMercari_Closed_Failure(b *testing.B) {
	cb := mercari.New(
		mercari.WithTripFunc(mercari.NewTripFuncConsecutiveFailures(int64(b.N) + 1)),
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Do(bgCtx, func() (interface{}, error) { return 0, benchErr })
	}
}

func BenchmarkMercari_Open_Reject(b *testing.B) {
	cb := mercari.New(mercari.WithTripFunc(mercari.NewTripFuncConsecutiveFailures(1)))
	_, _ = cb.Do(bgCtx, func() (interface{}, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cb.Do(bgCtx, func() (interface{}, error) { return 0, nil })
	}
}

func BenchmarkMercari_Closed_Success_Parallel(b *testing.B) {
	cb := mercari.New()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Do(bgCtx, func() (interface{}, error) { return 0, nil })
		}
	})
}

func BenchmarkMercari_Open_Reject_Parallel(b *testing.B) {
	cb := mercari.New(mercari.WithTripFunc(mercari.NewTripFuncConsecutiveFailures(1)))
	_, _ = cb.Do(bgCtx, func() (interface{}, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cb.Do(bgCtx, func() (interface{}, error) { return 0, nil })
		}
	})
}

// ============================================================================
// cep21/circuit v4
// ============================================================================

func newCep21Circuit() *cep21.Circuit {
	mgr := &cep21.Manager{}
	return mgr.MustCreateCircuit("cep21")
}

func BenchmarkCep21_Closed_Success(b *testing.B) {
	c := newCep21Circuit()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Execute(bgCtx, func(_ context.Context) error { return nil }, nil)
	}
}

func BenchmarkCep21_Closed_Failure(b *testing.B) {
	c := newCep21Circuit()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Execute(bgCtx, func(_ context.Context) error { return benchErr }, nil)
	}
}

func BenchmarkCep21_Open_Reject(b *testing.B) {
	c := newCep21Circuit()
	// Trip by hand: cep21/circuit's API for forcing open is OpenCircuit().
	c.OpenCircuit(bgCtx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Execute(bgCtx, func(_ context.Context) error { return nil }, nil)
	}
}

func BenchmarkCep21_Closed_Success_Parallel(b *testing.B) {
	c := newCep21Circuit()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.Execute(bgCtx, func(_ context.Context) error { return nil }, nil)
		}
	})
}

func BenchmarkCep21_Open_Reject_Parallel(b *testing.B) {
	c := newCep21Circuit()
	c.OpenCircuit(bgCtx)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.Execute(bgCtx, func(_ context.Context) error { return nil }, nil)
		}
	})
}

// ============================================================================
// failsafe-go
// ============================================================================

func newFailsafeExecutor() failsafe.Executor[any] {
	cb := failsafecb.NewBuilder[any]().
		WithFailureThreshold(1 << 30). // never trip during success benchmarks
		Build()
	return failsafe.With[any](cb)
}

func newFailsafeExecutorTrippy() failsafe.Executor[any] {
	cb := failsafecb.NewBuilder[any]().
		WithFailureThreshold(1).
		WithDelay(time.Hour). // stay open
		Build()
	return failsafe.With[any](cb)
}

func BenchmarkFailsafe_Closed_Success(b *testing.B) {
	exec := newFailsafeExecutor()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = exec.GetWithExecution(func(_ failsafe.Execution[any]) (any, error) { return nil, nil })
	}
}

func BenchmarkFailsafe_Closed_Failure(b *testing.B) {
	exec := newFailsafeExecutor()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = exec.GetWithExecution(func(_ failsafe.Execution[any]) (any, error) { return nil, benchErr })
	}
}

func BenchmarkFailsafe_Open_Reject(b *testing.B) {
	exec := newFailsafeExecutorTrippy()
	_, _ = exec.GetWithExecution(func(_ failsafe.Execution[any]) (any, error) { return nil, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = exec.GetWithExecution(func(_ failsafe.Execution[any]) (any, error) { return nil, nil })
	}
}

func BenchmarkFailsafe_Closed_Success_Parallel(b *testing.B) {
	exec := newFailsafeExecutor()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = exec.GetWithExecution(func(_ failsafe.Execution[any]) (any, error) { return nil, nil })
		}
	})
}

func BenchmarkFailsafe_Open_Reject_Parallel(b *testing.B) {
	exec := newFailsafeExecutorTrippy()
	_, _ = exec.GetWithExecution(func(_ failsafe.Execution[any]) (any, error) { return nil, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = exec.GetWithExecution(func(_ failsafe.Execution[any]) (any, error) { return nil, nil })
		}
	})
}

// ============================================================================
// exaring/hoglet — sliding window only, lower-level interface
// ============================================================================

func newHoglet() *hoglet.Circuit {
	c, _ := hoglet.NewCircuit(
		hoglet.NewSlidingWindowBreaker(1*time.Second, 0.5),
	)
	return c
}

func BenchmarkHoglet_Closed_Success(b *testing.B) {
	c := newHoglet()
	wrapped := hoglet.Wrap(c, func(_ context.Context, _ struct{}) (int, error) { return 0, nil })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wrapped(bgCtx, struct{}{})
	}
}

func BenchmarkHoglet_Closed_Failure(b *testing.B) {
	// Use a high threshold so we don't trip.
	c, _ := hoglet.NewCircuit(hoglet.NewSlidingWindowBreaker(1*time.Hour, 0.999))
	wrapped := hoglet.Wrap(c, func(_ context.Context, _ struct{}) (int, error) { return 0, benchErr })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wrapped(bgCtx, struct{}{})
	}
}

func BenchmarkHoglet_Open_Reject(b *testing.B) {
	c, _ := hoglet.NewCircuit(hoglet.NewSlidingWindowBreaker(1*time.Second, 0.0001))
	wrapped := hoglet.Wrap(c, func(_ context.Context, _ struct{}) (int, error) { return 0, benchErr })
	// Trip the breaker.
	_, _ = wrapped(bgCtx, struct{}{})
	_, _ = wrapped(bgCtx, struct{}{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wrapped(bgCtx, struct{}{})
	}
}

func BenchmarkHoglet_Closed_Success_Parallel(b *testing.B) {
	c := newHoglet()
	wrapped := hoglet.Wrap(c, func(_ context.Context, _ struct{}) (int, error) { return 0, nil })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = wrapped(bgCtx, struct{}{})
		}
	})
}

func BenchmarkHoglet_Open_Reject_Parallel(b *testing.B) {
	c, _ := hoglet.NewCircuit(hoglet.NewSlidingWindowBreaker(1*time.Second, 0.0001))
	wrapped := hoglet.Wrap(c, func(_ context.Context, _ struct{}) (int, error) { return 0, benchErr })
	_, _ = wrapped(bgCtx, struct{}{})
	_, _ = wrapped(bgCtx, struct{}{})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = wrapped(bgCtx, struct{}{})
		}
	})
}

// ============================================================================
// rubyist/circuitbreaker
// ============================================================================

func BenchmarkRubyist_Closed_Success(b *testing.B) {
	cb := rubyist.NewBreaker()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.Call(func() error { return nil }, 0)
	}
}

func BenchmarkRubyist_Closed_Failure(b *testing.B) {
	cb := rubyist.NewBreaker()
	cb.ShouldTrip = func(_ *rubyist.Breaker) bool { return false } // never trip
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.Call(func() error { return benchErr }, 0)
	}
}

func BenchmarkRubyist_Open_Reject(b *testing.B) {
	cb := rubyist.NewBreaker()
	cb.Trip()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.Call(func() error { return nil }, 0)
	}
}

func BenchmarkRubyist_Closed_Success_Parallel(b *testing.B) {
	cb := rubyist.NewBreaker()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cb.Call(func() error { return nil }, 0)
		}
	})
}

func BenchmarkRubyist_Open_Reject_Parallel(b *testing.B) {
	cb := rubyist.NewBreaker()
	cb.Trip()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cb.Call(func() error { return nil }, 0)
		}
	})
}
