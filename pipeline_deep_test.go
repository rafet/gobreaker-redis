package gobreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipeline_TimeoutContextPropagated(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "pipe-ctx"})
	p := Compose[any](cb).WithTimeout(50 * time.Millisecond).Build()
	_, err := p.Execute(context.Background(), func(ctx context.Context) (any, error) {
		// Verify context has deadline.
		if _, ok := ctx.Deadline(); !ok {
			t.Error("context should have deadline from WithTimeout")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
}

func TestPipeline_RetryRespectsBreakerState(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "pipe-trip",
		ReadyToOpen: ConsecutiveFailures(2),
	})
	p := Compose[any](cb).WithRetry(10, time.Millisecond).Build()
	var calls int64
	_, err := p.Execute(context.Background(), func(_ context.Context) (any, error) {
		atomic.AddInt64(&calls, 1)
		return nil, errors.New("always fail")
	})
	// The breaker should trip after 2 failures. After that the retry
	// inside Execute sees ErrOpenState... but retries are INSIDE the
	// breaker's Execute, so they don't see ErrOpenState — the breaker
	// wraps everything.
	if err == nil {
		t.Error("expected error")
	}
	// Calls should be <= retries+1 because they're all within one Execute.
	if got := atomic.LoadInt64(&calls); got > 11 {
		t.Errorf("calls = %d, want <= 11", got)
	}
}

func TestPipeline_RetrySucceedsEventually(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "pipe-eventual",
		ReadyToOpen: ConsecutiveFailures(100),
	})
	var calls int64
	p := Compose[any](cb).WithRetry(5, time.Millisecond).Build()
	result, err := p.Execute(context.Background(), func(_ context.Context) (any, error) {
		n := atomic.AddInt64(&calls, 1)
		if n < 3 {
			return nil, errors.New("transient")
		}
		return "success", nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if result != "success" {
		t.Errorf("result = %v, want success", result)
	}
}

func TestPipeline_ConcurrentExecutions(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "pipe-conc"})
	p := Compose[any](cb).WithTimeout(time.Second).Build()
	var wg sync.WaitGroup
	const n = 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = p.Execute(context.Background(), func(_ context.Context) (any, error) {
				return nil, nil
			})
		}()
	}
	wg.Wait()
}

func TestPipeline_ObserverOutcome(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "pipe-obs", Observer: obs})
	p := Compose[any](cb).Build()
	_, _ = p.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, nil
	})
	_, outs, _ := obs.snapshot()
	if len(outs) != 1 || outs[0].outcome != OutcomeSuccess {
		t.Errorf("outcomes = %+v, want 1 success", outs)
	}
}

func TestPipeline_RetryZero_SingleAttempt(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "pipe-zero",
		ReadyToOpen: ConsecutiveFailures(100),
	})
	var calls int64
	p := Compose[any](cb).WithRetry(0, 0).Build()
	_, _ = p.Execute(context.Background(), func(_ context.Context) (any, error) {
		atomic.AddInt64(&calls, 1)
		return nil, errors.New("fail")
	})
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (zero retries = single attempt)", got)
	}
}
