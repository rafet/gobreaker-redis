package gobreaker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipeline_BasicExecution(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "pipe"})
	p := Compose[any](cb).Build()
	result, err := p.Execute(context.Background(), func(_ context.Context) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "ok" {
		t.Errorf("result = %v, want ok", result)
	}
}

func TestPipeline_Timeout(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "pipe-timeout"})
	p := Compose[any](cb).WithTimeout(50 * time.Millisecond).Build()
	_, err := p.Execute(context.Background(), func(ctx context.Context) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return "late", nil
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
}

func TestPipeline_Retry(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "pipe-retry",
		ReadyToOpen: ConsecutiveFailures(100), // don't trip
	})
	var calls int64
	boom := errors.New("transient")
	p := Compose[any](cb).WithRetry(3, time.Millisecond).Build()
	_, err := p.Execute(context.Background(), func(_ context.Context) (any, error) {
		n := atomic.AddInt64(&calls, 1)
		if n < 4 {
			return nil, boom
		}
		return "recovered", nil
	})
	if err != nil {
		t.Fatalf("err = %v after retries", err)
	}
	if got := atomic.LoadInt64(&calls); got != 4 {
		t.Errorf("calls = %d, want 4 (1 + 3 retries)", got)
	}
}

func TestPipeline_RetryExhausted(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "pipe-exhaust",
		ReadyToOpen: ConsecutiveFailures(100),
	})
	boom := errors.New("permanent")
	p := Compose[any](cb).WithRetry(2, time.Millisecond).Build()
	_, err := p.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want permanent error after retry exhaustion", err)
	}
}

func TestPipeline_TimeoutPlusRetry(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "pipe-both",
		ReadyToOpen: ConsecutiveFailures(100),
	})
	var calls int64
	p := Compose[any](cb).
		WithTimeout(200 * time.Millisecond).
		WithRetry(5, time.Millisecond).
		Build()

	_, err := p.Execute(context.Background(), func(ctx context.Context) (any, error) {
		atomic.AddInt64(&calls, 1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return nil, errors.New("slow fail")
		}
	})
	// Should exhaust retries within the timeout window.
	if err == nil {
		t.Error("expected error")
	}
}

func TestPipeline_BreakerOpenBlocksAll(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "pipe-open"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	p := Compose[any](cb).WithRetry(10, time.Millisecond).Build()
	_, err := p.Execute(context.Background(), func(_ context.Context) (any, error) {
		t.Error("should not run when open")
		return nil, nil
	})
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("err = %v, want ErrOpenState", err)
	}
}

func TestPipeline_NoTimeoutNoRetry(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "pipe-bare"})
	p := Compose[any](cb).Build()
	result, err := p.Execute(context.Background(), func(_ context.Context) (any, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 42 {
		t.Errorf("result = %v, want 42", result)
	}
}
