package gobreaker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestHedge_PrimaryFast_NoHedge(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-fast"})
	var calls int64
	result, err := Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			return "fast", nil
		},
		HedgeDelay(time.Second), // very long — hedge should not fire
	)
	if err != nil {
		t.Fatal(err)
	}
	if result != "fast" {
		t.Errorf("result = %v, want fast", result)
	}
	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("calls = %d, want 1 (no hedge needed)", calls)
	}
}

func TestHedge_PrimarySlow_HedgeFires(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-slow"})
	var calls int64
	result, err := Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			// Both primary and hedge sleep until context done.
			// The hedge should complete at similar time.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
				return "done", nil
			}
		},
		HedgeDelay(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result != "done" {
		t.Errorf("result = %v, want done", result)
	}
	// Both primary and hedge should have been dispatched.
	if got := atomic.LoadInt64(&calls); got < 2 {
		t.Errorf("calls = %d, want >= 2 (hedge should fire)", got)
	}
}

func TestHedge_BreakerOpen_RejectsImmediately(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-open"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	_, err := Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			t.Error("should not run when open")
			return nil, nil
		},
		HedgeDelay(time.Millisecond),
	)
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("err = %v, want ErrOpenState", err)
	}
}

func TestHedge_ContextCancellation(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-ctx"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately

	_, err := Hedge(ctx, cb,
		func(ctx context.Context) (any, error) {
			return nil, nil
		},
		HedgeDelay(time.Second),
	)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestHedge_PrimaryError_ReturnsError(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-err"})
	boom := errors.New("boom")
	_, err := Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			return nil, boom
		},
		HedgeDelay(time.Second),
	)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
}

func TestHedge_DefaultDelay(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-default"})
	result, err := Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			return "ok", nil
		},
		// No HedgeDelay option — defaults to 100ms.
	)
	if err != nil {
		t.Fatal(err)
	}
	if result != "ok" {
		t.Errorf("result = %v, want ok", result)
	}
}

func TestHedge_MaxRequests(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-max"})
	var calls int64
	_, _ = Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			time.Sleep(20 * time.Millisecond)
			return "ok", nil
		},
		HedgeDelay(5*time.Millisecond),
		HedgeMaxRequests(2),
	)
	got := atomic.LoadInt64(&calls)
	if got > 2 {
		t.Errorf("calls = %d, want <= 2 (MaxRequests=2)", got)
	}
}

func BenchmarkHedge_NoHedge(b *testing.B) {
	cb, _ := New[int](context.Background(), Settings{Name: "hedge-bench"})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Hedge(ctx, cb,
			func(ctx context.Context) (int, error) { return 0, nil },
			HedgeDelay(time.Hour),
		)
	}
}
