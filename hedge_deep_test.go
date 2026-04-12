package gobreaker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHedge_ObserverSeesAllRequests(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-obs", Observer: obs})
	_, _ = Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			time.Sleep(20 * time.Millisecond)
			return "ok", nil
		},
		HedgeDelay(5*time.Millisecond),
	)
	reqs, outs, _ := obs.snapshot()
	// At least 1 request admitted (primary), possibly 2 (primary + hedge).
	if len(reqs) < 1 {
		t.Errorf("observer saw %d requests, want >= 1", len(reqs))
	}
	if len(outs) < 1 {
		t.Errorf("observer saw %d outcomes, want >= 1", len(outs))
	}
}

func TestHedge_BothRequestsCountInBreaker(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-count"})
	var calls int64
	_, _ = Hedge(context.Background(), cb,
		func(ctx context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			select {
			case <-time.After(30 * time.Millisecond):
				return "ok", nil
			case <-ctx.Done():
				return "cancelled", ctx.Err()
			}
		},
		HedgeDelay(5*time.Millisecond),
	)
	// Give the cancelled goroutine a moment to report its outcome.
	time.Sleep(50 * time.Millisecond)
	c, _ := cb.Counts(context.Background())
	// Both requests should have reported their outcomes.
	if c.InFlights > 0 {
		t.Errorf("InFlights = %d, want 0 after hedge completion (allow time for cancelled goroutine)", c.InFlights)
	}
}

func TestHedge_NilOptions(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-nil"})
	result, err := Hedge(context.Background(), cb,
		func(_ context.Context) (any, error) { return "fast", nil },
	)
	if err != nil || result != "fast" {
		t.Errorf("result = (%v, %v), want (fast, nil)", result, err)
	}
}

func TestHedge_MaxRequests_ClampedTo2(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-clamp"})
	var calls int64
	_, _ = Hedge(context.Background(), cb,
		func(_ context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			time.Sleep(20 * time.Millisecond)
			return "ok", nil
		},
		HedgeDelay(5*time.Millisecond),
		HedgeMaxRequests(0), // should clamp to 2
	)
	if got := atomic.LoadInt64(&calls); got > 2 {
		t.Errorf("calls = %d with MaxRequests(0), want <= 2", got)
	}
}

func TestHedge_ParentContextTimeout(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-parent-ctx"})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := Hedge(ctx, cb,
		func(ctx context.Context) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		HedgeDelay(10*time.Millisecond),
	)
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestHedge_PrimaryPanic_RecoveredAsError(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-panic"})
	// Hedge runs requests in goroutines, so panics cannot propagate
	// to the caller. Instead they are recovered and surfaced as errors.
	_, err := Hedge(context.Background(), cb,
		func(_ context.Context) (any, error) { panic("boom") },
		HedgeDelay(time.Hour),
	)
	if err == nil {
		t.Fatal("expected error from panicked request")
	}
	if err.Error() == "" {
		t.Error("error message should describe the panic")
	}
}

func TestHedge_ConcurrentSafety(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-conc"})
	var wg sync.WaitGroup
	const n = 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = Hedge(context.Background(), cb,
				func(_ context.Context) (any, error) { return nil, nil },
				HedgeDelay(time.Millisecond),
			)
		}()
	}
	wg.Wait()
}
