package gobreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Edge cases that no existing test covers.

func TestDedup_ContextCancellation_WaitersUnblock(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-ctx"})
	d := NewDeduplicator[any](cb)

	ctx, cancel := context.WithCancel(context.Background())
	gate := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = d.ExecuteDedup(ctx, "k", func(ctx context.Context) (any, error) {
			<-gate
			return "done", nil
		})
	}()

	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond) // let first goroutine register
		_, _ = d.ExecuteDedup(ctx, "k", func(_ context.Context) (any, error) {
			return nil, nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	close(gate)
	wg.Wait()
	// No hang = pass.
}

func TestPipeline_ExpiredParentContext(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "pipe-expired"})
	p := Compose[any](cb).WithTimeout(time.Second).Build()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.Execute(ctx, func(ctx context.Context) (any, error) {
		return nil, ctx.Err()
	})
	if err == nil {
		t.Error("expected error from expired context")
	}
}

func TestGroup_ReapConcurrentWithGet(t *testing.T) {
	g, _ := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
		MaxIdle:  50 * time.Millisecond,
	})
	// Create some entries.
	for i := 0; i < 10; i++ {
		_, _ = g.Get(context.Background(), string(rune('a'+i)))
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(60 * time.Millisecond) // past MaxIdle
		g.Reap()
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = g.Get(context.Background(), "hot")
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	// No panic, no race = pass.
}

func TestUpdateSettings_DuringHalfOpenRecovery(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:    "cfg-halfopen",
		Timeout: time.Second,
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	clock.Advance(2 * time.Second) // past timeout

	// Tighten ReadyToClose while recovering.
	cb.UpdateSettings(func(s *Settings) {
		s.ReadyToClose = ConsecutiveSuccesses(3) // need 3, not 1
		s.HalfOpenMaxInFlights = 5
	})

	// One success should NOT close (now needs 3).
	_ = succeed(t, cb)
	state, _ := cb.State(context.Background())
	if state == StateClosed {
		t.Error("should still be half-open (ReadyToClose tightened to 3)")
	}
}

func TestNopBreaker_ConcurrentSafe(t *testing.T) {
	var b Breaker[int] = NopBreaker[int]{BreakerName: "nop"}
	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = b.Execute(context.Background(), func(_ context.Context) (int, error) { return 0, nil })
				_, _ = b.State(context.Background())
				_, _ = b.Counts(context.Background())
			}
		}()
	}
	wg.Wait()
}

func TestAdaptiveFailureRateWithWindow_NilGenerationStart(t *testing.T) {
	f := AdaptiveFailureRateWithWindow(10, 0.5, 10*time.Second, time.Now, nil)
	// nil generationStart means the window guard is skipped.
	if !f(Counts{TotalSuccesses: 5, TotalFailures: 5}) {
		t.Error("should fire with nil generationStart (guard skipped)")
	}
}

func TestExecuteWithFallback_ThenHedge(t *testing.T) {
	// ExecuteWithFallback and Hedge are two different composition
	// patterns. This test verifies they can be used on the same
	// breaker sequentially without interference.
	cb, _ := newTestBreaker(t, Settings{Name: "hedge-fb"})

	// Normal call with fallback.
	result, err := cb.ExecuteWithFallback(context.Background(),
		func(_ context.Context) (any, error) { return "primary", nil },
		func(_ context.Context, _ error) (any, error) { return "fallback", nil },
	)
	if err != nil || result != "primary" {
		t.Errorf("ExecuteWithFallback = (%v, %v)", result, err)
	}

	// Hedged call.
	result, err = Hedge(context.Background(), cb,
		func(_ context.Context) (any, error) { return "hedged", nil },
		HedgeDelay(time.Hour),
	)
	if err != nil || result != "hedged" {
		t.Errorf("Hedge = (%v, %v)", result, err)
	}
}

func TestForceOpen_ThenImmediateExecute(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "force-exec"})
	_ = cb.ForceOpen(context.Background())
	_, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		t.Error("should not run")
		return nil, nil
	})
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("err = %v, want ErrOpenState", err)
	}
}

func TestReset_ThenImmediateExecute(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "reset-exec"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	_ = cb.Reset(context.Background())
	if err := succeed(t, cb); err != nil {
		t.Errorf("Execute after Reset: %v", err)
	}
}

func TestGroup_GetAfterReap(t *testing.T) {
	g, _ := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g"},
		MaxIdle:  time.Millisecond,
	})
	_, _ = g.Get(context.Background(), "old")
	time.Sleep(5 * time.Millisecond)
	g.Reap()
	if g.Len() != 0 {
		t.Errorf("Len after Reap = %d, want 0", g.Len())
	}
	// Get after Reap should create fresh.
	cb, err := g.Get(context.Background(), "old")
	if err != nil || cb == nil {
		t.Errorf("Get after Reap: (%v, %v)", cb, err)
	}
	if g.Len() != 1 {
		t.Errorf("Len after re-Get = %d, want 1", g.Len())
	}
}

func TestLatencyTracker_EmptyPercentile(t *testing.T) {
	lt := NewLatencyTracker(100)
	if lt.Percentile(0.99) != 0 {
		t.Error("P99 of empty tracker should be 0")
	}
}

func TestLatencyTracker_SingleSample(t *testing.T) {
	lt := NewLatencyTracker(100)
	lt.OnOutcome("x", OutcomeSuccess, 42*time.Millisecond)
	if lt.Percentile(0.99) != 42*time.Millisecond {
		t.Errorf("P99 of single sample = %v, want 42ms", lt.Percentile(0.99))
	}
}

func TestHedge_BreakerTripsFromHedge(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "hedge-trip",
		ReadyToOpen: ConsecutiveFailures(2),
	})
	boom := errors.New("boom")
	for i := 0; i < 3; i++ {
		_, _ = Hedge(context.Background(), cb,
			func(_ context.Context) (any, error) { return nil, boom },
			HedgeDelay(time.Hour),
		)
	}
	state, _ := cb.State(context.Background())
	if state != StateOpen {
		t.Errorf("state = %v, want open (hedged failures should trip)", state)
	}
}

func TestClosedExpiry_ZeroInterval(t *testing.T) {
	e := closedExpiryFor(time.Now(), 0)
	if !e.IsZero() {
		t.Errorf("closedExpiryFor(0) = %v, want zero", e)
	}
}

func TestClosedExpiry_NegativeInterval(t *testing.T) {
	e := closedExpiryFor(time.Now(), -time.Second)
	if !e.IsZero() {
		t.Errorf("closedExpiryFor(-1s) = %v, want zero", e)
	}
}

func TestClosedExpiry_PositiveInterval(t *testing.T) {
	now := time.Now()
	e := closedExpiryFor(now, 10*time.Second)
	want := now.Add(10 * time.Second)
	if !e.Equal(want) {
		t.Errorf("closedExpiryFor(10s) = %v, want %v", e, want)
	}
}
