package gobreaker

import (
	"context"
	"testing"
	"time"
)

// Tests that specifically target mutation test survival in the fast
// path (executeFast / reportFastInline). These functions mirror the
// generic executeStore/report code, so mutation testing finds that
// mutating either copy can survive if only the other is tested.

func TestFastPath_VersionIncrements(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fp-ver"})
	snap1 := cb.inlineSnap
	_ = succeed(t, cb)
	snap2 := cb.inlineSnap
	if snap2.Version <= snap1.Version {
		t.Errorf("Version did not increment: %d -> %d", snap1.Version, snap2.Version)
	}
}

func TestFastPath_GenerationIncrementsOnTrip(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fp-gen"})
	gen1 := cb.inlineSnap.Generation
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	gen2 := cb.inlineSnap.Generation
	if gen2 <= gen1 {
		t.Errorf("Generation did not increment on trip: %d -> %d", gen1, gen2)
	}
}

func TestFastPath_GenerationIncrementsOnRollover(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:     "fp-rollover",
		Interval: time.Second,
	})
	gen1 := cb.inlineSnap.Generation
	clock.Advance(2 * time.Second)
	_ = succeed(t, cb) // trigger rollover
	gen2 := cb.inlineSnap.Generation
	if gen2 <= gen1 {
		t.Errorf("Generation did not increment on rollover: %d -> %d", gen1, gen2)
	}
}

func TestFastPath_GenerationIncrementsOnHalfOpenTransition(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:    "fp-ho-gen",
		Timeout: time.Second,
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	genOpen := cb.inlineSnap.Generation
	clock.Advance(2 * time.Second)
	_ = succeed(t, cb) // open → half-open → closed
	genAfter := cb.inlineSnap.Generation
	if genAfter <= genOpen {
		t.Errorf("Generation did not increment through half-open: %d -> %d", genOpen, genAfter)
	}
}

func TestFastPath_CountsResetOnTrip(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fp-reset"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	c := cb.inlineSnap.Counts
	if c.TotalFailures != 0 {
		t.Errorf("TotalFailures = %d after trip, want 0 (reset on transition)", c.TotalFailures)
	}
}

func TestFastPath_ObserverCheckOnAdmitReject(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{
		Name:     "fp-obs-reject",
		Observer: obs,
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	// Reset observer.
	obs.mu.Lock()
	obs.requests = nil
	obs.mu.Unlock()

	_ = succeed(t, cb) // rejected
	reqs, _, _ := obs.snapshot()
	foundReject := false
	for _, r := range reqs {
		if !r.admitted {
			foundReject = true
		}
	}
	if !foundReject {
		t.Error("observer did not see rejection")
	}
}

func TestFastPath_ObserverCheckOnAdmitAccept(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{
		Name:     "fp-obs-accept",
		Observer: obs,
	})
	_ = succeed(t, cb)
	reqs, _, _ := obs.snapshot()
	foundAccept := false
	for _, r := range reqs {
		if r.admitted {
			foundAccept = true
		}
	}
	if !foundAccept {
		t.Error("observer did not see acceptance")
	}
}

func TestFastPath_StateChangeCallbackOnTrip(t *testing.T) {
	var transitions int
	cb, _ := newTestBreaker(t, Settings{
		Name: "fp-cb-trip",
		OnStateChange: func(_ string, _, _ State, _ Counts) {
			transitions++
		},
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	if transitions != 1 {
		t.Errorf("transitions = %d, want 1 (closed → open)", transitions)
	}
}

func TestFastPath_ReportVersion(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fp-report-ver"})
	v1 := cb.inlineSnap.Version
	_ = succeed(t, cb) // admit increments, report increments
	v2 := cb.inlineSnap.Version
	// Admit + report = at least 2 version bumps per Execute.
	if v2-v1 < 2 {
		t.Errorf("Version only incremented by %d per Execute, want >= 2", v2-v1)
	}
}

func TestFastPath_ExpirySetOnOpen(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:    "fp-expiry",
		Timeout: 30 * time.Second,
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	if cb.inlineSnap.Expiry.IsZero() {
		t.Error("Expiry should be set when breaker opens")
	}
}

func TestFastPath_ExpiryResetOnClose(t *testing.T) {
	cb, clock := newTestBreaker(t, Settings{
		Name:    "fp-expiry-close",
		Timeout: time.Second,
	})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	clock.Advance(2 * time.Second)
	_ = succeed(t, cb) // recover
	// With zero Interval, expiry in closed state should be zero.
	if !cb.inlineSnap.Expiry.IsZero() {
		t.Errorf("Expiry after close = %v, want zero", cb.inlineSnap.Expiry)
	}
}

func TestControl_VersionIncrements(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "ctrl-ver"})
	v1 := cb.inlineSnap.Version
	_ = cb.ForceOpen(context.Background())
	v2 := cb.inlineSnap.Version
	if v2 <= v1 {
		t.Errorf("Version did not increment on ForceOpen: %d -> %d", v1, v2)
	}
	_ = cb.ForceClosed(context.Background())
	v3 := cb.inlineSnap.Version
	if v3 <= v2 {
		t.Errorf("Version did not increment on ForceClosed: %d -> %d", v2, v3)
	}
}

func TestControl_GenerationIncrements(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "ctrl-gen"})
	g1 := cb.inlineSnap.Generation
	_ = cb.ForceOpen(context.Background())
	g2 := cb.inlineSnap.Generation
	if g2 <= g1 {
		t.Errorf("Generation did not increment on ForceOpen: %d -> %d", g1, g2)
	}
}
