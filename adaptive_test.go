package gobreaker

import (
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestAdaptiveFailureRate_BelowMinRequests(t *testing.T) {
	f := AdaptiveFailureRate(10, 0.5)
	if f(Counts{TotalSuccesses: 3, TotalFailures: 6}) {
		t.Error("fired below minRequests (9 < 10)")
	}
}

func TestAdaptiveFailureRate_AtMinRequests_AboveRatio(t *testing.T) {
	f := AdaptiveFailureRate(10, 0.5)
	if !f(Counts{TotalSuccesses: 4, TotalFailures: 6}) {
		t.Error("did not fire at 60% failure rate >= 50% threshold")
	}
}

func TestAdaptiveFailureRate_AtMinRequests_BelowRatio(t *testing.T) {
	f := AdaptiveFailureRate(10, 0.5)
	if f(Counts{TotalSuccesses: 7, TotalFailures: 3}) {
		t.Error("fired at 30% failure rate < 50% threshold")
	}
}

func TestAdaptiveFailureRate_HighTraffic(t *testing.T) {
	f := AdaptiveFailureRate(100, 0.1)
	// 1000 requests, 50 failures = 5% < 10% → should NOT fire
	if f(Counts{TotalSuccesses: 950, TotalFailures: 50}) {
		t.Error("fired at 5% failure rate on high traffic")
	}
	// 1000 requests, 150 failures = 15% >= 10% → should fire
	if !f(Counts{TotalSuccesses: 850, TotalFailures: 150}) {
		t.Error("did not fire at 15% failure rate on high traffic")
	}
}

func TestAdaptiveFailureRate_ExclusionsIgnored(t *testing.T) {
	f := AdaptiveFailureRate(10, 0.5)
	// 5 successes + 5 failures + 1000 exclusions = 10 completed
	// Failure ratio = 5/10 = 50% >= 50%
	if !f(Counts{TotalSuccesses: 5, TotalFailures: 5, TotalExclusions: 1000}) {
		t.Error("exclusions should not affect the ratio denominator")
	}
}

func TestAdaptiveFailureRateWithWindow_BeforeWindowAge(t *testing.T) {
	start := time.Now()
	now := func() time.Time { return start.Add(5 * time.Second) }
	f := AdaptiveFailureRateWithWindow(10, 0.5, 10*time.Second, now, &start)

	// 5 seconds < 10 second window → should not fire regardless of ratio
	if f(Counts{TotalSuccesses: 0, TotalFailures: 100}) {
		t.Error("fired before window age elapsed")
	}
}

func TestAdaptiveFailureRateWithWindow_AfterWindowAge(t *testing.T) {
	start := time.Now()
	now := func() time.Time { return start.Add(15 * time.Second) }
	f := AdaptiveFailureRateWithWindow(10, 0.5, 10*time.Second, now, &start)

	if !f(Counts{TotalSuccesses: 5, TotalFailures: 5}) {
		t.Error("did not fire after window age with 50% failure rate")
	}
}

func TestSlowStartThreshold_BelowMinRequests(t *testing.T) {
	f := SlowStartThreshold(10, 100, 0.8, 0.5)
	if f(Counts{TotalSuccesses: 0, TotalFailures: 9}) {
		t.Error("fired below minRequests")
	}
}

func TestSlowStartThreshold_AtMinRequests_LenientPhase(t *testing.T) {
	f := SlowStartThreshold(10, 100, 0.8, 0.5)
	// At 10 requests, effective ratio = 0.8 (lenient)
	// 7 failures / 10 = 70% < 80% → should NOT fire
	if f(Counts{TotalSuccesses: 3, TotalFailures: 7}) {
		t.Error("fired at 70% during lenient phase (threshold 80%)")
	}
	// 9 failures / 10 = 90% >= 80% → should fire
	if !f(Counts{TotalSuccesses: 1, TotalFailures: 9}) {
		t.Error("did not fire at 90% during lenient phase (threshold 80%)")
	}
}

func TestSlowStartThreshold_AtMaxRequests_StrictPhase(t *testing.T) {
	f := SlowStartThreshold(10, 100, 0.8, 0.5)
	// At 100+ requests, effective ratio = 0.5 (strict)
	// 60 failures / 120 = 50% >= 50% → should fire
	if !f(Counts{TotalSuccesses: 60, TotalFailures: 60}) {
		t.Error("did not fire at 50% during strict phase (threshold 50%)")
	}
	// 40 failures / 120 = 33% < 50% → should NOT fire
	if f(Counts{TotalSuccesses: 80, TotalFailures: 40}) {
		t.Error("fired at 33% during strict phase (threshold 50%)")
	}
}

func TestSlowStartThreshold_InterpolationMidpoint(t *testing.T) {
	f := SlowStartThreshold(0, 100, 1.0, 0.5)
	// At 50 requests (midpoint), effective ratio = 0.75
	// 38 failures / 50 = 76% >= 75% → should fire
	if !f(Counts{TotalSuccesses: 12, TotalFailures: 38}) {
		t.Error("did not fire at 76% at midpoint (threshold ~75%)")
	}
	// 36 failures / 50 = 72% < 75% → should NOT fire
	if f(Counts{TotalSuccesses: 14, TotalFailures: 36}) {
		t.Error("fired at 72% at midpoint (threshold ~75%)")
	}
}

func TestSlowStartThreshold_BeyondMaxClamped(t *testing.T) {
	f := SlowStartThreshold(10, 100, 0.8, 0.5)
	// At 200 requests (beyond max), should use endRatio (0.5)
	if f(Counts{TotalSuccesses: 120, TotalFailures: 80}) {
		t.Error("fired at 40% beyond max (threshold 50%)")
	}
	if !f(Counts{TotalSuccesses: 90, TotalFailures: 110}) {
		t.Error("did not fire at 55% beyond max (threshold 50%)")
	}
}

func TestSlowStartThreshold_MinMaxGuards(t *testing.T) {
	// minRequests 0 should be clamped to 1.
	f := SlowStartThreshold(0, 10, 0.8, 0.5)
	if f(Counts{TotalSuccesses: 0, TotalFailures: 0}) {
		t.Error("fired with zero completed requests")
	}

	// maxRequests <= minRequests should be clamped.
	f2 := SlowStartThreshold(10, 5, 0.8, 0.5) // max < min
	// Should not panic, and should work with clamped max.
	_ = f2(Counts{TotalSuccesses: 5, TotalFailures: 15})
}

// Property: AdaptiveFailureRate never fires below minRequests.
func TestProperty_AdaptiveFailureRate_Precondition(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		minReq := rapid.Uint64Range(1, 1000).Draw(rt, "min")
		ratio := rapid.Float64Range(0, 1).Draw(rt, "ratio")
		succ := rapid.Uint64Range(0, minReq-1).Draw(rt, "succ")
		fail := rapid.Uint64Range(0, minReq-1-succ).Draw(rt, "fail")

		f := AdaptiveFailureRate(minReq, ratio)
		if f(Counts{TotalSuccesses: succ, TotalFailures: fail}) {
			rt.Fatalf("fired with %d completed < %d minRequests", succ+fail, minReq)
		}
	})
}

// Property: SlowStartThreshold interpolation is monotonic — the
// effective ratio decreases (gets stricter) as traffic increases.
func TestProperty_SlowStartThreshold_MonotonicStrictness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		startR := rapid.Float64Range(0.5, 1.0).Draw(rt, "start")
		endR := rapid.Float64Range(0.0, startR).Draw(rt, "end")
		minReq := rapid.Uint64Range(1, 50).Draw(rt, "min")
		maxReq := rapid.Uint64Range(minReq+1, 200).Draw(rt, "max")

		f := SlowStartThreshold(minReq, maxReq, startR, endR)

		// At minRequests with 100% failures → lenient phase → may or may not fire
		// At maxRequests with same ratio → strict phase → if lenient fires, strict must fire
		allFail := func(n uint64) Counts {
			return Counts{TotalSuccesses: 0, TotalFailures: n}
		}
		fireAtMin := f(allFail(minReq))
		fireAtMax := f(allFail(maxReq))

		if fireAtMin && !fireAtMax {
			rt.Fatalf("fired at min=%d but not at max=%d with 100%% failures", minReq, maxReq)
		}
	})
}
