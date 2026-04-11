package gobreaker

import "testing"

func TestConsecutiveFailures(t *testing.T) {
	f := ConsecutiveFailures(3)
	if f(Counts{ConsecutiveFailures: 2}) {
		t.Error("should not fire at 2 failures")
	}
	if !f(Counts{ConsecutiveFailures: 3}) {
		t.Error("should fire at 3 failures")
	}
	if !f(Counts{ConsecutiveFailures: 100}) {
		t.Error("should fire at 100 failures")
	}
}

func TestConsecutiveSuccesses(t *testing.T) {
	f := ConsecutiveSuccesses(2)
	if f(Counts{ConsecutiveSuccesses: 1}) {
		t.Error("should not fire at 1 success")
	}
	if !f(Counts{ConsecutiveSuccesses: 2}) {
		t.Error("should fire at 2 successes")
	}
}

func TestFailureRatio(t *testing.T) {
	f := FailureRatio(10, 0.5)

	// Below minRequests: never fires.
	if f(Counts{TotalSuccesses: 0, TotalFailures: 9}) {
		t.Error("should not fire below minRequests")
	}

	// Exactly at minRequests, ratio above threshold.
	if !f(Counts{TotalSuccesses: 4, TotalFailures: 6}) {
		t.Error("6/10 should fire at threshold 0.5")
	}

	// At minRequests, ratio at threshold.
	if !f(Counts{TotalSuccesses: 5, TotalFailures: 5}) {
		t.Error("5/10 should fire at threshold 0.5 (boundary)")
	}

	// At minRequests, ratio below threshold.
	if f(Counts{TotalSuccesses: 6, TotalFailures: 4}) {
		t.Error("4/10 should not fire at threshold 0.5")
	}
}

func TestFailureRatioExclusionsExcluded(t *testing.T) {
	// Exclusions must not enter the denominator.
	f := FailureRatio(4, 0.5)
	c := Counts{
		TotalSuccesses:  2,
		TotalFailures:   2,
		TotalExclusions: 100, // ignored
	}
	if !f(c) {
		t.Error("2 successes + 2 failures should fire at 0.5 even with exclusions")
	}
}

func TestFailureRatioPanicsOnBadInput(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("FailureRatio(0, 0.5) should panic")
		}
	}()
	_ = FailureRatio(0, 0.5)
}

func TestFailureRatioPanicsOnBadRatio(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("FailureRatio(1, 1.5) should panic")
		}
	}()
	_ = FailureRatio(1, 1.5)
}

func TestOr(t *testing.T) {
	never := func(Counts) bool { return false }
	always := func(Counts) bool { return true }

	if Or()(Counts{}) {
		t.Error("empty Or should not fire")
	}
	if Or(never, never)(Counts{}) {
		t.Error("Or(never, never) should not fire")
	}
	if !Or(never, always)(Counts{}) {
		t.Error("Or(never, always) should fire")
	}
}

func TestAnd(t *testing.T) {
	never := func(Counts) bool { return false }
	always := func(Counts) bool { return true }

	if !And()(Counts{}) {
		t.Error("empty And should fire")
	}
	if !And(always, always)(Counts{}) {
		t.Error("And(always, always) should fire")
	}
	if And(always, never)(Counts{}) {
		t.Error("And(always, never) should not fire")
	}
}

func TestNeverAlways(t *testing.T) {
	if Never(Counts{Requests: 1000}) {
		t.Error("Never should never fire")
	}
	if !Always(Counts{}) {
		t.Error("Always should always fire")
	}
}
