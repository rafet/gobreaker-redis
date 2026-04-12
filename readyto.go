package gobreaker

// ReadyToFunc is the type of every state-transition predicate. It receives a
// snapshot of Counts (always a copy — safe to read without locking) and
// returns true if the breaker should perform the transition. Implementations
// must be pure: they are evaluated while the breaker holds its internal lock
// and may be called frequently.
type ReadyToFunc func(counts Counts) bool

// ConsecutiveFailures returns a ReadyToFunc that fires once Counts has
// recorded n or more consecutive failures. This is the default predicate for
// the closed → open transition.
func ConsecutiveFailures(n uint64) ReadyToFunc {
	return func(c Counts) bool { return c.ConsecutiveFailures >= n }
}

// ConsecutiveSuccesses returns a ReadyToFunc that fires once Counts has
// recorded n or more consecutive successes. This is the default predicate for
// the half-open → closed transition.
func ConsecutiveSuccesses(n uint64) ReadyToFunc {
	return func(c Counts) bool { return c.ConsecutiveSuccesses >= n }
}

// FailureRatio returns a ReadyToFunc that fires when at least minRequests
// requests have completed in the current generation and the failure ratio
// (TotalFailures / (TotalSuccesses + TotalFailures)) is at least ratio.
//
// Excluded outcomes are not counted in the denominator: only requests that
// produced a definite success or failure participate. If fewer than
// minRequests have completed, the predicate returns false regardless of
// ratio.
//
// FailureRatio panics if ratio is not in [0, 1] or if minRequests is zero,
// because both conditions indicate a programming error rather than a
// recoverable runtime state.
func FailureRatio(minRequests uint64, ratio float64) ReadyToFunc {
	if minRequests == 0 {
		panic("gobreaker: FailureRatio requires minRequests > 0")
	}
	if ratio < 0 || ratio > 1 {
		panic("gobreaker: FailureRatio requires ratio in [0, 1]")
	}
	return func(c Counts) bool {
		completed := c.TotalSuccesses + c.TotalFailures
		if completed < minRequests {
			return false
		}
		return float64(c.TotalFailures)/float64(completed) >= ratio
	}
}

// Or returns a ReadyToFunc that fires if any of the supplied predicates fire.
// An empty Or returns a function that never fires.
func Or(funcs ...ReadyToFunc) ReadyToFunc {
	return func(c Counts) bool {
		for _, f := range funcs {
			if f(c) {
				return true
			}
		}
		return false
	}
}

// And returns a ReadyToFunc that fires only if all supplied predicates fire.
// An empty And returns a function that always fires.
func And(funcs ...ReadyToFunc) ReadyToFunc {
	return func(c Counts) bool {
		for _, f := range funcs {
			if !f(c) {
				return false
			}
		}
		return true
	}
}

// Never is a ReadyToFunc that always returns false. Useful for disabling a
// transition entirely (e.g. ReadyToReopen: Never to make half-open behave
// like a one-shot probe that never re-trips on the first failure — generally
// not what you want).
func Never(Counts) bool { return false }

// Always is a ReadyToFunc that always returns true. Useful for the half-open
// → open transition to reopen on the first failure (the conventional
// behavior).
func Always(Counts) bool { return true }
