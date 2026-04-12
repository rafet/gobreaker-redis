package gobreaker

import "time"

// AdaptiveFailureRate returns a ReadyToFunc that uses percentage-based
// failure detection instead of absolute counts. This adapts to traffic
// volume automatically: at high traffic 10 failures is noise; at low
// traffic 10 failures is catastrophic.
//
// Parameters:
//   - minRequests: minimum completed requests (successes + failures)
//     in the current generation before the predicate can fire. This
//     prevents tripping on a tiny warmup sample.
//   - ratio: failure ratio threshold in [0, 1]. The predicate fires
//     when TotalFailures / (TotalSuccesses + TotalFailures) >= ratio.
//   - Excluded outcomes are NOT counted in the denominator.
//
// Example:
//
//	ReadyToOpen: gobreaker.AdaptiveFailureRate(20, 0.5)
//	// Trip when at least 20 requests have completed AND >=50% failed.
//
// AdaptiveFailureRate is equivalent to FailureRatio(minRequests, ratio)
// but named differently to signal the intent: the threshold adapts to
// traffic volume, not to a fixed count.
func AdaptiveFailureRate(minRequests uint64, ratio float64) ReadyToFunc {
	return FailureRatio(minRequests, ratio)
}

// AdaptiveFailureRateWithWindow returns a ReadyToFunc that combines
// percentage-based failure detection with a time-based minimum sample
// window. The predicate fires only when:
//
//  1. At least minRequests have completed, AND
//  2. The current generation has been running for at least windowAge, AND
//  3. The failure ratio >= ratio.
//
// The window age guard prevents tripping during the initial burst after
// a generation reset, where a few fast failures can produce a
// misleadingly high ratio. It pairs naturally with Settings.Interval
// to create a rolling evaluation window.
//
// The Counts struct does not carry timing information, so windowAge is
// evaluated against the generation's elapsed time. The caller must
// supply a clock function (typically time.Now); the predicate captures
// it at construction time.
func AdaptiveFailureRateWithWindow(minRequests uint64, ratio float64, windowAge time.Duration, now func() time.Time, generationStart *time.Time) ReadyToFunc {
	if now == nil {
		now = time.Now
	}
	base := FailureRatio(minRequests, ratio)
	return func(c Counts) bool {
		if generationStart != nil && now().Sub(*generationStart) < windowAge {
			return false
		}
		return base(c)
	}
}

// SlowStartThreshold returns a ReadyToFunc that starts with a lenient
// threshold and becomes stricter as more requests arrive. This is
// useful for services that have variable warmup characteristics.
//
// At minRequests, the effective ratio is startRatio. At maxRequests,
// it is endRatio. Between the two it interpolates linearly.
//
// Example: start lenient (80% failure required), tighten to 50%:
//
//	ReadyToOpen: gobreaker.SlowStartThreshold(10, 100, 0.8, 0.5)
func SlowStartThreshold(minRequests, maxRequests uint64, startRatio, endRatio float64) ReadyToFunc {
	if minRequests == 0 {
		minRequests = 1
	}
	if maxRequests <= minRequests {
		maxRequests = minRequests + 1
	}
	return func(c Counts) bool {
		completed := c.TotalSuccesses + c.TotalFailures
		if completed < minRequests {
			return false
		}
		// Interpolate the effective ratio.
		progress := float64(completed-minRequests) / float64(maxRequests-minRequests)
		if progress > 1 {
			progress = 1
		}
		effectiveRatio := startRatio + (endRatio-startRatio)*progress
		actualRatio := float64(c.TotalFailures) / float64(completed)
		return actualRatio >= effectiveRatio
	}
}
