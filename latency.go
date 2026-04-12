package gobreaker

import (
	"sync"
	"time"

	"github.com/rafet/gobreaker-redis/v2/internal/ringbuf"
)

// LatencyTracker records request latencies in a fixed-size ring buffer
// and exposes percentile queries. It is used by the latency-aware
// ReadyToOpen predicates (LatencyP99Above, LatencyP50Above, etc.).
//
// A LatencyTracker must be registered as a Settings.Observer so it
// receives OnOutcome events. Then it can be used inside ReadyToOpen:
//
//	tracker := gobreaker.NewLatencyTracker(1000)  // last 1000 requests
//	cb, _ := gobreaker.New[any](ctx, gobreaker.Settings{
//	    Observer: tracker,
//	    ReadyToOpen: gobreaker.Or(
//	        gobreaker.ConsecutiveFailures(5),
//	        tracker.P99Above(2 * time.Second),
//	    ),
//	})
//
// LatencyTracker is safe for concurrent use.
type LatencyTracker struct {
	NopObserver // satisfy the full Observer interface

	mu   sync.Mutex
	ring *ringbuf.Ring
}

// NewLatencyTracker creates a tracker that remembers the last
// windowSize request latencies. A larger window gives smoother
// percentile estimates; a smaller window reacts faster to changes.
func NewLatencyTracker(windowSize int) *LatencyTracker {
	return &LatencyTracker{ring: ringbuf.New(windowSize)}
}

// OnOutcome records the latency of each admitted request. Rejected
// requests (OutcomeRejected) are excluded because they have no
// meaningful latency — the breaker did not invoke the protected
// function.
func (lt *LatencyTracker) OnOutcome(_ string, outcome Outcome, latency time.Duration) {
	if outcome == OutcomeRejected {
		return
	}
	lt.mu.Lock()
	lt.ring.Add(latency)
	lt.mu.Unlock()
}

// OnStateChange resets the latency buffer when the breaker transitions.
// This prevents stale latency data from a previous generation from
// influencing the new generation's trip decision.
func (lt *LatencyTracker) OnStateChange(_ string, _, _ State, _ Counts) {
	lt.mu.Lock()
	lt.ring.Reset()
	lt.mu.Unlock()
}

// Percentile returns the p-th percentile of recorded latencies. p is
// in [0, 1]: 0.5 is the median, 0.99 is P99. If no latencies have
// been recorded, returns 0.
func (lt *LatencyTracker) Percentile(p float64) time.Duration {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.ring.Percentile(p)
}

// Len returns the number of latencies currently in the buffer.
func (lt *LatencyTracker) Len() int {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.ring.Len()
}

// P99Above returns a ReadyToFunc that fires when the P99 latency
// exceeds threshold AND at least minSamples latencies have been
// recorded. The minSamples guard prevents tripping on a tiny number
// of slow warmup requests.
func (lt *LatencyTracker) P99Above(threshold time.Duration, minSamples ...int) ReadyToFunc {
	return lt.percentileAbove(0.99, threshold, minSamples...)
}

// P95Above is like P99Above but for the 95th percentile.
func (lt *LatencyTracker) P95Above(threshold time.Duration, minSamples ...int) ReadyToFunc {
	return lt.percentileAbove(0.95, threshold, minSamples...)
}

// P50Above is like P99Above but for the median.
func (lt *LatencyTracker) P50Above(threshold time.Duration, minSamples ...int) ReadyToFunc {
	return lt.percentileAbove(0.50, threshold, minSamples...)
}

// PercentileAbove returns a ReadyToFunc for an arbitrary percentile.
func (lt *LatencyTracker) PercentileAbove(p float64, threshold time.Duration, minSamples ...int) ReadyToFunc {
	return lt.percentileAbove(p, threshold, minSamples...)
}

func (lt *LatencyTracker) percentileAbove(p float64, threshold time.Duration, minSamples ...int) ReadyToFunc {
	minN := 1
	if len(minSamples) > 0 && minSamples[0] > 0 {
		minN = minSamples[0]
	}
	return func(_ Counts) bool {
		lt.mu.Lock()
		defer lt.mu.Unlock()
		if lt.ring.Len() < minN {
			return false
		}
		return lt.ring.Percentile(p) > threshold
	}
}
