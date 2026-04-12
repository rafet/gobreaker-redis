// Package ringbuf provides a fixed-size circular buffer of time.Duration
// values with O(1) append and O(n log n) percentile queries. It is used
// by the latency-aware tripping predicates to track recent request
// latencies without unbounded memory growth.
//
// The buffer is NOT goroutine-safe. The caller (CircuitBreaker) is
// responsible for synchronizing access.
package ringbuf

import (
	"sort"
	"time"
)

// Ring is a circular buffer of time.Duration values with a fixed
// capacity. When the buffer is full, the oldest value is overwritten.
type Ring struct {
	data  []time.Duration
	pos   int  // next write position
	full  bool // true once we've wrapped around at least once
	count int  // total writes (not capped — monotonic)
}

// New creates a Ring with the given capacity. Capacity must be >= 1.
func New(capacity int) *Ring {
	if capacity < 1 {
		capacity = 1
	}
	return &Ring{data: make([]time.Duration, capacity)}
}

// Add appends a duration to the ring, overwriting the oldest if full.
func (r *Ring) Add(d time.Duration) {
	r.data[r.pos] = d
	r.pos++
	r.count++
	if r.pos >= len(r.data) {
		r.pos = 0
		r.full = true
	}
}

// Len returns the number of values currently in the buffer (up to
// capacity).
func (r *Ring) Len() int {
	if r.full {
		return len(r.data)
	}
	return r.pos
}

// Count returns the total number of Add calls (monotonic, not capped
// by capacity).
func (r *Ring) Count() int {
	return r.count
}

// Percentile returns the p-th percentile of the values currently in the
// buffer. p must be in [0, 1]: 0.5 is the median, 0.99 is P99. If the
// buffer is empty, Percentile returns 0.
//
// The implementation copies the live portion of the ring into a
// temporary slice and sorts it. This is O(n log n) and allocates — it
// is called once per ReadyToOpen evaluation, not once per request.
func (r *Ring) Percentile(p float64) time.Duration {
	n := r.Len()
	if n == 0 {
		return 0
	}
	if p <= 0 {
		p = 0
	}
	if p >= 1 {
		p = 1
	}

	// Copy the live values.
	tmp := make([]time.Duration, n)
	if r.full {
		copy(tmp, r.data)
	} else {
		copy(tmp, r.data[:r.pos])
	}
	sort.Slice(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })

	// Nearest-rank method.
	idx := int(float64(n-1) * p)
	return tmp[idx]
}

// Reset clears the buffer. After Reset, Len() == 0 and Count() == 0.
func (r *Ring) Reset() {
	r.pos = 0
	r.full = false
	r.count = 0
}
