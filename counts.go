package gobreaker

// Counts is a snapshot of request statistics for a single generation of a
// CircuitBreaker. The fields are deliberately exported so user-supplied
// ReadyToOpen/ReadyToClose/ReadyToReopen functions can inspect them without
// going through accessor methods. A Counts value is always a copy and is safe
// to read without locking.
//
// A "generation" is the period during which a Counts is accumulated. The
// CircuitBreaker resets Counts to the zero value at every state transition and
// at every Interval boundary in the closed state. The semantics match
// sony/gobreaker, with two additions: InFlights tracks requests that have been
// admitted but have not yet reported their outcome, and TotalExclusions tracks
// outcomes that IsExcluded chose to ignore.
type Counts struct {
	Requests             uint64 // total admitted requests in this generation
	InFlights            uint64 // currently in-flight (admitted, no outcome yet)
	TotalSuccesses       uint64
	TotalFailures        uint64
	TotalExclusions      uint64
	ConsecutiveSuccesses uint64
	ConsecutiveFailures  uint64
}

// onRequest accounts for a newly admitted request.
func (c *Counts) onRequest() {
	c.Requests++
	c.InFlights++
}

// onSuccess accounts for a request that completed successfully.
func (c *Counts) onSuccess() {
	if c.InFlights > 0 {
		c.InFlights--
	}
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

// onFailure accounts for a request that completed with a counted failure.
func (c *Counts) onFailure() {
	if c.InFlights > 0 {
		c.InFlights--
	}
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

// onExclusion accounts for a request whose outcome was ignored by IsExcluded.
// Excluded outcomes do not affect the consecutive counters: they neither
// confirm health nor signal failure.
func (c *Counts) onExclusion() {
	if c.InFlights > 0 {
		c.InFlights--
	}
	c.TotalExclusions++
}

// reset zeroes the counts. Called at every generation boundary.
func (c *Counts) reset() {
	*c = Counts{}
}
