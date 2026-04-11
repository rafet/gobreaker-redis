package gobreaker

import "testing"

func TestCountsOnRequest(t *testing.T) {
	var c Counts
	c.onRequest()
	c.onRequest()
	c.onRequest()
	if c.Requests != 3 {
		t.Errorf("Requests = %d, want 3", c.Requests)
	}
	if c.InFlights != 3 {
		t.Errorf("InFlights = %d, want 3", c.InFlights)
	}
}

func TestCountsOnSuccess(t *testing.T) {
	var c Counts
	c.onRequest()
	c.onRequest()
	c.onSuccess()
	c.onSuccess()

	if c.InFlights != 0 {
		t.Errorf("InFlights = %d, want 0", c.InFlights)
	}
	if c.TotalSuccesses != 2 {
		t.Errorf("TotalSuccesses = %d, want 2", c.TotalSuccesses)
	}
	if c.ConsecutiveSuccesses != 2 {
		t.Errorf("ConsecutiveSuccesses = %d, want 2", c.ConsecutiveSuccesses)
	}
	if c.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", c.ConsecutiveFailures)
	}
}

func TestCountsOnFailure(t *testing.T) {
	var c Counts
	c.onRequest()
	c.onRequest()
	c.onFailure()
	c.onFailure()

	if c.InFlights != 0 {
		t.Errorf("InFlights = %d, want 0", c.InFlights)
	}
	if c.TotalFailures != 2 {
		t.Errorf("TotalFailures = %d, want 2", c.TotalFailures)
	}
	if c.ConsecutiveFailures != 2 {
		t.Errorf("ConsecutiveFailures = %d, want 2", c.ConsecutiveFailures)
	}
}

func TestCountsOnExclusion(t *testing.T) {
	var c Counts
	c.onRequest()
	c.onRequest()
	c.ConsecutiveFailures = 5
	c.ConsecutiveSuccesses = 3
	c.onExclusion()

	// Exclusion must NOT touch consecutive counters: it conveys neither
	// health nor failure.
	if c.ConsecutiveFailures != 5 {
		t.Errorf("ConsecutiveFailures changed: %d, want 5", c.ConsecutiveFailures)
	}
	if c.ConsecutiveSuccesses != 3 {
		t.Errorf("ConsecutiveSuccesses changed: %d, want 3", c.ConsecutiveSuccesses)
	}
	if c.TotalExclusions != 1 {
		t.Errorf("TotalExclusions = %d, want 1", c.TotalExclusions)
	}
	if c.InFlights != 1 {
		t.Errorf("InFlights = %d, want 1", c.InFlights)
	}
}

func TestCountsConsecutiveResetOnAlternation(t *testing.T) {
	var c Counts
	c.onRequest()
	c.onSuccess()
	c.onRequest()
	c.onSuccess()
	c.onRequest()
	c.onFailure()

	if c.ConsecutiveSuccesses != 0 {
		t.Errorf("ConsecutiveSuccesses = %d, want 0", c.ConsecutiveSuccesses)
	}
	if c.ConsecutiveFailures != 1 {
		t.Errorf("ConsecutiveFailures = %d, want 1", c.ConsecutiveFailures)
	}
	if c.TotalSuccesses != 2 {
		t.Errorf("TotalSuccesses = %d, want 2", c.TotalSuccesses)
	}
	if c.TotalFailures != 1 {
		t.Errorf("TotalFailures = %d, want 1", c.TotalFailures)
	}
}

func TestCountsInFlightDoesNotUnderflow(t *testing.T) {
	var c Counts
	// Reporting an outcome without a matching admission must not panic
	// or wrap around to ~uint64.
	c.onSuccess()
	c.onFailure()
	c.onExclusion()
	if c.InFlights != 0 {
		t.Errorf("InFlights = %d, want 0", c.InFlights)
	}
}

func TestCountsReset(t *testing.T) {
	c := Counts{
		Requests:             10,
		InFlights:            2,
		TotalSuccesses:       5,
		TotalFailures:        3,
		TotalExclusions:      2,
		ConsecutiveSuccesses: 1,
		ConsecutiveFailures:  4,
	}
	c.reset()
	if c != (Counts{}) {
		t.Errorf("reset did not zero Counts: %+v", c)
	}
}
