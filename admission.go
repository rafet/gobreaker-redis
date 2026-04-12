package gobreaker

import (
	"math"
	"math/rand/v2"
	"time"
)

// AdmissionStrategy controls how traffic is gradually re-admitted
// during the half-open state. Without a strategy, the breaker admits
// up to HalfOpenMaxInFlights requests immediately. With a strategy,
// it additionally gates admission on a time-based probability ramp.
//
// Set it on Settings.HalfOpenAdmission.
type AdmissionStrategy interface {
	// Admit returns true if a request should be admitted given the
	// elapsed time since the breaker entered the half-open state.
	// elapsed is always >= 0.
	Admit(elapsed time.Duration) bool
}

// NoRamp is the default: all requests up to HalfOpenMaxInFlights are
// admitted immediately. Equivalent to nil HalfOpenAdmission.
type NoRamp struct{}

// Admit always returns true.
func (NoRamp) Admit(time.Duration) bool { return true }

// LinearRamp admits traffic linearly from 0% to 100% over the given
// duration. At elapsed == 0, probability is 0. At elapsed >= duration,
// probability is 1. Between the two it interpolates linearly.
//
// This eliminates the "П_П_П" traffic peak pattern where closing the
// breaker suddenly floods the recovering backend with full traffic.
type LinearRamp struct {
	Duration time.Duration
}

// Admit returns true with probability elapsed/Duration.
func (r LinearRamp) Admit(elapsed time.Duration) bool {
	if elapsed >= r.Duration {
		return true
	}
	if elapsed <= 0 || r.Duration <= 0 {
		return false
	}
	p := float64(elapsed) / float64(r.Duration)
	return rand.Float64() < p
}

// ExponentialRamp admits traffic on an exponential curve from ~0% to
// 100% over the given duration. The curve starts very slowly and
// accelerates, which is more conservative than LinearRamp: it keeps
// load minimal during the early recovery phase and ramps quickly once
// confidence builds.
type ExponentialRamp struct {
	Duration time.Duration
}

// Admit returns true with probability (e^(k*t) - 1) / (e^k - 1)
// where t = elapsed/Duration and k controls the curve steepness.
func (r ExponentialRamp) Admit(elapsed time.Duration) bool {
	if elapsed >= r.Duration {
		return true
	}
	if elapsed <= 0 || r.Duration <= 0 {
		return false
	}
	const k = 3.0 // steepness — 3 gives a nice S-ish curve
	t := float64(elapsed) / float64(r.Duration)
	p := (math.Exp(k*t) - 1) / (math.Exp(k) - 1)
	return rand.Float64() < p
}

// StepRamp admits traffic in discrete steps. Each step defines a
// minimum elapsed time and the admission probability at that point.
// Between steps, the probability is held constant at the previous
// step's level.
//
// Example: 10% after 5s, 50% after 15s, 100% after 30s:
//
//	StepRamp{
//	    Steps: []Step{
//	        {After: 5 * time.Second, Probability: 0.10},
//	        {After: 15 * time.Second, Probability: 0.50},
//	        {After: 30 * time.Second, Probability: 1.00},
//	    },
//	}
type StepRamp struct {
	Steps []Step
}

// Step defines a discrete admission probability that becomes active
// after a minimum elapsed time.
type Step struct {
	After       time.Duration
	Probability float64
}

// Admit returns true based on the step matching the elapsed time.
func (r StepRamp) Admit(elapsed time.Duration) bool {
	p := 0.0
	for _, s := range r.Steps {
		if elapsed >= s.After {
			p = s.Probability
		} else {
			break
		}
	}
	if p >= 1 {
		return true
	}
	if p <= 0 {
		return false
	}
	return rand.Float64() < p
}
