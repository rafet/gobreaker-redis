package gobreaker

import "time"

// Outcome describes how a request finished from the breaker's perspective.
type Outcome int

// Possible outcomes.
const (
	OutcomeSuccess   Outcome = iota // request completed and IsSuccessful returned true
	OutcomeFailure                  // request completed and IsSuccessful returned false
	OutcomeExclusion                // request completed and IsExcluded returned true
	OutcomeRejected                 // breaker did not admit the request (ErrOpenState/ErrTooManyRequests)
)

// String implements fmt.Stringer.
func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeFailure:
		return "failure"
	case OutcomeExclusion:
		return "exclusion"
	case OutcomeRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// Observer receives a stream of structured events from a CircuitBreaker. It
// is the integration point for metrics and tracing libraries: implement the
// interface to forward events to Prometheus, OpenTelemetry, statsd, or any
// custom sink.
//
// Implementations MUST be safe for concurrent use and MUST return promptly:
// they are invoked synchronously on the goroutine that processes a request.
// Long-running work (network calls, blocking writes) should be dispatched
// to a background goroutine inside the implementation.
//
// All methods are optional in the sense that NopObserver provides a no-op
// embedded base; embed it to opt out of events you do not care about.
type Observer interface {
	// OnRequest is fired immediately after the breaker decides whether
	// to admit a request. admitted reflects that decision; state is the
	// breaker state observed at admission time.
	OnRequest(name string, admitted bool, state State)

	// OnOutcome is fired after an admitted request completes. latency
	// is wall-clock time spent inside the wrapped function. For
	// rejected requests, OnOutcome is NOT called — only OnRequest with
	// admitted=false.
	OnOutcome(name string, outcome Outcome, latency time.Duration)

	// OnStateChange is fired whenever the breaker transitions between
	// states. The counts argument is the per-generation Counts as
	// observed immediately before the transition (matching the value
	// passed to Settings.OnStateChange).
	OnStateChange(name string, from, to State, counts Counts)
}

// NopObserver is an Observer that ignores every event. Embed it to satisfy
// the Observer interface while implementing only the methods you care
// about.
type NopObserver struct{}

// OnRequest implements Observer.
func (NopObserver) OnRequest(string, bool, State) {}

// OnOutcome implements Observer.
func (NopObserver) OnOutcome(string, Outcome, time.Duration) {}

// OnStateChange implements Observer.
func (NopObserver) OnStateChange(string, State, State, Counts) {}
