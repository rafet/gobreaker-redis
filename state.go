package gobreaker

import "fmt"

// State represents the state of a CircuitBreaker.
type State int32

// Possible states. The numeric values are stable across releases and may be
// persisted to a Store; do not reorder.
const (
	StateClosed   State = 0
	StateHalfOpen State = 1
	StateOpen     State = 2
)

// String implements fmt.Stringer.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// IsValid reports whether s is a known state value.
func (s State) IsValid() bool {
	return s == StateClosed || s == StateHalfOpen || s == StateOpen
}
