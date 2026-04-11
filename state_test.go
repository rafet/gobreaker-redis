package gobreaker

import (
	"testing"
)

func TestStateString(t *testing.T) {
	cases := []struct {
		s    State
		want string
	}{
		{StateClosed, "closed"},
		{StateHalfOpen, "half-open"},
		{StateOpen, "open"},
		{State(99), "unknown(99)"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("State(%d).String() = %q, want %q", c.s, got, c.want)
		}
	}
}

func TestStateConstantsAreStable(t *testing.T) {
	// These values are persisted to Stores. Changing them is a backwards
	// incompatible change. This test exists to make that change loud.
	if StateClosed != 0 {
		t.Errorf("StateClosed = %d, want 0", StateClosed)
	}
	if StateHalfOpen != 1 {
		t.Errorf("StateHalfOpen = %d, want 1", StateHalfOpen)
	}
	if StateOpen != 2 {
		t.Errorf("StateOpen = %d, want 2", StateOpen)
	}
}

func TestStateIsValid(t *testing.T) {
	if !StateClosed.IsValid() || !StateHalfOpen.IsValid() || !StateOpen.IsValid() {
		t.Error("known states should be valid")
	}
	if State(42).IsValid() {
		t.Error("unknown state should not be valid")
	}
}
