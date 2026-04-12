package gobreaker

import (
	"context"
	"testing"
)

func TestNopBreaker_Execute(t *testing.T) {
	var b Breaker[string] = NopBreaker[string]{BreakerName: "nop"}
	result, err := b.Execute(context.Background(), func(_ context.Context) (string, error) {
		return "hello", nil
	})
	if err != nil || result != "hello" {
		t.Errorf("Execute = (%q, %v), want (hello, nil)", result, err)
	}
}

func TestNopBreaker_State(t *testing.T) {
	b := NopBreaker[any]{BreakerName: "nop"}
	state, err := b.State(context.Background())
	if err != nil || state != StateClosed {
		t.Errorf("State = (%v, %v), want (closed, nil)", state, err)
	}
}

func TestNopBreaker_Counts(t *testing.T) {
	b := NopBreaker[any]{BreakerName: "nop"}
	c, err := b.Counts(context.Background())
	if err != nil || c != (Counts{}) {
		t.Errorf("Counts = (%+v, %v), want (zero, nil)", c, err)
	}
}

func TestNopBreaker_Name(t *testing.T) {
	b := NopBreaker[any]{BreakerName: "test-nop"}
	if b.Name() != "test-nop" {
		t.Errorf("Name = %q", b.Name())
	}
}

func TestBreaker_InterfaceSatisfied(t *testing.T) {
	// Compile-time: CircuitBreaker satisfies Breaker.
	var _ Breaker[any] = (*CircuitBreaker[any])(nil)
	// NopBreaker too.
	var _ Breaker[any] = NopBreaker[any]{}
}
