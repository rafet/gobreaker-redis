package gobreaker

import "context"

// Breaker is the interface that CircuitBreaker[T] satisfies. It
// enables mocking in tests and allows alternative implementations
// (e.g. a noop breaker for development environments) to be swapped
// in without changing the call site.
//
// This is the interface sony/gobreaker v3 is heading toward. We ship
// it in v2.x as an opt-in so callers that want testability can
// program against it today.
type Breaker[T any] interface {
	// Execute runs req through the breaker and returns its result.
	Execute(ctx context.Context, req func(context.Context) (T, error)) (T, error)

	// State returns the current state of the breaker.
	State(ctx context.Context) (State, error)

	// Counts returns the current Counts snapshot.
	Counts(ctx context.Context) (Counts, error)

	// Name returns the breaker's name.
	Name() string
}

// compile-time assertion
var _ Breaker[any] = (*CircuitBreaker[any])(nil)

// NopBreaker is a Breaker that passes every request through without
// any circuit-breaking logic. Use it in tests or development
// environments where you want to disable the breaker without changing
// the call site.
type NopBreaker[T any] struct {
	BreakerName string
}

// Execute runs req directly.
func (n NopBreaker[T]) Execute(ctx context.Context, req func(context.Context) (T, error)) (T, error) {
	return req(ctx)
}

// State always returns StateClosed.
func (n NopBreaker[T]) State(context.Context) (State, error) {
	return StateClosed, nil
}

// Counts always returns zero Counts.
func (n NopBreaker[T]) Counts(context.Context) (Counts, error) {
	return Counts{}, nil
}

// Name returns the configured name.
func (n NopBreaker[T]) Name() string { return n.BreakerName }
