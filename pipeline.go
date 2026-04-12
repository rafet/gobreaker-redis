package gobreaker

import (
	"context"
	"time"
)

// Pipeline composes multiple resilience strategies around a function.
// The composition is evaluated in reverse registration order:
//
//	pipeline := Compose[T](cb).WithTimeout(5s).WithRetry(3, delay).Build()
//	pipeline.Execute(ctx, fn)
//
//	// Execution order: CircuitBreaker → Timeout → Retry → fn
//
// Pipeline is immutable and safe for concurrent use.
type Pipeline[T any] struct {
	wrap func(ctx context.Context, fn func(context.Context) (T, error)) (T, error)
}

// PipelineBuilder constructs a Pipeline step by step.
type PipelineBuilder[T any] struct {
	cb         *CircuitBreaker[T]
	timeout    time.Duration
	retries    int
	retryDelay time.Duration
}

// Compose starts a pipeline rooted at the given CircuitBreaker.
func Compose[T any](cb *CircuitBreaker[T]) *PipelineBuilder[T] {
	return &PipelineBuilder[T]{cb: cb}
}

// WithTimeout adds a per-call timeout. If the wrapped function does
// not return within d, the context is cancelled.
func (b *PipelineBuilder[T]) WithTimeout(d time.Duration) *PipelineBuilder[T] {
	b.timeout = d
	return b
}

// WithRetry adds retry logic. On failure (as classified by the
// breaker's IsSuccessful), the function is retried up to n times
// with the given fixed delay between attempts.
func (b *PipelineBuilder[T]) WithRetry(n int, delay time.Duration) *PipelineBuilder[T] {
	b.retries = n
	b.retryDelay = delay
	return b
}

// Build constructs the immutable Pipeline.
func (b *PipelineBuilder[T]) Build() *Pipeline[T] {
	cb := b.cb
	timeout := b.timeout
	retries := b.retries
	retryDelay := b.retryDelay

	return &Pipeline[T]{
		wrap: func(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
			// Outer layer: circuit breaker.
			return cb.Execute(ctx, func(ctx context.Context) (T, error) {
				// Middle layer: timeout.
				if timeout > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, timeout)
					defer cancel()
				}

				// Inner layer: retry.
				var lastResult T
				var lastErr error
				attempts := retries + 1
				if attempts < 1 {
					attempts = 1
				}
				for i := 0; i < attempts; i++ {
					lastResult, lastErr = fn(ctx)
					if lastErr == nil {
						return lastResult, nil
					}
					if i < attempts-1 && retryDelay > 0 {
						timer := time.NewTimer(retryDelay)
						select {
						case <-timer.C:
						case <-ctx.Done():
							timer.Stop()
							return lastResult, ctx.Err()
						}
						timer.Stop()
					}
				}
				return lastResult, lastErr
			})
		},
	}
}

// Execute runs the wrapped function through the composed pipeline.
func (p *Pipeline[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	return p.wrap(ctx, fn)
}
