package gobreaker

import (
	"context"
	"sync"
)

// Deduplicator wraps a CircuitBreaker with singleflight-style request
// deduplication. When multiple goroutines call ExecuteDedup with the
// same key concurrently, only one real request is dispatched; the
// others wait and share the result.
//
// This is especially valuable in the half-open state, where probe
// traffic should be minimal. Without deduplication, N goroutines
// hitting the same key could dispatch N probes; with it, exactly one
// goes through.
//
// Deduplicator is safe for concurrent use.
type Deduplicator[T any] struct {
	cb *CircuitBreaker[T]
	mu sync.Mutex
	in map[string]*call[T]
}

type call[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

// NewDeduplicator wraps an existing CircuitBreaker with deduplication.
func NewDeduplicator[T any](cb *CircuitBreaker[T]) *Deduplicator[T] {
	return &Deduplicator[T]{
		cb: cb,
		in: make(map[string]*call[T]),
	}
}

// ExecuteDedup behaves like CircuitBreaker.Execute, but if another
// goroutine is already executing a request with the same key, the
// caller waits for that request to complete and receives the same
// result.
//
// The key is an opaque string that identifies the logical request.
// Two requests are considered duplicates if and only if they have the
// same key.
//
// The deduplication scope is the in-flight window: once the original
// request completes (success, failure, or panic), the key is removed
// and subsequent calls dispatch a fresh request.
func (d *Deduplicator[T]) ExecuteDedup(ctx context.Context, key string, req func(ctx context.Context) (T, error)) (T, error) {
	d.mu.Lock()
	if c, ok := d.in[key]; ok {
		d.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call[T]{}
	c.wg.Add(1)
	d.in[key] = c
	d.mu.Unlock()

	// Panic safety: if Execute re-raises a panic from the wrapped
	// function, wg.Done() and the key cleanup MUST still run.
	// Without this defer, a panic would deadlock every goroutine
	// waiting on c.wg.Wait() and permanently lock the key.
	defer func() {
		c.wg.Done()
		d.mu.Lock()
		delete(d.in, key)
		d.mu.Unlock()
	}()

	c.val, c.err = d.cb.Execute(ctx, req)

	return c.val, c.err
}

// Execute is a pass-through to the underlying CircuitBreaker.Execute
// without deduplication. It exists so Deduplicator can serve as a
// drop-in replacement for CircuitBreaker in code that mixes
// deduplicated and non-deduplicated calls.
func (d *Deduplicator[T]) Execute(ctx context.Context, req func(ctx context.Context) (T, error)) (T, error) {
	return d.cb.Execute(ctx, req)
}

// CB returns the underlying CircuitBreaker.
func (d *Deduplicator[T]) CB() *CircuitBreaker[T] {
	return d.cb
}
