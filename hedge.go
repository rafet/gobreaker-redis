package gobreaker

import (
	"context"
	"time"
)

// HedgeOption configures a hedged execution.
type HedgeOption func(*hedgeConfig)

type hedgeConfig struct {
	delay   time.Duration
	maxReqs int
}

// HedgeDelay sets the time to wait before issuing the speculative
// request. If the primary returns before the delay, no hedge is sent.
func HedgeDelay(d time.Duration) HedgeOption {
	return func(c *hedgeConfig) { c.delay = d }
}

// HedgeMaxRequests sets the maximum total requests (primary +
// hedges). Defaults to 2 (one primary, one hedge).
func HedgeMaxRequests(n int) HedgeOption {
	return func(c *hedgeConfig) {
		if n < 2 {
			n = 2
		}
		c.maxReqs = n
	}
}

// Hedge executes req through the breaker. If the primary request has
// not returned after the configured delay, a speculative second
// request is dispatched. The first result to arrive is returned; the
// slower request's context is cancelled.
//
// Both the primary and the hedge go through the breaker's admission
// and outcome accounting. If the breaker is open, Hedge returns
// ErrOpenState without dispatching anything.
//
// Hedge reduces tail latency at the cost of additional load. Use it
// only when the protected service is idempotent and can tolerate
// duplicate requests.
//
// Example:
//
//	resp, err := gobreaker.Hedge(ctx, cb,
//	    func(ctx context.Context) (*http.Response, error) {
//	        return http.DefaultClient.Do(req.WithContext(ctx))
//	    },
//	    gobreaker.HedgeDelay(100 * time.Millisecond),
//	)
func Hedge[T any](
	ctx context.Context,
	cb *CircuitBreaker[T],
	req func(ctx context.Context) (T, error),
	opts ...HedgeOption,
) (T, error) {
	cfg := hedgeConfig{delay: 100 * time.Millisecond, maxReqs: 2}
	for _, o := range opts {
		o(&cfg)
	}

	type result struct {
		val T
		err error
	}

	// Primary request.
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()

	ch := make(chan result, cfg.maxReqs)

	go func() {
		v, err := cb.Execute(primaryCtx, req)
		ch <- result{v, err}
	}()

	// Wait for either the primary to return or the hedge delay.
	timer := time.NewTimer(cfg.delay)
	defer timer.Stop()

	select {
	case r := <-ch:
		return r.val, r.err
	case <-timer.C:
		// Primary is slow — dispatch a hedge.
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}

	// Hedge request.
	hedgeCtx, hedgeCancel := context.WithCancel(ctx)
	defer hedgeCancel()

	go func() {
		v, err := cb.Execute(hedgeCtx, req)
		ch <- result{v, err}
	}()

	// Return whichever finishes first.
	select {
	case r := <-ch:
		// Cancel the slower one.
		primaryCancel()
		hedgeCancel()
		return r.val, r.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
