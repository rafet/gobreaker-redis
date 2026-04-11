// Package httpcb provides HTTP-specific helpers for use with
// github.com/rafet/gobreaker-redis/v2.
//
// The core gobreaker package is intentionally protocol-agnostic: it knows
// nothing about HTTP status codes, retryable failures, or RFC semantics.
// Most circuit breakers in production wrap HTTP requests, though, so this
// package supplies the boilerplate-free recipes for the common cases.
//
// # Quick start
//
//	cb, _ := gobreaker.New[*http.Response](ctx, gobreaker.Settings{
//	    Name:         "user-service",
//	    IsSuccessful: httpcb.OnlyServerErrors,
//	    IsExcluded:   gobreaker.IgnoreContextErrors,
//	})
//
// IsSuccessful presets:
//
//   - OnlyServerErrors: count 5xx and connection errors as failures, treat
//     everything else (including 4xx) as success. This is the recommended
//     default for service-to-service calls — a 4xx is the caller's fault
//     and says nothing about the upstream's health.
//   - RetryableStatuses(codes...): treat exactly the listed status codes
//     (and connection errors) as failures. Use this when you have a
//     specific list — e.g. 429, 502, 503, 504.
//   - StatusInRange(min, max): treat any status in the inclusive range as
//     a failure.
//
// All presets share the same shape: they accept the (resp, err) pair from
// http.Client.Do via the Result helper. The presets themselves are
// IsSuccessful functions whose error parameter is constructed via Result.
package httpcb

import (
	"errors"
	"fmt"
	"net/http"
)

// StatusError is the error type produced by Result when a non-nil response
// has a status code that the caller wants to treat as a failure. It is
// returned in addition to the *http.Response so the response body can still
// be inspected.
type StatusError struct {
	StatusCode int
	Status     string
}

// Error implements error.
func (e *StatusError) Error() string {
	return fmt.Sprintf("httpcb: HTTP status %s", e.Status)
}

// AsStatusError extracts a *StatusError from err if one is present anywhere
// in the chain. Returns nil if err does not wrap a StatusError.
func AsStatusError(err error) *StatusError {
	var se *StatusError
	if errors.As(err, &se) {
		return se
	}
	return nil
}

// Result wraps an (*http.Response, error) pair so that the response status
// can participate in IsSuccessful classification. Use it inside the
// Execute callback:
//
//	resp, err := cb.Execute(ctx, func(ctx context.Context) (*http.Response, error) {
//	    resp, err := http.DefaultClient.Do(req.WithContext(ctx))
//	    return httpcb.Result(resp, err)
//	})
//
// If err is non-nil, Result returns it unchanged: connection errors,
// timeouts, and DNS failures bypass the status logic.
//
// If resp is non-nil, Result wraps the response in a StatusError that the
// IsSuccessful presets can inspect. The response is also returned so the
// caller can read its body or headers regardless of how the breaker
// classifies it.
func Result(resp *http.Response, err error) (*http.Response, error) {
	if err != nil {
		return resp, err
	}
	if resp == nil {
		return nil, errors.New("httpcb: nil response with nil error")
	}
	return resp, &StatusError{StatusCode: resp.StatusCode, Status: resp.Status}
}

// OnlyServerErrors is an IsSuccessful function that classifies a request as
// successful unless it returned a connection error or a 5xx status. This is
// the recommended default for service-to-service HTTP calls because 4xx
// responses indicate caller error and should not trip the breaker.
func OnlyServerErrors(err error) bool {
	if err == nil {
		return true
	}
	if se := AsStatusError(err); se != nil {
		return se.StatusCode < 500
	}
	// Non-status errors (connection refused, DNS, timeout) are failures.
	return false
}

// RetryableStatuses returns an IsSuccessful function that treats only the
// listed status codes — plus connection errors — as failures. All other
// responses (including unlisted error codes) count as successes.
//
// Typical usage covers the canonical "transient" set: 429, 502, 503, 504.
func RetryableStatuses(codes ...int) func(error) bool {
	set := make(map[int]struct{}, len(codes))
	for _, c := range codes {
		set[c] = struct{}{}
	}
	return func(err error) bool {
		if err == nil {
			return true
		}
		if se := AsStatusError(err); se != nil {
			_, isFailure := set[se.StatusCode]
			return !isFailure
		}
		// Connection errors are always failures.
		return false
	}
}

// StatusInRange returns an IsSuccessful function that treats any HTTP
// status in the inclusive range [low, high] — plus connection errors — as
// failures.
func StatusInRange(low, high int) func(error) bool {
	if low > high {
		low, high = high, low
	}
	return func(err error) bool {
		if err == nil {
			return true
		}
		if se := AsStatusError(err); se != nil {
			return se.StatusCode < low || se.StatusCode > high
		}
		return false
	}
}
