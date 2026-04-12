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
// Result behaves as follows:
//
//   - If err is non-nil (connection refused, DNS failure, timeout), it is
//     returned unchanged. The breaker counts these as failures by default.
//   - If resp.StatusCode is in the 2xx range, Result returns a nil error so
//     callers can use the conventional `if err != nil` pattern without
//     special-casing the success path.
//   - For any other status code, Result wraps the response in a
//     StatusError that the IsSuccessful presets (OnlyServerErrors,
//     RetryableStatuses, StatusInRange) can inspect. The caller can still
//     read the response body — *http.Response is returned regardless.
//
// This contract makes the package usable from outside cb.Execute too: a
// caller can write `resp, err := httpcb.Result(http.Get(url))` and treat
// non-2xx responses as errors using normal Go control flow, then choose
// which of those errors should trip the breaker via IsSuccessful.
func Result(resp *http.Response, err error) (*http.Response, error) {
	if err != nil {
		return resp, err
	}
	if resp == nil {
		return nil, errors.New("httpcb: nil response with nil error")
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
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
//
// Note: the `low > high` swap guard accepts misordered arguments
// graciously. The `low > high` vs `low >= high` mutation is equivalent
// at the boundary (low == high), so mutation testing reports it as
// surviving — there is no test that can distinguish the two because
// both produce the same outcome on the same input. This is a textbook
// equivalent mutant.
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
