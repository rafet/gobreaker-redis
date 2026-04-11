package httpcb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// regression_test.go contains tests for every bug found during the v2
// CodeRabbit review pass that involves the httpcb subpackage, plus
// every gap surfaced by the audit. Naming convention: REG_<topic>.

// REG_Result_2xxReturnsNilError is the regression test for CodeRabbit
// review issue #9. Bug: Result wrapped every non-error response in a
// StatusError, including 2xx responses, breaking the conventional Go
// `if err != nil` pattern for callers.
func TestREG_Result_2xxReturnsNilError(t *testing.T) {
	cases := []int{200, 201, 202, 204, 226, 299}
	for _, code := range cases {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			resp := &http.Response{StatusCode: code, Status: fmt.Sprintf("%d OK", code)}
			got, err := Result(resp, nil) //nolint:bodyclose // synthetic
			if err != nil {
				t.Errorf("Result(%d) returned err = %v, want nil", code, err)
			}
			if got != resp {
				t.Errorf("Result(%d) lost the response", code)
			}
		})
	}
}

// REG_Result_NonSuccessWrapsAsStatusError verifies the contract for
// non-2xx responses: a StatusError carrying the same status is
// returned, the response is preserved.
func TestREG_Result_NonSuccessWrapsAsStatusError(t *testing.T) {
	cases := []int{301, 304, 400, 401, 404, 418, 429, 500, 502, 503, 504, 599}
	for _, code := range cases {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			resp := &http.Response{StatusCode: code, Status: fmt.Sprintf("%d X", code)}
			got, err := Result(resp, nil) //nolint:bodyclose // synthetic
			if got != resp {
				t.Errorf("lost response")
			}
			se := AsStatusError(err)
			if se == nil {
				t.Fatalf("err = %v, want StatusError", err)
			}
			if se.StatusCode != code {
				t.Errorf("StatusCode = %d, want %d", se.StatusCode, code)
			}
		})
	}
}

// REG_AsStatusError_WrappedError verifies that AsStatusError unwraps
// errors carrying a StatusError anywhere in the chain.
func TestREG_AsStatusError_WrappedError(t *testing.T) {
	se := &StatusError{StatusCode: 503, Status: "503 Service Unavailable"}
	wrapped := fmt.Errorf("layer 1: %w", fmt.Errorf("layer 2: %w", se))
	got := AsStatusError(wrapped)
	if got != se {
		t.Errorf("AsStatusError did not unwrap: got %v, want %v", got, se)
	}
}

// REG_OnlyServerErrors_HandlesNonStatusError verifies that OnlyServerErrors
// treats arbitrary non-StatusError errors as failures (the only
// reasonable interpretation of "the request did not produce an HTTP
// response", e.g. dial error).
func TestREG_OnlyServerErrors_HandlesNonStatusError(t *testing.T) {
	if OnlyServerErrors(errors.New("dial tcp: connection refused")) {
		t.Error("connection error should be classified as failure")
	}
}

// REG_RetryableStatuses_EmptyList verifies that an empty status list is
// equivalent to "every status code is success" — only connection errors
// trip the breaker.
func TestREG_RetryableStatuses_EmptyList(t *testing.T) {
	f := RetryableStatuses()
	if !f(&StatusError{StatusCode: 500}) {
		t.Error("empty RetryableStatuses should classify 500 as success")
	}
	if f(errors.New("dial err")) {
		t.Error("connection error should still be a failure")
	}
}

// REG_StatusInRange_BoundariesAreInclusive verifies the documented
// inclusive contract.
func TestREG_StatusInRange_BoundariesAreInclusive(t *testing.T) {
	f := StatusInRange(500, 599)
	if f(&StatusError{StatusCode: 500}) {
		t.Error("500 must be in [500, 599]")
	}
	if f(&StatusError{StatusCode: 599}) {
		t.Error("599 must be in [500, 599]")
	}
	if !f(&StatusError{StatusCode: 499}) {
		t.Error("499 must NOT be in [500, 599]")
	}
	if !f(&StatusError{StatusCode: 600}) {
		t.Error("600 must NOT be in [500, 599]")
	}
}

// REG_Result_NilWithError verifies that Result preserves a non-nil
// error even when the response is nil (the dial-failed case).
func TestREG_Result_NilWithError(t *testing.T) {
	want := errors.New("dial timeout")
	resp, err := Result(nil, want) //nolint:bodyclose // resp is nil
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
	if resp != nil {
		t.Errorf("resp = %v, want nil", resp)
	}
}

// REG_Result_RespNilNoError ensures the (nil, nil) edge case is rejected
// instead of returning a misleading nil/nil pair.
func TestREG_Result_RespNilNoError(t *testing.T) {
	_, err := Result(nil, nil) //nolint:bodyclose
	if err == nil {
		t.Error("Result(nil, nil) returned nil error; want a sentinel")
	}
}

// REG_EndToEnd_Result200OKThroughBreaker is the end-to-end version of
// REG_Result_2xxReturnsNilError. It verifies that a real httptest server
// returning 200 OK results in a nil error from cb.Execute, the
// conventional Go pattern.
func TestREG_EndToEnd_Result200OKThroughBreaker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	cb, err := gobreaker.New[[]byte](context.Background(), gobreaker.Settings{
		Name:         "endpoint-200",
		IsSuccessful: OnlyServerErrors,
	})
	if err != nil {
		t.Fatal(err)
	}

	body, err := cb.Execute(context.Background(), func(ctx context.Context) ([]byte, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, http.NoBody)
		resp, statusErr := Result(http.DefaultClient.Do(req)) //nolint:bodyclose // closed below
		if resp != nil {
			defer resp.Body.Close()
		}
		if statusErr != nil {
			return nil, statusErr
		}
		return []byte("ok"), nil
	})

	if err != nil {
		t.Errorf("200 OK returned err = %v; expected nil", err)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}

	state, _ := cb.State(context.Background())
	if state != gobreaker.StateClosed {
		t.Errorf("state after success = %v, want closed", state)
	}
}

// REG_EndToEnd_Result4xxNotCountedAsFailure verifies the OnlyServerErrors
// preset's headline behavior: a 404 returns an error to the caller
// (StatusError), but does NOT count as a breaker failure.
func TestREG_EndToEnd_Result4xxNotCountedAsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	cb, err := gobreaker.New[[]byte](context.Background(), gobreaker.Settings{
		Name:         "endpoint-404",
		IsSuccessful: OnlyServerErrors,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, _ = cb.Execute(context.Background(), func(ctx context.Context) ([]byte, error) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, http.NoBody)
			resp, statusErr := Result(http.DefaultClient.Do(req)) //nolint:bodyclose // closed below
			if resp != nil {
				_ = resp.Body.Close()
			}
			return nil, statusErr
		})
	}

	state, _ := cb.State(context.Background())
	if state != gobreaker.StateClosed {
		t.Errorf("state after 10x 404 = %v, want closed (4xx must NOT trip the breaker)", state)
	}
	c, _ := cb.Counts(context.Background())
	if c.TotalFailures != 0 {
		t.Errorf("TotalFailures = %d, want 0", c.TotalFailures)
	}
}

// REG_EndToEnd_Result5xxCountedAsFailure verifies the converse: a 503
// counts as a failure and trips the breaker.
func TestREG_EndToEnd_Result5xxCountedAsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cb, err := gobreaker.New[[]byte](context.Background(), gobreaker.Settings{
		Name:         "endpoint-503",
		IsSuccessful: OnlyServerErrors,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		_, _ = cb.Execute(context.Background(), func(ctx context.Context) ([]byte, error) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, http.NoBody)
			resp, statusErr := Result(http.DefaultClient.Do(req)) //nolint:bodyclose // closed below
			if resp != nil {
				_ = resp.Body.Close()
			}
			return nil, statusErr
		})
	}

	state, _ := cb.State(context.Background())
	if state != gobreaker.StateOpen {
		t.Errorf("state after 5x 503 = %v, want open", state)
	}
}
