package httpcb

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

func TestStatusErrorMessage(t *testing.T) {
	se := &StatusError{StatusCode: 503, Status: "503 Service Unavailable"}
	if se.Error() == "" {
		t.Error("empty error message")
	}
}

func TestAsStatusError(t *testing.T) {
	if AsStatusError(nil) != nil {
		t.Error("AsStatusError(nil) should be nil")
	}
	if AsStatusError(errors.New("plain")) != nil {
		t.Error("AsStatusError on non-StatusError should be nil")
	}
	se := &StatusError{StatusCode: 502, Status: "502 Bad Gateway"}
	if got := AsStatusError(se); got != se {
		t.Errorf("AsStatusError direct: got %v, want %v", got, se)
	}
}

func TestResultPropagatesError(t *testing.T) {
	want := errors.New("dial timeout")
	resp, err := Result(nil, want) //nolint:bodyclose // resp is nil
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
	if resp != nil {
		t.Errorf("resp = %v, want nil", resp)
	}
}

func TestResultWrapsResponse(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable"}
	got, err := Result(resp, nil) //nolint:bodyclose // synthetic response, no body
	if got != resp {
		t.Errorf("Result lost response")
	}
	se := AsStatusError(err)
	if se == nil {
		t.Fatal("error is not a StatusError")
	}
	if se.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("StatusCode = %d, want %d", se.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestResultNilResponseAndNilError(t *testing.T) {
	_, err := Result(nil, nil) //nolint:bodyclose // resp is nil
	if err == nil {
		t.Error("expected error for nil/nil")
	}
}

func TestOnlyServerErrors(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, true},
		{errors.New("connection refused"), false},
		{&StatusError{StatusCode: 200}, true},
		{&StatusError{StatusCode: 404}, true},
		{&StatusError{StatusCode: 499}, true},
		{&StatusError{StatusCode: 500}, false},
		{&StatusError{StatusCode: 502}, false},
		{&StatusError{StatusCode: 599}, false},
	}
	for _, c := range cases {
		if got := OnlyServerErrors(c.err); got != c.want {
			t.Errorf("OnlyServerErrors(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestRetryableStatuses(t *testing.T) {
	f := RetryableStatuses(429, 502, 503, 504)
	cases := []struct {
		err  error
		want bool
	}{
		{nil, true},
		{&StatusError{StatusCode: 200}, true},
		{&StatusError{StatusCode: 404}, true},
		{&StatusError{StatusCode: 429}, false},
		{&StatusError{StatusCode: 500}, true}, // not in list
		{&StatusError{StatusCode: 502}, false},
		{&StatusError{StatusCode: 503}, false},
		{&StatusError{StatusCode: 504}, false},
		{errors.New("connection error"), false},
	}
	for _, c := range cases {
		if got := f(c.err); got != c.want {
			t.Errorf("f(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestStatusInRange(t *testing.T) {
	f := StatusInRange(500, 599)
	cases := []struct {
		err  error
		want bool
	}{
		{nil, true},
		{&StatusError{StatusCode: 200}, true},
		{&StatusError{StatusCode: 499}, true},
		{&StatusError{StatusCode: 500}, false},
		{&StatusError{StatusCode: 599}, false},
		{&StatusError{StatusCode: 600}, true},
		{errors.New("dial err"), false},
	}
	for _, c := range cases {
		if got := f(c.err); got != c.want {
			t.Errorf("f(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestStatusInRangeNormalizesArguments(t *testing.T) {
	// Calling with min > max should swap, not crash.
	f := StatusInRange(599, 500)
	if f(&StatusError{StatusCode: 500}) {
		t.Error("500 should be in [500, 599]")
	}
}

// TestEndToEndWithCircuitBreaker proves the recipe in the package docstring
// works against an actual httptest server and a real CircuitBreaker.
func TestEndToEndWithCircuitBreaker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	// We return []byte from the wrapped function so the body is closed
	// inside the closure — the breaker never sees an open response.
	cb, err := gobreaker.New[[]byte](context.Background(), gobreaker.Settings{
		Name:         "endpoint",
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
