package httpmw

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

func newCB(t *testing.T) *gobreaker.CircuitBreaker[int] {
	t.Helper()
	cb, err := gobreaker.New[int](context.Background(), gobreaker.Settings{
		Name:        t.Name(),
		Timeout:     time.Minute,
		ReadyToOpen: gobreaker.ConsecutiveFailures(3),
	})
	if err != nil {
		t.Fatal(err)
	}
	return cb
}

func handler200(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func handler500(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte("error"))
}

func TestWrap_ClosedPassesThrough(t *testing.T) {
	cb := newCB(t)
	h := Wrap(cb, http.HandlerFunc(handler200))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("body = %q, want ok", rec.Body.String())
	}
}

func TestWrap_OpenReturns503(t *testing.T) {
	cb := newCB(t)
	h := Wrap(cb, http.HandlerFunc(handler500))

	// Trip: 3 failures.
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	}

	// Next request should be rejected.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	ra := rec.Header().Get("Retry-After")
	if ra != "30" {
		t.Errorf("Retry-After = %q, want 30", ra)
	}
}

func TestWrap_CustomOptions(t *testing.T) {
	cb := newCB(t)
	h := Wrap(cb, http.HandlerFunc(handler500),
		WithRetryAfter(60),
		WithStatusCode(http.StatusTooManyRequests),
		WithErrorBody(`{"error":"circuit open"}`),
	)

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "60" {
		t.Errorf("Retry-After = %q, want 60", rec.Header().Get("Retry-After"))
	}
}

func TestWrap_4xxDoesNotTrip(t *testing.T) {
	cb := newCB(t)
	notFound := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	h := Wrap(cb, notFound)

	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("iter %d: status = %d, want 404", i, rec.Code)
		}
	}
	// Should still be closed — 4xx is not a server error.
	state, _ := cb.State(context.Background())
	if state != gobreaker.StateClosed {
		t.Errorf("state after 10x 404 = %v, want closed", state)
	}
}

func TestWrapGroup_PerPathBreakers(t *testing.T) {
	g, err := gobreaker.NewGroup[int](context.Background(), gobreaker.GroupSettings{
		Settings: gobreaker.Settings{
			Name:        "mw",
			ReadyToOpen: gobreaker.ConsecutiveFailures(2),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad" {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})
	h := WrapGroup(g, PathKey, backend)

	// Trip /bad.
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bad", nil))
	}

	// /bad should be open.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bad", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/bad status = %d, want 503", rec.Code)
	}

	// /good should still work.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/good", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/good status = %d, want 200", rec.Code)
	}
}

func TestWrap_ServerErrorFunc(t *testing.T) {
	cb := newCB(t)
	// Treat 429 as server error too.
	h := Wrap(cb, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}), WithServerErrorFunc(func(code int) bool {
		return code >= 500 || code == 429
	}))

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	}

	state, _ := cb.State(context.Background())
	if state != gobreaker.StateOpen {
		t.Errorf("state after 3x 429 = %v, want open", state)
	}
}

func TestStatusRecorder_DefaultsTo200(t *testing.T) {
	inner := httptest.NewRecorder()
	rec := &statusRecorder{ResponseWriter: inner, statusCode: http.StatusOK}
	_, _ = rec.Write([]byte("hello"))
	if rec.statusCode != http.StatusOK {
		t.Errorf("statusCode = %d, want 200", rec.statusCode)
	}
}

func TestPathKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	if got := PathKey(r); got != "/api/users" {
		t.Errorf("PathKey = %q, want /api/users", got)
	}
}

func TestMethodPathKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/api/users", nil)
	if got := MethodPathKey(r); got != "POST /api/users" {
		t.Errorf("MethodPathKey = %q, want POST /api/users", got)
	}
}

// Benchmark the middleware overhead.
func BenchmarkWrap_Closed(b *testing.B) {
	cb, _ := gobreaker.New[int](context.Background(), gobreaker.Settings{Name: "bench-mw"})
	h := Wrap(cb, http.HandlerFunc(handler200))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}

var _ = strconv.Itoa // suppress unused import
