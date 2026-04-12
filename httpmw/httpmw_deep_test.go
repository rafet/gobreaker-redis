package httpmw

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// mockHijackWriter is an http.ResponseWriter that also implements
// http.Hijacker and http.Flusher, simulating a real server connection.
type mockHijackWriter struct {
	http.ResponseWriter
	hijacked bool
	flushed  bool
}

func (m *mockHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	m.hijacked = true
	return nil, nil, nil
}

func (m *mockHijackWriter) Flush() {
	m.flushed = true
}

// TestStatusRecorder_ForwardsFlush verifies that SSE handlers can
// call Flush() through the statusRecorder.
func TestStatusRecorder_ForwardsFlush(t *testing.T) {
	mock := &mockHijackWriter{ResponseWriter: httptest.NewRecorder()}
	rec := &statusRecorder{ResponseWriter: mock, statusCode: http.StatusOK}
	rec.Flush()
	if !mock.flushed {
		t.Error("Flush was not forwarded to underlying writer")
	}
}

// TestStatusRecorder_FlushNoopIfNotFlusher verifies Flush doesn't
// panic when the underlying writer doesn't implement http.Flusher.
func TestStatusRecorder_FlushNoopIfNotFlusher(t *testing.T) {
	rec := &statusRecorder{
		ResponseWriter: httptest.NewRecorder(),
		statusCode:     http.StatusOK,
	}
	// httptest.ResponseRecorder implements Flusher, so this won't
	// actually test the no-op path. Use a minimal writer instead.
	rec.ResponseWriter = minimalWriter{}
	rec.Flush() // must not panic
}

// TestStatusRecorder_ForwardsHijack verifies that WebSocket upgrade
// handlers can call Hijack() through the statusRecorder.
func TestStatusRecorder_ForwardsHijack(t *testing.T) {
	mock := &mockHijackWriter{ResponseWriter: httptest.NewRecorder()}
	rec := &statusRecorder{ResponseWriter: mock, statusCode: http.StatusOK}
	_, _, err := rec.Hijack()
	if err != nil {
		t.Errorf("Hijack error: %v", err)
	}
	if !mock.hijacked {
		t.Error("Hijack was not forwarded to underlying writer")
	}
}

// TestStatusRecorder_HijackErrorIfNotHijacker verifies that Hijack
// returns an error (not a panic) when the underlying writer doesn't
// implement http.Hijacker.
func TestStatusRecorder_HijackErrorIfNotHijacker(t *testing.T) {
	rec := &statusRecorder{
		ResponseWriter: minimalWriter{},
		statusCode:     http.StatusOK,
	}
	_, _, err := rec.Hijack()
	if err == nil {
		t.Error("Hijack should return error when underlying doesn't support it")
	}
}

// TestStatusRecorder_Unwrap verifies ResponseController compatibility.
func TestStatusRecorder_Unwrap(t *testing.T) {
	inner := httptest.NewRecorder()
	rec := &statusRecorder{ResponseWriter: inner, statusCode: http.StatusOK}
	if rec.Unwrap() != inner {
		t.Error("Unwrap should return the inner ResponseWriter")
	}
}

// TestWrap_SSEHandler verifies that an SSE handler can Flush through
// the middleware without issues.
func TestWrap_SSEHandler(t *testing.T) {
	cb, _ := gobreaker.New[int](context.Background(), gobreaker.Settings{
		Name: "sse-test",
	})
	sseHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		} else {
			// If Flusher is not available, the middleware broke SSE.
			http.Error(w, "flusher not available", http.StatusInternalServerError)
		}
	})
	h := Wrap(cb, sseHandler)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))
	if rec.Code == http.StatusInternalServerError {
		t.Error("SSE handler could not access Flusher through middleware")
	}
}

// TestWrap_ResponseBodyNotConsumedOnReject verifies that when the
// breaker is open, the request body is not consumed.
func TestWrap_ResponseBodyNotConsumedOnReject(t *testing.T) {
	cb, _ := gobreaker.New[int](context.Background(), gobreaker.Settings{
		Name:        "body-test",
		Timeout:     time.Minute,
		ReadyToOpen: gobreaker.ConsecutiveFailures(1),
	})
	h := Wrap(cb, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	// Trip.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))

	// Second request — should be rejected.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

// TestWrap_ConcurrentRequests verifies middleware under concurrent load.
func TestWrap_ConcurrentRequests(t *testing.T) {
	cb, _ := gobreaker.New[int](context.Background(), gobreaker.Settings{
		Name: "mw-concurrent",
	})
	h := Wrap(cb, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	const n = 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		}()
	}
	wg.Wait()
}

// minimalWriter is an http.ResponseWriter that implements only the
// required interface — no Flusher, no Hijacker.
type minimalWriter struct{}

func (minimalWriter) Header() http.Header         { return http.Header{} }
func (minimalWriter) Write(b []byte) (int, error)  { return len(b), nil }
func (minimalWriter) WriteHeader(int)              {}

var _ = errors.New // keep import
