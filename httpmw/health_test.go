package httpmw

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

func newHealthCB(t *testing.T, name string) *gobreaker.CircuitBreaker[any] {
	t.Helper()
	cb, err := gobreaker.New[any](context.Background(), gobreaker.Settings{
		Name:        name,
		Timeout:     time.Minute,
		ReadyToOpen: gobreaker.ConsecutiveFailures(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	return cb
}

type healthResp struct {
	Ready    bool              `json:"ready"`
	Breakers map[string]string `json:"breakers"`
}

func TestHealthHandler_AllClosed(t *testing.T) {
	cb1 := newHealthCB(t, "svc-a")
	cb2 := newHealthCB(t, "svc-b")
	h := HealthHandler(cb1, cb2)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	var resp healthResp
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if !resp.Ready {
		t.Error("ready = false, want true")
	}
	if resp.Breakers["svc-a"] != "closed" {
		t.Errorf("svc-a = %q", resp.Breakers["svc-a"])
	}
}

func TestHealthHandler_OneOpen(t *testing.T) {
	cb1 := newHealthCB(t, "healthy")
	cb2 := newHealthCB(t, "unhealthy")
	// Trip cb2.
	for i := 0; i < 2; i++ {
		_, _ = cb2.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, errors.New("fail")
		})
	}
	h := HealthHandler(cb1, cb2)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	var resp healthResp
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Ready {
		t.Error("ready = true, want false")
	}
	if resp.Breakers["unhealthy"] != "open" {
		t.Errorf("unhealthy = %q, want open", resp.Breakers["unhealthy"])
	}
	if resp.Breakers["healthy"] != "closed" {
		t.Errorf("healthy = %q, want closed", resp.Breakers["healthy"])
	}
}

func TestHealthHandler_Empty(t *testing.T) {
	h := HealthHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (no breakers = healthy)", rec.Code)
	}
}

func TestHealthHandler_ContentType(t *testing.T) {
	h := HealthHandler(newHealthCB(t, "x"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}
