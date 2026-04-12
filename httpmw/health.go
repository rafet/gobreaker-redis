package httpmw

import (
	"context"
	"encoding/json"
	"net/http"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// breakerLike is a minimal interface for health checking. Both
// CircuitBreaker[T] and Group satisfy it for each concrete T.
type breakerLike interface {
	Name() string
	State(ctx context.Context) (gobreaker.State, error)
}

// HealthHandler returns an http.Handler that reports the aggregate
// health of the given circuit breakers as a Kubernetes readiness
// probe. If any breaker is in the open state, the handler responds
// with 503 Service Unavailable; otherwise 200 OK.
//
// The response body is a JSON object with the state of each breaker:
//
//	{"ready":false,"breakers":{"user-service":"open","payment":"closed"}}
//
// Usage:
//
//	http.Handle("/readyz", httpmw.HealthHandler(cb1, cb2, cb3))
func HealthHandler(breakers ...breakerLike) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ready := true
		states := make(map[string]string, len(breakers))

		for _, b := range breakers {
			state, err := b.State(ctx)
			if err != nil {
				states[b.Name()] = "error"
				ready = false
				continue
			}
			states[b.Name()] = state.String()
			if state == gobreaker.StateOpen {
				ready = false
			}
		}

		w.Header().Set("Content-Type", "application/json")
		status := http.StatusOK
		if !ready {
			status = http.StatusServiceUnavailable
		}
		w.WriteHeader(status)

		resp := struct {
			Ready    bool              `json:"ready"`
			Breakers map[string]string `json:"breakers"`
		}{
			Ready:    ready,
			Breakers: states,
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
}
