// Package httpmw provides a standard net/http middleware that wraps
// handlers with a gobreaker-redis circuit breaker.
//
// When the breaker is open the middleware responds immediately with
// 503 Service Unavailable and a Retry-After header, without invoking
// the downstream handler. When closed or half-open the request passes
// through normally and the response status code determines the
// breaker outcome.
//
// # Quick start
//
//	cb, _ := gobreaker.New[int](ctx, gobreaker.Settings{Name: "api"})
//	mux.Handle("/api/", httpmw.Wrap(cb, apiHandler))
//
// # Per-path breakers
//
//	group, _ := gobreaker.NewGroup[int](ctx, gobreaker.GroupSettings{...})
//	mux.Handle("/api/", httpmw.WrapGroup(group, httpmw.PathKey, apiHandler))
package httpmw

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// Option configures the middleware behavior.
type Option func(*config)

type config struct {
	retryAfterSeconds int
	errorBody         string
	statusCode        int
	isServerError     func(statusCode int) bool
}

func defaults() config {
	return config{
		retryAfterSeconds: 30,
		errorBody:         "service unavailable",
		statusCode:        http.StatusServiceUnavailable,
		isServerError:     func(code int) bool { return code >= 500 },
	}
}

// WithRetryAfter sets the Retry-After header value in seconds.
func WithRetryAfter(seconds int) Option {
	return func(c *config) { c.retryAfterSeconds = seconds }
}

// WithErrorBody sets the response body written when the breaker is open.
func WithErrorBody(body string) Option {
	return func(c *config) { c.errorBody = body }
}

// WithStatusCode sets the HTTP status code returned when the breaker is
// open. Defaults to 503.
func WithStatusCode(code int) Option {
	return func(c *config) { c.statusCode = code }
}

// WithServerErrorFunc overrides the function that determines which
// response status codes count as breaker failures. The default
// classifies any 5xx as a failure.
func WithServerErrorFunc(fn func(statusCode int) bool) Option {
	return func(c *config) { c.isServerError = fn }
}

// Wrap returns a new http.Handler that routes requests through the
// given CircuitBreaker. The breaker's Execute wraps the downstream
// handler; its outcome is determined by the response status code.
func Wrap(cb *gobreaker.CircuitBreaker[int], next http.Handler, opts ...Option) http.Handler {
	cfg := defaults()
	for _, o := range opts {
		o(&cfg)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := cb.Execute(r.Context(), func(ctx context.Context) (int, error) {
			rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rec, r.WithContext(ctx))
			if cfg.isServerError(rec.statusCode) {
				return rec.statusCode, fmt.Errorf("httpmw: server error %d", rec.statusCode)
			}
			return rec.statusCode, nil
		})
		if err != nil && (errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)) {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", cfg.retryAfterSeconds))
			http.Error(w, cfg.errorBody, cfg.statusCode)
			return
		}
	})
}

// KeyFunc extracts a breaker key from an HTTP request.
type KeyFunc func(r *http.Request) string

// PathKey is a KeyFunc that uses the URL path as the breaker key.
func PathKey(r *http.Request) string { return r.URL.Path }

// MethodPathKey is a KeyFunc that uses "METHOD /path" as the key.
func MethodPathKey(r *http.Request) string { return r.Method + " " + r.URL.Path }

// WrapGroup returns a middleware that uses a different breaker per key
// (extracted by keyFn) from the given Group. This is the per-endpoint
// or per-tenant pattern.
func WrapGroup(g *gobreaker.Group[int], keyFn KeyFunc, next http.Handler, opts ...Option) http.Handler {
	cfg := defaults()
	for _, o := range opts {
		o(&cfg)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := keyFn(r)
		_, err := g.Execute(r.Context(), key, func(ctx context.Context) (int, error) {
			rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rec, r.WithContext(ctx))
			if cfg.isServerError(rec.statusCode) {
				return rec.statusCode, fmt.Errorf("httpmw: server error %d", rec.statusCode)
			}
			return rec.statusCode, nil
		})
		if err != nil && (errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)) {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", cfg.retryAfterSeconds))
			http.Error(w, cfg.errorBody, cfg.statusCode)
			return
		}
	})
}

// statusRecorder intercepts WriteHeader to capture the status code
// without buffering the full response.
type statusRecorder struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.statusCode = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}
