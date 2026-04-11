//go:build integration

// Package compat runs the respstore test suite against real RESP-compatible
// backends instead of miniredis. It is gated by the `integration` build tag
// so it never runs as part of the default `go test ./...` invocation.
//
// # Local execution
//
// Set BACKENDS to a comma-separated list of "name=addr" pairs and run:
//
//	BACKENDS="redis=localhost:6379,valkey=localhost:6380" \
//	    go test -tags=integration ./compat/...
//
// If BACKENDS is unset, the suite probes the four common backends on their
// conventional local ports (Redis 6379, Valkey 6380, KeyDB 6381,
// DragonflyDB 6382) and runs against whichever ones are reachable. This is
// the workflow used during development.
//
// # CI execution
//
// CI runs the suite once per backend with REDIS_ADDR set to the backend's
// address and BACKEND set to the backend's name. See
// .github/workflows/test.yml.
package compat

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
	"github.com/rafet/gobreaker-redis/v2/respstore"
)

// backend describes one RESP-compatible server to run the suite against.
type backend struct {
	name string
	addr string
}

// discoverBackends resolves the list of backends to test, in order:
//
//  1. CI mode: a single REDIS_ADDR + BACKEND env pair (set by GitHub Actions).
//  2. Explicit local mode: a BACKENDS env var with "name=addr,..." format.
//  3. Default local mode: probe the four conventional local ports and skip
//     unreachable ones.
func discoverBackends(t *testing.T) []backend {
	t.Helper()

	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		name := os.Getenv("BACKEND")
		if name == "" {
			name = "redis"
		}
		return []backend{{name: name, addr: addr}}
	}

	if list := os.Getenv("BACKENDS"); list != "" {
		var out []backend
		for _, pair := range strings.Split(list, ",") {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) != 2 {
				t.Fatalf("BACKENDS entry %q is not name=addr", pair)
			}
			out = append(out, backend{name: parts[0], addr: parts[1]})
		}
		return out
	}

	defaults := []backend{
		{"redis", "localhost:6379"},
		{"valkey", "localhost:6380"},
		{"keydb", "localhost:6381"},
		{"dragonfly", "localhost:6382"},
	}
	var reachable []backend
	for _, b := range defaults {
		if isReachable(b.addr) {
			reachable = append(reachable, b)
		}
	}
	if len(reachable) == 0 {
		t.Skip("no backends reachable on conventional local ports; set REDIS_ADDR or BACKENDS to run integration tests")
	}
	return reachable
}

// isReachable performs a quick PING against addr to decide whether the
// backend is up. It is used only when discovering backends.
func isReachable(addr string) bool {
	c := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 500 * time.Millisecond})
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return c.Ping(ctx).Err() == nil
}

// newRealStore connects to one backend and returns a respstore.Store along
// with a cleanup function that wipes the keys this test wrote.
func newRealStore(t *testing.T, b backend) *respstore.Store {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr:        b.addr,
		DialTimeout: 2 * time.Second,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("backend %s at %s unreachable: %v", b.name, b.addr, err)
	}
	prefix := "compat:" + b.name + ":" + sanitize(t.Name())
	store := respstore.New(client, respstore.WithKeyPrefix(prefix))
	t.Cleanup(func() {
		keys, _ := client.Keys(context.Background(), prefix+":*").Result()
		if len(keys) > 0 {
			_ = client.Del(context.Background(), keys...).Err()
		}
		_ = client.Close()
	})
	return store
}

// sanitize converts a Go test name into a Redis-key-safe slug.
func sanitize(name string) string {
	replacer := strings.NewReplacer("/", "-", " ", "-", "#", "-")
	return replacer.Replace(name)
}

// runOnEachBackend runs fn against every discovered backend as a subtest.
// Each subtest gets its own respstore.Store with isolated key prefix.
func runOnEachBackend(t *testing.T, fn func(t *testing.T, store *respstore.Store)) {
	t.Helper()
	for _, b := range discoverBackends(t) {
		b := b
		t.Run(b.name, func(t *testing.T) {
			store := newRealStore(t, b)
			fn(t, store)
		})
	}
}

func TestCompatBasicTrip(t *testing.T) {
	runOnEachBackend(t, func(t *testing.T, store *respstore.Store) {
		cb, err := gobreaker.New[any](context.Background(), gobreaker.Settings{
			Name:    "trip",
			Store:   store,
			Timeout: 30 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}

		errBoom := errors.New("boom")
		for i := 0; i < 5; i++ {
			_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
				return nil, errBoom
			})
		}

		state, err := cb.State(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if state != gobreaker.StateOpen {
			t.Errorf("after 5 failures: state = %v, want open", state)
		}
	})
}

func TestCompatCrossInstance(t *testing.T) {
	runOnEachBackend(t, func(t *testing.T, store *respstore.Store) {
		mk := func() *gobreaker.CircuitBreaker[any] {
			cb, err := gobreaker.New[any](context.Background(), gobreaker.Settings{
				Name:  "shared",
				Store: store,
			})
			if err != nil {
				t.Fatal(err)
			}
			return cb
		}

		a := mk()
		b := mk()

		for i := 0; i < 5; i++ {
			_, _ = a.Execute(context.Background(), func(_ context.Context) (any, error) {
				return nil, errors.New("x")
			})
		}

		state, err := b.State(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if state != gobreaker.StateOpen {
			t.Errorf("instance B observed %v, want open (cross-instance state should be visible)", state)
		}

		// And instance B should reject new requests.
		_, err = b.Execute(context.Background(), func(_ context.Context) (any, error) {
			t.Error("instance B should not invoke req")
			return nil, nil
		})
		if !errors.Is(err, gobreaker.ErrOpenState) {
			t.Errorf("instance B Execute err = %v, want ErrOpenState", err)
		}
	})
}

func TestCompatHalfOpenRecovery(t *testing.T) {
	runOnEachBackend(t, func(t *testing.T, store *respstore.Store) {
		// Use a tiny timeout so the test runs in real wall-clock time
		// without burning seconds.
		cb, err := gobreaker.New[any](context.Background(), gobreaker.Settings{
			Name:    "recover",
			Store:   store,
			Timeout: 200 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Trip.
		for i := 0; i < 5; i++ {
			_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
				return nil, errors.New("x")
			})
		}

		// Wait past the open timeout, then a single successful probe
		// should close the breaker.
		time.Sleep(250 * time.Millisecond)
		_, err = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("recovery probe: %v", err)
		}
		state, _ := cb.State(context.Background())
		if state != gobreaker.StateClosed {
			t.Errorf("after successful probe: state = %v, want closed", state)
		}
	})
}

func TestCompatTTLEnforced(t *testing.T) {
	runOnEachBackend(t, func(t *testing.T, _ *respstore.Store) {
		// We need a fresh store with TTL enabled, not the one passed
		// in (which has no TTL). Re-resolve via discovery.
		bs := discoverBackends(t)
		var b backend
		for _, candidate := range bs {
			if strings.HasPrefix(t.Name(), "TestCompatTTLEnforced/"+candidate.name) ||
				strings.HasSuffix(t.Name(), "/"+candidate.name) {
				b = candidate
				break
			}
		}
		if b.addr == "" {
			b = bs[0]
		}

		client := redis.NewClient(&redis.Options{Addr: b.addr})
		defer func() { _ = client.Close() }()

		prefix := "compat:" + b.name + ":ttl:" + sanitize(t.Name())
		store := respstore.New(client,
			respstore.WithKeyPrefix(prefix),
			respstore.WithTTL(2*time.Second),
		)
		t.Cleanup(func() {
			keys, _ := client.Keys(context.Background(), prefix+":*").Result()
			if len(keys) > 0 {
				_ = client.Del(context.Background(), keys...).Err()
			}
		})

		_, err := gobreaker.New[any](context.Background(), gobreaker.Settings{
			Name:  "ttl-test",
			Store: store,
		})
		if err != nil {
			t.Fatal(err)
		}

		key := store.Key("ttl-test")
		ttl, err := client.TTL(context.Background(), key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if ttl <= 0 || ttl > 2*time.Second {
			t.Errorf("TTL = %v, want in (0, 2s]", ttl)
		}
	})
}

func TestCompatConcurrentUpdates(t *testing.T) {
	runOnEachBackend(t, func(t *testing.T, store *respstore.Store) {
		cb, err := gobreaker.New[any](context.Background(), gobreaker.Settings{
			Name:  "concurrent",
			Store: store,
		})
		if err != nil {
			t.Fatal(err)
		}

		const goroutines = 8
		const perGoroutine = 25
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < perGoroutine; j++ {
					_, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
						return "ok", nil
					})
					if err != nil {
						t.Errorf("Execute: %v", err)
						return
					}
				}
			}()
		}
		wg.Wait()

		c, _ := cb.Counts(context.Background())
		want := uint64(goroutines * perGoroutine)
		if c.TotalSuccesses != want {
			t.Errorf("TotalSuccesses = %d, want %d (CAS atomicity broken — lost updates)", c.TotalSuccesses, want)
		}
		if c.InFlights != 0 {
			t.Errorf("InFlights = %d, want 0", c.InFlights)
		}
	})
}
