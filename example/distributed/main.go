// Distributed example: two CircuitBreaker instances in the same process,
// each backed by the SAME RespStore. They model two replicas of a service
// that share breaker state through Redis.
//
// In real life the two instances would live in two separate processes (or
// pods, or machines). The Store guarantees that whatever state one instance
// observes, the other one will see on its next Update.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
	"github.com/rafet/gobreaker-redis/v2/respstore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()

	store := respstore.New(client, respstore.WithKeyPrefix("dist-demo"))
	ctx := context.Background()

	makeBreaker := func(label string) (*gobreaker.CircuitBreaker[string], error) {
		return gobreaker.New[string](ctx, gobreaker.Settings{
			Name:        "shared-service",
			Store:       store,
			Timeout:     10 * time.Second,
			ReadyToOpen: gobreaker.ConsecutiveFailures(3),
			OnStateChange: func(name string, from, to gobreaker.State, c gobreaker.Counts) {
				fmt.Printf("[%s] %s: %s -> %s (after %d consec failures)\n",
					label, name, from, to, c.ConsecutiveFailures)
			},
		})
	}

	replicaA, err := makeBreaker("replica-A")
	if err != nil {
		return fmt.Errorf("replica-A init: %w", err)
	}
	replicaB, err := makeBreaker("replica-B")
	if err != nil {
		return fmt.Errorf("replica-B init: %w", err)
	}

	// Replica A sees three failures and trips the shared breaker. We
	// expect each Execute call to return errBlip until the breaker
	// trips, after which it returns ErrOpenState. Anything else is a
	// real failure (e.g. the store is unreachable) and should abort
	// the demo.
	errBlip := errors.New("upstream blip")
	for i := 0; i < 3; i++ {
		_, err := replicaA.Execute(ctx, func(_ context.Context) (string, error) {
			return "", errBlip
		})
		if err != nil && !errors.Is(err, errBlip) && !errors.Is(err, gobreaker.ErrOpenState) {
			return fmt.Errorf("replica-A execute (iter %d): %w", i, err)
		}
	}

	// Replica B observes the open state without producing any failures.
	state, err := replicaB.State(ctx)
	if err != nil {
		return fmt.Errorf("replica-B state read: %w", err)
	}
	fmt.Printf("\nreplica-B observes: %s\n", state)

	// And replica B's next admission attempt is rejected.
	_, err = replicaB.Execute(ctx, func(_ context.Context) (string, error) {
		// We do not panic here because that would skip every
		// deferred close. Instead we return a sentinel that the
		// caller can match.
		return "", errors.New("replica-B Execute body should not run while open")
	})
	switch {
	case errors.Is(err, gobreaker.ErrOpenState):
		fmt.Println("replica-B correctly rejected the request")
		return nil
	case err != nil:
		return fmt.Errorf("replica-B execute: %w", err)
	default:
		return errors.New("replica-B execute returned without error — breaker did not reject")
	}
}
