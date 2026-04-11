// Basic example: a single CircuitBreaker backed by an in-memory LocalStore.
//
// This is the smallest possible breaker. It is suitable for tests, local
// development, and any single-process application that does not need
// cross-process state coordination. For multi-process deployments, see
// example/distributed.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

func main() {
	ctx := context.Background()

	cb, err := gobreaker.New[string](ctx, gobreaker.Settings{
		Name:        "demo",
		ReadyToOpen: gobreaker.ConsecutiveFailures(3),
	})
	if err != nil {
		log.Fatal(err)
	}

	// Simulate three failures: the breaker will trip on the third.
	for i := 1; i <= 5; i++ {
		out, err := cb.Execute(ctx, func(ctx context.Context) (string, error) {
			return "", errors.New("upstream is sad")
		})
		switch {
		case errors.Is(err, gobreaker.ErrOpenState):
			fmt.Printf("call %d: rejected (breaker open)\n", i)
		case err != nil:
			fmt.Printf("call %d: failed: %v\n", i, err)
		default:
			fmt.Printf("call %d: %s\n", i, out)
		}
	}

	state, _ := cb.State(ctx)
	counts, _ := cb.Counts(ctx)
	fmt.Printf("\nfinal state: %s\ncounts: %+v\n", state, counts)
}
