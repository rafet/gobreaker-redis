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
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
	"github.com/rafet/gobreaker-redis/v2/respstore"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()

	store := respstore.New(client, respstore.WithKeyPrefix("dist-demo"))
	ctx := context.Background()

	makeBreaker := func(label string) *gobreaker.CircuitBreaker[string] {
		cb, err := gobreaker.New[string](ctx, gobreaker.Settings{
			Name:        "shared-service",
			Store:       store,
			Timeout:     10 * time.Second,
			ReadyToOpen: gobreaker.ConsecutiveFailures(3),
			OnStateChange: func(name string, from, to gobreaker.State, c gobreaker.Counts) {
				fmt.Printf("[%s] %s: %s -> %s (after %d consec failures)\n",
					label, name, from, to, c.ConsecutiveFailures)
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		return cb
	}

	replicaA := makeBreaker("replica-A")
	replicaB := makeBreaker("replica-B")

	// Replica A sees three failures and trips the shared breaker.
	for i := 0; i < 3; i++ {
		_, _ = replicaA.Execute(ctx, func(ctx context.Context) (string, error) {
			return "", errors.New("upstream blip")
		})
	}

	// Replica B observes the open state without producing any failures.
	state, _ := replicaB.State(ctx)
	fmt.Printf("\nreplica-B observes: %s\n", state)

	// And replica B's next admission attempt is rejected.
	_, err := replicaB.Execute(ctx, func(ctx context.Context) (string, error) {
		return "should not run", nil
	})
	if errors.Is(err, gobreaker.ErrOpenState) {
		fmt.Println("replica-B correctly rejected the request")
	}
}
