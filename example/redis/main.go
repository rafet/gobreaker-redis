// Redis example: a single CircuitBreaker backed by RespStore against a
// real Redis server (or any RESP-compatible alternative: Valkey, KeyDB,
// DragonflyDB, ElastiCache, MemoryDB, Upstash).
//
// Set REDIS_ADDR to point at your server (default: localhost:6379).
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
	store := respstore.New(client,
		respstore.WithKeyPrefix("myapp"),
		respstore.WithTTL(24*time.Hour), // expire abandoned breakers
	)

	ctx := context.Background()
	cb, err := gobreaker.New[string](ctx, gobreaker.Settings{
		Name:        "user-service",
		Store:       store,
		Timeout:     30 * time.Second,
		ReadyToOpen: gobreaker.ConsecutiveFailures(3),
	})
	if err != nil {
		_ = client.Close()
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	for i := 1; i <= 5; i++ {
		_, err := cb.Execute(ctx, func(ctx context.Context) (string, error) {
			return "", errors.New("simulated failure")
		})
		if errors.Is(err, gobreaker.ErrOpenState) {
			fmt.Printf("call %d: rejected (breaker open) — visible across all processes sharing %q\n", i, store.Key("user-service"))
		} else {
			fmt.Printf("call %d: %v\n", i, err)
		}
	}

	state, _ := cb.State(ctx)
	fmt.Printf("\nstate persisted in Redis under %q: %s\n", store.Key("user-service"), state)
	fmt.Println("Inspect with: redis-cli HGETALL " + store.Key("user-service"))
}
