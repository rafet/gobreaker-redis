// Group example: a single Group of CircuitBreakers, one per tenant, all
// sharing a single Store and a single set of defaults.
//
// This is the answer to "I need a breaker per host / per tenant / per
// downstream service id" without writing your own map and your own locks.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

func main() {
	ctx := context.Background()

	// One Group serves every tenant. The shared Store is implicit (a
	// LocalStore here, but plug in respstore.New(...) for distributed).
	g, err := gobreaker.NewGroup[string](ctx, gobreaker.GroupSettings{
		Settings: gobreaker.Settings{
			Name:        "outbound",
			Timeout:     30 * time.Second,
			ReadyToOpen: gobreaker.ConsecutiveFailures(3),
		},
		// Optional: customize per-key. Here we give the "premium" tenant
		// a more lenient threshold.
		PerKeySettings: func(key string, base gobreaker.Settings) gobreaker.Settings {
			if key == "premium" {
				base.ReadyToOpen = gobreaker.ConsecutiveFailures(10)
			}
			return base
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Different tenants accumulate independent state.
	tenants := []string{"tenant-a", "tenant-b", "premium"}
	for _, tenant := range tenants {
		for i := 0; i < 4; i++ {
			_, err := g.Execute(ctx, tenant, func(ctx context.Context) (string, error) {
				return "", errors.New("upstream error")
			})
			if errors.Is(err, gobreaker.ErrOpenState) {
				fmt.Printf("%-10s call %d: REJECTED (open)\n", tenant, i+1)
			} else {
				fmt.Printf("%-10s call %d: failed\n", tenant, i+1)
			}
		}
	}

	fmt.Printf("\nbreakers cached in memory: %d\nkeys: %v\n", g.Len(), g.Keys())

	for _, tenant := range tenants {
		cb, _ := g.Get(ctx, tenant)
		state, _ := cb.State(ctx)
		fmt.Printf("%-10s -> %s\n", tenant, state)
	}
}
