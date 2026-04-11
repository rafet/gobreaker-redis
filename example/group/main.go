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
	errUpstream := errors.New("upstream error")
	tenants := []string{"tenant-a", "tenant-b", "premium"}
	for _, tenant := range tenants {
		for i := 0; i < 4; i++ {
			_, err := g.Execute(ctx, tenant, func(_ context.Context) (string, error) {
				return "", errUpstream
			})
			switch {
			case errors.Is(err, gobreaker.ErrOpenState):
				fmt.Printf("%-10s call %d: REJECTED (open)\n", tenant, i+1)
			case errors.Is(err, errUpstream):
				fmt.Printf("%-10s call %d: failed\n", tenant, i+1)
			case err != nil:
				log.Fatalf("%s call %d: unexpected error: %v", tenant, i+1, err)
			}
		}
	}

	fmt.Printf("\nbreakers cached in memory: %d\nnames: %v\n", g.Len(), g.Names())

	for _, tenant := range tenants {
		cb, err := g.Get(ctx, tenant)
		if err != nil {
			log.Fatalf("get breaker for %q: %v", tenant, err)
		}
		state, err := cb.State(ctx)
		if err != nil {
			log.Fatalf("state read for %q: %v", tenant, err)
		}
		fmt.Printf("%-10s -> %s\n", tenant, state)
	}
}
