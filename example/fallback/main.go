// Fallback example: ExecuteWithFallback returns a cached value when the
// upstream is open. This is the Hystrix-style "graceful degradation"
// pattern that issue sony/gobreaker#22 has been asking about for years.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

type User struct {
	ID   string
	Name string
}

// In-memory cache that the fallback consults.
var cache = map[string]*User{
	"42": {ID: "42", Name: "(cached) Alice"},
}

func fetchFromUpstream(ctx context.Context, id string) (*User, error) {
	return nil, errors.New("upstream timeout")
}

func main() {
	ctx := context.Background()
	cb, err := gobreaker.New[*User](ctx, gobreaker.Settings{
		Name:        "user-service",
		ReadyToOpen: gobreaker.ConsecutiveFailures(2),
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		user, err := cb.ExecuteWithFallback(ctx,
			func(ctx context.Context) (*User, error) { return fetchFromUpstream(ctx, "42") },
			// OnOpenOnly: only fall back when the breaker rejects the
			// request. For real upstream errors during the warm-up
			// phase, propagate the error so the caller can react.
			gobreaker.OnOpenOnly[*User](func(ctx context.Context, _ error) (*User, error) {
				if u, ok := cache["42"]; ok {
					return u, nil
				}
				return nil, errors.New("no cached value")
			}),
		)
		if err != nil {
			fmt.Printf("call %d: error %v\n", i+1, err)
			continue
		}
		fmt.Printf("call %d: %s\n", i+1, user.Name)
	}
}
