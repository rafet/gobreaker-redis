// HTTP example: wrap http.Client.Do with a CircuitBreaker that treats
// 5xx responses as failures and 4xx responses as successes — the
// recommended default for service-to-service calls.
//
// The breaker also excludes context cancellation errors, so a caller
// timing out does not poison the upstream's health metric.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
	"github.com/rafet/gobreaker-redis/v2/httpcb"
)

func main() {
	ctx := context.Background()

	cb, err := gobreaker.New[[]byte](ctx, gobreaker.Settings{
		Name:         "github-api",
		Timeout:      30 * time.Second,
		IsSuccessful: httpcb.OnlyServerErrors,
		IsExcluded:   gobreaker.IgnoreContextErrors,
	})
	if err != nil {
		log.Fatal(err)
	}

	body, err := get(ctx, cb, "https://api.github.com/zen")
	if err != nil {
		if se := httpcb.AsStatusError(err); se != nil {
			fmt.Printf("HTTP %d: %s\n", se.StatusCode, se.Status)
		} else if errors.Is(err, gobreaker.ErrOpenState) {
			fmt.Println("breaker is open — using fallback")
		} else {
			fmt.Printf("error: %v\n", err)
		}
		return
	}
	fmt.Println(string(body))
}

// get wraps http.Get in the breaker. The wrapped function reads (and closes)
// the body itself, returning the bytes through the breaker so the caller
// never has to deal with an unclosed http.Response.
func get(ctx context.Context, cb *gobreaker.CircuitBreaker[[]byte], url string) ([]byte, error) {
	return cb.Execute(ctx, func(ctx context.Context) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
		if err != nil {
			return nil, err
		}
		resp, statusErr := httpcb.Result(http.DefaultClient.Do(req)) //nolint:bodyclose // closed below
		if resp != nil {
			defer resp.Body.Close()
			body, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return nil, readErr
			}
			return body, statusErr
		}
		return nil, statusErr
	})
}
