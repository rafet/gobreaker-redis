# gobreaker-redis

[![Go Reference](https://pkg.go.dev/badge/github.com/rafet/gobreaker-redis/v2.svg)](https://pkg.go.dev/github.com/rafet/gobreaker-redis/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/rafet/gobreaker-redis)](https://goreportcard.com/report/github.com/rafet/gobreaker-redis)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A distributed-first circuit breaker for Go. Built around three principles:

1. **Redis-backed shared state is a first-class concern**, not a bolt-on. The state machine is decoupled from persistence so a single CircuitBreaker behaves identically whether it runs alone or alongside hundreds of replicas sharing a Redis cluster.
2. **Every state transition is customizable.** `ReadyToOpen`, `ReadyToClose`, and `ReadyToReopen` are three separate predicates instead of one ambiguous `MaxRequests` knob. The half-open phase is fully under user control.
3. **Common pitfalls are fixed by design, not by documentation.** Deadlocks in `OnStateChange`, lost counts on transition, ambiguous half-open semantics, in-flight tracking, panic safety, and silent Redis failures are all handled at the type level.

The package is a deliberate alternative to [sony/gobreaker](https://github.com/sony/gobreaker). It draws on the same conceptual model — the canonical Closed/Half-Open/Open state machine from Michael Nygard's *Release It!* — but rebuilds the API around problems the sony issue tracker has been collecting for years (see [DESIGN.md](docs/DESIGN.md) for the receipts).

## Installation

```bash
go get github.com/rafet/gobreaker-redis/v2
```

Requires Go 1.22+.

## Quick start

```go
package main

import (
    "context"
    "log"
    "net/http"
    "time"

    gobreaker "github.com/rafet/gobreaker-redis/v2"
)

func main() {
    ctx := context.Background()

    cb, err := gobreaker.New[*http.Response](ctx, gobreaker.Settings{
        Name:    "user-service",
        Timeout: 30 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }

    req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example.com/users/1", http.NoBody)
    if err != nil {
        log.Fatal(err)
    }

    resp, err := cb.Execute(ctx, func(ctx context.Context) (*http.Response, error) {
        return http.DefaultClient.Do(req.WithContext(ctx))
    })
    if err != nil {
        log.Printf("call failed: %v", err)
        return
    }
    defer resp.Body.Close()
}
```

This gives you a single-process breaker backed by an in-memory `LocalStore`. To turn it into a distributed breaker shared across replicas, swap one line:

```go
import "github.com/rafet/gobreaker-redis/v2/respstore"

store := respstore.New(redis.NewClient(&redis.Options{Addr: "localhost:6379"}))

cb, err := gobreaker.New[*http.Response](ctx, gobreaker.Settings{
    Name:    "user-service",
    Store:   store,    // <-- only change
    Timeout: 30 * time.Second,
})
```

The same code now coordinates state across every process that talks to the same Redis (or Valkey, or KeyDB, or DragonflyDB — see [BACKENDS.md](docs/BACKENDS.md)).

## What's in the box

| Feature | Where |
|---|---|
| Generic `CircuitBreaker[T]` with `context.Context` first | `gobreaker.go` |
| `ReadyToOpen` / `ReadyToClose` / `ReadyToReopen` predicates | `readyto.go`, `settings.go` |
| `Counts` with `InFlights`, `TotalExclusions`, consecutive counters | `counts.go` |
| `IsExcluded` for neutral outcomes (e.g. context cancellation) | `settings.go` |
| Deadlock-safe `OnStateChange(name, from, to, counts)` | `gobreaker.go` |
| Atomic Lua-CAS Redis store, no distributed lock | `respstore/` |
| `LocalStore` for tests and single-process apps | `localstore.go` |
| `Group` for per-key / per-tenant breakers | `group.go` |
| `ExecuteWithFallback` and `OnOpenOnly` helpers | `fallback.go` |
| `httpcb.OnlyServerErrors`, `RetryableStatuses`, `StatusInRange` | `httpcb/` |
| `Observer` interface for metrics/tracing | `observer.go` |
| `OnStoreFailure: FallbackToLocal` for Redis outage survival | `settings.go` |

## Examples

| Example | What it shows |
|---|---|
| [`example/basic`](example/basic) | Smallest possible breaker, in-memory |
| [`example/redis`](example/redis) | Redis-backed breaker, single instance |
| [`example/distributed`](example/distributed) | Two breaker instances sharing Redis state |
| [`example/group`](example/group) | Per-tenant breakers via `Group` |
| [`example/http`](example/http) | HTTP client with `httpcb.OnlyServerErrors` |
| [`example/fallback`](example/fallback) | Cached fallback via `ExecuteWithFallback` |

## Why another circuit breaker?

If you only need single-process breakers and your team already runs sony/gobreaker, **keep using it**. It's well-known, simple, and battle-tested for that use case.

This package exists because the sony issue tracker has accumulated a class of problems that don't have clean solutions inside its design:

| Problem | sony issue | Our answer |
|---|---|---|
| `OnStateChange` deadlocks if you call `cb.Counts()` from inside | [#37](https://github.com/sony/gobreaker/issues/37) (open since 2020) | `OnStateChange` runs without the lock; `cb.Counts()` from inside is safe |
| `Counts` is reset on state change, last-known counts are lost | [#72](https://github.com/sony/gobreaker/issues/72) | `OnStateChange(name, from, to, counts)` receives pre-transition counts |
| `MaxRequests` ambiguity, `ErrTooManyRequests` confusion | [#30](https://github.com/sony/gobreaker/issues/30), [#49](https://github.com/sony/gobreaker/issues/49), [#53](https://github.com/sony/gobreaker/issues/53) | `MaxRequests` removed. `HalfOpenMaxInFlights` (admission cap) is separate from `ReadyToClose` (success threshold) |
| Half-open success criteria not customizable | [#63](https://github.com/sony/gobreaker/issues/63) | `ReadyToClose` and `ReadyToReopen` are first-class predicates |
| Slow upstream requests don't trip the breaker | [#91](https://github.com/sony/gobreaker/issues/91) | `Counts.InFlights` exposed; user predicates can react to in-flight pressure |
| No fallback function | [#22](https://github.com/sony/gobreaker/issues/22) | `ExecuteWithFallback` + `OnOpenOnly` helper |
| No HTTP-specific helpers | [#46](https://github.com/sony/gobreaker/issues/46) | `httpcb` package with `OnlyServerErrors`, `RetryableStatuses`, `StatusInRange` |
| Per-host / per-tenant breakers require user-managed maps | [#43](https://github.com/sony/gobreaker/issues/43), [#19](https://github.com/sony/gobreaker/issues/19) | `Group` with `PerKeySettings` and `KeyToName` |
| Context cancellation poisons counters | [#105](https://github.com/sony/gobreaker/issues/105) | `IsExcluded` + `IgnoreContextErrors` helper |
| Distributed implementation uses redsync (lock-based) | sony/v2/redis | We use Lua-CAS — no lock, single round-trip on uncontended writes |
| Redis outage = silent failure or panic | sony/v2/redis | `OnStoreFailure: FallbackToLocal` keeps the breaker working in-process while Redis recovers |

See [docs/DESIGN.md](docs/DESIGN.md) for the long-form rationale.

## Backend compatibility

`respstore` uses `redis/go-redis/v9`'s `UniversalClient` interface. Anything that speaks the Redis protocol works with no code changes:

- Redis 6+, Redis Cluster, Redis Sentinel
- Valkey 7+ (single + cluster)
- KeyDB 6+ (single + cluster)
- DragonflyDB 1.0+
- AWS ElastiCache, MemoryDB
- Upstash, Redis Cloud, Aiven, ScaleGrid

See [docs/BACKENDS.md](docs/BACKENDS.md) for the matrix and configuration recipes.

## History and migration

This package was originally a Redis-backed circuit breaker built on a fork of sony/gobreaker (Jan 2024). In late 2024 sony introduced its own `DistributedCircuitBreaker`, and in Aug 2025 they shipped a `redis` sub-module on top of redsync. With v2, this library re-emerges as an opinionated alternative: same problem space, different design choices, fewer footguns.

If you used the original `github.com/rafet/gobreaker-redis` (pre-v2), see [docs/MIGRATION.md](docs/MIGRATION.md). v2 is a complete rewrite and is **not** API-compatible.

## License

MIT — see [LICENSE](LICENSE).
