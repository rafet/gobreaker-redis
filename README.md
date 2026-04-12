# gobreaker-redis

[![Go Reference](https://pkg.go.dev/badge/github.com/rafet/gobreaker-redis/v2.svg)](https://pkg.go.dev/github.com/rafet/gobreaker-redis/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/rafet/gobreaker-redis)](https://goreportcard.com/report/github.com/rafet/gobreaker-redis)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A distributed circuit breaker for Go. One line turns a local breaker into a shared one across all your replicas.

```go
cb, _ := gobreaker.New[*http.Response](ctx, gobreaker.Settings{
    Name:    "user-service",
    Store:   respstore.New(redisClient),  // remove this line for single-process
    Timeout: 30 * time.Second,
})

resp, err := cb.Execute(ctx, func(ctx context.Context) (*http.Response, error) {
    return http.DefaultClient.Do(req.WithContext(ctx))
})
```

Works with **Redis, Valkey, KeyDB, DragonflyDB, ElastiCache, MemoryDB, Upstash** — anything that speaks the Redis protocol.

## Install

```bash
go get github.com/rafet/gobreaker-redis/v2
```

## Why this one?

There are many Go circuit breakers. Here's why this one exists:

| | gobreaker-redis | Others |
|---|---|---|
| **Distributed** | Built-in. Add `Store: respstore.New(client)` and your breakers sync across replicas via Lua-CAS (no locks, single round-trip) | Most are single-process only. Sony v2 has Redis support via redsync (lock-based, more round-trips) |
| **Redis goes down?** | `FallbackToLocal` keeps your breaker working in-process until Redis recovers | Errors on every call, or silent failures |
| **Control** | Three separate predicates (`ReadyToOpen`, `ReadyToClose`, `ReadyToReopen`) — you decide exactly when each transition happens | Single `MaxRequests` knob that conflates admission limit with success threshold |
| **Per-tenant** | `Group` gives you one breaker per key with zero boilerplate | Roll your own `sync.Map` |
| **Fallback** | `ExecuteWithFallback` + `OnOpenOnly` | Check errors manually |
| **HTTP** | `httpcb.OnlyServerErrors` — 4xx is the caller's fault, don't trip | Write your own `IsSuccessful` |
| **Fast** | 72 ns/op, 0 allocs — tied with sony/gobreaker | See benchmarks below |

## Quick start

**Single-process** (no Redis needed):

```go
cb, err := gobreaker.New[string](ctx, gobreaker.Settings{
    Name:    "payment-api",
    Timeout: 30 * time.Second,
})
```

**Distributed** (add one line):

```go
store := respstore.New(redis.NewClient(&redis.Options{Addr: "localhost:6379"}))

cb, err := gobreaker.New[string](ctx, gobreaker.Settings{
    Name:    "payment-api",
    Store:   store,
    Timeout: 30 * time.Second,
})
```

**Per-tenant**:

```go
group, _ := gobreaker.NewGroup[string](ctx, gobreaker.GroupSettings{
    Settings: gobreaker.Settings{Name: "outbound", Store: store},
})

result, err := group.Execute(ctx, "tenant-42", myFunc)
```

**With fallback**:

```go
resp, err := cb.ExecuteWithFallback(ctx,
    func(ctx context.Context) (*User, error) { return fetchUser(ctx, id) },
    gobreaker.OnOpenOnly(func(ctx context.Context, _ error) (*User, error) {
        return cache.Get(id), nil
    }),
)
```

## Performance

Zero allocations. Tied with the fastest mutex-based libraries. [Full methodology](docs/BENCHMARKS.md).

| Library | Closed (ns/op) | Open Reject (ns/op) | Parallel (ns/op) | Allocs |
|---|---:|---:|---:|---:|
| mercari/go-circuitbreaker | 12.8 | 9.9 | 68.7 | 0 |
| sony/gobreaker v1 | 75.0 | 44.3 | 233.7 | 0 |
| sony/gobreaker v2 | 76.4 | 37.2 | 250.8 | 0 |
| **gobreaker-redis** | **77.6** | **44.5** | **241.1** | **0** |
| rubyist/circuitbreaker | 79.1 | 70.4 | 320.3 | 0 |
| failsafe-go | 233.6 | 214.2 | 412.8 | 13-16 |
| cep21/circuit v4 | 269.1 | 86.9 | 204.8 | 1-6 |
| exaring/hoglet | 613.1 | 49.9 | 492.8 | 1-5 |

> mercari is 6x faster because it uses lock-free atomic counters and gives up distributed support, per-key grouping, customizable transitions, and observability. We are tied with sony/gobreaker — the ~3ns gap is within measurement noise. For real workloads where the protected call takes microseconds, the difference is invisible.

## Feature matrix

| Feature | gobreaker-redis | sony v2 | mercari | cep21 | failsafe-go |
|---|:---:|:---:|:---:|:---:|:---:|
| Generics (`[T any]`) | ✅ | ✅ | - | - | ✅ |
| `context.Context` | ✅ | - | ✅ | ✅ | ✅ |
| Distributed (Redis) | ✅ | ✅ | - | - | - |
| Multi-backend (Valkey/KeyDB/Dragonfly) | ✅ | - | - | - | - |
| Redis outage fallback | ✅ | - | - | - | - |
| Custom transitions | ✅ | - | - | ✅ | - |
| Latency-aware tripping (P50/P99) | ✅ | - | - | - | - |
| Adaptive thresholds | ✅ | - | - | - | - |
| Gradual half-open ramp | ✅ | - | - | - | - |
| Per-key breakers (`Group`) | ✅ | - | - | ✅ | - |
| Group TTL / eviction | ✅ | - | - | - | - |
| Fallback function | ✅ | - | - | ✅ | ✅ |
| Hedging (speculative execution) | ✅ | - | - | - | ✅ |
| Pipeline (CB + retry + timeout) | ✅ | - | - | - | ✅ |
| Request deduplication | ✅ | - | - | - | - |
| Force open / close / reset | ✅ | - | - | ✅ | - |
| Runtime config update | ✅ | - | - | - | - |
| Observer / metrics | ✅ | - | - | ✅ | ✅ |
| HTTP middleware | ✅ | - | - | - | - |
| HTTP status presets | ✅ | - | - | - | - |
| K8s readiness endpoint | ✅ | - | - | - | - |
| Breaker interface (mockable) | ✅ | - | - | - | - |
| In-flight tracking | ✅ | - | - | ✅ | - |
| Zero alloc hot path | ✅ | ✅ | ✅ | - | - |
| Panic safety | ✅ | ✅ | - | - | - |
| Retry / Bulkhead / Rate limit | - | - | - | - | ✅ |

## Backend compatibility

Any RESP-compatible server works with zero code changes:

Redis 6+ / Cluster / Sentinel | Valkey 7+ | KeyDB 6+ | DragonflyDB 1.0+ | ElastiCache | MemoryDB | Upstash

See [docs/BACKENDS.md](docs/BACKENDS.md) for configuration recipes.

## Examples

| Example | |
|---|---|
| [basic](example/basic) | Simplest breaker, in-memory |
| [redis](example/redis) | Redis-backed, single instance |
| [distributed](example/distributed) | Two replicas sharing state |
| [group](example/group) | Per-tenant breakers |
| [http](example/http) | HTTP client with status classification |
| [fallback](example/fallback) | Cached fallback on open |

## Documentation

- [DESIGN.md](docs/DESIGN.md) — architecture and rationale
- [BENCHMARKS.md](docs/BENCHMARKS.md) — full benchmark methodology
- [BACKENDS.md](docs/BACKENDS.md) — backend compatibility matrix
- [MIGRATION.md](docs/MIGRATION.md) — v1 to v2 migration guide
- [CONTRIBUTING.md](CONTRIBUTING.md) — test discipline and conventions

## License

MIT
