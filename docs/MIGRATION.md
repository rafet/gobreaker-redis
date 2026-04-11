# Migrating from v1 to v2

`gobreaker-redis` v2 is a complete rewrite. The original (v1) package was a thin Redis-backed fork of sony/gobreaker that, by the time v2 began, had broken tests, an example that did not compile, and several state-machine bugs. v2 keeps the *idea* — a circuit breaker whose state lives in Redis — and discards the implementation.

If you used v1 in production, **none of your code will compile against v2 without changes**. This document lists the changes you will need to make.

## Module path

```diff
- import gobreaker "github.com/rafet/gobreaker-redis"
+ import gobreaker "github.com/rafet/gobreaker-redis/v2"
```

## Constructor signature

```diff
- cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
-     Name:           "user-service",
-     RedisClient:    redisClient,
-     RedisKeyPrefix: "myapp",
- })

+ store := respstore.New(redisClient, respstore.WithKeyPrefix("myapp"))
+ cb, err := gobreaker.New[*http.Response](ctx, gobreaker.Settings{
+     Name:  "user-service",
+     Store: store,
+ })
+ if err != nil {
+     return err
+ }
```

The constructor now:

- takes a `context.Context`,
- returns an error instead of panicking,
- is generic on the return type of the wrapped function,
- accepts a `Store` field instead of a Redis client and key prefix,
- requires the Redis adapter to be imported from a sub-package.

## Execute signature

```diff
- result, err := cb.Execute(func() (interface{}, error) {
-     return doThing()
- })
- typed := result.(*MyType)

+ typed, err := cb.Execute(ctx, func(ctx context.Context) (*MyType, error) {
+     return doThing(ctx)
+ })
```

`Execute` now:

- takes a `context.Context`,
- forwards the context to the wrapped function,
- returns the typed value (no `interface{}` cast),
- returns the typed zero value on error.

## Settings field renames and removals

| v1 field | v2 equivalent |
|---|---|
| `RedisClient` | (moved to `respstore.New(client, ...)`) |
| `RedisKeyPrefix` | (moved to `respstore.WithKeyPrefix(...)`) |
| `MaxRequests` | **removed**. Split into `HalfOpenMaxInFlights` (admission cap) and `ReadyToClose` (success threshold) |
| `ReadyToTrip` | renamed to `ReadyToOpen` |
| `OnStateChange func(name, from, to)` | now `func(name, from, to, counts Counts)` — counts are pre-transition |
| `IsSuccessful` | unchanged |
| (none) | new: `ReadyToClose`, `ReadyToReopen`, `IsExcluded`, `Observer`, `OnStoreFailure`, `Store` |

## `Counts` struct

The v1 `Counts` had three numeric fields and a back-pointer to the breaker. The v2 `Counts` has seven numeric fields and is a pure value type (no back-pointer):

```go
type Counts struct {
    Requests             uint64
    InFlights            uint64  // NEW
    TotalSuccesses       uint64
    TotalFailures        uint64
    TotalExclusions      uint64  // NEW
    ConsecutiveSuccesses uint64
    ConsecutiveFailures  uint64
}
```

`ReadyToOpen` callbacks that read counters via the v1 accessor methods (`GetConsecutiveFailures()`) need to read the fields directly:

```diff
- ReadyToTrip: func(c gobreaker.Counts) bool { return c.GetConsecutiveFailures() > 5 }
+ ReadyToOpen: gobreaker.ConsecutiveFailures(5)
```

Or, equivalently:

```go
ReadyToOpen: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 }
```

## Error handling

v1 panicked on initialization errors. v2 returns them:

```diff
- cb := gobreaker.NewCircuitBreaker(settings)  // panics if RedisClient is nil
+ cb, err := gobreaker.New[T](ctx, settings)
+ if err != nil { return err }
```

## Redis outage behavior

v1 silently ignored Redis errors and treated every Redis-backed lookup as if it returned the closed state. v2 has explicit policy:

- `OnStoreFailure: gobreaker.FallbackToLocal` (default) — degrade to a local in-memory store while Redis is down. The breaker keeps working in single-process mode and resumes coordination when Redis returns.
- `OnStoreFailure: gobreaker.FailFast` — surface `ErrStoreUnavailable` to callers immediately.

There is no third option that mimics the v1 silent-ignore behavior on purpose: silent failures are how production incidents start.

## State persistence format

The on-wire format changed. v1 stored state across multiple top-level keys (`prefix:name:cb:state`, `prefix:name:cb:counts`, ...). v2 stores everything as a single Redis HASH (`prefix:name`). Existing v1 keys will not be read by v2 — drain them or accept that the breakers start fresh after upgrade.

## Per-host / per-tenant breakers

If you maintained your own `map[string]*CircuitBreaker` in v1, replace it with `gobreaker.NewGroup`:

```diff
- var breakers sync.Map
- func breakerFor(host string) *gobreaker.CircuitBreaker {
-     if cb, ok := breakers.Load(host); ok { return cb.(*gobreaker.CircuitBreaker) }
-     cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{Name: host, ...})
-     breakers.Store(host, cb)
-     return cb
- }

+ group, _ := gobreaker.NewGroup[*http.Response](ctx, gobreaker.GroupSettings{
+     Settings: gobreaker.Settings{Name: "outbound", Store: store},
+ })
+ // resp, err := group.Execute(ctx, host, func(ctx context.Context) (*http.Response, error) { ... })
```

`Group` handles concurrent first-touch correctly, supports per-key overrides, and ages out idle entries via `Delete`.
