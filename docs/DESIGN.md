# Design

This document explains the non-obvious design choices in `gobreaker-redis/v2`. It is the long-form companion to the README.

## Layered architecture

```
                       ┌─────────────────────────┐
   user code  ────────►│   CircuitBreaker[T]     │
                       │  (state machine, pure)  │
                       └────────────┬────────────┘
                                    │ Snapshot
                                    │ UpdateFunc
                                    ▼
                       ┌─────────────────────────┐
                       │       Store interface   │
                       └────┬────────────────┬───┘
                            │                │
                  ┌─────────▼──────┐  ┌──────▼──────┐
                  │   LocalStore   │  │  RespStore  │
                  │  (in-memory)   │  │ (Lua-CAS on │
                  │                │  │  any RESP   │
                  │                │  │  backend)   │
                  └────────────────┘  └─────────────┘
```

The state machine is **pure**: it has no I/O, no clock, no goroutines of its own. Every state transition is expressed as `func(current Snapshot, now time.Time) (Snapshot, error)` — a deterministic transformation. The Store contract serializes this transformation atomically.

This separation has two consequences:

1. **The state machine is the same in single-process and distributed mode.** A single-process breaker uses `LocalStore`, which serializes Updates with a `sync.Mutex`. A distributed breaker uses `RespStore`, which serializes Updates with a Lua script atop CAS. Same predicates, same transitions, same edge cases — only the contention model differs.
2. **Adding a new backend means writing a new Store, not touching the breaker.** Want DynamoDB? Spanner? etcd? Implement `Get` and `Update` and you are done.

## Why three predicates instead of `MaxRequests`?

sony/gobreaker has a `MaxRequests` knob that does double duty: it caps in-flight requests in the half-open state AND defines the success threshold ("close after MaxRequests consecutive successes"). Users have repeatedly found this confusing — see issues [#30](https://github.com/sony/gobreaker/issues/30), [#49](https://github.com/sony/gobreaker/issues/49), [#53](https://github.com/sony/gobreaker/issues/53).

We split the responsibilities:

| Responsibility | Field | Type |
|---|---|---|
| Cap in-flight admissions in half-open | `HalfOpenMaxInFlights` | `uint64` |
| Closed → Open trigger | `ReadyToOpen` | `func(Counts) bool` |
| Half-Open → Closed trigger | `ReadyToClose` | `func(Counts) bool` |
| Half-Open → Open trigger | `ReadyToReopen` | `func(Counts) bool` |

This is the same direction sony's own [v3 draft spec](https://github.com/sony/gobreaker/wiki/Draft-Specification-for-V3) is heading, but v3 is unreleased and we wanted these semantics now.

A user can express the conventional "any failure reopens, single success closes" with the defaults — `ReadyToReopen: Always` and `ReadyToClose: ConsecutiveSuccesses(1)`. They can also build something more nuanced:

```go
ReadyToOpen:   gobreaker.FailureRatio(20, 0.5),         // open after 20 reqs at 50% failure
ReadyToClose:  gobreaker.ConsecutiveSuccesses(5),       // need 5 in a row
ReadyToReopen: gobreaker.FailureRatio(3, 0.34),         // reopen if any of 3 probes fail
```

## Why `Counts` is passed to `OnStateChange`

sony/gobreaker resets `Counts` to zero when the state changes. The `OnStateChange` callback then sees... the freshly zeroed counts. The actual numbers that triggered the transition are gone forever — see issue [#72](https://github.com/sony/gobreaker/issues/72).

We capture the pre-transition `Counts` inside the state machine and forward them to both `Settings.OnStateChange` and `Observer.OnStateChange`. The signature is intentionally larger than sony's:

```go
OnStateChange func(name string, from State, to State, counts Counts)
```

When you observe `closed -> open`, `counts.ConsecutiveFailures` tells you exactly how many in a row tripped the breaker. When you observe `half-open -> open`, `counts.TotalFailures` tells you how many of the probes failed. This is the data you actually want for alerting and dashboards.

## Why `OnStateChange` does not hold the lock

sony/gobreaker invokes `OnStateChange` while still holding its internal mutex. Calling back into `cb.Counts()` from inside the callback deadlocks immediately. Issue [#37](https://github.com/sony/gobreaker/issues/37) has been open since 2020 with a workaround ("spawn a goroutine inside the callback") that nobody likes.

We fix it structurally: `fireStateChanges` is called **after** the Store update returns, with no locks held. Re-entering `cb.State()`, `cb.Counts()`, or even `cb.Execute()` from inside the callback is safe.

The cost is one extra CPU memory allocation per transition (the `[]stateChange` slice). Worth it.

## Why CAS instead of redsync

sony/v2/redis uses [redsync](https://github.com/go-redsync/redsync) (Redlock) to serialize updates. Each update is at minimum **3 round-trips**: lock, read+write, unlock. Lock TTLs must be tuned, and contention patterns are awkward (waiters poll).

`RespStore` uses optimistic concurrency control: a single Lua script reads the current `version`, compares it to the expected value, and writes the new fields atomically if they match. Each successful uncontended update is **1 round-trip** (the Lua script). On contention, the Update retries inside the same call, each retry being one more round-trip.

For circuit-breaker workloads — where updates are mostly uncontended single counter increments on a hot key — CAS wins on both latency and operational simplicity. There is no lock TTL to tune, no risk of orphaned locks, no Redlock controversy.

## Why HASH fields instead of a JSON blob

`RespStore` persists each Snapshot as a Redis HASH with one field per attribute (`v`, `s`, `g`, `cr`, `ci`, ...) instead of a single JSON blob. Two reasons:

1. **Inspectable in production.** An operator with `redis-cli` can `HGETALL gobreaker:user-service` and read the breaker state without parsing JSON. The field names are short but documented in `respstore/codec.go`.
2. **Cheaper updates.** When only `Counts` changes (the common case), we still rewrite the HASH with HSET multi-pair. The wire format is compact: each field is a short ASCII integer.

The downside is that the field schema is part of the on-wire ABI. Renaming a field is a breaking change; we treat the field name constants in `codec.go` as a versioned interface.

## Why `OnStoreFailure: FallbackToLocal` is the default

sony/v2/redis surfaces every Redis error to the caller. If Redis goes away, every `Execute` call returns an error and the protected service is effectively unavailable from the client's perspective — even though the **client** is healthy.

We default to `FallbackToLocal`. When a Store operation fails, the breaker transparently degrades to a per-process in-memory `LocalStore` until Redis recovers. The breaker keeps working; cross-process coordination is paused; the protected service stays reachable. When Redis comes back, the next successful Update writes the local state back to the shared Store.

For workloads where stale or divergent breaker state is more dangerous than rejecting traffic, we provide `OnStoreFailure: FailFast`. But that is an opt-in for a small minority of users.

## Why context cancellation is excluded by default — almost

The package ships `IgnoreContextErrors`, which marks `context.Canceled` and `context.DeadlineExceeded` as exclusions. It is **not** the default `IsExcluded`. Users have to opt in.

Why? Because "the request was cancelled" can mean one of two things:

1. The caller cancelled the request (e.g. an HTTP handler whose context expired). Says nothing about upstream health → exclude.
2. The upstream took longer than its own deadline allowed. **Does** say something about upstream health → count as failure.

Without knowing which one, the safe default is the conservative one (count it). Users who control the context propagation pattern explicitly enable `IgnoreContextErrors` when they know cancellations come from the caller side.

## Why generics

`CircuitBreaker[T]` parameterises on the return type of the wrapped function. This eliminates the `interface{}` cast that sony/gobreaker still requires:

```go
// sony
result, err := cb.Execute(func() (interface{}, error) { return doThing() })
typed := result.(*MyType) // panic if you guess wrong

// us
typed, err := cb.Execute(ctx, func(ctx context.Context) (*MyType, error) { return doThing(ctx) })
```

It also lets us return the typed zero value on error, so callers do not need to write `var zero *MyType` boilerplate.

## What we did NOT add

A few things were considered and consciously left out of v2.0:

- **Latency-aware tripping** (P50/P99 ringbuffer). Listed as a "NICE-to-have" in the original plan; can be added in v2.2 without breaking API. The infrastructure (`InFlights`, `Observer.OnOutcome`) is already in place.
- **Gradual half-open admission ramp**. Same — can be layered on top of `HalfOpenMaxInFlights` later.
- **Background reaper for `Group`**. Lazy reaping has subtle correctness implications and an explicit `Delete` is sufficient for v2.0. The `Group` type is small enough that adding eviction later is not a breaking change.
- **MessagePack / protobuf serialization**. JSON-style integer hash fields are already cheap and inspectable. Binary serialization would buy a few percent off the wire and a lot of opacity. Not worth it for v2.0.
- **Distributed lock for half-open probes**. The half-open phase is already protected by `HalfOpenMaxInFlights` enforced through CAS. A second lock would add round-trips for marginal benefit.

## What we will probably add in v2.1+

- A `metrics/` adapter for Prometheus exposed through the existing `Observer` interface.
- An `otelobserver/` adapter for OpenTelemetry traces and meters.
- Redis Streams-based async state propagation as an alternative to the polling/CAS model (for very-high-cardinality `Group`s).
- Per-key TTL override on the `Group` so per-tenant breakers can age out independently.
