# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added (v2.0.0 — complete rewrite)

#### Core
- Generic `CircuitBreaker[T any]` parameterised on the wrapped return type.
- `context.Context` is the first argument to every public method.
- New `Settings` API with three independent transition predicates: `ReadyToOpen`, `ReadyToClose`, `ReadyToReopen`.
- `HalfOpenMaxInFlights` admission cap, separate from the success threshold (was conflated with `MaxRequests` in v1 / sony).
- `IsExcluded` callback for neutral outcomes; `IgnoreContextErrors` helper for the common cancel/timeout case.
- `Counts` extended with `InFlights`, `TotalExclusions`, and `ConsecutiveSuccesses`/`ConsecutiveFailures` as plain fields (no accessor methods, no back-pointer).
- `OnStateChange(name, from, to, counts)` receives the pre-transition `Counts` and is invoked **without** the breaker lock held — calling `cb.Counts()` from inside is safe.
- `OnStoreFailure: FallbackToLocal` policy keeps the breaker working in-process while the shared Store is unreachable. `FailFast` is available for stricter deployments.
- Panic safety: panics in the wrapped function are recorded as failures and re-raised without breaking accounting.
- `Observer` interface for metrics and tracing (`OnRequest`, `OnOutcome`, `OnStateChange`); `NopObserver` no-op base.
- Builder helpers in `readyto.go`: `ConsecutiveFailures`, `ConsecutiveSuccesses`, `FailureRatio`, `Or`, `And`, `Always`, `Never`.

#### Storage
- `Store` interface with explicit `Snapshot` value, version-based optimistic concurrency, and `UpdateFunc` transformation.
- `LocalStore` in-memory implementation, race-tested.
- `respstore.Store` Redis-compatible adapter using `redis/go-redis/v9` and a single Lua-CAS script.
- Key TTL support via `respstore.WithTTL` to garbage-collect abandoned breakers.
- Snapshots persisted as Redis HASH fields (operator-inspectable via `redis-cli HGETALL`).
- Backend-agnostic by construction: works with Redis, Valkey, KeyDB, DragonflyDB, ElastiCache, MemoryDB, Upstash, and any RESP-compatible server. Verified by the `compat/` integration matrix.

#### High-level helpers
- `Group[T]` for per-key / per-tenant breakers, with `KeyToName` and `PerKeySettings` overrides.
- `ExecuteWithFallback` and `OnOpenOnly` for Hystrix-style graceful degradation.
- `httpcb` package: `OnlyServerErrors`, `RetryableStatuses(codes...)`, `StatusInRange(min, max)`, and `Result(resp, err)` adapter for `http.Client.Do`.

#### Tooling
- 90+ unit tests with 90%+ coverage; all tests run under `-race`.
- `compat/` integration suite (`-tags=integration`) covering Redis, Valkey, KeyDB, DragonflyDB.
- `golangci-lint` configuration with strict rules.
- GitHub Actions workflows for build/lint/test/examples/compat across Go 1.22, 1.23, 1.24.

### Removed
- The entire v1 surface area. v1 was structurally broken (tests and example did not compile, state machine had multiple race conditions, generation increment was non-atomic). v2 is a complete rewrite.
- `MaxRequests` field — split into `HalfOpenMaxInFlights` and `ReadyToClose`.
- `Counts.GetXxx()` accessor methods — fields are now read directly.
- `Counts.CB` back-pointer — `Counts` is a pure value type.
- Implicit reliance on `go-redis v6` (deprecated since 2020). v2 uses `redis/go-redis/v9`.

### Migration

See [docs/MIGRATION.md](docs/MIGRATION.md) for the full v1 → v2 migration guide.
