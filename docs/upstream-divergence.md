# Upstream divergence

This document records why `gobreaker-redis` is treated as an independent package
and not as a fork that tracks `sony/gobreaker`.

## Origin

- Initial commit (`9d8f072`) was based on `sony/gobreaker` at the v0.x line.
- Last shared commit with upstream: `70f7cbc` (sony/gobreaker, 2023-08-13,
  "Remove unused test variable").
- v2 of this package (`c288bd2`, 2026-04-12) is a complete rewrite, not a merge
  from upstream's v2 line.

## Current state (snapshot 2026-04-27)

- 28 commits ahead of `sony/gobreaker@master`.
- 32 commits behind `sony/gobreaker@master` (last upstream commit `fed8e9e`,
  2026-02-07).
- Total Go LOC in this repo: ~15.3K.

## Why we do not merge upstream

Public APIs are structurally incompatible:

| Concern              | This package                                  | sony/gobreaker v2                       |
|----------------------|-----------------------------------------------|-----------------------------------------|
| Type system          | Generic `CircuitBreaker[T]`                   | Non-generic, `interface{}` returns      |
| Distributed store    | `respstore` — Lua-CAS, lock-free, 1 RTT       | `RedisStore` — redsync mutex, multi-RTT |
| Settings struct      | Three predicates + admission strategies       | Single `MaxRequests` knob               |
| Half-open admission  | `AdmissionStrategy` (linear/exp/step ramp)    | `MaxRequests` counter                   |
| Per-key breakers     | First-class `Group[T]` with TTL reaping       | Not provided                            |
| Auxiliary features   | Hedging, dedup, pipeline, fallback, observers | Not provided                            |

A `git merge upstream/master` would not produce a buildable tree, and
cherry-picking would not gain anything we do not already have.

## Upstream features we would otherwise want — already implemented here

- **Tri-state outcome / `IsExcluded`** (upstream #107, #112) — implemented in
  `gobreaker.go`, `settings.go`, `counts.go` (`OutcomeExclusion`,
  `Counts.TotalExclusions`, `IgnoreContextErrors` helper).
- **`redis.UniversalClient` interface** (upstream #108) — `respstore/store.go`
  uses `redis.UniversalClient` directly, supporting Cluster/Sentinel.
- **Time-based rolling window** (upstream #90, #95) — covered by
  `AdaptiveFailureRateWithWindow` and the bucketed counts implementation.
- **Distributed clock skew handling** (upstream #109) — handled differently in
  the Lua-CAS protocol; see `clockskew_test.go`.

## What we deliberately do not adopt

- `RedisStore` + redsync-based distributed mutex — superseded by the
  lock-free Lua-CAS design in `respstore`.
- The counter/two-step file split refactors (#98, #100) — our layout is
  already factored along different seams.

## Maintenance policy

- We do **not** carry an `upstream` remote. The divergence is intentional and
  permanent; tracking it produces noise without value.
- If a specific upstream fix is wanted, fetch upstream ad hoc, cherry-pick,
  drop the remote again, and note it in `CHANGELOG.md`.
- Atıf is preserved in `LICENSE` and the README "Credits" section.
