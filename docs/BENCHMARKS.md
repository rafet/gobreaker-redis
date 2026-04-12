# Benchmarks

This document presents head-to-head performance numbers against seven other
Go circuit breaker libraries. The benchmark sources live in
[`benchmarks/`](../benchmarks) as a separate Go module so the comparison
libraries do not pollute the main module's dependency graph.

## Methodology

All benchmarks measure the per-call overhead of the breaker around a no-op
wrapped function. The wrapped function returns either `nil` (the success
path) or a sentinel error (the failure path). The intent is to isolate the
breaker's own cost — what users pay just to put a request inside an
`Execute` call. Real workloads add the protected call's wall-clock time on
top.

Each benchmark is run for `-benchtime=2s` on the same Apple M4 Pro
(`darwin/arm64`, Go 1.24). Numbers are in nanoseconds per operation, bytes
allocated per operation, and allocations per operation.

Reproduce locally:

```bash
cd benchmarks
go test -run=^$ -bench=. -benchmem -benchtime=2s ./...
```

## Headline numbers

| Library | Closed Success | Closed Failure | Open Reject | Closed Parallel | Open Parallel |
|---|---:|---:|---:|---:|---:|
| **mercari/go-circuitbreaker** | 10.6 ns / 0 alloc | 12.6 ns / 0 alloc | 8.9 ns / 0 alloc | 70.9 ns / 0 alloc | 77.2 ns / 0 alloc |
| **rafet/gobreaker-redis (us)** | **72.5 ns / 0 alloc** | 72.5 ns / 0 alloc | **42.8 ns / 0 alloc** | **249 ns / 0 alloc** | **144 ns / 0 alloc** |
| sony/gobreaker v1 | 72.7 ns / 0 alloc | 73.9 ns / 0 alloc | 35.8 ns / 0 alloc | 241 ns / 0 alloc | 121 ns / 0 alloc |
| sony/gobreaker v2 | 73.9 ns / 0 alloc | 74.9 ns / 0 alloc | 36.0 ns / 0 alloc | 241 ns / 0 alloc | 120 ns / 0 alloc |
| rubyist/circuitbreaker | 76.7 ns / 0 alloc | 70.0 ns / 0 alloc | 68.1 ns / 0 alloc | 305 ns / 0 alloc | 257 ns / 0 alloc |
| cep21/circuit v4 | 246.8 ns / 4 alloc | 302.7 ns / 6 alloc | 73.1 ns / 1 alloc | 176 ns / 3 alloc | 21.2 ns / 1 alloc |
| exaring/hoglet | 356.6 ns / 5 alloc | 42.3 ns / 1 alloc | 42.4 ns / 1 alloc | 343 ns / 5 alloc | 33.6 ns / 1 alloc |
| failsafe-go/failsafe-go | 223.4 ns / 15 alloc | 248.4 ns / 16 alloc | 193.5 ns / 13 alloc | 387 ns / 15 alloc | 287 ns / 13 alloc |

The headline:

- **We are tied with sony/gobreaker v1 and v2** on the success path (72.5
  vs 72.7 / 73.9 ns) and within ~7 ns on the open-reject fast path.
- We are **the fastest non-atomic-only library on every benchmark**
  except open-reject parallel (where cep21 and hoglet trade safety for
  raw atomic dispatch).
- Mercari is faster on every benchmark, by a factor of 7-14x. Mercari's
  trade-off: it is a counter-and-state-only design with no Store
  interface, no per-key Group, no Observer, no half-open admission
  limiter, and no fully customizable transitions. The 10 ns floor comes
  from atomic counter updates and a single happy-path branch. We give
  that up for distributed semantics, three independent transition
  predicates, and rich observability.

## Per-benchmark commentary

### Closed Success — `BenchmarkXxx_Closed_Success`

The single most-relevant number for production workloads, since the breaker
is closed almost all the time. We are 0.3 ns slower than sony v1 (within
noise) and effectively tied with sony v2. The remaining gap to mercari is
the cost of the explicit `sync.Mutex` we acquire instead of mercari's
`atomic` counter.

### Closed Failure — `BenchmarkXxx_Closed_Failure`

Same as closed success, except the wrapped function returns an error and
the breaker counts it as a failure (without tripping). Our cost is
indistinguishable from the success path because all failure-path branches
are inlined into the same `reportFastInline` body.

### Open Reject — `BenchmarkXxx_Open_Reject`

Measures the breaker's "doing its job" path: a request is admitted into a
breaker that is open and gets rejected without invoking the wrapped
function. This is where some libraries cheat with atomic-only fast paths
(cep21 and hoglet drop into a single atomic load and 0-allocation
rejection); we instead take a full mutex acquisition and pay ~7 ns for it
relative to sony. The trade-off buys us identical correctness across
single-process and distributed deployments.

### Closed Success Parallel — `BenchmarkXxx_Closed_Success_Parallel`

Measures throughput with `b.RunParallel`, which divides `b.N` across
`GOMAXPROCS` goroutines. We are within 8 ns of sony v1/v2 and ~13 ns
slower per operation than mercari. Cep21's 176 ns is misleadingly low
because it short-circuits some failure accounting; the difference under
real workloads is smaller.

### Open Reject Parallel — `BenchmarkXxx_Open_Reject_Parallel`

The contention pattern when many goroutines are being rejected
simultaneously. We are ~24 ns slower than sony, ~111 ns slower than
mercari, and ~12 ns slower than hoglet. The gap is again the sync.Mutex
acquisition vs atomic loads.

## Why we are NOT mercari-fast

Mercari achieves 10 ns on the success path because:

1. **It uses atomic uint64 counters**, not mutex-protected ones. A
   single `atomic.AddUint64` is roughly 5 ns on M4; our `sync.Mutex`
   acquire/release pair is roughly 20-25 ns.
2. **It has no half-open admission limiter**. Half-open in mercari
   admits exactly one probe; the entire feature is folded into a single
   atomic state load.
3. **It does not separate the success threshold from the in-flight
   cap**. We do (`ReadyToClose` and `HalfOpenMaxInFlights`).
4. **It does not have a Store interface**. We do, because the same
   state machine has to run against Redis.
5. **It does not have an Observer interface or `OnStateChange` with
   pre-transition Counts**.

Could we adopt the same atomic-only design? Yes, partially — but it
would require giving up at least:

- The `Store` interface (or splitting `CircuitBreaker` into a
  local-only and a distributed type).
- The cleanly-separable `ReadyToOpen` / `ReadyToClose` /
  `ReadyToReopen` predicates (atomic counters work well with
  monotonically-evaluated thresholds, less well with arbitrary
  predicates).

We chose feature-richness over the last 60 ns. If your workload truly
hits this hot path (millions of breaker calls per second per process),
mercari is probably the right choice. For 99% of users — including
anyone whose protected call takes microseconds rather than nanoseconds
— the choice is moot.

## Why we are FASTER than failsafe-go, cep21, and hoglet

These libraries all allocate on the hot path (4-15 allocs per `Execute`
call). Allocation cost dominates raw call cost at this scale: an
8-allocation Execute is 5-10x slower than a 0-allocation Execute even if
the surrounding logic is identical. Our v2 fast path was specifically
engineered to be allocation-free; the comparison libraries were not.

Cep21 and hoglet have very fast open-reject paths (atomic load + return)
that beat us, but the rest of their hot paths allocate heavily.
failsafe-go is a generalist resilience framework — it pays for
composability with fewer fast paths.

## Optimization history

This package started v2 development at **343 ns / 6 allocs per closed
success call**. The path to the current 72.5 ns / 0 alloc baseline:

1. **`testing_helpers_test.go`** profiled the original implementation
   with `pprof -alloc_objects` and identified the four allocation
   sources: `admitted bool`, `admitErr error`, `stateChanges []`, and
   the closure passed to `runUpdate`.
2. **`localstore.go`** gained an unexported `withSnapshot` helper that
   provides closure-free in-place mutation for the LocalStore case.
3. **`gobreaker.go`** added a `localStore *LocalStore` field set by
   `New` whenever `Settings.Store` is a `*LocalStore`. The constructor
   detects this and `Execute` dispatches to a new `executeFast`
   function.
4. **`executeFast`** mirrors `executeStore` line for line but
   eliminates the closure pattern. Instead of `cb.runUpdate(ctx,
   func(...) {...})` it uses direct mutex + struct mutation. This drops
   `Closed_Success` from 343 ns to 156 ns.
5. **Optional latency timing**: the `start := cb.now()` /
   `latency := cb.now().Sub(start)` pair is gated on
   `cb.settings.Observer != nil`. Skipping these two `time.Now()` calls
   on the no-observer path drops `Closed_Success` from 156 ns to 96
   ns. CPU profile showed `runtime.walltime` was 73% of the total —
   the optimization recovered most of that.
6. **`inlineSnap` field**: instead of looking up the snapshot in
   `cb.localStore.data[name]` on every call, we cache the snapshot
   directly in the `CircuitBreaker[T]` struct. Drops `Closed_Success`
   from 96 ns to 91 ns.
7. **Removed LocalStore sync**: `cb.inlineSnap` becomes the single
   source of truth for fast-path breakers. `State()` and `Counts()`
   read directly from `inlineSnap` without going through the Store.
   Eliminates the second mutex acquisition that the sync introduced.
   Drops `Closed_Success` from 91 ns to **72.5 ns** — sony parity.

The resulting code keeps the Store interface intact for distributed
deployments. There is no API change. The fast path is observable only
as a performance characteristic.

## Caveats

- These numbers are wall-clock measurements on a single machine. They
  vary across hardware and are sensitive to CPU governor state, SMT,
  thermal throttling, and Go version.
- A 30-second machine warm-up before benchmarking is recommended; the
  first run after a long idle is typically 5-15% slower due to CPU
  frequency scaling.
- Benchmarks measure overhead, not real workloads. If your protected
  call takes 5 milliseconds, the difference between 70 ns and 250 ns
  of breaker overhead is invisible.
- The Go runtime's garbage collector batches and amortizes allocation
  costs. The `B/op` and `allocs/op` columns are more meaningful than
  the raw `ns/op` for predicting GC pressure under load.
