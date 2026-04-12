# Cross-library benchmarks

This subdirectory is a **separate Go module** so that the comparison
benchmarks can pull in seven competing circuit-breaker libraries
without polluting the main module's dependency graph.

## What we measure

For each library, we run the same five micro-benchmarks against an
in-memory single-process configuration (everyone is on equal footing —
nobody's distributed Store is involved):

1. `BenchmarkExecute_Closed_Success` — happy path, single goroutine
2. `BenchmarkExecute_Closed_Failure` — failure path that does NOT trip
3. `BenchmarkExecute_Open_Reject` — fast rejection (the breaker doing
   its job)
4. `BenchmarkExecute_Closed_Success_Parallel` — happy path, GOMAXPROCS
   goroutines
5. `BenchmarkExecute_Open_Reject_Parallel` — fast rejection under load

Output is normalized to `ns/op`, `B/op`, and `allocs/op`. The
results table is in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md).

## Libraries compared

| Package | Stars | Notes |
|---|---|---|
| `github.com/rafet/gobreaker-redis/v2` (us) | — | This package |
| `github.com/sony/gobreaker` (v1) | 3578 | The classic |
| `github.com/sony/gobreaker/v2` | — | Generics rewrite |
| `github.com/mercari/go-circuitbreaker` | 376 | functional options, context-aware |
| `github.com/cep21/circuit/v4` | 815 | Hystrix-style |
| `github.com/failsafe-go/failsafe-go` | 2193 | All-in-one resilience |
| `github.com/exaring/hoglet` | 18 | low-overhead, sliding window |
| `github.com/rubyist/circuitbreaker` | 1166 | the original |

## Running

```bash
cd benchmarks
go test -run=^$ -bench=. -benchmem -benchtime=2s ./...
```

A 30-second machine warm-up is recommended for stable numbers — the
first run after a long idle is typically 5-15% slower.
