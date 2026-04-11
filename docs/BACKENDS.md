# Backend compatibility

`respstore` uses the [`redis.UniversalClient`](https://pkg.go.dev/github.com/redis/go-redis/v9#UniversalClient) interface from `redis/go-redis/v9`. Any backend that speaks the Redis protocol (RESP2 or RESP3) and implements the small subset of commands listed below works with no code changes.

## Required commands

The Lua-CAS script in `respstore/lua.go` uses these commands:

| Command | Purpose | Available since |
|---|---|---|
| `EVAL` / `EVALSHA` | Run the CAS script | Redis 2.6, all forks |
| `HGET` | Read the version inside the script | Redis 2.0, all forks |
| `HSET` (multi-pair) | Write the new fields | Redis 4.0+, all forks |
| `PEXPIRE` | Refresh the optional TTL | Redis 2.6, all forks |
| `HGETALL` | Read the full snapshot from Go (`Get`) | Redis 2.0, all forks |

There is no dependency on streams, modules, JSON, search, or anything else. Plain RESP is all that's needed.

## Tested backends

The unit tests run against [miniredis](https://github.com/alicebob/miniredis), which embeds an in-process Lua interpreter. The integration matrix in [`compat/`](../compat) (run via `go test -tags=integration`) verifies the same behavior against real backends.

| Backend | Versions | Notes |
|---|---|---|
| **Redis** | 6.x, 7.x | Reference implementation |
| **Valkey** | 7.x, 8.x | Linux Foundation fork of Redis 7. Drop-in. |
| **KeyDB** | 6.x | Multi-threaded Redis fork. Drop-in. |
| **DragonflyDB** | 1.0+ | Modern Redis-compatible server. Lua via gopher-lua. Drop-in. |
| **AWS ElastiCache** | Redis-compatible engine | Use `NewClusterClient` for cluster mode |
| **AWS MemoryDB** | Redis-compatible | Same as ElastiCache |
| **Upstash Redis** | Latest | Single-instance and global. Use TLS. |
| **Redis Cloud** | Any plan | Standard Redis client |
| **Aiven for Redis** | Any plan | Standard Redis client |

If you use a backend not listed above and find that the CAS script does not run, please open an issue with the server name and version.

## Single instance

```go
client := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "",
    DB:       0,
})
store := respstore.New(client)
```

Works for: local Redis, single-node Valkey, single-node KeyDB, DragonflyDB, single-node Upstash.

## Cluster

```go
client := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{
        "node-1.cluster.local:6379",
        "node-2.cluster.local:6379",
        "node-3.cluster.local:6379",
    },
    Password: os.Getenv("REDIS_PASSWORD"),
})
store := respstore.New(client)
```

Works for: Redis Cluster, Valkey Cluster, KeyDB Cluster, ElastiCache (cluster mode enabled), MemoryDB.

The Lua script uses a single key (the Snapshot HASH) so cluster slot routing is straightforward — no cross-slot operations.

## Sentinel

```go
client := redis.NewFailoverClient(&redis.FailoverOptions{
    MasterName:    "mymaster",
    SentinelAddrs: []string{"sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"},
})
store := respstore.New(client)
```

Works for any Sentinel-managed Redis or KeyDB topology.

## TLS

```go
client := redis.NewClient(&redis.Options{
    Addr: "redis.example.com:6379",
    TLSConfig: &tls.Config{
        ServerName: "redis.example.com",
    },
})
```

This works for Upstash, ElastiCache (in-transit encryption), and any managed Redis with TLS enabled.

## Key namespacing

`respstore` namespaces every key with a prefix that defaults to `gobreaker`. Override it for environment isolation:

```go
store := respstore.New(client,
    respstore.WithKeyPrefix("prod:gobreaker"),
)
```

Two services that should NOT share breaker state must use different prefixes. Two services that SHOULD share state (e.g. multiple replicas of the same service) must use the same prefix and the same breaker `Name`.

## TTL for abandoned breakers

By default, a breaker key lives forever. If you create breakers dynamically (e.g. via `Group` keyed on user-supplied identifiers), set a TTL so abandoned keys age out:

```go
store := respstore.New(client,
    respstore.WithTTL(24 * time.Hour),
)
```

The TTL is refreshed on every successful Update. An active breaker never expires.

## Inspecting state

Snapshots are stored as Redis HASH values. Operators can inspect them with `redis-cli`:

```
$ redis-cli HGETALL gobreaker:user-service
 1) "v"      → version
 2) "47"
 3) "s"      → state (0=closed, 1=half-open, 2=open)
 4) "2"
 5) "g"      → generation
 6) "12"
 7) "gs"     → generation start (unix nano)
 8) "1738012345000000000"
 9) "ex"     → expiry (unix nano, 0 = none)
10) "1738012375000000000"
11) "cr"     → counts.requests
12) "100"
13) "ci"     → counts.in_flights
14) "0"
15) "cts"    → counts.total_successes
16) "60"
17) "ctf"    → counts.total_failures
18) "30"
19) "cte"    → counts.total_exclusions
20) "5"
21) "ccs"    → counts.consecutive_successes
22) "0"
23) "ccf"    → counts.consecutive_failures
24) "10"
```

The field name constants are documented in [`respstore/codec.go`](../respstore/codec.go). Renaming a field is a breaking change.
