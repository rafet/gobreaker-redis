// Package respstore implements gobreaker.Store on top of any RESP-compatible
// data store: Redis, Valkey, KeyDB, DragonflyDB, ElastiCache, MemoryDB,
// Upstash, and any other backend that speaks the Redis protocol over the
// go-redis/v9 client.
//
// # How it works
//
// Each CircuitBreaker is persisted as a Redis HASH whose fields encode the
// Snapshot. Updates use optimistic concurrency control: a single Lua script
// performs CAS by comparing the in-memory expected version against the
// version stored in the HASH. The script runs atomically, so concurrent
// processes contend safely without an external distributed lock.
//
// This is a deliberate departure from sony/gobreaker/v2/redis, which uses
// redsync (Redlock) to serialize updates. CAS is lighter than locking when
// contention is low, which is the common case for circuit breakers: most
// updates are uncontended single increments. When contention does occur the
// store retries the UpdateFunc inside the same call.
//
// # Backend compatibility
//
// The store accepts any redis.UniversalClient, so the same code works for
// single-instance Redis, Redis Cluster, Sentinel-managed Redis, Valkey,
// KeyDB, and DragonflyDB without changes:
//
//	// Redis or Valkey or KeyDB or DragonflyDB (single instance)
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//
//	// Redis Cluster, Valkey Cluster, KeyDB Cluster
//	client := redis.NewClusterClient(&redis.ClusterOptions{
//	    Addrs: []string{"node1:6379", "node2:6379"},
//	})
//
//	store := respstore.New(client, respstore.WithKeyPrefix("myapp"))
//	cb, _ := gobreaker.New[*http.Response](ctx, gobreaker.Settings{
//	    Name:  "user-service",
//	    Store: store,
//	})
//
// # Key TTL
//
// By default the store does not set a TTL on snapshot keys. Pass
// WithTTL to expire abandoned breakers automatically. The TTL is refreshed
// on every successful Update, so an active breaker never expires.
package respstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// Store is a gobreaker.Store backed by any RESP-compatible server reachable
// through a go-redis UniversalClient.
//
// Store is safe for concurrent use by multiple goroutines and by multiple
// processes sharing the same backing server.
type Store struct {
	client    redis.UniversalClient
	prefix    string
	ttl       time.Duration
	maxRetry  int
	cas       *redis.Script
	now       func() time.Time
	ownClient bool
}

// ErrNilClient is returned by New when the supplied UniversalClient is nil.
// It is a configuration error: a nil client cannot satisfy the Store
// contract because every operation requires a network round-trip.
var ErrNilClient = errors.New("respstore: nil redis client")

// New constructs a Store that talks to the given UniversalClient. The Store
// does not take ownership of the client: the caller is responsible for
// closing it.
//
// New panics if client is nil. This is intentional: a nil client is a
// programming error caught at construction time, and there is no sensible
// fallback. Callers that need to defer the connection decision should pass
// a real client wrapped in a connection retry layer instead.
//
// The default key prefix is "gobreaker"; pass WithKeyPrefix to change it.
// The default CAS retry budget is 100; pass WithMaxRetries to change it.
// The default key TTL is zero (no expiry); pass WithTTL to enable it.
func New(client redis.UniversalClient, opts ...Option) *Store {
	if client == nil {
		panic(ErrNilClient)
	}
	s := &Store{
		client: client,
		prefix: "gobreaker",
		// 100 retries comfortably absorbs bursts of contention from
		// dozens of concurrent breakers updating the same key. Each
		// retry is a single round-trip on a hot key, so the worst-case
		// latency added by the budget is small.
		maxRetry: 100,
		cas:      redis.NewScript(luaCAS),
		now:      time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Option configures a Store at construction time.
type Option func(*Store)

// WithKeyPrefix sets the namespace prefix for all keys written by this
// Store. The full key for a breaker named "user-service" with prefix "myapp"
// is "myapp:user-service".
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) { s.prefix = prefix }
}

// WithTTL sets a TTL that is refreshed on every successful Update. Use this
// to garbage-collect breakers that are no longer in use; pick a value that
// is comfortably larger than your longest expected breaker idle period.
//
// A zero TTL disables expiry (the default).
func WithTTL(ttl time.Duration) Option {
	return func(s *Store) { s.ttl = ttl }
}

// WithMaxRetries bounds the number of CAS retries for a single Update call.
// If the budget is exhausted, Update returns gobreaker.ErrSnapshotConflict.
// The default is 10, which is enough to absorb routine contention without
// becoming a livelock attractor.
func WithMaxRetries(n int) Option {
	if n < 1 {
		n = 1
	}
	return func(s *Store) { s.maxRetry = n }
}

// Key returns the full Redis key used to persist a breaker with the given
// name. Exposed for operators who want to inspect or migrate state via
// redis-cli.
func (s *Store) Key(name string) string {
	return s.prefix + ":" + name
}

// Get returns the current snapshot for name. If the key does not exist,
// Get returns the zero Snapshot and a nil error.
func (s *Store) Get(ctx context.Context, name string) (gobreaker.Snapshot, error) {
	res, err := s.client.HGetAll(ctx, s.Key(name)).Result()
	if err != nil {
		return gobreaker.Snapshot{}, fmt.Errorf("respstore: HGETALL %s: %w", s.Key(name), err)
	}
	return decodeSnapshot(res)
}

// Update implements gobreaker.Store.Update with CAS-based atomicity. fn may
// be invoked multiple times if a concurrent process commits a new snapshot
// between the read and the write; the store retries up to maxRetry times.
func (s *Store) Update(ctx context.Context, name string, fn gobreaker.UpdateFunc) (gobreaker.Snapshot, error) {
	key := s.Key(name)
	ttlMs := int64(s.ttl / time.Millisecond)

	for attempt := 0; attempt < s.maxRetry; attempt++ {
		current, err := s.Get(ctx, name)
		if err != nil {
			return gobreaker.Snapshot{}, err
		}

		next, err := fn(current, s.now())
		if err != nil {
			return gobreaker.Snapshot{}, err
		}

		expectedVersion := fmt.Sprintf("%d", current.Version)
		nextVersion := current.Version + 1
		newVersionStr := fmt.Sprintf("%d", nextVersion)
		ttlStr := fmt.Sprintf("%d", ttlMs)

		fields := encodeSnapshot(next)
		argv := make([]interface{}, 0, 3+len(fields))
		argv = append(argv, expectedVersion, newVersionStr, ttlStr)
		for _, f := range fields {
			argv = append(argv, f)
		}

		raw, err := s.cas.Run(ctx, s.client, []string{key}, argv...).Result()
		if err != nil {
			return gobreaker.Snapshot{}, fmt.Errorf("respstore: CAS script %s: %w", key, err)
		}
		arr, ok := raw.([]interface{})
		if !ok || len(arr) != 2 {
			return gobreaker.Snapshot{}, fmt.Errorf("respstore: unexpected CAS reply %T: %v", raw, raw)
		}
		ok = (toInt(arr[0]) == 1)
		if ok {
			next.Version = nextVersion
			return next, nil
		}
		// CAS conflict — retry from the top with the latest version.
		// We deliberately do not back off: the contention window for a
		// circuit-breaker update is microseconds and a tight retry
		// minimizes admission latency.
	}
	return gobreaker.Snapshot{}, fmt.Errorf("%w after %d attempts on %s", gobreaker.ErrSnapshotConflict, s.maxRetry, key)
}

// Close releases resources held by the Store. It does NOT close the
// underlying client, which is owned by the caller.
func (s *Store) Close() error {
	// Nothing to release in the default constructor. NewWithAddress takes
	// ownership of the client and overrides this.
	if s.ownClient {
		return s.client.Close()
	}
	return nil
}

// NewWithAddress is a convenience constructor that builds a single-instance
// go-redis client for the given address and constructs a Store that owns
// the client. Calling Close on the returned Store closes the client.
//
// For Cluster, Sentinel, or any production deployment, build the client
// explicitly and pass it to New so you can configure pool size, dial
// timeout, TLS, and friends.
func NewWithAddress(addr string, opts ...Option) (*Store, error) {
	if addr == "" {
		return nil, errors.New("respstore: address is required")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	s := New(client, opts...)
	s.ownClient = true
	return s, nil
}

// toInt coerces a Lua script return value (which go-redis decodes as int64
// or, on some servers, as a string) into an int.
func toInt(v interface{}) int {
	switch x := v.(type) {
	case int64:
		return int(x)
	case int:
		return x
	case string:
		if x == "1" {
			return 1
		}
		return 0
	}
	return 0
}
