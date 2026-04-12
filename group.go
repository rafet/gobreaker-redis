package gobreaker

import (
	"context"
	"sync"
	"time"
)

// GroupSettings configures a Group of CircuitBreakers that share a single
// template Settings, a single Store, and a common naming scheme.
//
// A Group is the answer to the per-key / per-tenant / per-host breaker
// pattern. Instead of constructing one CircuitBreaker per logical target by
// hand, callers ask the Group to do it on demand:
//
//	g, _ := gobreaker.NewGroup[*http.Response](ctx, gobreaker.GroupSettings{
//	    Settings: gobreaker.Settings{
//	        Name:    "outbound",
//	        Timeout: 30 * time.Second,
//	        Store:   redisStore, // shared across all keys
//	    },
//	})
//	resp, err := g.Execute(ctx, "tenant:42:provider-x", func(ctx context.Context) (*http.Response, error) {
//	    return http.DefaultClient.Do(req.WithContext(ctx))
//	})
//
// The Group lazily creates a CircuitBreaker the first time a key is seen,
// caches it for subsequent calls, and reuses it across goroutines. The
// per-key breakers share GroupSettings.Settings.Store, so their state is
// already coordinated across processes when the Store is distributed.
type GroupSettings struct {
	// Settings is the template Settings used to construct each per-key
	// CircuitBreaker. The Name field is treated as a prefix: the
	// per-key breaker name becomes Settings.Name + ":" + key (or just
	// the key, if Name is empty). Override KeyToName for full control.
	Settings Settings

	// KeyToName transforms a user-supplied key into the breaker name
	// used by the Store. If nil, defaults to a function that returns
	// Settings.Name + ":" + key (or just key when Settings.Name is empty).
	//
	// Override this when keys contain characters that should be sanitized
	// before reaching the Store, or when a custom namespacing scheme is
	// required.
	KeyToName func(key string) string

	// PerKeySettings, if non-nil, is called once per key (the first time
	// the key is seen) to produce the final Settings for that key's
	// breaker. The base argument is the template Settings with Name
	// already set. PerKeySettings may freely override any field except
	// Name, which is restored after the call to keep keyspace integrity.
	//
	// This is the hook that lets a Group route different keys to
	// different ReadyToOpen thresholds, IsExcluded rules, or fallbacks
	// without losing the shared Store and KeyToName conventions.
	PerKeySettings func(key string, base Settings) Settings

	// MaxIdle is the maximum duration a cached breaker can remain unused
	// before it becomes eligible for eviction by Reap. A zero value
	// means entries never expire based on idle time.
	MaxIdle time.Duration

	// MaxSize is the maximum number of breakers cached in the Group. When
	// a new breaker is created and the cache exceeds MaxSize, the oldest
	// (least-recently-used) entry is evicted lazily. A zero value means
	// the cache is unbounded.
	MaxSize int
}

// Group manages a collection of CircuitBreakers keyed by an opaque string,
// such as a tenant id, hostname, or composite "service:operation" tag.
//
// A Group is safe for concurrent use by multiple goroutines.
type Group[T any] struct {
	template       Settings
	keyToName      func(string) string
	perKeyOverride func(key string, base Settings) Settings
	maxIdle        time.Duration
	maxSize        int

	mu       sync.RWMutex
	breakers map[string]*groupEntry[T]
}

// groupEntry wraps a CircuitBreaker with metadata for eviction.
type groupEntry[T any] struct {
	cb       *CircuitBreaker[T]
	lastUsed time.Time
}

// NewGroup constructs a Group from the given GroupSettings. The Settings
// embedded in GroupSettings are validated as if they were used directly,
// except that the Name field is allowed to be empty (it acts as a prefix).
func NewGroup[T any](ctx context.Context, gs GroupSettings) (*Group[T], error) {
	// Validate the template by temporarily filling in a non-empty name.
	probe := gs.Settings
	if probe.Name == "" {
		probe.Name = "<group-template>"
	}
	if err := probe.Validate(); err != nil {
		return nil, err
	}

	if gs.Settings.Store == nil {
		gs.Settings.Store = NewLocalStore()
	}

	g := &Group[T]{
		template:       gs.Settings,
		keyToName:      gs.KeyToName,
		perKeyOverride: gs.PerKeySettings,
		maxIdle:        gs.MaxIdle,
		maxSize:        gs.MaxSize,
		breakers:       make(map[string]*groupEntry[T]),
	}
	if g.keyToName == nil {
		g.keyToName = g.defaultKeyToName
	}
	_ = ctx // reserved for future preflight against the Store
	return g, nil
}

// defaultKeyToName builds the per-key breaker name from the template Name
// and the user-supplied key.
func (g *Group[T]) defaultKeyToName(key string) string {
	if g.template.Name == "" {
		return key
	}
	return g.template.Name + ":" + key
}

// Get returns the CircuitBreaker for the given key, creating it if it does
// not already exist. The first call for a new key incurs a single Store
// initialization round-trip; subsequent calls hit the in-memory cache.
//
// The cache is keyed by the *derived* breaker name (the result of
// KeyToName), not by the raw user key. This means two distinct user keys
// that map to the same derived name will share a single breaker — which
// is the right answer because they share Snapshot state in the Store
// anyway. Without this, Group would build two breaker instances for one
// logical breaker, splitting local fallback state and violating the
// "names must be distinct within a process" contract documented on
// Settings.Name.
//
// Get is safe to call from multiple goroutines and serializes creation so
// that concurrent first-touches do not race.
func (g *Group[T]) Get(ctx context.Context, key string) (*CircuitBreaker[T], error) {
	name := g.keyToName(key)

	g.mu.RLock()
	entry, ok := g.breakers[name]
	g.mu.RUnlock()
	if ok {
		// Hot path: update last-used time under write lock.
		g.mu.Lock()
		entry.lastUsed = time.Now()
		g.mu.Unlock()
		return entry.cb, nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	// Double-checked locking: between releasing the read lock and
	// acquiring the write lock, another goroutine may have created
	// the breaker. We MUST re-check the cache here; without this
	// guard, two concurrent first-touches for the same key would
	// produce two distinct CircuitBreaker instances and split local
	// fallback state.
	//
	// This branch is hit only by losers of a creation race, which
	// makes it nearly impossible to exercise deterministically from
	// a Go test (the race window is microseconds wide and depends on
	// the runtime scheduler). The accompanying test
	// TestREG_Group_DoubleCheckedLockingRace constructs a synthetic
	// race using a gatedStore, but coverage tools can still report
	// this branch as 0 because the second goroutine's RLock check
	// observes the cached entry directly (the writer holding the
	// lock blocks readers in sync.RWMutex). The branch is correct
	// and load-bearing nonetheless. See CONTRIBUTING.md for the
	// coverage policy.
	if entry, ok := g.breakers[name]; ok {
		entry.lastUsed = time.Now()
		return entry.cb, nil
	}

	settings := g.template
	settings.Name = name
	if g.perKeyOverride != nil {
		settings = g.perKeyOverride(key, settings)
		// Restore Name in case the override changed it: keyspace
		// integrity must be enforced by the Group, not by user code.
		settings.Name = name
	}

	cb, err := New[T](ctx, settings)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	g.breakers[name] = &groupEntry[T]{cb: cb, lastUsed: now}

	// Lazy eviction: if we exceeded MaxSize, evict the single oldest entry.
	if g.maxSize > 0 && len(g.breakers) > g.maxSize {
		g.evictOldestLocked()
	}

	return cb, nil
}

// evictOldestLocked removes the single least-recently-used entry from the
// breakers map. Caller must hold g.mu for writing.
func (g *Group[T]) evictOldestLocked() {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for k, e := range g.breakers {
		if first || e.lastUsed.Before(oldestTime) {
			oldestKey = k
			oldestTime = e.lastUsed
			first = false
		}
	}
	if !first {
		delete(g.breakers, oldestKey)
	}
}

// Execute is a shorthand for Get followed by CircuitBreaker.Execute. It
// returns errors from both phases without distinguishing them at the type
// level: callers that need to differentiate "could not create breaker" from
// "request rejected by breaker" should use Get explicitly.
func (g *Group[T]) Execute(ctx context.Context, key string, req func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	cb, err := g.Get(ctx, key)
	if err != nil {
		return zero, err
	}
	return cb.Execute(ctx, req)
}

// Delete removes the in-memory cache entry for the breaker that key maps
// to. The breaker's Snapshot in the underlying Store is unaffected: use
// the Store's TTL or its own deletion API to remove the persisted state.
//
// Delete uses the derived name (KeyToName(key)), so any two user keys
// that resolve to the same breaker share a single cache entry and
// therefore a single Delete call.
//
// Returns true if an entry was removed, false if no entry existed.
func (g *Group[T]) Delete(key string) bool {
	name := g.keyToName(key)
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.breakers[name]; ok {
		delete(g.breakers, name)
		return true
	}
	return false
}

// Len returns the number of distinct breakers currently cached in memory.
// Two user keys that map to the same derived name count as one. The
// distributed Store may hold more.
func (g *Group[T]) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.breakers)
}

// Names returns the derived names of all breakers currently cached in
// memory, in arbitrary order. The returned values are the breaker names as
// persisted in the Store, not the raw user keys passed to Get/Execute.
func (g *Group[T]) Names() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	names := make([]string, 0, len(g.breakers))
	for k := range g.breakers {
		names = append(names, k)
	}
	return names
}

// Keys is an alias for Names retained for backwards compatibility within
// the v2 line. New code should call Names.
//
// Deprecated: use Names instead.
func (g *Group[T]) Keys() []string {
	return g.Names()
}

// Reap evicts cached breakers that are no longer needed and returns the
// number of entries removed.
//
// When MaxIdle > 0, any entry whose lastUsed time is older than MaxIdle
// is evicted. When MaxSize > 0, entries exceeding MaxSize are evicted
// in least-recently-used order. Both checks are applied in sequence.
//
// Reap is safe to call from any goroutine. Callers may run it on a
// timer (e.g. via time.Ticker) to bound memory growth.
func (g *Group[T]) Reap() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	evicted := 0

	// Phase 1: evict idle entries.
	if g.maxIdle > 0 {
		cutoff := time.Now().Add(-g.maxIdle)
		for k, e := range g.breakers {
			if e.lastUsed.Before(cutoff) {
				delete(g.breakers, k)
				evicted++
			}
		}
	}

	// Phase 2: evict oldest entries until at or below MaxSize.
	for g.maxSize > 0 && len(g.breakers) > g.maxSize {
		g.evictOldestLocked()
		evicted++
	}

	return evicted
}
