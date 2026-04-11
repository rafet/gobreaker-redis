package gobreaker

import (
	"context"
	"sync"
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
}

// Group manages a collection of CircuitBreakers keyed by an opaque string,
// such as a tenant id, hostname, or composite "service:operation" tag.
//
// A Group is safe for concurrent use by multiple goroutines.
type Group[T any] struct {
	template       Settings
	keyToName      func(string) string
	perKeyOverride func(key string, base Settings) Settings

	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker[T]
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
		breakers:       make(map[string]*CircuitBreaker[T]),
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
// Get is safe to call from multiple goroutines and serializes creation so
// that concurrent first-touches do not race.
func (g *Group[T]) Get(ctx context.Context, key string) (*CircuitBreaker[T], error) {
	g.mu.RLock()
	cb, ok := g.breakers[key]
	g.mu.RUnlock()
	if ok {
		return cb, nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	// Double-check after acquiring the write lock — another goroutine
	// may have created the breaker while we were waiting.
	if cb, ok := g.breakers[key]; ok {
		return cb, nil
	}

	settings := g.template
	settings.Name = g.keyToName(key)
	if g.perKeyOverride != nil {
		settings = g.perKeyOverride(key, settings)
		// Restore Name in case the override changed it: keyspace
		// integrity must be enforced by the Group, not by user code.
		settings.Name = g.keyToName(key)
	}

	cb, err := New[T](ctx, settings)
	if err != nil {
		return nil, err
	}
	g.breakers[key] = cb
	return cb, nil
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

// Delete removes the in-memory cache entry for key. The breaker's Snapshot
// in the underlying Store is unaffected: use the Store's TTL or its own
// deletion API to remove the persisted state.
//
// Returns true if an entry was removed, false if no entry existed.
func (g *Group[T]) Delete(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.breakers[key]; ok {
		delete(g.breakers, key)
		return true
	}
	return false
}

// Len returns the number of breakers currently cached in memory. The
// distributed Store may hold more.
func (g *Group[T]) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.breakers)
}

// Keys returns the keys of all breakers currently cached in memory, in
// arbitrary order.
func (g *Group[T]) Keys() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	keys := make([]string, 0, len(g.breakers))
	for k := range g.breakers {
		keys = append(keys, k)
	}
	return keys
}
