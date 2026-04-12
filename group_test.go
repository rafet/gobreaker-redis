package gobreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestGroup(t *testing.T, settings Settings) *Group[any] {
	t.Helper()
	g, err := NewGroup[any](context.Background(), GroupSettings{Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestGroupRejectsBadTemplate(t *testing.T) {
	_, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Interval: -1},
	})
	if !errors.Is(err, ErrInvalidSettings) {
		t.Errorf("err = %v, want ErrInvalidSettings", err)
	}
}

func TestGroupCreatesBreakerOnDemand(t *testing.T) {
	g := newTestGroup(t, Settings{Name: "outbound"})
	cb, err := g.Get(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if cb == nil {
		t.Fatal("Get returned nil breaker")
	}
	if cb.Name() != "outbound:tenant-a" {
		t.Errorf("Name = %q, want outbound:tenant-a", cb.Name())
	}
	if g.Len() != 1 {
		t.Errorf("Len = %d, want 1", g.Len())
	}
}

func TestGroupCachesBreaker(t *testing.T) {
	g := newTestGroup(t, Settings{Name: "outbound"})
	cb1, _ := g.Get(context.Background(), "k")
	cb2, _ := g.Get(context.Background(), "k")
	if cb1 != cb2 {
		t.Error("Get returned different breakers for the same key")
	}
}

func TestGroupKeyToNameWithoutPrefix(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: ""}, // empty: key is the full name
	})
	// Empty Name: but Validate would fail. The constructor temporarily
	// fills in a probe Name for validation, so empty here is OK.
	if err != nil {
		t.Fatal(err)
	}
	cb, err := g.Get(context.Background(), "raw-key")
	if err != nil {
		t.Fatal(err)
	}
	if cb.Name() != "raw-key" {
		t.Errorf("Name = %q, want raw-key", cb.Name())
	}
}

func TestGroupCustomKeyToName(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "ignored"},
		KeyToName: func(key string) string {
			return "cb:" + key + ":v1"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cb, _ := g.Get(context.Background(), "x")
	if cb.Name() != "cb:x:v1" {
		t.Errorf("Name = %q, want cb:x:v1", cb.Name())
	}
}

func TestGroupPerKeyOverride(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "g", Timeout: 60 * time.Second},
		PerKeySettings: func(key string, base Settings) Settings {
			if key == "fast" {
				base.Timeout = 5 * time.Second
				base.Name = "should-be-ignored" // Group restores it
			}
			return base
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cb, _ := g.Get(context.Background(), "fast")
	if cb.settings.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", cb.settings.Timeout)
	}
	if cb.Name() != "g:fast" {
		t.Errorf("Name = %q, want g:fast (override must be restored)", cb.Name())
	}
}

func TestGroupExecute(t *testing.T) {
	g := newTestGroup(t, Settings{Name: "g"})
	out, err := g.Execute(context.Background(), "k", func(ctx context.Context) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if out != "ok" {
		t.Errorf("out = %v, want ok", out)
	}
}

func TestGroupDelete(t *testing.T) {
	g := newTestGroup(t, Settings{Name: "g"})
	_, _ = g.Get(context.Background(), "k")
	if !g.Delete("k") {
		t.Error("Delete returned false for existing key")
	}
	if g.Delete("k") {
		t.Error("Delete returned true for missing key")
	}
	if g.Len() != 0 {
		t.Errorf("Len = %d, want 0", g.Len())
	}
}

func TestGroupNames(t *testing.T) {
	g := newTestGroup(t, Settings{Name: "g"})
	for _, k := range []string{"a", "b", "c"} {
		_, _ = g.Get(context.Background(), k)
	}
	names := g.Names()
	if len(names) != 3 {
		t.Errorf("len(Names) = %d, want 3", len(names))
	}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	// Names returns derived breaker names, not raw keys.
	for _, want := range []string{"g:a", "g:b", "g:c"} {
		if !got[want] {
			t.Errorf("Names missing %q", want)
		}
	}

	// Keys is a deprecated alias and must return the same set.
	keys := g.Keys() //nolint:staticcheck // intentionally exercising the deprecated alias
	if len(keys) != 3 {
		t.Errorf("len(Keys) = %d, want 3", len(keys))
	}
}

func TestGroupConcurrentGetSerializesCreation(t *testing.T) {
	g := newTestGroup(t, Settings{Name: "g"})
	const goroutines = 50
	var (
		wg      sync.WaitGroup
		breaker *CircuitBreaker[any]
		mu      sync.Mutex
	)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			cb, err := g.Get(context.Background(), "shared")
			if err != nil {
				t.Errorf("Get: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if breaker == nil {
				breaker = cb
			} else if breaker != cb {
				t.Error("two goroutines got different breakers for the same key")
			}
		}()
	}
	wg.Wait()
	if g.Len() != 1 {
		t.Errorf("Len = %d, want 1", g.Len())
	}
}

func TestGroupShareSnapshotAcrossKeys(t *testing.T) {
	// Two distinct keys must NOT see each other's state.
	g := newTestGroup(t, Settings{Name: "g"})
	cbA, _ := g.Get(context.Background(), "a")
	cbB, _ := g.Get(context.Background(), "b")

	for i := 0; i < 5; i++ {
		_, _ = cbA.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, errBoom
		})
	}
	stateA, _ := cbA.State(context.Background())
	stateB, _ := cbB.State(context.Background())
	if stateA != StateOpen {
		t.Errorf("cbA state = %v, want open", stateA)
	}
	if stateB != StateClosed {
		t.Errorf("cbB state = %v, want closed (independent breakers should not bleed)", stateB)
	}
}

func TestGroup_MaxIdle_Reap(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "idle"},
		MaxIdle:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"a", "b", "c"} {
		if _, err := g.Get(context.Background(), k); err != nil {
			t.Fatal(err)
		}
	}
	if g.Len() != 3 {
		t.Fatalf("Len = %d, want 3", g.Len())
	}

	// Wait for entries to become idle.
	time.Sleep(80 * time.Millisecond)

	n := g.Reap()
	if n != 3 {
		t.Errorf("Reap evicted %d, want 3", n)
	}
	if g.Len() != 0 {
		t.Errorf("Len after Reap = %d, want 0", g.Len())
	}
}

func TestGroup_MaxSize_EvictsOldest(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "sz"},
		MaxSize:  2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create entries with slight time separation so ordering is deterministic.
	for _, k := range []string{"a", "b"} {
		if _, err := g.Get(context.Background(), k); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if g.Len() != 2 {
		t.Fatalf("Len = %d, want 2", g.Len())
	}

	// Adding a third key should evict the oldest ("a").
	if _, err := g.Get(context.Background(), "c"); err != nil {
		t.Fatal(err)
	}
	if g.Len() != 2 {
		t.Errorf("Len = %d, want 2 (oldest should have been evicted)", g.Len())
	}

	names := map[string]bool{}
	for _, n := range g.Names() {
		names[n] = true
	}
	if names["sz:a"] {
		t.Error("oldest entry 'sz:a' was not evicted")
	}
	if !names["sz:b"] || !names["sz:c"] {
		t.Errorf("expected sz:b and sz:c to remain, got %v", g.Names())
	}
}

func TestGroup_Reap_NoEvictionIfFresh(t *testing.T) {
	g, err := NewGroup[any](context.Background(), GroupSettings{
		Settings: Settings{Name: "fresh"},
		MaxIdle:  10 * time.Second, // very long idle threshold
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"a", "b", "c"} {
		if _, err := g.Get(context.Background(), k); err != nil {
			t.Fatal(err)
		}
	}

	n := g.Reap()
	if n != 0 {
		t.Errorf("Reap evicted %d, want 0 (entries are fresh)", n)
	}
	if g.Len() != 3 {
		t.Errorf("Len = %d, want 3", g.Len())
	}
}
