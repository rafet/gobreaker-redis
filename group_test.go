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

func TestGroupKeys(t *testing.T) {
	g := newTestGroup(t, Settings{Name: "g"})
	for _, k := range []string{"a", "b", "c"} {
		_, _ = g.Get(context.Background(), k)
	}
	keys := g.Keys()
	if len(keys) != 3 {
		t.Errorf("len(Keys) = %d, want 3", len(keys))
	}
	got := map[string]bool{}
	for _, k := range keys {
		got[k] = true
	}
	for _, k := range []string{"a", "b", "c"} {
		if !got[k] {
			t.Errorf("Keys missing %q", k)
		}
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
