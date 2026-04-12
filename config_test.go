package gobreaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUpdateSettings_ChangesTimeout(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "cfg", Timeout: time.Minute})
	cb.UpdateSettings(func(s *Settings) {
		s.Timeout = 5 * time.Second
	})
	if cb.settings.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", cb.settings.Timeout)
	}
}

func TestUpdateSettings_ChangesReadyToOpen(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{
		Name:        "cfg-open",
		ReadyToOpen: ConsecutiveFailures(100), // very lenient
	})
	// 5 failures should not trip.
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	if stateOf(t, cb) != StateClosed {
		t.Fatal("should be closed with lenient threshold")
	}

	// Tighten threshold.
	cb.UpdateSettings(func(s *Settings) {
		s.ReadyToOpen = ConsecutiveFailures(1)
	})

	// Next failure should trip.
	_ = failBreaker(t, cb)
	if stateOf(t, cb) != StateOpen {
		t.Error("should be open after tightening threshold")
	}
}

func TestUpdateSettings_PreservesName(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "preserve"})
	cb.UpdateSettings(func(s *Settings) {
		s.Name = "hacked" // should be overwritten
	})
	if cb.Name() != "preserve" {
		t.Errorf("Name = %q, want preserve", cb.Name())
	}
}

func TestUpdateSettings_PreservesStore(t *testing.T) {
	store := NewLocalStore()
	cb, err := New[any](context.Background(), Settings{
		Name:  "preserve-store",
		Store: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.UpdateSettings(func(s *Settings) {
		s.Store = nil // should be overwritten
	})
	if cb.settings.Store != store {
		t.Error("Store was replaced; should be preserved")
	}
}

func TestUpdateSettings_DoesNotResetState(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "no-reset"})
	for i := 0; i < 3; i++ {
		_ = failBreaker(t, cb)
	}
	c, _ := cb.Counts(context.Background())
	if c.ConsecutiveFailures != 3 {
		t.Fatalf("setup: ConsecutiveFailures = %d", c.ConsecutiveFailures)
	}

	cb.UpdateSettings(func(s *Settings) {
		s.Timeout = 99 * time.Second
	})

	c, _ = cb.Counts(context.Background())
	if c.ConsecutiveFailures != 3 {
		t.Errorf("ConsecutiveFailures = %d after UpdateSettings, want 3 (preserved)", c.ConsecutiveFailures)
	}
}

func TestUpdateSettings_OnOpenBreaker(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "open-update"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	if stateOf(t, cb) != StateOpen {
		t.Fatal("setup")
	}

	cb.UpdateSettings(func(s *Settings) {
		s.Timeout = time.Millisecond
	})
	if cb.settings.Timeout != time.Millisecond {
		t.Errorf("Timeout = %v, want 1ms", cb.settings.Timeout)
	}

	// Breaker is still open (state not reset).
	if stateOf(t, cb) != StateOpen {
		t.Error("state should still be open after settings update")
	}
}

func TestUpdateSettings_ConcurrentSafe(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "concurrent-cfg"})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			cb.UpdateSettings(func(s *Settings) {
				s.Timeout = time.Duration(i) * time.Millisecond
			})
		}
	}()
	for i := 0; i < 1000; i++ {
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, nil
		})
	}
	<-done
}

func TestUpdateSettings_NilPredicateWarning(t *testing.T) {
	// Setting a predicate to nil won't panic here — only on the
	// next Execute. This test documents the behavior.
	cb, _ := newTestBreaker(t, Settings{Name: "nil-pred"})
	cb.UpdateSettings(func(s *Settings) {
		s.ReadyToOpen = nil
	})
	// Verify the setting was applied (even though it's dangerous).
	if cb.settings.ReadyToOpen != nil {
		t.Error("ReadyToOpen should be nil after update")
	}
}

func TestUpdateSettings_ChangesIsExcluded(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "excl-update"})

	// Default: context.Canceled counts as failure.
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, context.Canceled
	})
	c, _ := cb.Counts(context.Background())
	if c.TotalFailures != 1 {
		t.Fatalf("TotalFailures = %d, want 1", c.TotalFailures)
	}

	_ = cb.Reset(context.Background())

	// Switch to excluding context errors.
	cb.UpdateSettings(func(s *Settings) {
		s.IsExcluded = IgnoreContextErrors
	})

	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, context.Canceled
	})
	c, _ = cb.Counts(context.Background())
	if c.TotalFailures != 0 {
		t.Errorf("TotalFailures = %d after enabling IsExcluded, want 0", c.TotalFailures)
	}
	if c.TotalExclusions != 1 {
		t.Errorf("TotalExclusions = %d, want 1", c.TotalExclusions)
	}
}

func TestUpdateSettings_WithRespStore(t *testing.T) {
	// This test exercises the generic (non-fast) path by using a
	// failingStore with FallbackToLocal so we don't need real Redis.
	cb, err := New[any](context.Background(), Settings{
		Name:           "store-cfg",
		Store:          failingStore{},
		OnStoreFailure: FallbackToLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.UpdateSettings(func(s *Settings) {
		s.Timeout = 42 * time.Second
	})
	if cb.settings.Timeout != 42*time.Second {
		t.Errorf("Timeout = %v, want 42s", cb.settings.Timeout)
	}
}

var _ = errors.New // keep import
