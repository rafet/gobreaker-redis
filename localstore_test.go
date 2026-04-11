package gobreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLocalStoreGetEmpty(t *testing.T) {
	s := NewLocalStore()
	snap, err := s.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Get on missing key returned error: %v", err)
	}
	if !snap.IsZero() {
		t.Errorf("Get on missing key returned %+v, want zero", snap)
	}
}

func TestLocalStoreUpdateInitializes(t *testing.T) {
	s := NewLocalStore()
	snap, err := s.Update(context.Background(), "k", func(current Snapshot, now time.Time) (Snapshot, error) {
		if !current.IsZero() {
			t.Errorf("expected zero current, got %+v", current)
		}
		return Snapshot{State: StateClosed, Generation: 1, GenerationStart: now}, nil
	})
	if err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if snap.Version != 1 {
		t.Errorf("Version = %d, want 1", snap.Version)
	}
	if snap.State != StateClosed {
		t.Errorf("State = %v, want closed", snap.State)
	}
}

func TestLocalStoreUpdateIncrementsVersion(t *testing.T) {
	s := NewLocalStore()
	noop := func(c Snapshot, now time.Time) (Snapshot, error) { return c, nil }
	for i := uint64(1); i <= 5; i++ {
		snap, err := s.Update(context.Background(), "k", noop)
		if err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
		if snap.Version != i {
			t.Errorf("Update %d: Version = %d, want %d", i, snap.Version, i)
		}
	}
}

func TestLocalStoreUpdatePropagatesError(t *testing.T) {
	s := NewLocalStore()
	want := errors.New("nope")
	_, err := s.Update(context.Background(), "k", func(c Snapshot, now time.Time) (Snapshot, error) {
		return Snapshot{}, want
	})
	if !errors.Is(err, want) {
		t.Errorf("error = %v, want %v", err, want)
	}
	// Failed update must not increment version: a subsequent successful
	// update should produce Version=1.
	snap, err := s.Update(context.Background(), "k", func(c Snapshot, now time.Time) (Snapshot, error) {
		return c, nil
	})
	if err != nil {
		t.Fatalf("second Update: %v", err)
	}
	if snap.Version != 1 {
		t.Errorf("Version after failed+success = %d, want 1", snap.Version)
	}
}

func TestLocalStoreConcurrent(t *testing.T) {
	s := NewLocalStore()
	const goroutines = 50
	const perGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				_, err := s.Update(context.Background(), "k", func(c Snapshot, now time.Time) (Snapshot, error) {
					c.Counts.Requests++
					return c, nil
				})
				if err != nil {
					t.Errorf("Update: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	snap, err := s.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := uint64(goroutines * perGoroutine)
	if snap.Counts.Requests != want {
		t.Errorf("Requests = %d, want %d (lost updates indicate broken atomicity)", snap.Counts.Requests, want)
	}
	if snap.Version != want {
		t.Errorf("Version = %d, want %d", snap.Version, want)
	}
}

func TestLocalStoreClose(t *testing.T) {
	s := NewLocalStore()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestLocalStoreClock(t *testing.T) {
	s := NewLocalStore()
	frozen := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	s.setClock(func() time.Time { return frozen })
	_, err := s.Update(context.Background(), "k", func(c Snapshot, now time.Time) (Snapshot, error) {
		if !now.Equal(frozen) {
			t.Errorf("clock = %v, want %v", now, frozen)
		}
		return c, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
