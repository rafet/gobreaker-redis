package gobreaker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDedup_KeyRemovedAfterCompletion(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-cleanup"})
	d := NewDeduplicator[any](cb)
	_, _ = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
		return nil, nil
	})
	// After completion, internal map should be clean.
	d.mu.Lock()
	n := len(d.in)
	d.mu.Unlock()
	if n != 0 {
		t.Errorf("in-flight map has %d entries after completion, want 0", n)
	}
}

func TestDedup_ObserverSeesOneExecution(t *testing.T) {
	obs := &recordingObserver{}
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-obs", Observer: obs})
	d := NewDeduplicator[any](cb)

	gate := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			_, _ = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
				<-gate
				return nil, nil
			})
		}()
	}
	time.Sleep(20 * time.Millisecond) // let goroutines settle
	close(gate)
	wg.Wait()

	_, outs, _ := obs.snapshot()
	// Observer should see exactly 1 outcome (the single real execution).
	if len(outs) != 1 {
		t.Errorf("outcomes = %d, want 1 (dedup should produce single execution)", len(outs))
	}
}

func TestDedup_BreakerOpen_AllCallersGetError(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-open"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	d := NewDeduplicator[any](cb)
	var wg sync.WaitGroup
	const n = 10
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
				t.Error("should not run when open")
				return nil, nil
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			t.Errorf("goroutine %d: err = nil, want ErrOpenState", i)
		}
	}
}

func TestDedup_HeavyConcurrency(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-heavy"})
	d := NewDeduplicator[any](cb)
	var calls int64
	var wg sync.WaitGroup
	const goroutines = 100
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = d.ExecuteDedup(context.Background(), "hot", func(_ context.Context) (any, error) {
					atomic.AddInt64(&calls, 1)
					return nil, nil
				})
			}
		}()
	}
	wg.Wait()
	got := atomic.LoadInt64(&calls)
	// With dedup, calls should be << goroutines*100 (10000).
	// Without dedup it would be exactly 10000.
	if got >= 10000 {
		t.Errorf("calls = %d, dedup should reduce this significantly from 10000", got)
	}
	t.Logf("dedup reduced calls: %d / 10000 (%.1f%% reduction)", got, (1-float64(got)/10000)*100)
}
