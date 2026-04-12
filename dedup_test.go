package gobreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDedup_SingleCaller(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup"})
	d := NewDeduplicator[any](cb)
	result, err := d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
		return "only-one", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "only-one" {
		t.Errorf("result = %v, want only-one", result)
	}
}

func TestDedup_ConcurrentSameKey_SingleExecution(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-concurrent"})
	d := NewDeduplicator[any](cb)

	var calls int64
	gate := make(chan struct{})

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	results := make([]any, goroutines)
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			results[i], errs[i] = d.ExecuteDedup(context.Background(), "shared", func(_ context.Context) (any, error) {
				atomic.AddInt64(&calls, 1)
				<-gate
				return "shared-result", nil
			})
		}()
	}

	// Wait for all goroutines to be waiting.
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()

	got := atomic.LoadInt64(&calls)
	if got != 1 {
		t.Errorf("calls = %d, want 1 (deduplication should coalesce)", got)
	}

	for i := 0; i < goroutines; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: err = %v", i, errs[i])
		}
		if results[i] != "shared-result" {
			t.Errorf("goroutine %d: result = %v, want shared-result", i, results[i])
		}
	}
}

func TestDedup_DifferentKeys_SeparateExecutions(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-diff"})
	d := NewDeduplicator[any](cb)

	var calls int64
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = d.ExecuteDedup(context.Background(), "key-a", func(_ context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			return nil, nil
		})
	}()
	go func() {
		defer wg.Done()
		_, _ = d.ExecuteDedup(context.Background(), "key-b", func(_ context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			return nil, nil
		})
	}()
	wg.Wait()

	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Errorf("calls = %d, want 2 (different keys = separate calls)", got)
	}
}

func TestDedup_SequentialSameKey_FreshCalls(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-seq"})
	d := NewDeduplicator[any](cb)

	var calls int64
	for i := 0; i < 3; i++ {
		_, _ = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
			atomic.AddInt64(&calls, 1)
			return nil, nil
		})
	}
	if got := atomic.LoadInt64(&calls); got != 3 {
		t.Errorf("calls = %d, want 3 (sequential = no dedup)", got)
	}
}

func TestDedup_ErrorShared(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-err"})
	d := NewDeduplicator[any](cb)

	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 2)

	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			_, errs[i] = d.ExecuteDedup(context.Background(), "k", func(_ context.Context) (any, error) {
				time.Sleep(10 * time.Millisecond)
				return nil, errBoom
			})
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, errBoom) {
			t.Errorf("goroutine %d: err = %v, want errBoom", i, err)
		}
	}
}

func TestDedup_Execute_PassThrough(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-pass"})
	d := NewDeduplicator[any](cb)
	result, err := d.Execute(context.Background(), func(_ context.Context) (any, error) {
		return "direct", nil
	})
	if err != nil || result != "direct" {
		t.Errorf("Execute = (%v, %v), want (direct, nil)", result, err)
	}
}

func TestDedup_CB(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "dedup-cb"})
	d := NewDeduplicator[any](cb)
	if d.CB() != cb {
		t.Error("CB() returned different breaker")
	}
}
