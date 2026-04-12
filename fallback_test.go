package gobreaker

import (
	"context"
	"errors"
	"testing"
)

func TestExecuteWithFallbackHappyPath(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fb-ok"})
	out, err := cb.ExecuteWithFallback(
		context.Background(),
		func(ctx context.Context) (any, error) { return "primary", nil },
		func(ctx context.Context, _ error) (any, error) { return "fallback", nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if out != "primary" {
		t.Errorf("out = %v, want primary", out)
	}
}

func TestExecuteWithFallbackOnRequestError(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fb-err"})
	out, err := cb.ExecuteWithFallback(
		context.Background(),
		func(ctx context.Context) (any, error) { return nil, errBoom },
		func(ctx context.Context, _ error) (any, error) { return "fallback", nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if out != "fallback" {
		t.Errorf("out = %v, want fallback", out)
	}
}

func TestExecuteWithFallbackOnOpenState(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fb-open"})
	for i := 0; i < 5; i++ {
		_ = failBreaker(t, cb)
	}
	out, err := cb.ExecuteWithFallback(
		context.Background(),
		func(ctx context.Context) (any, error) {
			t.Error("primary should not run when open")
			return nil, nil
		},
		func(ctx context.Context, e error) (any, error) {
			if !errors.Is(e, ErrOpenState) {
				t.Errorf("fallback received %v, want ErrOpenState", e)
			}
			return "from-cache", nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if out != "from-cache" {
		t.Errorf("out = %v, want from-cache", out)
	}
}

func TestOnOpenOnlyPropagatesOtherErrors(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "open-only"})
	fallbackCalls := 0
	wrap := OnOpenOnly[any](func(ctx context.Context, _ error) (any, error) {
		fallbackCalls++
		return "fallback", nil
	})
	// Real request error: should propagate, fallback NOT called.
	_, err := cb.ExecuteWithFallback(
		context.Background(),
		func(ctx context.Context) (any, error) { return nil, errBoom },
		wrap,
	)
	if !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want errBoom", err)
	}
	if fallbackCalls != 0 {
		t.Errorf("fallback called %d times, want 0 (real error should propagate)", fallbackCalls)
	}

	// Now trip the breaker.
	for i := 0; i < 4; i++ {
		_ = failBreaker(t, cb)
	}
	if got := stateOf(t, cb); got != StateOpen {
		t.Fatalf("setup: state = %v, want open", got)
	}

	// Open state: fallback SHOULD run.
	_, err = cb.ExecuteWithFallback(
		context.Background(),
		func(ctx context.Context) (any, error) { return "x", nil },
		wrap,
	)
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if fallbackCalls != 1 {
		t.Errorf("fallback called %d times, want 1", fallbackCalls)
	}
}

func TestExecuteWithFallbackNilFallback(t *testing.T) {
	cb, _ := newTestBreaker(t, Settings{Name: "fb-nil"})
	_, err := cb.ExecuteWithFallback(
		context.Background(),
		func(ctx context.Context) (any, error) { return nil, errBoom },
		nil,
	)
	if !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want errBoom (nil fallback should propagate)", err)
	}
}
