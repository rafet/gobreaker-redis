package gobreaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNoRamp_AlwaysAdmits(t *testing.T) {
	r := NoRamp{}
	for _, d := range []time.Duration{0, time.Millisecond, time.Hour} {
		if !r.Admit(d) {
			t.Errorf("NoRamp.Admit(%v) = false", d)
		}
	}
}

func TestLinearRamp_Boundaries(t *testing.T) {
	r := LinearRamp{Duration: 10 * time.Second}
	// At elapsed=0 → 0% → never admits
	admitted := 0
	for i := 0; i < 1000; i++ {
		if r.Admit(0) {
			admitted++
		}
	}
	if admitted > 0 {
		t.Errorf("LinearRamp admitted %d/1000 at elapsed=0; want 0", admitted)
	}

	// At elapsed >= Duration → 100% → always admits
	admitted = 0
	for i := 0; i < 1000; i++ {
		if r.Admit(10 * time.Second) {
			admitted++
		}
	}
	if admitted != 1000 {
		t.Errorf("LinearRamp admitted %d/1000 at elapsed=Duration; want 1000", admitted)
	}
}

func TestLinearRamp_Midpoint(t *testing.T) {
	r := LinearRamp{Duration: 10 * time.Second}
	// At elapsed=5s → 50% ± tolerance
	admitted := 0
	const trials = 10000
	for i := 0; i < trials; i++ {
		if r.Admit(5 * time.Second) {
			admitted++
		}
	}
	ratio := float64(admitted) / float64(trials)
	if ratio < 0.4 || ratio > 0.6 {
		t.Errorf("LinearRamp at 50%%: ratio = %.2f, want ~0.50", ratio)
	}
}

func TestExponentialRamp_Boundaries(t *testing.T) {
	r := ExponentialRamp{Duration: 10 * time.Second}
	// At elapsed=0 → ~0%
	admitted := 0
	for i := 0; i < 1000; i++ {
		if r.Admit(0) {
			admitted++
		}
	}
	if admitted > 0 {
		t.Errorf("ExponentialRamp admitted %d at elapsed=0", admitted)
	}

	// At elapsed >= Duration → 100%
	admitted = 0
	for i := 0; i < 1000; i++ {
		if r.Admit(10 * time.Second) {
			admitted++
		}
	}
	if admitted != 1000 {
		t.Errorf("ExponentialRamp admitted %d at elapsed=Duration; want 1000", admitted)
	}
}

func TestExponentialRamp_MoreConservativeThanLinear(t *testing.T) {
	linear := LinearRamp{Duration: 10 * time.Second}
	expo := ExponentialRamp{Duration: 10 * time.Second}

	// At 30% elapsed, exponential should admit less than linear.
	const trials = 10000
	linearAdmit, expoAdmit := 0, 0
	for i := 0; i < trials; i++ {
		if linear.Admit(3 * time.Second) {
			linearAdmit++
		}
		if expo.Admit(3 * time.Second) {
			expoAdmit++
		}
	}
	if expoAdmit >= linearAdmit {
		t.Errorf("exponential (%d) should admit less than linear (%d) in early phase", expoAdmit, linearAdmit)
	}
}

func TestStepRamp_Progression(t *testing.T) {
	r := StepRamp{
		Steps: []Step{
			{After: 5 * time.Second, Probability: 0.10},
			{After: 15 * time.Second, Probability: 0.50},
			{After: 30 * time.Second, Probability: 1.00},
		},
	}

	// Before first step: 0%
	admitted := 0
	for i := 0; i < 1000; i++ {
		if r.Admit(4 * time.Second) {
			admitted++
		}
	}
	if admitted > 0 {
		t.Errorf("StepRamp admitted %d before first step", admitted)
	}

	// At step 1 (5s): ~10%
	admitted = 0
	const trials = 10000
	for i := 0; i < trials; i++ {
		if r.Admit(10 * time.Second) {
			admitted++
		}
	}
	ratio := float64(admitted) / float64(trials)
	if ratio < 0.05 || ratio > 0.15 {
		t.Errorf("StepRamp at step 1: ratio = %.2f, want ~0.10", ratio)
	}

	// After last step (30s): 100%
	admitted = 0
	for i := 0; i < 1000; i++ {
		if r.Admit(30 * time.Second) {
			admitted++
		}
	}
	if admitted != 1000 {
		t.Errorf("StepRamp admitted %d at final step; want 1000", admitted)
	}
}

func TestStepRamp_Empty(t *testing.T) {
	r := StepRamp{}
	if r.Admit(time.Hour) {
		t.Error("empty StepRamp should never admit")
	}
}

func TestLinearRamp_NegativeElapsed(t *testing.T) {
	r := LinearRamp{Duration: 10 * time.Second}
	if r.Admit(-time.Second) {
		t.Error("negative elapsed should not admit")
	}
}

func TestLinearRamp_ZeroDuration(t *testing.T) {
	r := LinearRamp{Duration: 0}
	// Zero duration means "no ramp" — elapsed >= 0 is always true,
	// so all requests are admitted immediately.
	if !r.Admit(time.Second) {
		t.Error("zero duration should always admit (no ramp)")
	}
}

// TestAdmission_EndToEnd wires a LinearRamp into a real CircuitBreaker
// and verifies that half-open admission is probabilistic.
func TestAdmission_EndToEnd(t *testing.T) {
	clock := newFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	store := NewLocalStore()
	store.setClock(clock.Now)

	cb, err := New[any](context.Background(), Settings{
		Name:                 "ramp-e2e",
		Store:                store,
		Timeout:              10 * time.Second,
		HalfOpenMaxInFlights: 100, // high cap — ramp is the bottleneck
		HalfOpenAdmission:    LinearRamp{Duration: 10 * time.Second},
		ReadyToOpen:          ConsecutiveFailures(1),
		ReadyToClose:         ConsecutiveSuccesses(1000), // never close in this test
	})
	if err != nil {
		t.Fatal(err)
	}
	cb.setClock(clock.Now)

	// Trip the breaker.
	_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
		return nil, errors.New("trip")
	})

	// Advance past timeout → half-open.
	clock.Advance(11 * time.Second)

	// At elapsed=0 from half-open entry, admit rate should be ~0%.
	rejected := 0
	const trials = 100
	for i := 0; i < trials; i++ {
		_, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, nil
		})
		if errors.Is(err, ErrTooManyRequests) {
			rejected++
		}
	}
	// Most should be rejected at t=0.
	if rejected < 80 {
		t.Errorf("at elapsed=0: only %d/%d rejected; LinearRamp should reject most", rejected, trials)
	}

	// Advance to 100% of ramp duration.
	clock.Advance(10 * time.Second)

	// At elapsed=Duration, admit rate should be 100%.
	rejected = 0
	for i := 0; i < trials; i++ {
		_, err := cb.Execute(context.Background(), func(_ context.Context) (any, error) {
			return nil, nil
		})
		if errors.Is(err, ErrTooManyRequests) {
			rejected++
		}
	}
	if rejected > 5 {
		t.Errorf("at elapsed=Duration: %d/%d rejected; should be ~0", rejected, trials)
	}
}
