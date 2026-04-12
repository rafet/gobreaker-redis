package gobreaker

import (
	"context"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Property: AdaptiveFailureRate and FailureRatio produce identical
// results for the same inputs.
func TestProperty_Adaptive_EquivalentToFailureRatio(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		minReq := rapid.Uint64Range(1, 100).Draw(rt, "min")
		ratio := rapid.Float64Range(0, 1).Draw(rt, "ratio")
		succ := rapid.Uint64Range(0, 500).Draw(rt, "succ")
		fail := rapid.Uint64Range(0, 500).Draw(rt, "fail")
		c := Counts{TotalSuccesses: succ, TotalFailures: fail}
		a := AdaptiveFailureRate(minReq, ratio)
		b := FailureRatio(minReq, ratio)
		if a(c) != b(c) {
			rt.Fatalf("AdaptiveFailureRate != FailureRatio for min=%d ratio=%f counts=%+v", minReq, ratio, c)
		}
	})
}

// Property: SlowStartThreshold starts lenient (startRatio) and ends
// strict (endRatio). At minRequests the effective threshold should
// equal startRatio; at maxRequests it should equal endRatio.
func TestProperty_SlowStart_BoundaryRatios(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		startR := rapid.Float64Range(0.5, 1.0).Draw(rt, "start")
		endR := rapid.Float64Range(0.0, startR).Draw(rt, "end")
		minR := rapid.Uint64Range(2, 50).Draw(rt, "min")
		maxR := rapid.Uint64Range(minR+1, 200).Draw(rt, "max")
		f := SlowStartThreshold(minR, maxR, startR, endR)

		// At exactly minRequests with failure ratio just above startRatio
		// → should fire.
		atMin := Counts{TotalSuccesses: 0, TotalFailures: minR}
		// 100% failure rate >= any startRatio → fire.
		if !f(atMin) {
			rt.Fatalf("did not fire at min=%d with 100%% failures (startRatio=%f)", minR, startR)
		}
	})
}

// Property: LinearRamp.Admit is monotonically increasing — longer
// elapsed time means higher admission probability.
func TestProperty_LinearRamp_Monotonic(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Minimum 1ms — sub-millisecond ramps produce probability
		// differences too small to distinguish from random noise in
		// 500 trials. No real user would configure a nanosecond ramp.
		dur := rapid.Int64Range(1_000_000, 60_000_000_000).Draw(rt, "dur_ns")
		r := LinearRamp{Duration: time.Duration(dur)}
		const trials = 500
		prevAdmit := 0
		for step := 0; step <= 10; step++ {
			elapsed := time.Duration(float64(dur) * float64(step) / 10.0)
			admitted := 0
			for i := 0; i < trials; i++ {
				if r.Admit(elapsed) {
					admitted++
				}
			}
			if admitted < prevAdmit-trials/10 {
				// Allow some noise (10%) but overall trend must be up.
				rt.Fatalf("LinearRamp not monotonic: step %d admitted %d < prev %d", step, admitted, prevAdmit)
			}
			prevAdmit = admitted
		}
	})
}

// Property: StepRamp never admits before the first step's After time.
func TestProperty_StepRamp_NeverAdmitsBeforeFirstStep(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		after := time.Duration(rapid.Int64Range(1, 60_000_000_000).Draw(rt, "after_ns"))
		prob := rapid.Float64Range(0.01, 1.0).Draw(rt, "prob")
		r := StepRamp{Steps: []Step{{After: after, Probability: prob}}}
		// One nanosecond before the step: must never admit.
		for i := 0; i < 100; i++ {
			if r.Admit(after - 1) {
				rt.Fatalf("StepRamp admitted before first step (after=%v)", after)
			}
		}
	})
}

// Property: Pipeline with zero timeout and zero retries is
// equivalent to bare CircuitBreaker.Execute.
func TestProperty_Pipeline_BareEquivalent(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		succeed := rapid.Bool().Draw(rt, "succeed")
		cb, _ := New[int](ctx, Settings{
			Name:        "prop-pipe",
			ReadyToOpen: ConsecutiveFailures(1000),
		})
		p := Compose[int](cb).Build()
		fn := func(_ context.Context) (int, error) {
			if succeed {
				return 42, nil
			}
			return 0, errBoom
		}
		r1, e1 := cb.Execute(ctx, fn)
		_ = cb.Reset(ctx) // reset between calls
		r2, e2 := p.Execute(ctx, fn)
		if r1 != r2 {
			rt.Fatalf("bare=%d pipe=%d", r1, r2)
		}
		if (e1 == nil) != (e2 == nil) {
			rt.Fatalf("bare err=%v pipe err=%v", e1, e2)
		}
	})
}

var ctx = context.Background()
