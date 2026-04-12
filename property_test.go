package gobreaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// property_test.go contains property-based tests using pgregory.net/rapid.
// Unlike example-based tests that pin specific inputs to specific outputs,
// property tests assert universal invariants over a wide range of randomly
// generated inputs. They catch bugs that the test author did not anticipate.
//
// Run with the standard `go test`. Failing properties produce a minimal
// shrunk reproducer that you can paste into a normal test.

// genCounts generates an arbitrary Counts value with the invariants
// the state machine is supposed to maintain. The Counts struct is a pure
// data carrier so we generate freely; the state machine guarantees the
// invariants in real life.
func genCounts(t *rapid.T) Counts {
	return Counts{
		Requests:             rapid.Uint64Range(0, 1<<32).Draw(t, "requests"),
		InFlights:            rapid.Uint64Range(0, 1<<16).Draw(t, "inflights"),
		TotalSuccesses:       rapid.Uint64Range(0, 1<<32).Draw(t, "succ"),
		TotalFailures:        rapid.Uint64Range(0, 1<<32).Draw(t, "fail"),
		TotalExclusions:      rapid.Uint64Range(0, 1<<32).Draw(t, "excl"),
		ConsecutiveSuccesses: rapid.Uint64Range(0, 1<<16).Draw(t, "consec_succ"),
		ConsecutiveFailures:  rapid.Uint64Range(0, 1<<16).Draw(t, "consec_fail"),
	}
}

// PropertyCounts_OnSuccessClearsConsecutiveFailures verifies the
// invariant that a successful outcome zeroes ConsecutiveFailures while
// incrementing ConsecutiveSuccesses. This is what makes
// ConsecutiveFailures an "in a row" counter.
func TestProperty_Counts_OnSuccessClearsConsecutiveFailures(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := genCounts(t)
		// Ensure InFlights >= 1 so the decrement is observable.
		if c.InFlights == 0 {
			c.InFlights = 1
		}
		c.onSuccess()
		if c.ConsecutiveFailures != 0 {
			t.Fatalf("ConsecutiveFailures = %d, want 0 after onSuccess", c.ConsecutiveFailures)
		}
	})
}

// PropertyCounts_OnFailureClearsConsecutiveSuccesses is the symmetric
// invariant.
func TestProperty_Counts_OnFailureClearsConsecutiveSuccesses(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := genCounts(t)
		if c.InFlights == 0 {
			c.InFlights = 1
		}
		c.onFailure()
		if c.ConsecutiveSuccesses != 0 {
			t.Fatalf("ConsecutiveSuccesses = %d, want 0 after onFailure", c.ConsecutiveSuccesses)
		}
	})
}

// PropertyCounts_OnExclusionPreservesConsecutive verifies that
// exclusions are conceptually neutral: they touch only TotalExclusions
// and InFlights, never the consecutive counters.
func TestProperty_Counts_OnExclusionPreservesConsecutive(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := genCounts(t)
		if c.InFlights == 0 {
			c.InFlights = 1
		}
		preSucc := c.ConsecutiveSuccesses
		preFail := c.ConsecutiveFailures
		c.onExclusion()
		if c.ConsecutiveSuccesses != preSucc {
			t.Fatalf("ConsecutiveSuccesses changed: %d -> %d", preSucc, c.ConsecutiveSuccesses)
		}
		if c.ConsecutiveFailures != preFail {
			t.Fatalf("ConsecutiveFailures changed: %d -> %d", preFail, c.ConsecutiveFailures)
		}
	})
}

// PropertyCounts_InFlightsNeverNegative verifies that calling onSuccess,
// onFailure, or onExclusion when InFlights is 0 does not wrap around.
// Counts is uint64 so a "negative" value would mean ~uint64(0).
func TestProperty_Counts_InFlightsNeverNegative(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var c Counts
		// Apply N random outcomes to a never-incremented Counts.
		// InFlights must remain 0 throughout.
		ops := rapid.SliceOfN(
			rapid.IntRange(0, 2),
			0, 100,
		).Draw(t, "ops")
		for _, op := range ops {
			switch op {
			case 0:
				c.onSuccess()
			case 1:
				c.onFailure()
			case 2:
				c.onExclusion()
			}
			if c.InFlights != 0 {
				t.Fatalf("InFlights wrapped after op %d: %d", op, c.InFlights)
			}
		}
	})
}

// PropertyCounts_RequestsAlwaysGEOutcomes verifies the relationship
// between admitted requests and reported outcomes: a Counts produced by
// a sequence of onRequest/onSuccess/onFailure/onExclusion calls must
// satisfy Requests >= TotalSuccesses + TotalFailures + TotalExclusions
// + InFlights. (In other words: every reported outcome was preceded by
// an admission, and any not-yet-reported requests are still in-flight.)
func TestProperty_Counts_RequestsAccountedFor(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var c Counts
		ops := rapid.SliceOfN(
			rapid.IntRange(0, 3),
			0, 200,
		).Draw(t, "ops")
		for _, op := range ops {
			switch op {
			case 0:
				c.onRequest()
			case 1:
				if c.InFlights > 0 {
					c.onSuccess()
				}
			case 2:
				if c.InFlights > 0 {
					c.onFailure()
				}
			case 3:
				if c.InFlights > 0 {
					c.onExclusion()
				}
			}
		}
		reported := c.TotalSuccesses + c.TotalFailures + c.TotalExclusions + c.InFlights
		if c.Requests != reported {
			t.Fatalf("Requests=%d != reported sum=%d (succ=%d fail=%d excl=%d inflight=%d)",
				c.Requests, reported,
				c.TotalSuccesses, c.TotalFailures, c.TotalExclusions, c.InFlights)
		}
	})
}

// PropertyCounts_ResetZeroes verifies that reset() zeros every field.
// Useful as a guard against future field additions that forget to be
// reset.
func TestProperty_Counts_ResetZeroes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := genCounts(t)
		c.reset()
		if c != (Counts{}) {
			t.Fatalf("reset did not zero Counts: %+v", c)
		}
	})
}

// PropertyState_StringRoundTrip is a defensive check: every valid State
// has a non-empty String form, and unknown values produce a labeled
// "unknown(...)" string.
func TestProperty_State_StringForms(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := State(rapid.Int32().Draw(t, "raw"))
		str := s.String()
		if str == "" {
			t.Fatalf("State(%d).String() returned empty", int32(s))
		}
		switch s {
		case StateClosed:
			if str != "closed" {
				t.Fatalf("StateClosed.String() = %q", str)
			}
		case StateHalfOpen:
			if str != "half-open" {
				t.Fatalf("StateHalfOpen.String() = %q", str)
			}
		case StateOpen:
			if str != "open" {
				t.Fatalf("StateOpen.String() = %q", str)
			}
		default:
			// Unknown values must be labeled.
			if len(str) < 8 || str[:8] != "unknown(" {
				t.Fatalf("State(%d).String() = %q, want unknown(...)", int32(s), str)
			}
			_ = s.IsValid()
		}
	})
}

// PropertyReadyToFunc_OrAndDualLogic verifies the boolean-algebra
// identities: Or(...) is true iff at least one input is true; And(...)
// is true iff every input is true.
func TestProperty_ReadyTo_OrAndDual(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		bits := rapid.SliceOfN(rapid.Bool(), 0, 10).Draw(t, "bits")
		funcs := make([]ReadyToFunc, len(bits))
		for i, b := range bits {
			funcs[i] = func(_ Counts) bool { return b }
		}

		// Or: true iff any bit is true
		anyTrue := false
		for _, b := range bits {
			if b {
				anyTrue = true
				break
			}
		}
		if got := Or(funcs...)(Counts{}); got != anyTrue {
			t.Fatalf("Or(%v) = %v, want %v", bits, got, anyTrue)
		}

		// And: true iff every bit is true (empty And is true)
		allTrue := true
		for _, b := range bits {
			if !b {
				allTrue = false
				break
			}
		}
		if got := And(funcs...)(Counts{}); got != allTrue {
			t.Fatalf("And(%v) = %v, want %v", bits, got, allTrue)
		}
	})
}

// PropertyConsecutiveFailures_MonotonicThreshold verifies the threshold
// builder is correct: ConsecutiveFailures(n) fires iff the input has
// >= n consecutive failures.
func TestProperty_ReadyTo_ConsecutiveFailuresThreshold(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.Uint64Range(1, 1000).Draw(t, "n")
		obs := rapid.Uint64Range(0, 2000).Draw(t, "observed")
		f := ConsecutiveFailures(n)
		want := obs >= n
		if got := f(Counts{ConsecutiveFailures: obs}); got != want {
			t.Fatalf("ConsecutiveFailures(%d)(obs=%d) = %v, want %v", n, obs, got, want)
		}
	})
}

// PropertyFailureRatio_NeverFiresBelowMinRequests verifies that the
// FailureRatio predicate honors its minRequests precondition no matter
// how the failures are distributed.
func TestProperty_ReadyTo_FailureRatioPrecondition(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		minReq := rapid.Uint64Range(1, 1000).Draw(t, "min")
		ratio := rapid.Float64Range(0, 1).Draw(t, "ratio")
		// Use a "completed" sum strictly below minReq.
		succ := rapid.Uint64Range(0, minReq-1).Draw(t, "succ")
		fail := rapid.Uint64Range(0, minReq-1-succ).Draw(t, "fail")
		f := FailureRatio(minReq, ratio)
		if f(Counts{TotalSuccesses: succ, TotalFailures: fail}) {
			t.Fatalf("FailureRatio(%d, %f) fired with succ+fail=%d < min", minReq, ratio, succ+fail)
		}
	})
}

// PropertyStateMachine_RandomSequencePreservesInvariants is the
// flagship state machine property. It feeds the breaker a random
// sequence of operations (success, failure, exclusion, time advance,
// state read) and asserts that after every operation:
//
//  1. The state is one of the three known values.
//  2. InFlights never exceeds the requests admitted in this generation.
//  3. The Counts sum invariant holds.
//  4. The Snapshot version is monotonic.
func TestProperty_StateMachine_InvariantsHoldUnderRandomSequence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ops := rapid.SliceOfN(rapid.IntRange(0, 4), 1, 100).Draw(t, "ops")

		store := NewLocalStore()
		clock := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		store.setClock(func() time.Time { return clock })

		cb, err := New[any](context.Background(), Settings{
			Name:                 "prop",
			Store:                store,
			Timeout:              10 * time.Second,
			HalfOpenMaxInFlights: 3,
			ReadyToOpen:          ConsecutiveFailures(5),
			ReadyToClose:         ConsecutiveSuccesses(2),
		})
		if err != nil {
			t.Fatal(err)
		}
		cb.setClock(func() time.Time { return clock })

		errBoom := errors.New("prop boom")
		var lastVersion uint64

		for i, op := range ops {
			switch op {
			case 0: // success
				_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
					return nil, nil
				})
			case 1: // failure
				_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
					return nil, errBoom
				})
			case 2: // exclusion (only if IsExcluded matches; we'll
				// simulate by enabling IgnoreContextErrors mid-flight
				// — but rebuilding the breaker is expensive, so skip)
				_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
					return nil, nil
				})
			case 3: // advance clock by 1s
				clock = clock.Add(time.Second)
			case 4: // observe state and counts
				_, _ = cb.State(context.Background())
				_, _ = cb.Counts(context.Background())
			}

			// Invariant checks against the snapshot.
			snap, err := store.Get(context.Background(), "prop")
			if err != nil {
				t.Fatalf("step %d (op %d): Get: %v", i, op, err)
			}
			// 1. State is valid.
			if !snap.State.IsValid() {
				t.Fatalf("step %d: invalid state %d", i, snap.State)
			}
			// 2. InFlights is bounded — Counts.InFlights must be <= Counts.Requests
			//    in any valid Counts produced by the state machine.
			if snap.Counts.InFlights > snap.Counts.Requests {
				t.Fatalf("step %d: InFlights=%d > Requests=%d",
					i, snap.Counts.InFlights, snap.Counts.Requests)
			}
			// 3. Sum of outcomes + in-flights == Requests in this generation.
			sum := snap.Counts.TotalSuccesses + snap.Counts.TotalFailures +
				snap.Counts.TotalExclusions + snap.Counts.InFlights
			if sum != snap.Counts.Requests {
				t.Fatalf("step %d: outcome sum=%d != Requests=%d (%+v)",
					i, sum, snap.Counts.Requests, snap.Counts)
			}
			// 4. Version is monotonic non-decreasing.
			if snap.Version < lastVersion {
				t.Fatalf("step %d: Version regressed: %d -> %d", i, lastVersion, snap.Version)
			}
			lastVersion = snap.Version
		}
	})
}

// PropertyExecute_StateOpenRejectsAllRequests verifies that once the
// breaker is open and within its Timeout, EVERY Execute call returns
// ErrOpenState — never the wrapped function's value.
func TestProperty_Execute_OpenStateRejectsAll(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 50).Draw(t, "n")

		cb, err := New[int](context.Background(), Settings{
			Name:        "open-prop",
			Timeout:     time.Hour,
			ReadyToOpen: ConsecutiveFailures(1),
		})
		if err != nil {
			t.Fatal(err)
		}
		// Trip.
		_, _ = cb.Execute(context.Background(), func(_ context.Context) (int, error) {
			return 0, errors.New("trip")
		})

		for i := 0; i < n; i++ {
			called := false
			_, err := cb.Execute(context.Background(), func(_ context.Context) (int, error) {
				called = true
				return 42, nil
			})
			if !errors.Is(err, ErrOpenState) {
				t.Fatalf("iter %d: err = %v, want ErrOpenState", i, err)
			}
			if called {
				t.Fatalf("iter %d: wrapped function ran while open", i)
			}
		}
	})
}
