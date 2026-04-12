package gobreaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fuzz_test.go contains Go native fuzz targets for the circuit breaker
// state machine. Run with:
//
//	go test -fuzz=FuzzExecuteSequence -fuzztime=60s
//
// Property tests (rapid) cover structured random inputs with explicit
// invariant assertions. Fuzz tests cover *unstructured* byte sequences
// and catch panics, hangs, and memory corruption that structured
// generators cannot produce.

// FuzzExecuteSequence feeds a random byte sequence to a circuit breaker,
// interpreting each byte as an operation:
//
//	0x00-0x3F → success
//	0x40-0x7F → failure
//	0x80-0xBF → exclusion (context.Canceled)
//	0xC0-0xFF → advance clock by 1 second
//
// After the full sequence, the test asserts the core invariants:
//   - State is valid (0, 1, or 2)
//   - InFlights == 0 (every admitted request reported an outcome)
//   - No panic occurred during any operation
func FuzzExecuteSequence(f *testing.F) {
	// Seed corpus with interesting patterns.
	f.Add([]byte{0x00})                                           // single success
	f.Add([]byte{0x40})                                           // single failure
	f.Add([]byte{0x80})                                           // single exclusion
	f.Add([]byte{0xC0})                                           // single time advance
	f.Add([]byte{0x40, 0x40, 0x40, 0x40, 0x40})                  // 5 failures → trip
	f.Add([]byte{0x40, 0x40, 0x40, 0x40, 0x40, 0xC0, 0xC0, 0x00}) // trip → timeout → recover
	f.Add(bytes60ConsecFailures())                                // 60 consecutive failures
	f.Add(bytesAlternating(100))                                  // alternating success/failure

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 500 {
			t.Skip("too many ops")
		}

		clock := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		store := NewLocalStore()
		store.setClock(func() time.Time { return clock })

		cb, err := New[any](context.Background(), Settings{
			Name:                 "fuzz",
			Store:                store,
			Timeout:              5 * time.Second,
			Interval:             10 * time.Second,
			HalfOpenMaxInFlights: 3,
			ReadyToOpen:          ConsecutiveFailures(5),
			ReadyToClose:         ConsecutiveSuccesses(2),
			IsExcluded:           IgnoreContextErrors,
		})
		if err != nil {
			t.Fatal(err)
		}
		cb.setClock(func() time.Time { return clock })

		errFuzz := errors.New("fuzz failure")

		for _, op := range ops {
			switch {
			case op < 0x40: // success
				_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
					return nil, nil
				})
			case op < 0x80: // failure
				_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
					return nil, errFuzz
				})
			case op < 0xC0: // exclusion
				_, _ = cb.Execute(context.Background(), func(_ context.Context) (any, error) {
					return nil, context.Canceled
				})
			default: // advance clock 1s
				clock = clock.Add(time.Second)
			}
		}

		// Post-sequence invariants.
		state, err := cb.State(context.Background())
		if err != nil {
			t.Fatalf("State: %v", err)
		}
		if !state.IsValid() {
			t.Fatalf("invalid state: %d", state)
		}

		counts, err := cb.Counts(context.Background())
		if err != nil {
			t.Fatalf("Counts: %v", err)
		}
		if counts.InFlights != 0 {
			t.Fatalf("InFlights = %d after all ops completed; want 0", counts.InFlights)
		}

		sum := counts.TotalSuccesses + counts.TotalFailures + counts.TotalExclusions
		if sum > counts.Requests {
			t.Fatalf("outcome sum %d > Requests %d", sum, counts.Requests)
		}
	})
}

func bytes60ConsecFailures() []byte {
	b := make([]byte, 60)
	for i := range b {
		b[i] = 0x40
	}
	return b
}

func bytesAlternating(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		if i%2 == 0 {
			b[i] = 0x00
		} else {
			b[i] = 0x40
		}
	}
	return b
}
