package respstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	gobreaker "github.com/rafet/gobreaker-redis/v2"
	"pgregory.net/rapid"
)

// newPropertyStore is the helper used by property-based tests. It
// cannot reuse newTestStoreLocal because that helper expects a
// *testing.T (it uses miniredis.RunT for cleanup integration), and
// rapid.Check passes a *rapid.T which is not assignment-compatible.
// Building one fresh per check is cheap.
func newPropertyStore() (*Store, func()) {
	mr, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := New(client)
	cleanup := func() {
		_ = client.Close()
		mr.Close()
	}
	return store, cleanup
}

// property_test.go contains property-based tests for the respstore
// codec and Store. The codec tests verify the
// `decodeSnapshot(encodeSnapshot(s)) == s` roundtrip property — the
// single most important invariant the codec must uphold, and the kind
// of bug that example-based tests miss because the author forgets a
// field.

// genSnapshot produces an arbitrary Snapshot whose fields cover the
// representable space of every codec field. We deliberately exclude
// the Version field because the Store assigns it during Update; codec
// roundtrips operate on the data fields below the version layer.
func genSnapshot(t *rapid.T) gobreaker.Snapshot {
	state := gobreaker.State(rapid.IntRange(0, 2).Draw(t, "state"))
	// Use a wide but bounded time range so we hit nanosecond
	// precision without overflowing.
	gs := time.Unix(0, rapid.Int64Range(0, 1<<60).Draw(t, "gs"))
	ex := time.Unix(0, rapid.Int64Range(0, 1<<60).Draw(t, "ex"))
	// Half of the time, force the time fields to zero so the codec's
	// "0 means zero time" path is exercised too.
	if rapid.Bool().Draw(t, "gsZero") {
		gs = time.Time{}
	}
	if rapid.Bool().Draw(t, "exZero") {
		ex = time.Time{}
	}
	return gobreaker.Snapshot{
		State:           state,
		Generation:      rapid.Uint64().Draw(t, "gen"),
		GenerationStart: gs,
		Expiry:          ex,
		Counts: gobreaker.Counts{
			Requests:             rapid.Uint64().Draw(t, "req"),
			InFlights:            rapid.Uint64().Draw(t, "inf"),
			TotalSuccesses:       rapid.Uint64().Draw(t, "succ"),
			TotalFailures:        rapid.Uint64().Draw(t, "fail"),
			TotalExclusions:      rapid.Uint64().Draw(t, "excl"),
			ConsecutiveSuccesses: rapid.Uint64().Draw(t, "csucc"),
			ConsecutiveFailures:  rapid.Uint64().Draw(t, "cfail"),
		},
	}
}

// PropertyCodec_RoundTrip is the headline invariant: every Snapshot
// produced by encodeSnapshot decodes back to itself. The version is
// reapplied separately because encodeSnapshot does not include it.
func TestProperty_Codec_RoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		original := genSnapshot(t)
		fields := encodeSnapshot(original)

		// Translate the slice into the map shape decodeSnapshot
		// expects.
		m := map[string]string{
			"v": "0", // version is irrelevant to the data layer
		}
		for i := 0; i < len(fields); i += 2 {
			m[fields[i]] = fields[i+1]
		}

		decoded, err := decodeSnapshot(m)
		if err != nil {
			t.Fatalf("decodeSnapshot returned error: %v", err)
		}

		// Compare every field. We avoid `==` on Snapshot because
		// time.Time has internal pointer fields; use Equal for the
		// time fields and field-wise comparison for the rest.
		if decoded.State != original.State {
			t.Fatalf("State: %v != %v", decoded.State, original.State)
		}
		if decoded.Generation != original.Generation {
			t.Fatalf("Generation: %d != %d", decoded.Generation, original.Generation)
		}
		if !decoded.GenerationStart.Equal(original.GenerationStart) {
			t.Fatalf("GenerationStart: %v != %v", decoded.GenerationStart, original.GenerationStart)
		}
		if !decoded.Expiry.Equal(original.Expiry) {
			t.Fatalf("Expiry: %v != %v", decoded.Expiry, original.Expiry)
		}
		if decoded.Counts != original.Counts {
			t.Fatalf("Counts: %+v != %+v", decoded.Counts, original.Counts)
		}
	})
}

// PropertyCodec_RoundTripThroughStore extends the roundtrip property
// to the full Store path: encode → write to miniredis → read back via
// HGETALL → decode. Catches mismatches between the codec and the Lua
// script's HSET argument shape.
func TestProperty_Codec_RoundTripThroughStore(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Build a fresh store per check so they don't interfere.
		store, cleanup := newPropertyStore()
		defer cleanup()
		original := genSnapshot(rt)

		got, err := store.Update(context.Background(), "k", func(_ gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
			return original, nil
		})
		if err != nil {
			rt.Fatalf("Update: %v", err)
		}
		if got.Version != 1 {
			rt.Fatalf("Version = %d, want 1", got.Version)
		}

		loaded, err := store.Get(context.Background(), "k")
		if err != nil {
			rt.Fatalf("Get: %v", err)
		}

		if loaded.State != original.State {
			rt.Fatalf("State mismatch: %v vs %v", loaded.State, original.State)
		}
		if loaded.Generation != original.Generation {
			rt.Fatalf("Generation: %d vs %d", loaded.Generation, original.Generation)
		}
		if !loaded.GenerationStart.Equal(original.GenerationStart) {
			rt.Fatalf("GenerationStart: %v vs %v", loaded.GenerationStart, original.GenerationStart)
		}
		if !loaded.Expiry.Equal(original.Expiry) {
			rt.Fatalf("Expiry: %v vs %v", loaded.Expiry, original.Expiry)
		}
		if loaded.Counts != original.Counts {
			rt.Fatalf("Counts: %+v vs %+v", loaded.Counts, original.Counts)
		}
	})
}

// PropertyStore_VersionMonotonic verifies that the version field is
// strictly increasing with every successful Update against the same
// key, regardless of what the closure does.
func TestProperty_Store_VersionMonotonic(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store, cleanup := newPropertyStore()
		defer cleanup()
		n := rapid.IntRange(1, 50).Draw(rt, "n")
		var prev uint64
		for i := 0; i < n; i++ {
			snap, err := store.Update(context.Background(), "v", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
				return c, nil
			})
			if err != nil {
				rt.Fatalf("Update %d: %v", i, err)
			}
			if snap.Version != prev+1 {
				rt.Fatalf("Version jumped: %d -> %d (expected %d)", prev, snap.Version, prev+1)
			}
			prev = snap.Version
		}
	})
}
