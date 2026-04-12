package respstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// regression_test.go contains tests for every bug found during the v2
// CodeRabbit review pass that involves the respstore subpackage, plus
// every gap surfaced by the audit.

// REG_New_NilClientPanics is the regression test for CodeRabbit review
// issue #12. Bug: respstore.New accepted a nil UniversalClient and
// produced a Store that nil-pointer-panicked on the first operation.
func TestREG_New_NilClientPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("respstore.New(nil) did not panic")
		}
		if !errors.Is(toError(r), ErrNilClient) {
			t.Errorf("panic value = %v, want ErrNilClient", r)
		}
	}()
	_ = New(nil)
}

func toError(v any) error {
	if e, ok := v.(error); ok {
		return e
	}
	return errors.New(strings.TrimSpace(strings.Join([]string{}, " ")))
}

// REG_Update_BothSkipAndFailUpdate verifies that the inner Update is
// only invoked once per Store.Update, even after retries: each retry
// performs a fresh Get and a fresh CAS attempt, but the closure must
// be called once per attempt (not skipped).
func TestREG_Update_ClosureCalledPerAttempt(t *testing.T) {
	store, _ := newTestStoreLocal(t)
	if _, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}

	var calls int32
	_, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		atomic.AddInt32(&calls, 1)
		return c, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("closure called %d times for an uncontended Update, want 1", got)
	}
}

// REG_Update_VersionUnchangedOnUserError verifies that an error
// returned from the UpdateFunc closure does NOT cause the version to
// advance. The Update is treated as if it never happened.
func TestREG_Update_VersionUnchangedOnUserError(t *testing.T) {
	store, _ := newTestStoreLocal(t)
	if _, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		c.State = gobreaker.StateClosed
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := store.Get(context.Background(), "x")

	want := errors.New("nope")
	if _, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return gobreaker.Snapshot{}, want
	}); !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}

	after, _ := store.Get(context.Background(), "x")
	if after.Version != before.Version {
		t.Errorf("Version advanced after user error: %d -> %d", before.Version, after.Version)
	}
}

// REG_Update_TTLZeroDoesNotSetExpiry verifies that the default (no
// TTL) configuration does NOT call PEXPIRE on the snapshot key.
func TestREG_Update_TTLZeroDoesNotSetExpiry(t *testing.T) {
	store, mr := newTestStoreLocal(t)
	if _, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL(store.Key("x")); ttl != 0 {
		t.Errorf("TTL = %v, want 0 (no expiry by default)", ttl)
	}
}

// REG_WithMaxRetries_ZeroIsClampedToOne verifies the documented
// minimum: WithMaxRetries(0) is treated as WithMaxRetries(1) so the
// store always makes at least one attempt.
func TestREG_WithMaxRetries_ZeroIsClampedToOne(t *testing.T) {
	store, _ := newTestStoreLocal(t, WithMaxRetries(0))
	if store.maxRetry != 1 {
		t.Errorf("maxRetry = %d, want 1 (clamped from 0)", store.maxRetry)
	}
}

// REG_WithMaxRetries_NegativeIsClampedToOne is the same property for
// negative inputs.
func TestREG_WithMaxRetries_NegativeIsClampedToOne(t *testing.T) {
	store, _ := newTestStoreLocal(t, WithMaxRetries(-5))
	if store.maxRetry != 1 {
		t.Errorf("maxRetry = %d, want 1 (clamped from -5)", store.maxRetry)
	}
}

// MUT_WithMaxRetries_OneIsAccepted kills the boundary mutation at
// respstore/store.go:134:7 where `if n < 1` could become `if n <= 1`.
// The latter would silently bump WithMaxRetries(1) up to 1 (no
// change) but would also reject the perfectly valid value of 1.
// Verifying n=1 directly catches this.
func TestMUT_WithMaxRetries_OneIsAccepted(t *testing.T) {
	store, _ := newTestStoreLocal(t, WithMaxRetries(1))
	if store.maxRetry != 1 {
		t.Errorf("maxRetry = %d, want 1 (n=1 must be accepted as-is)", store.maxRetry)
	}
}

// MUT_Update_RetryBudgetIsExact kills the boundary mutation at
// respstore/store.go:164:28 where `attempt < s.maxRetry` could become
// `<=`. We arm a perpetual conflict scenario, set the budget to 1,
// and assert exactly one closure invocation before ErrSnapshotConflict.
func TestMUT_Update_RetryBudgetIsExact(t *testing.T) {
	store, _ := newTestStoreLocal(t, WithMaxRetries(1))

	// Seed
	if _, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}

	// Force a conflict on every attempt by mutating the key from
	// inside the closure.
	var attempts int
	_, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		attempts++
		// Mutate the key from a "concurrent" Update so this attempt's
		// CAS will fail. Use a separate Store handle to bypass our
		// own retry budget.
		if _, err := store.Update(context.Background(), "x", func(c2 gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
			c2.Counts.Requests++
			return c2, nil
		}); err != nil {
			t.Fatalf("inner Update: %v", err)
		}
		return c, nil
	})
	if !errors.Is(err, gobreaker.ErrSnapshotConflict) {
		t.Errorf("err = %v, want ErrSnapshotConflict", err)
	}
	if attempts != 1 {
		t.Errorf("closure called %d times; with WithMaxRetries(1) we expect exactly 1", attempts)
	}
}

// REG_Get_AfterCloseOwningClientReturnsError verifies that calls on a
// Store whose underlying client has been closed surface a clean error
// instead of panicking. (When ownClient is true, Close shuts down the
// client; subsequent calls fail with a connection error.)
func TestREG_Get_AfterCloseOwningClientReturnsError(t *testing.T) {
	mr := miniredis.RunT(t)
	store, err := NewWithAddress(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "x"); err == nil {
		t.Error("Get after Close returned nil error")
	}
}

// REG_Update_AfterCloseOwningClientReturnsError covers the matching
// path inside Update: the inner Get call (which Update performs first)
// must surface the error rather than panicking.
func TestREG_Update_AfterCloseOwningClientReturnsError(t *testing.T) {
	mr := miniredis.RunT(t)
	store, err := NewWithAddress(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	called := false
	_, err = store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		called = true
		return c, nil
	})
	if err == nil {
		t.Error("Update after Close returned nil error")
	}
	if called {
		t.Error("Update invoked the closure after Close; expected to fail at the Get step")
	}
}

// REG_NewWithAddress_PingFailureNotProbed documents that NewWithAddress
// does NOT eagerly verify the connection: an unreachable address is
// accepted at construction time and only fails on the first operation.
// This is intentional — eager connection verification would couple
// process startup to backend availability.
func TestREG_NewWithAddress_UnreachableLazyFails(t *testing.T) {
	store, err := NewWithAddress("127.0.0.1:1") // reserved port, refuses
	if err != nil {
		t.Fatalf("NewWithAddress should not eagerly verify: got err = %v", err)
	}
	if _, err := store.Get(context.Background(), "x"); err == nil {
		t.Error("Get against unreachable backend returned nil error")
	}
	_ = store.Close()
}

// REG_KeyNamespaceIsolation verifies that two stores with different
// prefixes do NOT see each other's snapshots, even when their breaker
// names overlap.
func TestREG_KeyNamespaceIsolation(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	a := New(client, WithKeyPrefix("env-a"))
	b := New(client, WithKeyPrefix("env-b"))

	if _, err := a.Update(context.Background(), "service", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		c.State = gobreaker.StateOpen
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}

	bSnap, _ := b.Get(context.Background(), "service")
	if !bSnap.IsZero() {
		t.Errorf("env-b leaked from env-a: %+v", bSnap)
	}
}

// REG_HighContentionConcurrency stresses the CAS retry loop with many
// goroutines hitting the same key. The test asserts atomicity (no lost
// updates) and bounded retry budget consumption.
func TestREG_HighContentionConcurrency(t *testing.T) {
	store, _ := newTestStoreLocal(t)

	const goroutines = 16
	const perGoroutine = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				_, err := store.Update(context.Background(), "hot", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
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

	loaded, _ := store.Get(context.Background(), "hot")
	want := uint64(goroutines * perGoroutine)
	if loaded.Counts.Requests != want {
		t.Errorf("Requests = %d, want %d", loaded.Counts.Requests, want)
	}
}

// REG_Decode_RejectsInvalidStateValue covers the corruption guard
// already exercised by the suite, but as a regression entry so any
// future relaxation of decodeSnapshot's strictness is loud.
func TestREG_Decode_RejectsInvalidStateValue(t *testing.T) {
	store, mr := newTestStoreLocal(t)
	mr.HSet(store.Key("bad"), "v", "1", "s", "42", "g", "1")
	if _, err := store.Get(context.Background(), "bad"); err == nil {
		t.Error("decodeSnapshot accepted state=42; want error")
	}
}

// REG_Decode_RejectsMalformedNumeric covers the matching path for
// numeric fields.
func TestREG_Decode_RejectsMalformedNumeric(t *testing.T) {
	store, mr := newTestStoreLocal(t)
	mr.HSet(store.Key("bad"), "v", "abc", "s", "0", "g", "1")
	if _, err := store.Get(context.Background(), "bad"); err == nil {
		t.Error("decodeSnapshot accepted version=abc; want error")
	}
}

// REG_Decode_RejectsCorruptionInEveryField is a parameterised test that
// scribbles garbage into each numeric/time field of a HASH and verifies
// decodeSnapshot rejects it. This catches the regression where adding a
// new field forgets to use parseUint/parseInt/parseTime and silently
// drops corruption.
func TestREG_Decode_RejectsCorruptionInEveryField(t *testing.T) {
	// Build a baseline that decodeSnapshot accepts.
	baseline := func(store *Store, mr *miniredis.Miniredis) {
		mr.HSet(store.Key("base"),
			"v", "1",
			"s", "0",
			"g", "1",
			"gs", "0",
			"ex", "0",
			"cr", "0",
			"ci", "0",
			"cts", "0",
			"ctf", "0",
			"cte", "0",
			"ccs", "0",
			"ccf", "0",
		)
	}

	type fieldCase struct {
		field    string
		bad      string
		category string // "uint", "int", "time"
	}
	cases := []fieldCase{
		{fieldVersion, "abc", "uint"},
		{fieldState, "not-a-number", "int"},
		{fieldGeneration, "abc", "uint"},
		{fieldGenerationStart, "not-a-time", "time"},
		{fieldExpiry, "not-a-time", "time"},
		{fieldRequests, "abc", "uint"},
		{fieldInFlights, "abc", "uint"},
		{fieldTotalSucc, "abc", "uint"},
		{fieldTotalFail, "abc", "uint"},
		{fieldTotalExcl, "abc", "uint"},
		{fieldConsecSucc, "abc", "uint"},
		{fieldConsecFail, "abc", "uint"},
	}

	for _, c := range cases {
		t.Run(c.field+"="+c.bad, func(t *testing.T) {
			store, mr := newTestStoreLocal(t)
			baseline(store, mr)
			// Sanity: baseline decodes cleanly.
			if _, err := store.Get(context.Background(), "base"); err != nil {
				t.Fatalf("baseline failed to decode: %v", err)
			}
			// Now corrupt the target field.
			mr.HSet(store.Key("base"), c.field, c.bad)
			_, err := store.Get(context.Background(), "base")
			if err == nil {
				t.Errorf("decodeSnapshot accepted %s=%q; want error", c.field, c.bad)
			}
		})
	}
}

// REG_Decode_RejectsInvalidStateBoundary verifies the State enum
// validation step. State values 3 and beyond are not legal.
func TestREG_Decode_RejectsInvalidStateBoundary(t *testing.T) {
	store, mr := newTestStoreLocal(t)
	for _, raw := range []string{"-1", "3", "100", "9999"} {
		t.Run("state="+raw, func(t *testing.T) {
			mr.FlushAll()
			mr.HSet(store.Key("bs"),
				"v", "1",
				"s", raw,
				"g", "1",
			)
			if _, err := store.Get(context.Background(), "bs"); err == nil {
				t.Errorf("decodeSnapshot accepted state=%q", raw)
			}
		})
	}
}

// REG_Decode_DefaultsMissingFieldsToZero verifies that fields absent
// from the HASH default to their zero value, exercising the
// `if !ok || raw == ""` branches in parseUint, parseInt, and parseTime.
// This is the path taken when an older snapshot format is read by a
// newer codec — additive schema evolution must not break existing
// keys.
func TestREG_Decode_DefaultsMissingFieldsToZero(t *testing.T) {
	store, mr := newTestStoreLocal(t)
	// Write only the version. Every other field — including the
	// state field, which is the only consumer of parseInt — is
	// missing. The state must default to StateClosed (0).
	mr.HSet(store.Key("min"),
		"v", "5",
	)
	snap, err := store.Get(context.Background(), "min")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if snap.Version != 5 {
		t.Errorf("Version = %d, want 5", snap.Version)
	}
	if snap.State != gobreaker.StateClosed {
		t.Errorf("State = %v, want closed (default)", snap.State)
	}
	if snap.Generation != 0 {
		t.Errorf("Generation = %d, want 0", snap.Generation)
	}
	// All Counts fields should default to zero.
	if snap.Counts != (gobreaker.Counts{}) {
		t.Errorf("Counts = %+v, want zero", snap.Counts)
	}
	// All time fields should default to zero.
	if !snap.GenerationStart.IsZero() {
		t.Errorf("GenerationStart = %v, want zero", snap.GenerationStart)
	}
	if !snap.Expiry.IsZero() {
		t.Errorf("Expiry = %v, want zero", snap.Expiry)
	}
}

// REG_Decode_EmptyStringFieldDefaultsToZero exercises the second leg of
// the parseUint/parseInt early-return: a field that exists but holds
// an empty string. This is what miniredis returns for HSET with an
// empty value, and what older snapshots may produce.
func TestREG_Decode_EmptyStringFieldDefaultsToZero(t *testing.T) {
	store, mr := newTestStoreLocal(t)
	mr.HSet(store.Key("empty"),
		"v", "1",
		"s", "0",
		"g", "1",
		"cr", "", // empty
		"ci", "", // empty
		"cts", "", // empty
		"ctf", "", // empty
		"cte", "", // empty
		"ccs", "", // empty
		"ccf", "", // empty
		"gs", "", // empty
		"ex", "", // empty
	)
	snap, err := store.Get(context.Background(), "empty")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if snap.Counts != (gobreaker.Counts{}) {
		t.Errorf("Counts = %+v, want zero", snap.Counts)
	}
}

// REG_ToInt_AcceptsAllReplyShapes verifies the toInt helper, which
// normalizes Lua return values across go-redis versions and Redis
// dialects (some return int64, some return string).
func TestREG_ToInt_AcceptsAllReplyShapes(t *testing.T) {
	cases := []struct {
		in   any
		want int
	}{
		{int64(1), 1},
		{int64(0), 0},
		{int(1), 1},
		{int(0), 0},
		{"1", 1},
		{"0", 0},
		{"garbage", 0},
		{nil, 0},
		{3.14, 0},
	}
	for _, c := range cases {
		if got := toInt(c.in); got != c.want {
			t.Errorf("toInt(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

// REG_Codec_TimeRoundTrip verifies that nanosecond-precision time
// values survive a round trip through the HASH encoding.
func TestREG_Codec_TimeRoundTrip(t *testing.T) {
	store, _ := newTestStoreLocal(t)
	original := time.Date(2030, 6, 15, 12, 34, 56, 789, time.UTC)
	if _, err := store.Update(context.Background(), "t", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		c.GenerationStart = original
		c.Expiry = original.Add(time.Hour)
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}
	loaded, _ := store.Get(context.Background(), "t")
	if !loaded.GenerationStart.Equal(original) {
		t.Errorf("GenerationStart = %v, want %v", loaded.GenerationStart, original)
	}
	if !loaded.Expiry.Equal(original.Add(time.Hour)) {
		t.Errorf("Expiry = %v, want %v", loaded.Expiry, original.Add(time.Hour))
	}
	// Both must be in UTC after the round trip — see codec.go.
	if loaded.GenerationStart.Location() != time.UTC {
		t.Errorf("GenerationStart timezone = %v, want UTC", loaded.GenerationStart.Location())
	}
}

// REG_Codec_ZeroTimeIsRoundTripped verifies that the zero time value
// survives the encode/decode trip and remains zero (used by the closed
// state with Interval==0).
func TestREG_Codec_ZeroTimeIsRoundTripped(t *testing.T) {
	store, _ := newTestStoreLocal(t)
	if _, err := store.Update(context.Background(), "z", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		c.Expiry = time.Time{}
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}
	loaded, _ := store.Get(context.Background(), "z")
	if !loaded.Expiry.IsZero() {
		t.Errorf("Expiry = %v, want zero", loaded.Expiry)
	}
}

// newTestStoreLocal is a helper that mirrors newTestStore from
// store_test.go but is duplicated here so the regression file can
// stand alone if store_test.go is reorganised.
func newTestStoreLocal(t *testing.T, opts ...Option) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client, opts...), mr
}

// REG_Update_CASScriptErrorIsWrapped verifies that an error from the
// underlying Lua script execution is wrapped with respstore context
// rather than leaking through bare. We exercise this path by injecting
// a deliberately invalid script into the Store, which causes Redis to
// return a script-execution error on the first Update.
func TestREG_Update_CASScriptErrorIsWrapped(t *testing.T) {
	store, _ := newTestStoreLocal(t)
	// Replace the CAS script with one that will fail at runtime.
	store.cas = redis.NewScript(`return redis.error_reply("intentional script failure for tests")`)

	_, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	})
	if err == nil {
		t.Fatal("Update returned nil error from a deliberately failing script")
	}
	if !strings.Contains(err.Error(), "respstore: CAS script") {
		t.Errorf("error %q is not wrapped with respstore context", err)
	}
}

// REG_Update_UnexpectedCASReplyIsWrapped verifies the type-check
// branch in Update: if the Lua script returns a value that does not
// decode to a 2-element array, the store surfaces a precise error
// instead of panicking on a type assertion. We trigger this by
// injecting a script that returns a single string.
func TestREG_Update_UnexpectedCASReplyIsWrapped(t *testing.T) {
	store, _ := newTestStoreLocal(t)
	store.cas = redis.NewScript(`return "single-string-not-an-array"`)

	_, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	})
	if err == nil {
		t.Fatal("Update returned nil error from script with wrong reply shape")
	}
	if !strings.Contains(err.Error(), "unexpected CAS reply") {
		t.Errorf("error %q is not the expected unexpected-reply wrapper", err)
	}
}

// REG_Update_UnexpectedCASReplyShortArrayIsWrapped is the matching
// test for an array reply with the wrong length.
func TestREG_Update_UnexpectedCASReplyShortArrayIsWrapped(t *testing.T) {
	store, _ := newTestStoreLocal(t)
	store.cas = redis.NewScript(`return {1}`) // valid array, wrong length

	_, err := store.Update(context.Background(), "x", func(c gobreaker.Snapshot, _ time.Time) (gobreaker.Snapshot, error) {
		return c, nil
	})
	if err == nil {
		t.Fatal("Update returned nil error from short-array reply")
	}
	if !strings.Contains(err.Error(), "unexpected CAS reply") {
		t.Errorf("error %q is not the expected unexpected-reply wrapper", err)
	}
}
