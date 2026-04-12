package respstore

import (
	"testing"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// FuzzDecodeSnapshot feeds arbitrary byte sequences to decodeSnapshot
// (via a synthetic HASH field map) and asserts that the function never
// panics. It either returns a valid Snapshot or a clean error.
func FuzzDecodeSnapshot(f *testing.F) {
	// Seed: a valid minimal snapshot.
	f.Add("1", "0", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0")
	// Seed: empty strings.
	f.Add("", "", "", "", "", "", "", "", "", "", "", "")
	// Seed: garbage.
	f.Add("abc", "xyz", "-1", "99999999999999999999", "NaN", "Inf", "true", "null", "[]", "{}", "0x1", "\x00")

	f.Fuzz(func(t *testing.T, v, s, g, gs, ex, cr, ci, cts, ctf, cte, ccs, ccf string) {
		m := map[string]string{
			"v": v, "s": s, "g": g, "gs": gs, "ex": ex,
			"cr": cr, "ci": ci, "cts": cts, "ctf": ctf,
			"cte": cte, "ccs": ccs, "ccf": ccf,
		}

		snap, err := decodeSnapshot(m)
		if err != nil {
			// Clean error — acceptable.
			return
		}

		// If decodeSnapshot returned no error, the snapshot must be
		// internally consistent.
		if !snap.State.IsValid() && snap.State != gobreaker.StateClosed {
			t.Errorf("decodeSnapshot returned invalid state %d without error", snap.State)
		}
	})
}
