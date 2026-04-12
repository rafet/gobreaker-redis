package ringbuf

import (
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestNew_MinCapacity(t *testing.T) {
	r := New(0)
	if len(r.data) != 1 {
		t.Errorf("capacity clamped to %d, want 1", len(r.data))
	}
}

func TestAdd_SingleValue(t *testing.T) {
	r := New(10)
	r.Add(5 * time.Millisecond)
	if r.Len() != 1 {
		t.Errorf("Len = %d, want 1", r.Len())
	}
	if r.Count() != 1 {
		t.Errorf("Count = %d, want 1", r.Count())
	}
}

func TestAdd_FillExactly(t *testing.T) {
	r := New(3)
	r.Add(1 * time.Millisecond)
	r.Add(2 * time.Millisecond)
	if r.Len() != 2 {
		t.Errorf("Len = %d, want 2", r.Len())
	}
	if r.full {
		t.Error("should not be full before reaching capacity")
	}

	r.Add(3 * time.Millisecond) // fills capacity, pos wraps to 0
	if r.Len() != 3 {
		t.Errorf("Len = %d, want 3", r.Len())
	}
	// After writing exactly capacity items, pos has wrapped to 0
	// and full is set. This is correct: the ring is now at capacity.
	if !r.full {
		t.Error("should be full after writing exactly capacity items")
	}
}

func TestAdd_WrapsAround(t *testing.T) {
	r := New(3)
	r.Add(1 * time.Millisecond)
	r.Add(2 * time.Millisecond)
	r.Add(3 * time.Millisecond)
	r.Add(4 * time.Millisecond) // overwrites slot 0
	if r.Len() != 3 {
		t.Errorf("Len = %d, want 3", r.Len())
	}
	if r.Count() != 4 {
		t.Errorf("Count = %d, want 4", r.Count())
	}
	if !r.full {
		t.Error("should be full after wrap")
	}
}

func TestPercentile_Empty(t *testing.T) {
	r := New(10)
	if got := r.Percentile(0.5); got != 0 {
		t.Errorf("P50 of empty = %v, want 0", got)
	}
}

func TestPercentile_SingleValue(t *testing.T) {
	r := New(10)
	r.Add(42 * time.Millisecond)
	for _, p := range []float64{0, 0.5, 0.99, 1} {
		if got := r.Percentile(p); got != 42*time.Millisecond {
			t.Errorf("P%g of single = %v, want 42ms", p*100, got)
		}
	}
}

func TestPercentile_KnownDistribution(t *testing.T) {
	r := New(100)
	for i := 1; i <= 100; i++ {
		r.Add(time.Duration(i) * time.Millisecond)
	}
	// P50 → ~50ms, P99 → ~99ms
	p50 := r.Percentile(0.5)
	if p50 < 49*time.Millisecond || p50 > 51*time.Millisecond {
		t.Errorf("P50 = %v, want ~50ms", p50)
	}
	p99 := r.Percentile(0.99)
	if p99 < 98*time.Millisecond || p99 > 100*time.Millisecond {
		t.Errorf("P99 = %v, want ~99ms", p99)
	}
	p0 := r.Percentile(0)
	if p0 != 1*time.Millisecond {
		t.Errorf("P0 = %v, want 1ms", p0)
	}
	p100 := r.Percentile(1)
	if p100 != 100*time.Millisecond {
		t.Errorf("P100 = %v, want 100ms", p100)
	}
}

func TestPercentile_OverwrittenValues(t *testing.T) {
	r := New(5)
	// Write 10 values; only the last 5 survive.
	for i := 1; i <= 10; i++ {
		r.Add(time.Duration(i) * time.Millisecond)
	}
	// Ring contains [6,7,8,9,10] ms.
	p50 := r.Percentile(0.5)
	if p50 != 8*time.Millisecond {
		t.Errorf("P50 = %v, want 8ms", p50)
	}
}

func TestPercentile_BoundaryClamp(t *testing.T) {
	r := New(10)
	r.Add(time.Millisecond)
	// Negative percentile clamped to 0.
	if r.Percentile(-1) != time.Millisecond {
		t.Error("negative percentile should clamp")
	}
	// >1 percentile clamped to 1.
	if r.Percentile(2) != time.Millisecond {
		t.Error(">1 percentile should clamp")
	}
}

func TestReset(t *testing.T) {
	r := New(10)
	for i := 0; i < 20; i++ {
		r.Add(time.Millisecond)
	}
	r.Reset()
	if r.Len() != 0 {
		t.Errorf("Len after Reset = %d", r.Len())
	}
	if r.Count() != 0 {
		t.Errorf("Count after Reset = %d", r.Count())
	}
	if r.Percentile(0.5) != 0 {
		t.Error("Percentile after Reset should be 0")
	}
}

// Property: Len is always <= capacity.
func TestProperty_LenBounded(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		cap := rapid.IntRange(1, 100).Draw(rt, "cap")
		n := rapid.IntRange(0, 500).Draw(rt, "n")
		r := New(cap)
		for i := 0; i < n; i++ {
			r.Add(time.Duration(rapid.Int64Range(0, 1<<30).Draw(rt, "d")))
		}
		if r.Len() > cap {
			rt.Fatalf("Len %d > capacity %d", r.Len(), cap)
		}
	})
}

// Property: Percentile is monotonic — P(a) <= P(b) when a <= b.
func TestProperty_PercentileMonotonic(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		cap := rapid.IntRange(1, 50).Draw(rt, "cap")
		r := New(cap)
		n := rapid.IntRange(1, 200).Draw(rt, "n")
		for i := 0; i < n; i++ {
			r.Add(time.Duration(rapid.Int64Range(0, 1<<30).Draw(rt, "d")))
		}
		prev := r.Percentile(0)
		for p := 0.1; p <= 1.0; p += 0.1 {
			curr := r.Percentile(p)
			if curr < prev {
				rt.Fatalf("P%g = %v < P%g = %v (not monotonic)", p*100, curr, (p-0.1)*100, prev)
			}
			prev = curr
		}
	})
}

func BenchmarkAdd(b *testing.B) {
	r := New(1000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Add(time.Millisecond)
	}
}

func BenchmarkPercentile(b *testing.B) {
	r := New(1000)
	for i := 0; i < 1000; i++ {
		r.Add(time.Duration(i) * time.Microsecond)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Percentile(0.99)
	}
}
