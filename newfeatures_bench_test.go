package gobreaker

import (
	"context"
	"testing"
	"time"
)

func BenchmarkHedge_NoHedge_Internal(b *testing.B) {
	cb, _ := New[int](context.Background(), Settings{Name: "hedge-bench"})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Hedge(ctx, cb,
			func(_ context.Context) (int, error) { return 0, nil },
			HedgeDelay(time.Hour),
		)
	}
}

func BenchmarkPipeline_Bare(b *testing.B) {
	cb, _ := New[int](context.Background(), Settings{Name: "pipe-bench"})
	p := Compose[int](cb).Build()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Execute(ctx, func(_ context.Context) (int, error) { return 0, nil })
	}
}

func BenchmarkPipeline_WithRetry(b *testing.B) {
	cb, _ := New[int](context.Background(), Settings{Name: "pipe-retry-bench"})
	p := Compose[int](cb).WithRetry(2, 0).Build()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Execute(ctx, func(_ context.Context) (int, error) { return 0, nil })
	}
}

func BenchmarkDedup_NoContention(b *testing.B) {
	cb, _ := New[int](context.Background(), Settings{Name: "dedup-bench"})
	d := NewDeduplicator[int](cb)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.ExecuteDedup(ctx, "k", func(_ context.Context) (int, error) { return 0, nil })
	}
}

func BenchmarkForceOpen(b *testing.B) {
	cb, _ := New[int](context.Background(), Settings{Name: "force-bench"})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.ForceOpen(ctx)
		_ = cb.ForceClosed(ctx)
	}
}

func BenchmarkUpdateSettings(b *testing.B) {
	cb, _ := New[int](context.Background(), Settings{Name: "cfg-bench"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.UpdateSettings(func(s *Settings) {
			s.Timeout = time.Duration(i) * time.Millisecond
		})
	}
}

func BenchmarkAdmission_LinearRamp(b *testing.B) {
	r := LinearRamp{Duration: time.Minute}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Admit(30 * time.Second)
	}
}
