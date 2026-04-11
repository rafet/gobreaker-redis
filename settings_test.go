package gobreaker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestSettingsValidate(t *testing.T) {
	cases := []struct {
		name    string
		s       Settings
		wantErr bool
	}{
		{"empty name", Settings{}, true},
		{"negative interval", Settings{Name: "x", Interval: -1}, true},
		{"negative timeout", Settings{Name: "x", Timeout: -1}, true},
		{"valid", Settings{Name: "x"}, false},
		{"valid with positive durations", Settings{
			Name: "x", Interval: time.Second, Timeout: time.Minute,
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.s.Validate()
			if (err != nil) != c.wantErr {
				t.Errorf("Validate() = %v, wantErr %v", err, c.wantErr)
			}
			if c.wantErr && !errors.Is(err, ErrInvalidSettings) {
				t.Errorf("error %v does not wrap ErrInvalidSettings", err)
			}
		})
	}
}

func TestSettingsDefaults(t *testing.T) {
	s := Settings{Name: "x"}.defaults()
	if s.Timeout != 60*time.Second {
		t.Errorf("default Timeout = %v, want 60s", s.Timeout)
	}
	if s.HalfOpenMaxInFlights != 1 {
		t.Errorf("default HalfOpenMaxInFlights = %d, want 1", s.HalfOpenMaxInFlights)
	}
	if s.ReadyToOpen == nil || s.ReadyToClose == nil || s.ReadyToReopen == nil {
		t.Error("default ReadyTo* should be non-nil")
	}
	if s.IsSuccessful == nil || s.IsExcluded == nil {
		t.Error("default IsSuccessful/IsExcluded should be non-nil")
	}
	// Default ReadyToClose with HalfOpenMaxInFlights=1 should fire on a single success.
	if !s.ReadyToClose(Counts{ConsecutiveSuccesses: 1}) {
		t.Error("default ReadyToClose should fire on 1 consecutive success when HalfOpenMaxInFlights=1")
	}
	// Default ReadyToReopen should be Always.
	if !s.ReadyToReopen(Counts{}) {
		t.Error("default ReadyToReopen should be Always")
	}
	// Default ReadyToOpen should fire at 5 consecutive failures.
	if s.ReadyToOpen(Counts{ConsecutiveFailures: 4}) {
		t.Error("default ReadyToOpen should not fire at 4 failures")
	}
	if !s.ReadyToOpen(Counts{ConsecutiveFailures: 5}) {
		t.Error("default ReadyToOpen should fire at 5 failures")
	}
}

func TestIgnoreContextErrors(t *testing.T) {
	if IgnoreContextErrors(nil) {
		t.Error("nil should not be excluded")
	}
	if !IgnoreContextErrors(context.Canceled) {
		t.Error("context.Canceled should be excluded")
	}
	if !IgnoreContextErrors(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded should be excluded")
	}
	wrapped := fmt.Errorf("wrapping: %w", context.Canceled)
	if !IgnoreContextErrors(wrapped) {
		t.Error("wrapped context.Canceled should be excluded")
	}
	if IgnoreContextErrors(errors.New("some other error")) {
		t.Error("unrelated error should not be excluded")
	}
}
