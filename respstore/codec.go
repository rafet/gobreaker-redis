package respstore

import (
	"fmt"
	"strconv"
	"time"

	gobreaker "github.com/rafet/gobreaker-redis/v2"
)

// Field names used inside the Redis HASH that represents a Snapshot.
//
// The names are short to keep wire payloads small but verbose enough to
// remain readable when an operator runs `HGETALL key:name` against the
// backing store. Reordering or renaming a field is a backwards-incompatible
// change because it would invalidate existing persisted snapshots.
const (
	fieldVersion         = "v"
	fieldState           = "s"
	fieldGeneration      = "g"
	fieldGenerationStart = "gs"
	fieldExpiry          = "ex"
	fieldRequests        = "cr"
	fieldInFlights       = "ci"
	fieldTotalSucc       = "cts"
	fieldTotalFail       = "ctf"
	fieldTotalExcl       = "cte"
	fieldConsecSucc      = "ccs"
	fieldConsecFail      = "ccf"
)

// encodeSnapshot serializes a Snapshot to alternating field/value strings
// suitable for HSET. The version is encoded separately because the Lua
// script overwrites it explicitly to enforce CAS semantics.
func encodeSnapshot(s gobreaker.Snapshot) []string {
	return []string{
		fieldState, strconv.FormatInt(int64(s.State), 10),
		fieldGeneration, strconv.FormatUint(s.Generation, 10),
		fieldGenerationStart, formatTime(s.GenerationStart),
		fieldExpiry, formatTime(s.Expiry),
		fieldRequests, strconv.FormatUint(s.Counts.Requests, 10),
		fieldInFlights, strconv.FormatUint(s.Counts.InFlights, 10),
		fieldTotalSucc, strconv.FormatUint(s.Counts.TotalSuccesses, 10),
		fieldTotalFail, strconv.FormatUint(s.Counts.TotalFailures, 10),
		fieldTotalExcl, strconv.FormatUint(s.Counts.TotalExclusions, 10),
		fieldConsecSucc, strconv.FormatUint(s.Counts.ConsecutiveSuccesses, 10),
		fieldConsecFail, strconv.FormatUint(s.Counts.ConsecutiveFailures, 10),
	}
}

// decodeSnapshot parses the result of HGETALL into a Snapshot. A missing key
// (empty map) yields the zero Snapshot, matching the Store contract.
//
// decodeSnapshot is strict: any field that is present but malformed produces
// an error. We do not silently fall back to zero values because a corrupt
// snapshot could cause the breaker to admit traffic it should reject.
func decodeSnapshot(fields map[string]string) (gobreaker.Snapshot, error) {
	if len(fields) == 0 {
		return gobreaker.Snapshot{}, nil
	}

	var s gobreaker.Snapshot
	var err error

	if s.Version, err = parseUint(fields, fieldVersion); err != nil {
		return s, err
	}

	stateRaw, err := parseInt(fields, fieldState)
	if err != nil {
		return s, err
	}
	s.State = gobreaker.State(stateRaw)
	if !s.State.IsValid() {
		return s, fmt.Errorf("respstore: corrupt snapshot: invalid state %d", stateRaw)
	}

	if s.Generation, err = parseUint(fields, fieldGeneration); err != nil {
		return s, err
	}
	if s.GenerationStart, err = parseTime(fields, fieldGenerationStart); err != nil {
		return s, err
	}
	if s.Expiry, err = parseTime(fields, fieldExpiry); err != nil {
		return s, err
	}
	if s.Counts.Requests, err = parseUint(fields, fieldRequests); err != nil {
		return s, err
	}
	if s.Counts.InFlights, err = parseUint(fields, fieldInFlights); err != nil {
		return s, err
	}
	if s.Counts.TotalSuccesses, err = parseUint(fields, fieldTotalSucc); err != nil {
		return s, err
	}
	if s.Counts.TotalFailures, err = parseUint(fields, fieldTotalFail); err != nil {
		return s, err
	}
	if s.Counts.TotalExclusions, err = parseUint(fields, fieldTotalExcl); err != nil {
		return s, err
	}
	if s.Counts.ConsecutiveSuccesses, err = parseUint(fields, fieldConsecSucc); err != nil {
		return s, err
	}
	if s.Counts.ConsecutiveFailures, err = parseUint(fields, fieldConsecFail); err != nil {
		return s, err
	}

	return s, nil
}

// formatTime encodes a time.Time as nanoseconds since the Unix epoch. The
// zero time is encoded as "0" so the Lua script can roundtrip it without
// special-casing strings.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

func parseTime(m map[string]string, field string) (time.Time, error) {
	raw, ok := m[field]
	if !ok || raw == "0" || raw == "" {
		return time.Time{}, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("respstore: corrupt snapshot field %q: %w", field, err)
	}
	// Always normalize to UTC so that Snapshot equality (and therefore the
	// state machine's idempotence checks) does not depend on the
	// process's local timezone.
	return time.Unix(0, n).UTC(), nil
}

func parseUint(m map[string]string, field string) (uint64, error) {
	raw, ok := m[field]
	if !ok || raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("respstore: corrupt snapshot field %q: %w", field, err)
	}
	return n, nil
}

func parseInt(m map[string]string, field string) (int64, error) {
	raw, ok := m[field]
	if !ok || raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("respstore: corrupt snapshot field %q: %w", field, err)
	}
	return n, nil
}
