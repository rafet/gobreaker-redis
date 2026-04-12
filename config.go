package gobreaker

// UpdateSettings applies fn to the breaker's current Settings and
// replaces them atomically. fn receives a pointer to a copy of the
// current Settings; mutations are applied to the copy and swapped in
// when fn returns.
//
// The update runs under the breaker's internal lock so it is safe to
// call from any goroutine. The breaker's state and Counts are NOT
// reset — only the behavioral configuration changes. To also reset
// state, call Reset after UpdateSettings.
//
// UpdateSettings does NOT re-validate the Settings. It is the
// caller's responsibility to produce a valid configuration. Invalid
// settings (e.g. nil ReadyToOpen) will cause panics on the next
// Execute call.
//
// Fields that should NOT be changed at runtime:
//   - Name (changing it orphans the Store key)
//   - Store (changing it splits state across two backends)
//   - OnStoreFailure (changing it mid-flight is undefined)
//
// All other fields are safe to change.
//
// Example: tighten the trip threshold during an incident:
//
//	cb.UpdateSettings(func(s *Settings) {
//	    s.ReadyToOpen = gobreaker.ConsecutiveFailures(2)
//	    s.Timeout = 10 * time.Second
//	})
func (cb *CircuitBreaker[T]) UpdateSettings(fn func(s *Settings)) {
	if cb.localStore != nil {
		cb.inlineMu.Lock()
		s := cb.settings
		fn(&s)
		// Preserve identity fields.
		s.Name = cb.settings.Name
		s.Store = cb.settings.Store
		s.OnStoreFailure = cb.settings.OnStoreFailure
		cb.settings = s
		cb.inlineMu.Unlock()
		return
	}
	// Generic path: settings are read by executeStore under the
	// Store's own serialization. We protect the swap with the
	// breaker's fallback mutex.
	cb.mu.Lock()
	s := cb.settings
	fn(&s)
	s.Name = cb.settings.Name
	s.Store = cb.settings.Store
	s.OnStoreFailure = cb.settings.OnStoreFailure
	cb.settings = s
	cb.mu.Unlock()
}
