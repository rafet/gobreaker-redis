# Contributing

Thanks for considering a contribution. This document covers the test
discipline that the project asks of every change. The discipline is the
single most important reason this package can claim "production-ready"
with a straight face.

## Test discipline (the short version)

When you add a new public API or fix a bug, ask:

1. **Have I tested every input that can hit this code?** Nil arguments,
   zero values, empty strings, negative numbers, sentinel constants,
   bad enum values, post-Close use, concurrent access, wrapped errors.
2. **Have I tested every output path?** Happy path, every error
   branch, every observable side effect (events emitted, locks held,
   resources released, callbacks invoked).
3. **Did the test fail before the fix?** A bug-fix PR without a test
   that demonstrably fails on the unpatched code is incomplete. Add the
   test, run it on the broken code to see it red, then apply the fix.
4. **Is the test deterministic?** Avoid `time.Sleep` for synchronization;
   prefer channels, condition variables, or controllable clocks
   (`setClock` on `LocalStore`/`CircuitBreaker`).

If you cannot answer "yes" to all four for a given change, the change
is not ready for review.

## Negative-path checklist for new public APIs

Whenever you add a new exported function, type, or method, the test
file for that package should answer the following questions:

- [ ] **Nil parameter**: what happens if the caller passes `nil` for
      a pointer/interface parameter? (Either: rejected at the type
      level via panic, rejected via returned error, or handled
      gracefully. Pick one and test it.)
- [ ] **Zero value**: what happens if the caller passes a zero-value
      struct or interface? Especially relevant for `Settings`-like
      configuration types.
- [ ] **Boundary values**: what happens at numeric/string limits
      (`0`, `MaxUint64`, empty string, `""`)?
- [ ] **Bad enum value**: if the parameter is a typed integer (like
      `State` or `OnStoreFailure`), what happens for values outside
      the documented set?
- [ ] **Post-Close use**: if the type holds a resource that can be
      released, what happens when methods are called after `Close`?
- [ ] **Concurrent use**: is the type safe for concurrent use? If
      yes, exercise it from multiple goroutines under `-race`. If no,
      document the constraint.
- [ ] **Error path**: every error returned must be reachable from a
      test. Use the failure-injection helpers in
      `testing_helpers_test.go` (`flakyStore`,
      `closureRanFailingStore`, `gatedStore`).
- [ ] **Wrapped errors**: if the function returns a sentinel error,
      tests must use `errors.Is` to assert it is reachable through
      `fmt.Errorf("%w", ...)` chains.

## Failure-injection helpers

`testing_helpers_test.go` contains a small arsenal of `Store`
implementations that simulate realistic failure modes. Use them
instead of writing one-off mocks:

| Helper | What it simulates |
|---|---|
| `failingStore` | Every call fails immediately (`gobreaker_test.go`) |
| `closureRanFailingStore` | The `UpdateFunc` closure runs and the commit then fails. Use this to test paths that depend on side effects of the closure (e.g. `fireStateChanges`) |
| `flakyStore` | Skip / fail / pass sequencing — `passNextUpdates(n)` and `failNextUpdates(n)` let you place the failure at a specific position in a multi-call sequence (e.g. "admit succeeds, report fails") |
| `gatedStore` | Suspends `Update` calls on a channel; use `nextGate()` to gate the next call. Designed for deterministic race construction in `Group` tests |

If a new failure mode appears in the field that none of these can
exercise, add a new helper rather than mocking inside the test that
caught it.

## Regression test convention

When you fix a bug, add a test to the regression suite:

- `regression_test.go` (root package)
- `respstore/regression_test.go`
- `httpcb/regression_test.go`

Naming convention: `TestREG_<topic>_<symptom>`. The function comment
must explain what the bug was, citing the issue/PR/CodeRabbit comment
that found it.

The bug-fix PR must add the test first (red), then the fix (green).
Reviewers will ask for the failing-test commit if it is missing.

## Coverage policy

The CI pipeline does not enforce a hard coverage gate, but the
maintainers track per-function coverage and flag regressions. As of
v2.0.0:

- Core package: 96%+
- `httpcb`: 100%
- `respstore`: 80%+

A PR that drops any of these below the existing baseline must explain
the gap. The most common acceptable reason is "this is defensive code
guarding against a race window that cannot be deterministically
exercised in tests" — see the `L139-141` branch in `group.go` for the
canonical example. Document the gap with a comment in the source.

## Lint policy

The project uses `golangci-lint` v2 with the configuration in
`.golangci.yml`. CI is hard-gated on lint cleanliness. Run locally
with:

```bash
golangci-lint run ./...
```

The most common findings to expect:

- `errcheck`: every returned error must be checked or explicitly
  discarded with `_ =`. Tests that intentionally drop errors should
  use `_, _ = ...` and a comment explaining why.
- `bodyclose`: HTTP response bodies must be closed. Inside test
  helpers that pass nil responses, suppress with
  `//nolint:bodyclose // resp is nil`.
- `unused`: do not commit dead helper functions. If a helper is
  intentionally retained for symmetry, suppress with
  `//nolint:unused // part of the documented helper API`.
- `gocritic` `exitAfterDefer`: avoid `log.Fatal` after a `defer`.
  Refactor to a `run() error` style with explicit cleanup.

## Integration tests

`compat/compat_test.go` (`-tags=integration`) runs the same test
matrix against real backends (Redis, Valkey, KeyDB, DragonflyDB). Run
it locally before submitting changes that touch `respstore/`:

```bash
docker run -d --name gbr-redis     -p 6379:6379 redis:7-alpine
docker run -d --name gbr-valkey    -p 6380:6379 valkey/valkey:8-alpine
docker run -d --name gbr-keydb     -p 6381:6379 eqalpha/keydb:latest
docker run -d --name gbr-dragonfly --ulimit memlock=-1 -p 6382:6379 \
    docker.dragonflydb.io/dragonflydb/dragonfly:latest

go test -tags=integration -race ./compat/...

docker rm -f gbr-redis gbr-valkey gbr-keydb gbr-dragonfly
```

CI runs the same matrix on every PR; you do not strictly have to run
it locally, but for changes that touch the Lua script or the codec, a
local run catches problems faster than a 4-job CI cycle.

## Commits and PRs

- One logical change per commit. Mixed commits ("fix bug + refactor +
  add docs") are hard to revert.
- Every commit must build and pass tests on its own. Use `git rebase
  -i` to clean up history before pushing.
- The PR description should link to the issue or CodeRabbit comment
  the change addresses, and call out any user-visible API change.
- Do NOT bypass commit hooks (`--no-verify`) or sign-offs without
  asking.
