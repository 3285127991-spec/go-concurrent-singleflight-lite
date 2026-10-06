# singleflightlite

A small, dependency-free, concurrency-safe singleflight library for Go,
plus a minimal demo CLI. Same-key concurrent calls share one real
execution; different keys always run in parallel.

## Features

- `Do(ctx, key, fn) (value, shared, error)` — the first caller for a key
  becomes the leader and runs `fn`; concurrent callers with the same key
  attach to the same flight and share its result (`shared=true`).
- Cancelling any caller's context (leader or follower) only exits that
  caller; `fn` gets a detached copy of the leader's context, so the flight
  and all other callers keep going.
- `Forget(key)` — drops the in-flight entry so the next `Do` starts a new
  generation immediately; the old flight can never evict the new one.
- `Snapshot()` — currently joinable flights (`Key`, `Generation`,
  `Waiters`), sorted by key.
- `Len()` — number of currently joinable flights.
- `fn` runs in its own goroutine, never under the group's lock.
- Validation: empty key returns `ErrEmptyKey`, nil `fn` returns
  `ErrNilFunc`.

Deliberately not included: caching, TTL, retries, networking, persistence.

## Usage

```go
var g singleflightlite.Group

v, shared, err := g.Do(ctx, "user:42", func(ctx context.Context) (any, error) {
    return loadUser(ctx, 42) // runs at most once per concurrent burst
})
```

The zero value of `Group` is ready to use.

## Demo CLI

```sh
go run ./cmd/singleflightlite
```

Demonstrates same-key merging, follower cancellation and `Forget`.

## Test

```sh
go test -race ./...
```

Tests are channel-synchronized (no real sleeps) and cover shared
execution, cross-key parallelism, shared errors, follower/leader
cancellation, `Forget` generation isolation, argument validation,
`Snapshot`/`Len`, and a concurrent stress run.

## Layout

- `singleflight.go` — the library (package `singleflightlite`)
- `singleflight_test.go` — tests
- `cmd/singleflightlite/main.go` — demo CLI
