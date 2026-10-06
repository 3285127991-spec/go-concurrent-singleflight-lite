// Package singleflightlite provides a concurrency-safe singleflight Group.
//
// Concurrent calls to Do with the same key share a single execution of fn:
// the first caller (the leader) triggers the real work and every other
// caller (followers) waits for and receives the same result. Calls with
// different keys always run in parallel.
package singleflightlite

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrEmptyKey is returned by Do when key is empty.
	ErrEmptyKey = errors.New("singleflightlite: key must not be empty")
	// ErrNilFunc is returned by Do when fn is nil.
	ErrNilFunc = errors.New("singleflightlite: fn must not be nil")
)

// FlightInfo describes one in-flight, still joinable execution.
type FlightInfo struct {
	Key        string
	Generation uint64
	Waiters    int
}

// flight tracks one execution of fn. It stays in the group's map until fn
// returns, so late callers can still join it.
type flight struct {
	key     string
	gen     uint64
	done    chan struct{} // closed when fn has returned and val/err are set
	val     any
	err     error
	waiters int // callers currently attached (leader + followers)
}

// Group is a singleflight group. The zero value is ready to use.
type Group struct {
	mu      sync.Mutex
	m       map[string]*flight
	nextGen uint64
}

// Do executes fn for key, merging concurrent callers.
//
// While a flight for key is running, every Do with the same key attaches to
// it: exactly one leader runs fn and all followers share its result. The
// returned shared flag reports whether this call joined an already running
// flight (true) or started a new one (false).
//
// fn runs in its own goroutine, never under the group's lock. It receives a
// detached copy of the leader's context (values kept, cancellation
// stripped), so no caller's cancellation can abort the shared work. If any
// caller's ctx (leader or follower) is cancelled, only that caller returns
// early with ctx.Err(); the flight itself and all other callers are
// unaffected.
func (g *Group) Do(ctx context.Context, key string, fn func(context.Context) (any, error)) (any, bool, error) {
	if key == "" {
		return nil, false, ErrEmptyKey
	}
	if fn == nil {
		return nil, false, ErrNilFunc
	}

	g.mu.Lock()
	if f, ok := g.m[key]; ok {
		f.waiters++
		g.mu.Unlock()
		return waitFlight(ctx, g, f, true)
	}
	g.nextGen++
	f := &flight{key: key, gen: g.nextGen, done: make(chan struct{}), waiters: 1}
	if g.m == nil {
		g.m = make(map[string]*flight)
	}
	g.m[key] = f
	g.mu.Unlock()

	go g.run(context.WithoutCancel(ctx), f, fn)
	return waitFlight(ctx, g, f, false)
}

// waitFlight blocks until the flight finishes or ctx is cancelled.
func waitFlight(ctx context.Context, g *Group, f *flight, shared bool) (any, bool, error) {
	select {
	case <-f.done:
		return f.val, shared, f.err
	case <-ctx.Done():
		g.mu.Lock()
		f.waiters--
		g.mu.Unlock()
		return nil, shared, ctx.Err()
	}
}

// run executes fn and publishes the result. It removes the flight from the
// map only if it is still the current generation for its key, so a stale
// flight can never evict a newer one created after Forget.
func (g *Group) run(ctx context.Context, f *flight, fn func(context.Context) (any, error)) {
	val, err := fn(ctx)
	g.mu.Lock()
	f.val, f.err = val, err
	close(f.done)
	if cur, ok := g.m[f.key]; ok && cur == f {
		delete(g.m, f.key)
	}
	g.mu.Unlock()
}

// Forget drops the in-flight entry for key, if any. The currently running
// execution is not interrupted, but the next Do for key immediately starts
// a new generation instead of joining the old one.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}

// Snapshot returns the currently joinable flights, sorted by key.
func (g *Group) Snapshot() []FlightInfo {
	g.mu.Lock()
	infos := make([]FlightInfo, 0, len(g.m))
	for _, f := range g.m {
		infos = append(infos, FlightInfo{Key: f.key, Generation: f.gen, Waiters: f.waiters})
	}
	g.mu.Unlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key < infos[j].Key })
	return infos
}

// Len returns the number of currently joinable flights.
func (g *Group) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.m)
}
