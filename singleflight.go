// Package singleflightlite provides a concurrency-safe singleflight Group.
//
// Concurrent calls to Do with the same key are merged into one flight:
// a single leader executes fn while followers wait and share its result.
// Calls with different keys run in parallel. The Group never caches
// results: once a flight completes, a later Do starts a brand-new flight.
package singleflightlite

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrNilContext is returned by Do when ctx is nil.
	ErrNilContext = errors.New("singleflightlite: nil context")
	// ErrEmptyKey is returned by Do when key is empty.
	ErrEmptyKey = errors.New("singleflightlite: empty key")
	// ErrNilFunc is returned by Do when fn is nil.
	ErrNilFunc = errors.New("singleflightlite: nil fn")
)

// FlightInfo describes one in-flight, joinable flight.
type FlightInfo struct {
	Key        string
	Generation uint64
	Waiters    int
}

// flight tracks one execution generation for a key.
type flight struct {
	gen     uint64
	waiters int
	done    chan struct{} // closed when the leader finishes
	val     any
	err     error
}

// Group merges concurrent calls by key. The zero value is ready to use.
type Group struct {
	mu      sync.Mutex
	flights map[string]*flight
	nextGen uint64
}

// NewGroup returns an empty Group.
func NewGroup() *Group {
	return &Group{flights: make(map[string]*flight)}
}

// Do executes fn for key, merging concurrent calls with the same key.
//
// The first caller becomes the leader and runs fn (outside any lock);
// concurrent followers wait for the leader and share its result. The
// returned shared flag reports whether this call reused an in-flight
// result instead of executing fn itself.
//
// If a follower's ctx is canceled, only that follower returns (with
// ctx.Err()); the leader and other waiters are unaffected. The leader's
// ctx is passed to fn, so canceling it only affects the flight if fn
// honors the cancellation.
func (g *Group) Do(ctx context.Context, key string, fn func(context.Context) (any, error)) (any, bool, error) {
	if ctx == nil {
		return nil, false, ErrNilContext
	}
	if key == "" {
		return nil, false, ErrEmptyKey
	}
	if fn == nil {
		return nil, false, ErrNilFunc
	}

	g.mu.Lock()
	if g.flights == nil {
		g.flights = make(map[string]*flight)
	}
	if f, ok := g.flights[key]; ok {
		f.waiters++
		g.mu.Unlock()
		select {
		case <-f.done:
			return f.val, true, f.err
		case <-ctx.Done():
			g.mu.Lock()
			f.waiters--
			g.mu.Unlock()
			return nil, false, ctx.Err()
		}
	}
	g.nextGen++
	f := &flight{gen: g.nextGen, waiters: 1, done: make(chan struct{})}
	g.flights[key] = f
	g.mu.Unlock()

	val, err := fn(ctx)

	g.mu.Lock()
	f.val, f.err = val, err
	close(f.done)
	if g.flights[key] == f {
		delete(g.flights, key)
	}
	g.mu.Unlock()
	return val, false, err
}

// Forget removes any in-flight flight for key so the next Do starts a
// new generation immediately. The forgotten flight still completes
// normally for its existing waiters and cannot remove the new flight.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.flights, key)
	g.mu.Unlock()
}

// Snapshot returns the currently joinable flights, sorted by key.
func (g *Group) Snapshot() []FlightInfo {
	g.mu.Lock()
	infos := make([]FlightInfo, 0, len(g.flights))
	for key, f := range g.flights {
		infos = append(infos, FlightInfo{Key: key, Generation: f.gen, Waiters: f.waiters})
	}
	g.mu.Unlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key < infos[j].Key })
	return infos
}

// Len returns the number of currently joinable flights.
func (g *Group) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.flights)
}
