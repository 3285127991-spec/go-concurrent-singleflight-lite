package singleflightlite

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestSharedExecution merges concurrent callers of the same key into one
// real execution; followers get the same result with shared=true.
func TestSharedExecution(t *testing.T) {
	var g Group
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fn := func(context.Context) (any, error) {
		calls.Add(1)
		close(started)
		<-release
		return "v", nil
	}

	type result struct {
		val    any
		shared bool
		err    error
	}
	results := make(chan result, 3)
	go func() { v, s, err := g.Do(context.Background(), "k", fn); results <- result{v, s, err} }()
	<-started // leader is inside fn, flight is joinable
	go func() { v, s, err := g.Do(context.Background(), "k", fn); results <- result{v, s, err} }()
	go func() { v, s, err := g.Do(context.Background(), "k", fn); results <- result{v, s, err} }()
	waitFor(t, func() bool { return g.Snapshot()[0].Waiters == 3 }, "3 waiters attached")
	close(release)

	var sharedN int
	for i := 0; i < 3; i++ {
		r := <-results
		if r.err != nil || r.val != "v" {
			t.Fatalf("got (%v, %v), want (v, nil)", r.val, r.err)
		}
		if r.shared {
			sharedN++
		}
	}
	if sharedN != 2 {
		t.Fatalf("shared count = %d, want 2", sharedN)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn executed %d times, want 1", n)
	}
}

// TestDifferentKeysRunInParallel checks that distinct keys never block or
// merge with each other.
func TestDifferentKeysRunInParallel(t *testing.T) {
	var g Group
	startedA := make(chan struct{})
	startedB := make(chan struct{})
	release := make(chan struct{})
	mkFn := func(started chan struct{}, v string) func(context.Context) (any, error) {
		return func(context.Context) (any, error) {
			close(started)
			<-release
			return v, nil
		}
	}
	resA := make(chan any, 1)
	resB := make(chan any, 1)
	go func() { v, _, _ := g.Do(context.Background(), "a", mkFn(startedA, "va")); resA <- v }()
	go func() { v, _, _ := g.Do(context.Background(), "b", mkFn(startedB, "vb")); resB <- v }()

	// Both fns must be running even though neither has returned.
	<-startedA
	<-startedB
	if n := g.Len(); n != 2 {
		t.Fatalf("Len = %d, want 2", n)
	}
	close(release)
	if v := <-resA; v != "va" {
		t.Fatalf("key a got %v", v)
	}
	if v := <-resB; v != "vb" {
		t.Fatalf("key b got %v", v)
	}
}

// TestErrorIsShared verifies that a failing fn shares its error with all
// followers instead of re-executing.
func TestErrorIsShared(t *testing.T) {
	var g Group
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fnErr := errors.New("boom")
	fn := func(context.Context) (any, error) {
		calls.Add(1)
		close(started)
		<-release
		return nil, fnErr
	}
	errs := make(chan error, 2)
	go func() { _, _, err := g.Do(context.Background(), "k", fn); errs <- err }()
	<-started
	go func() { _, _, err := g.Do(context.Background(), "k", fn); errs <- err }()
	waitFor(t, func() bool { return g.Snapshot()[0].Waiters == 2 }, "follower attached")
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-errs; !errors.Is(err, fnErr) {
			t.Fatalf("got err %v, want %v", err, fnErr)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn executed %d times, want 1", n)
	}
}

// TestFollowerCancelOnlyExitsItself cancels a follower mid-flight: the
// follower gets ctx.Err() while the leader still receives the real result.
func TestFollowerCancelOnlyExitsItself(t *testing.T) {
	var g Group
	started := make(chan struct{})
	release := make(chan struct{})
	fn := func(context.Context) (any, error) {
		close(started)
		<-release
		return "v", nil
	}
	leaderDone := make(chan error, 1)
	go func() { _, _, err := g.Do(context.Background(), "k", fn); leaderDone <- err }()
	<-started

	followerCtx, cancel := context.WithCancel(context.Background())
	followerDone := make(chan error, 1)
	go func() { _, _, err := g.Do(followerCtx, "k", fn); followerDone <- err }()
	waitFor(t, func() bool { return g.Snapshot()[0].Waiters == 2 }, "follower attached")

	cancel()
	if err := <-followerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("follower err = %v, want context.Canceled", err)
	}
	select {
	case <-leaderDone:
		t.Fatal("leader returned before fn finished")
	default:
	}
	close(release)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader err = %v, want nil", err)
	}
}

// TestLeaderCancelDoesNotCancelFlight cancels the leader's context: the
// leader exits, fn keeps running, and a follower still gets the result.
func TestLeaderCancelDoesNotCancelFlight(t *testing.T) {
	var g Group
	started := make(chan struct{})
	release := make(chan struct{})
	fn := func(ctx context.Context) (any, error) {
		close(started)
		select {
		case <-release:
			return "v", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	leaderCtx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() { _, _, err := g.Do(leaderCtx, "k", fn); leaderDone <- err }()
	<-started
	followerDone := make(chan any, 1)
	go func() { v, _, _ := g.Do(context.Background(), "k", fn); followerDone <- v }()
	waitFor(t, func() bool { return g.Snapshot()[0].Waiters == 2 }, "follower attached")

	cancel()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader err = %v, want context.Canceled", err)
	}
	// fn is still blocked on release: the flight survives its leader.
	close(release)
	if v := <-followerDone; v != "v" {
		t.Fatalf("follower got %v, want v", v)
	}
}

// TestForgetStartsNewGeneration isolates generations: after Forget, a new
// Do starts a fresh flight and the old one cannot evict it on completion.
func TestForgetStartsNewGeneration(t *testing.T) {
	var g Group
	start1 := make(chan struct{})
	release1 := make(chan struct{})
	fn1 := func(context.Context) (any, error) { close(start1); <-release1; return "v1", nil }
	start2 := make(chan struct{})
	release2 := make(chan struct{})
	fn2 := func(context.Context) (any, error) { close(start2); <-release2; return "v2", nil }

	done1 := make(chan any, 1)
	go func() { v, _, _ := g.Do(context.Background(), "k", fn1); done1 <- v }()
	<-start1
	gen1 := g.Snapshot()[0].Generation

	g.Forget("k")
	if n := g.Len(); n != 0 {
		t.Fatalf("Len after Forget = %d, want 0", n)
	}

	// New Do for the same key must start a new flight immediately, not join
	// the still-running old one.
	done2 := make(chan any, 1)
	go func() { v, _, _ := g.Do(context.Background(), "k", fn2); done2 <- v }()
	<-start2
	snap := g.Snapshot()
	if len(snap) != 1 || snap[0].Generation <= gen1 {
		t.Fatalf("snapshot = %+v, want one flight with generation > %d", snap, gen1)
	}

	// Old flight finishes first; it must not remove the new flight.
	close(release1)
	if v := <-done1; v != "v1" {
		t.Fatalf("old flight got %v", v)
	}
	if n := g.Len(); n != 1 {
		t.Fatalf("Len after old flight done = %d, want 1 (new flight evicted by stale flight)", n)
	}
	close(release2)
	if v := <-done2; v != "v2" {
		t.Fatalf("new flight got %v", v)
	}
	if n := g.Len(); n != 0 {
		t.Fatalf("Len at end = %d, want 0", n)
	}
}

// TestInvalidArgs rejects empty keys and nil functions without touching
// group state.
func TestInvalidArgs(t *testing.T) {
	var g Group
	fn := func(context.Context) (any, error) { return 1, nil }
	if _, _, err := g.Do(context.Background(), "", fn); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key err = %v, want ErrEmptyKey", err)
	}
	if _, _, err := g.Do(context.Background(), "k", nil); !errors.Is(err, ErrNilFunc) {
		t.Fatalf("nil fn err = %v, want ErrNilFunc", err)
	}
	if n := g.Len(); n != 0 {
		t.Fatalf("Len = %d, want 0", n)
	}
}

// TestSnapshotAndLen checks contents, ordering and lifecycle of the
// observable in-flight state.
func TestSnapshotAndLen(t *testing.T) {
	var g Group
	release := make(chan struct{})
	started := make(chan string, 2)
	mkFn := func(key string) func(context.Context) (any, error) {
		return func(context.Context) (any, error) {
			started <- key
			<-release
			return key, nil
		}
	}
	go func() { _, _, _ = g.Do(context.Background(), "b", mkFn("b")) }()
	go func() { _, _, _ = g.Do(context.Background(), "a", mkFn("a")) }()
	<-started
	<-started
	// One follower joins key b.
	go func() { _, _, _ = g.Do(context.Background(), "b", mkFn("b")) }()
	waitFor(t, func() bool {
		for _, info := range g.Snapshot() {
			if info.Key == "b" && info.Waiters == 2 {
				return true
			}
		}
		return false
	}, "follower attached to b")

	if n := g.Len(); n != 2 {
		t.Fatalf("Len = %d, want 2", n)
	}
	snap := g.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(snap))
	}
	if snap[0].Key != "a" || snap[1].Key != "b" {
		t.Fatalf("snapshot not sorted by key: %+v", snap)
	}
	if snap[0].Waiters != 1 || snap[1].Waiters != 2 {
		t.Fatalf("waiters = %d, %d; want 1, 2", snap[0].Waiters, snap[1].Waiters)
	}
	if snap[0].Generation == 0 || snap[1].Generation == 0 || snap[0].Generation == snap[1].Generation {
		t.Fatalf("bad generations: %+v", snap)
	}
	close(release)
	waitFor(t, func() bool { return g.Len() == 0 }, "flights drained")
	if n := len(g.Snapshot()); n != 0 {
		t.Fatalf("snapshot len after drain = %d, want 0", n)
	}
}

// TestConcurrentStress hammers one group from many goroutines with mixed
// keys, Forget, Snapshot and Len; run with -race to validate safety.
func TestConcurrentStress(t *testing.T) {
	var g Group
	const workers = 64
	const iterations = 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("k%d", (w+i)%8)
				switch i % 5 {
				case 0:
					g.Forget(key)
				case 1:
					_ = g.Snapshot()
				case 2:
					_ = g.Len()
				default:
					v, _, err := g.Do(context.Background(), key, func(context.Context) (any, error) { return key, nil })
					if err != nil || v != key {
						t.Errorf("Do(%s) = (%v, %v)", key, v, err)
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// waitFor polls cond and fails the test if it never becomes true. It uses
// a runtime yield instead of sleeps so tests stay fast and deterministic.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	for i := 0; i < 100000; i++ {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for %s", what)
}
