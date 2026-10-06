package singleflightlite

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor spins until cond is true. Ordering comes from cond itself, not
// from sleeping; the timer is only a failure guard against deadlocks.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for !cond() {
		select {
		case <-timer.C:
			t.Fatalf("timeout waiting for %s", what)
		default:
			runtime.Gosched()
		}
	}
}

type doResult struct {
	val    any
	shared bool
	err    error
}

func doAsync(g *Group, ctx context.Context, key string, fn func(context.Context) (any, error)) <-chan doResult {
	ch := make(chan doResult, 1)
	go func() {
		v, s, err := g.Do(ctx, key, fn)
		ch <- doResult{v, s, err}
	}()
	return ch
}

func TestDoSharesExecution(t *testing.T) {
	g := NewGroup()
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fn := func(ctx context.Context) (any, error) {
		calls.Add(1)
		close(entered)
		<-release
		return "value", nil
	}

	leader := doAsync(g, context.Background(), "k", fn)
	<-entered // leader is inside fn, flight is joinable now

	const followers = 4
	outs := make([]<-chan doResult, followers)
	for i := range outs {
		outs[i] = doAsync(g, context.Background(), "k", fn)
	}
	waitFor(t, "followers to attach", func() bool {
		s := g.Snapshot()
		return len(s) == 1 && s[0].Waiters == 1+followers
	})
	close(release)

	if r := <-leader; r.val != "value" || r.err != nil || r.shared {
		t.Fatalf("leader = %+v, want value=value shared=false err=nil", r)
	}
	for i, out := range outs {
		if r := <-out; r.val != "value" || r.err != nil || !r.shared {
			t.Fatalf("follower %d = %+v, want value=value shared=true err=nil", i, r)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("fn executed %d times, want 1", got)
	}
	if got := g.Len(); got != 0 {
		t.Fatalf("Len() = %d after completion, want 0", got)
	}
}

func TestDoDifferentKeysRunInParallel(t *testing.T) {
	g := NewGroup()
	release := make(chan struct{})
	enteredA, enteredB := make(chan struct{}), make(chan struct{})
	fnA := func(ctx context.Context) (any, error) { close(enteredA); <-release; return "a", nil }
	fnB := func(ctx context.Context) (any, error) { close(enteredB); <-release; return "b", nil }

	ra := doAsync(g, context.Background(), "a", fnA)
	rb := doAsync(g, context.Background(), "b", fnB)

	// Both fns must be running before either is released: keys don't block each other.
	<-enteredA
	<-enteredB
	if got := g.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	close(release)
	if r := <-ra; r.val != "a" || r.err != nil {
		t.Fatalf("key a = %+v", r)
	}
	if r := <-rb; r.val != "b" || r.err != nil {
		t.Fatalf("key b = %+v", r)
	}
}

func TestDoErrorIsShared(t *testing.T) {
	g := NewGroup()
	want := errors.New("boom")
	entered := make(chan struct{})
	release := make(chan struct{})
	fn := func(ctx context.Context) (any, error) {
		close(entered)
		<-release
		return nil, want
	}

	leader := doAsync(g, context.Background(), "k", fn)
	<-entered
	follower := doAsync(g, context.Background(), "k", fn)
	waitFor(t, "follower to attach", func() bool {
		s := g.Snapshot()
		return len(s) == 1 && s[0].Waiters == 2
	})
	close(release)

	if r := <-leader; !errors.Is(r.err, want) || r.val != nil || r.shared {
		t.Fatalf("leader = %+v, want err=boom shared=false", r)
	}
	if r := <-follower; !errors.Is(r.err, want) || r.val != nil || !r.shared {
		t.Fatalf("follower = %+v, want err=boom shared=true", r)
	}
}

func TestFollowerCancelLeavesLeaderRunning(t *testing.T) {
	g := NewGroup()
	entered := make(chan struct{})
	release := make(chan struct{})
	fn := func(ctx context.Context) (any, error) {
		close(entered)
		<-release
		return "value", nil
	}

	leader := doAsync(g, context.Background(), "k", fn)
	<-entered

	ctx, cancel := context.WithCancel(context.Background())
	follower := doAsync(g, ctx, "k", fn)
	waitFor(t, "follower to attach", func() bool {
		s := g.Snapshot()
		return len(s) == 1 && s[0].Waiters == 2
	})

	cancel()
	if r := <-follower; !errors.Is(r.err, context.Canceled) || r.val != nil || r.shared {
		t.Fatalf("canceled follower = %+v, want err=context.Canceled", r)
	}
	// The leader's flight is untouched: still one waiter, still running.
	waitFor(t, "waiter count to drop", func() bool {
		s := g.Snapshot()
		return len(s) == 1 && s[0].Waiters == 1
	})

	close(release)
	if r := <-leader; r.val != "value" || r.err != nil {
		t.Fatalf("leader = %+v, want value=value err=nil", r)
	}
	if got := g.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
}

func TestForgetStartsNewGeneration(t *testing.T) {
	g := NewGroup()
	entered1, release1 := make(chan struct{}), make(chan struct{})
	fn1 := func(ctx context.Context) (any, error) { close(entered1); <-release1; return "old", nil }
	old := doAsync(g, context.Background(), "k", fn1)
	<-entered1

	g.Forget("k")
	if got := g.Len(); got != 0 {
		t.Fatalf("Len() = %d after Forget, want 0", got)
	}

	// A new Do on the same key must start a new generation immediately,
	// not join the forgotten flight.
	entered2, release2 := make(chan struct{}), make(chan struct{})
	fn2 := func(ctx context.Context) (any, error) { close(entered2); <-release2; return "new", nil }
	fresh := doAsync(g, context.Background(), "k", fn2)
	<-entered2

	s := g.Snapshot()
	if len(s) != 1 || s[0].Key != "k" || s[0].Generation != 2 {
		t.Fatalf("Snapshot() = %+v, want one flight key=k generation=2", s)
	}

	// The old flight finishing must not remove the new generation.
	close(release1)
	if r := <-old; r.val != "old" || r.err != nil {
		t.Fatalf("old flight = %+v", r)
	}
	if got := g.Len(); got != 1 {
		t.Fatalf("Len() = %d after old flight finished, want 1", got)
	}
	if s := g.Snapshot(); len(s) != 1 || s[0].Generation != 2 {
		t.Fatalf("Snapshot() = %+v after old flight finished, want generation=2", s)
	}

	close(release2)
	if r := <-fresh; r.val != "new" || r.err != nil {
		t.Fatalf("new flight = %+v", r)
	}
	if got := g.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
}

func TestDoInvalidArguments(t *testing.T) {
	g := NewGroup()
	fn := func(ctx context.Context) (any, error) { return nil, nil }

	if _, _, err := g.Do(nil, "k", fn); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil ctx err = %v, want ErrNilContext", err)
	}
	if _, _, err := g.Do(context.Background(), "", fn); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key err = %v, want ErrEmptyKey", err)
	}
	if _, _, err := g.Do(context.Background(), "k", nil); !errors.Is(err, ErrNilFunc) {
		t.Fatalf("nil fn err = %v, want ErrNilFunc", err)
	}
	if got := g.Len(); got != 0 {
		t.Fatalf("Len() = %d after invalid calls, want 0", got)
	}
	// Forgetting an unknown key is a no-op.
	g.Forget("missing")
}

func TestSnapshotAndLen(t *testing.T) {
	g := NewGroup()
	release := make(chan struct{})
	entered := make(map[string]chan struct{})
	for _, key := range []string{"b", "a", "c"} {
		entered[key] = make(chan struct{})
	}
	for _, key := range []string{"b", "a", "c"} {
		key := key
		fn := func(ctx context.Context) (any, error) { close(entered[key]); <-release; return key, nil }
		doAsync(g, context.Background(), key, fn)
		<-entered[key]
	}
	// Attach one follower to "a" so its waiter count differs.
	follower := doAsync(g, context.Background(), "a", func(ctx context.Context) (any, error) {
		return "unused", nil
	})
	waitFor(t, "follower to attach", func() bool {
		s := g.Snapshot()
		return len(s) == 3 && s[0].Waiters == 2
	})

	if got := g.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	s := g.Snapshot()
	if len(s) != 3 {
		t.Fatalf("Snapshot() len = %d, want 3", len(s))
	}
	wantKeys := []string{"a", "b", "c"} // sorted by key
	for i, info := range s {
		if info.Key != wantKeys[i] {
			t.Fatalf("Snapshot()[%d].Key = %q, want %q (sorted)", i, info.Key, wantKeys[i])
		}
		if info.Generation == 0 {
			t.Fatalf("Snapshot()[%d].Generation = 0, want non-zero", i)
		}
	}
	if s[0].Waiters != 2 || s[1].Waiters != 1 || s[2].Waiters != 1 {
		t.Fatalf("waiters = %d,%d,%d, want 2,1,1", s[0].Waiters, s[1].Waiters, s[2].Waiters)
	}

	close(release)
	if r := <-follower; r.val != "a" || !r.shared {
		t.Fatalf("follower = %+v, want value=a shared=true", r)
	}
	waitFor(t, "flights to drain", func() bool { return g.Len() == 0 })
	if s := g.Snapshot(); len(s) != 0 {
		t.Fatalf("Snapshot() = %+v after completion, want empty", s)
	}
}

func TestLeaderContextPassedToFn(t *testing.T) {
	g := NewGroup()
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
	got := make(chan any, 1)
	fn := func(ctx context.Context) (any, error) {
		got <- ctx.Value(ctxKey{})
		return nil, nil
	}
	if _, _, err := g.Do(ctx, "k", fn); err != nil {
		t.Fatalf("Do err = %v", err)
	}
	if v := <-got; v != "marker" {
		t.Fatalf("fn saw ctx value %v, want marker", v)
	}
}

func TestConcurrentStress(t *testing.T) {
	g := NewGroup()
	const keys = 8
	const perKey = 16
	var calls [keys]atomic.Int32

	var wg sync.WaitGroup
	errs := make(chan error, keys*perKey)
	for i := 0; i < keys*perKey; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := i % keys
			fn := func(ctx context.Context) (any, error) {
				calls[key].Add(1)
				return fmt.Sprintf("val-%d", key), nil
			}
			v, _, err := g.Do(context.Background(), fmt.Sprintf("key-%d", key), fn)
			if err != nil {
				errs <- err
				return
			}
			if want := fmt.Sprintf("val-%d", key); v != want {
				errs <- fmt.Errorf("key-%d: got %v, want %v", key, v, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for k := 0; k < keys; k++ {
		if got := calls[k].Load(); got < 1 {
			t.Fatalf("key-%d: fn executed %d times, want >= 1", k, got)
		}
	}
	if got := g.Len(); got != 0 {
		t.Fatalf("Len() = %d after stress, want 0", got)
	}
}
